# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Silver to Gold Star Schema Aggregation & Quality Gates
# MAGIC
# MAGIC **Transformations & Deliverables (docs/data_dictionary.md):**
# MAGIC 1. **Dimensional Modeling (Star Schema):**
# MAGIC    * `dim_repo` (SCD Type 1): Catalog of tracked database engines enriched with classification (`db_type`, `db_category`).
# MAGIC    * `dim_date`: Continuous calendar spine with year, quarter, month, ISO week, weekend flag, and analysis window flag.
# MAGIC    * `dim_actor` (SCD Type 1): Consolidated contributor identity with salted SHA-256 fallback and explicit `Unknown` member row.
# MAGIC 2. **Base Facts:**
# MAGIC    * `fact_commits`: Granular commit history with deterministic surrogate keys (`repo_key`, `date_key`, `actor_key`).
# MAGIC    * `fact_issues`: True issue events tracking lifecycle and duration (`hours_to_close`). Excludes pull requests.
# MAGIC    * `fact_pull_requests`: PR lifecycle tracking developer throughput and merge latency (`hours_to_merge`).
# MAGIC    * `fact_releases`: Release cadence measuring interval cadences (`days_since_previous_release`).
# MAGIC    * `fact_repo_snapshot`: Point-in-time community metrics (stars, forks, open issues).
# MAGIC 3. **Aggregate Fact (Dense Calendar Spine + Rolling Windows):**
# MAGIC    * `fact_repo_daily_activity`: High-value aggregate fact grain `(repo_key, date_key)`.
# MAGIC    * Formed by cross-joining `dim_repo` x `dim_date` to ensure zero activity days exist.
# MAGIC    * Computes rolling 4-week (28 rows) and 12-week (84 rows) human commit velocity sums.
# MAGIC 4. **Quality Gates & Audit Reconciliation:**
# MAGIC    * Primary key uniqueness assertion across all 9 Gold tables.
# MAGIC    * Referential integrity assertion (no orphan foreign keys).
# MAGIC    * Row count parity reconciliation vs. Silver layer recorded in `gold.audit_gold_reconciliation`.
# MAGIC 5. **Delta Optimization:**
# MAGIC    * Runs `OPTIMIZE` on Liquid Clustered tables (`CLUSTER BY`) and inspects file layouts.
# MAGIC    * Logs metrics into `ops.pipeline_execution_logs` with layer `'GOLD'`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog Name")
dbutils.widgets.text("batch_id", "", "Batch Run ID (Empty for auto-generated)")
dbutils.widgets.dropdown("gold_mode", "incremental", ["incremental", "full"], "Gold Processing Mode")
dbutils.widgets.text("analysis_since", "2026-04-01", "Analysis Window Start Date (YYYY-MM-DD)")
dbutils.widgets.text("analysis_until", "2026-09-30", "Analysis Window End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("run_optimize", "true", ["true", "false"], "Run Liquid Clustering OPTIMIZE")

# COMMAND ----------

import sys
import os
import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
    DoubleType,
    DateType,
    StructType,
    StructField,
)
from delta.tables import DeltaTable

# Ensure project src/ is importable
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.schemas import GOLD_SCHEMAS
from src.common import (
    get_params,
    log_run,
    merge_delta,
)
from src.gold_transforms import (
    REPO_DB_MAPPING,
    DEFAULT_ANALYSIS_WINDOW_START,
    DEFAULT_ANALYSIS_WINDOW_END,
    DIM_DATE_START,
    DIM_DATE_END,
    build_dim_repo,
    build_dim_date,
    build_dim_actor,
    build_fact_commits,
    build_fact_issues,
    build_fact_pull_requests,
    build_fact_releases,
    build_fact_repo_snapshot,
    build_fact_repo_daily_activity,
    build_fact_repo_stars_weekly,
)

# Parse parameters
catalog = dbutils.widgets.get("catalog").strip() or "workspace"
batch_id = dbutils.widgets.get("batch_id").strip() or f"gold_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
gold_mode = dbutils.widgets.get("gold_mode").strip().lower()
analysis_since = dbutils.widgets.get("analysis_since").strip() or DEFAULT_ANALYSIS_WINDOW_START
analysis_until = dbutils.widgets.get("analysis_until").strip() or DEFAULT_ANALYSIS_WINDOW_END
run_optimize = dbutils.widgets.get("run_optimize").strip().lower() in ("true", "1", "yes")

gold_schema = "gold"
silver_schema = "silver"
ops_schema = "ops"

print("=" * 70)
print("GOLD LAYER PIPELINE EXECUTION")
print(f"  Catalog:          {catalog}")
print(f"  Batch ID:         {batch_id}")
print(f"  Mode:             {gold_mode}")
print(f"  Analysis Window:  {analysis_since} -> {analysis_until}")
print(f"  Run OPTIMIZE:     {run_optimize}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### High-Watermark Detection for Incremental Runs

# COMMAND ----------

last_success_time = None
if gold_mode == "incremental":
    try:
        last_success_row = (
            spark.table(f"{catalog}.{ops_schema}.pipeline_execution_logs")
            .filter("layer = 'GOLD' AND status = 'SUCCESS'")
            .orderBy(F.col("end_time").desc())
            .select("end_time")
            .first()
        )
        if last_success_row and last_success_row["end_time"]:
            last_success_time = last_success_row["end_time"]
            print(f"[Incremental] High-watermark established: {last_success_time}")
        else:
            print("[Incremental] No previous successful Gold run found. Performing initial full load.")
    except Exception as e:
        print(f"[Incremental] Could not query logs table ({e}). Defaulting to full load.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read Silver Source Tables

# COMMAND ----------

df_silver_repo_meta = spark.table(f"{catalog}.{silver_schema}.repo_metadata")
df_silver_commits = spark.table(f"{catalog}.{silver_schema}.commits")
df_silver_issues = spark.table(f"{catalog}.{silver_schema}.issues")
df_silver_pull_requests = spark.table(f"{catalog}.{silver_schema}.pull_requests")
df_silver_releases = spark.table(f"{catalog}.{silver_schema}.releases")
df_silver_stars_weekly = spark.table(f"{catalog}.{silver_schema}.stars_weekly")

if last_success_time and gold_mode == "incremental":
    df_src_commits = df_silver_commits.filter(F.col("load_timestamp") >= F.lit(last_success_time))
    df_src_issues = df_silver_issues.filter(F.col("load_timestamp") >= F.lit(last_success_time))
    df_src_pulls = df_silver_pull_requests.filter(F.col("load_timestamp") >= F.lit(last_success_time))
    df_src_releases = df_silver_releases.filter(F.col("load_timestamp") >= F.lit(last_success_time))
    df_src_repo_meta = df_silver_repo_meta.filter(F.col("load_timestamp") >= F.lit(last_success_time))
    df_src_stars_weekly = df_silver_stars_weekly.filter(F.col("load_timestamp") >= F.lit(last_success_time))
else:
    df_src_commits = df_silver_commits
    df_src_issues = df_silver_issues
    df_src_pulls = df_silver_pull_requests
    df_src_releases = df_silver_releases
    df_src_repo_meta = df_silver_repo_meta
    df_src_stars_weekly = df_silver_stars_weekly

print("Silver source DataFrames prepared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Build and Merge Dimension Tables (dim_repo, dim_date, dim_actor)

# COMMAND ----------

# 2.1 dim_repo (SCD 1)
with log_run(spark, layer="GOLD", parameter="table=dim_repo,repo=ALL,entity=repo_metadata", batch_id=batch_id, catalog=catalog) as run_ctx_repo:
    df_dim_repo = build_dim_repo(df_silver_repo_meta)
    ins_repo, upd_repo = merge_delta(
        spark,
        df_dim_repo,
        f"{catalog}.{gold_schema}.dim_repo",
        ["repo_key"],
        update_all=True
    )
    run_ctx_repo["rows_inserted"] = ins_repo
    run_ctx_repo["rows_updated"] = upd_repo
    print(f"  ✓ dim_repo merged: {ins_repo} inserted, {upd_repo} updated")

# 2.2 dim_date (Calendar Spine)
with log_run(spark, layer="GOLD", parameter="table=dim_date,repo=ALL,entity=calendar", batch_id=batch_id, catalog=catalog) as run_ctx_date:
    df_dim_date = build_dim_date(
        spark,
        start_date_str=DIM_DATE_START,   # 2008-01-01: GitHub founding year, covers historical issue/release keys
        end_date_str=DIM_DATE_END,         # 2027-12-31: current planning horizon end
        window_since=analysis_since,
        window_until=analysis_until
    )
    ins_date, upd_date = merge_delta(
        spark,
        df_dim_date,
        f"{catalog}.{gold_schema}.dim_date",
        ["date_key"],
        update_all=True
    )
    run_ctx_date["rows_inserted"] = ins_date
    run_ctx_date["rows_updated"] = upd_date
    print(f"  ✓ dim_date merged: {ins_date} inserted, {upd_date} updated")

# 2.3 dim_actor (SCD 1 with Unknown Member)
with log_run(spark, layer="GOLD", parameter="table=dim_actor,repo=ALL,entity=actors", batch_id=batch_id, catalog=catalog) as run_ctx_actor:
    # Build dim_actor from available actors in commits and pull requests
    df_dim_actor = build_dim_actor(
        df_commits=df_silver_commits,
        df_issues=df_silver_issues,
        df_pulls=df_silver_pull_requests,
        spark=spark
    )
    ins_actor, upd_actor = merge_delta(
        spark,
        df_dim_actor,
        f"{catalog}.{gold_schema}.dim_actor",
        ["actor_key"],
        update_all=True
    )
    run_ctx_actor["rows_inserted"] = ins_actor
    run_ctx_actor["rows_updated"] = upd_actor
    print(f"  ✓ dim_actor merged: {ins_actor} inserted, {upd_actor} updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Build and Merge Base Fact Tables

# COMMAND ----------

# 3.1 fact_commits
with log_run(spark, layer="GOLD", parameter="table=fact_commits,repo=ALL,entity=commits", batch_id=batch_id, catalog=catalog) as run_ctx_fc:
    df_fact_commits = build_fact_commits(df_src_commits, analysis_since, analysis_until)
    ins_fc, upd_fc = merge_delta(
        spark,
        df_fact_commits,
        f"{catalog}.{gold_schema}.fact_commits",
        ["repo_key", "commit_sha"],
        update_all=True
    )
    run_ctx_fc["rows_inserted"] = ins_fc
    run_ctx_fc["rows_updated"] = upd_fc
    print(f"  ✓ fact_commits merged: {ins_fc} inserted, {upd_fc} updated")

# 3.2 fact_issues
with log_run(spark, layer="GOLD", parameter="table=fact_issues,repo=ALL,entity=issues", batch_id=batch_id, catalog=catalog) as run_ctx_fi:
    df_fact_issues = build_fact_issues(df_src_issues, analysis_since, analysis_until)
    ins_fi, upd_fi = merge_delta(
        spark,
        df_fact_issues,
        f"{catalog}.{gold_schema}.fact_issues",
        ["repo_key", "issue_number"],
        update_all=True
    )
    run_ctx_fi["rows_inserted"] = ins_fi
    run_ctx_fi["rows_updated"] = upd_fi
    print(f"  ✓ fact_issues merged: {ins_fi} inserted, {upd_fi} updated")

# 3.3 fact_pull_requests
with log_run(spark, layer="GOLD", parameter="table=fact_pull_requests,repo=ALL,entity=pull_requests", batch_id=batch_id, catalog=catalog) as run_ctx_fpr:
    df_fact_pull_requests = build_fact_pull_requests(df_src_pulls, analysis_since, analysis_until)
    ins_fpr, upd_fpr = merge_delta(
        spark,
        df_fact_pull_requests,
        f"{catalog}.{gold_schema}.fact_pull_requests",
        ["repo_key", "pr_number"],
        update_all=True
    )
    run_ctx_fpr["rows_inserted"] = ins_fpr
    run_ctx_fpr["rows_updated"] = upd_fpr
    print(f"  ✓ fact_pull_requests merged: {ins_fpr} inserted, {upd_fpr} updated")

# 3.4 fact_releases
with log_run(spark, layer="GOLD", parameter="table=fact_releases,repo=ALL,entity=releases", batch_id=batch_id, catalog=catalog) as run_ctx_fr:
    # Full silver releases must be provided to accurately calculate lag over release sequence
    df_fact_releases = build_fact_releases(df_silver_releases, analysis_since, analysis_until)
    ins_fr, upd_fr = merge_delta(
        spark,
        df_fact_releases,
        f"{catalog}.{gold_schema}.fact_releases",
        ["repo_key", "release_id"],
        update_all=True
    )
    run_ctx_fr["rows_inserted"] = ins_fr
    run_ctx_fr["rows_updated"] = upd_fr
    print(f"  ✓ fact_releases merged: {ins_fr} inserted, {upd_fr} updated")

# 3.5 fact_repo_snapshot
with log_run(spark, layer="GOLD", parameter="table=fact_repo_snapshot,repo=ALL,entity=repo_metadata", batch_id=batch_id, catalog=catalog) as run_ctx_fs:
    df_fact_repo_snapshot = build_fact_repo_snapshot(df_src_repo_meta, window_since=analysis_since, window_until=analysis_until)
    ins_fs, upd_fs = merge_delta(
        spark,
        df_fact_repo_snapshot,
        f"{catalog}.{gold_schema}.fact_repo_snapshot",
        ["repo_key", "snapshot_date_key"],
        update_all=True
    )
    run_ctx_fs["rows_inserted"] = ins_fs
    run_ctx_fs["rows_updated"] = upd_fs
    print(f"  ✓ fact_repo_snapshot merged: {ins_fs} inserted, {upd_fs} updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Build and Merge Aggregate Fact (fact_repo_daily_activity)
# MAGIC Computes daily aggregates over a dense calendar spine with rolling 28-day & 84-day windows.

# COMMAND ----------

with log_run(spark, layer="GOLD", parameter="table=fact_repo_daily_activity,repo=ALL,entity=daily_activity", batch_id=batch_id, catalog=catalog) as run_ctx_daily:
    # Always read full base Gold tables to ensure complete calendar spine and rolling window math
    t_gold_fc = spark.table(f"{catalog}.{gold_schema}.fact_commits")
    t_gold_fi = spark.table(f"{catalog}.{gold_schema}.fact_issues")
    t_gold_fpr = spark.table(f"{catalog}.{gold_schema}.fact_pull_requests")
    t_gold_fr = spark.table(f"{catalog}.{gold_schema}.fact_releases")
    t_gold_repo = spark.table(f"{catalog}.{gold_schema}.dim_repo")
    t_gold_date = spark.table(f"{catalog}.{gold_schema}.dim_date")

    df_fact_daily = build_fact_repo_daily_activity(
        df_fact_commits=t_gold_fc,
        df_fact_issues=t_gold_fi,
        df_fact_pull_requests=t_gold_fpr,
        df_fact_releases=t_gold_fr,
        df_dim_repo=t_gold_repo,
        df_dim_date=t_gold_date,
    )

    print("\n" + "=" * 80)
    print("EXPLAIN FORMATTED: gold.fact_repo_daily_activity")
    print("=" * 80)
    df_fact_daily.explain(mode="formatted")
    print("=" * 80 + "\n")

    ins_daily, upd_daily = merge_delta(
        spark,
        df_fact_daily,
        f"{catalog}.{gold_schema}.fact_repo_daily_activity",
        ["repo_key", "date_key"],
        update_all=True
    )
    run_ctx_daily["rows_inserted"] = ins_daily
    run_ctx_daily["rows_updated"] = upd_daily
    run_ctx_daily["rows_aggregated"] = ins_daily + upd_daily
    print(f"  ✓ fact_repo_daily_activity merged: {ins_daily} inserted, {upd_daily} updated ({ins_daily + upd_daily} aggregated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Build and Merge Weekly Star Velocity Fact (fact_repo_stars_weekly)
# MAGIC Computes weekly star aggregates with dense Sunday-based week spine and 4-week rolling velocity.

# COMMAND ----------

with log_run(spark, layer="GOLD", parameter="table=fact_repo_stars_weekly,repo=ALL,entity=stars_history", batch_id=batch_id, catalog=catalog) as run_ctx_stars:
    silver_table_name = f"{catalog}.{silver_schema}.stars_weekly"
    if not spark.catalog.tableExists(silver_table_name):
        run_ctx_stars["status"] = "FAILURE"
        run_ctx_stars["message"] = f"Source table {silver_table_name} does not exist."
        raise RuntimeError(f"Required source table {silver_table_name} does not exist.")

    df_silver_stars = spark.table(silver_table_name)
    if df_silver_stars.limit(1).count() == 0:
        run_ctx_stars["status"] = "FAILURE"
        run_ctx_stars["message"] = f"Source table {silver_table_name} is empty (0 rows)."
        raise RuntimeError(f"Required source table {silver_table_name} is empty (0 rows).")

    t_gold_repo = spark.table(f"{catalog}.{gold_schema}.dim_repo")
    t_gold_date = spark.table(f"{catalog}.{gold_schema}.dim_date")

    df_fact_stars_weekly = build_fact_repo_stars_weekly(
        df_silver_stars_weekly=df_silver_stars,
        df_dim_repo=t_gold_repo,
        df_dim_date=t_gold_date,
    )

    print("\n" + "=" * 80)
    print("EXPLAIN FORMATTED: gold.fact_repo_stars_weekly")
    print("=" * 80)
    df_fact_stars_weekly.explain(mode="formatted")
    print("=" * 80 + "\n")

    ins_stars, upd_stars = merge_delta(
        spark,
        df_fact_stars_weekly,
        f"{catalog}.{gold_schema}.fact_repo_stars_weekly",
        ["repo_key", "week_start_date_key"],
        update_all=True
    )
    run_ctx_stars["rows_inserted"] = ins_stars
    run_ctx_stars["rows_updated"] = upd_stars
    run_ctx_stars["rows_aggregated"] = ins_stars + upd_stars
    print(f"  ✓ fact_repo_stars_weekly merged: {ins_stars} inserted, {upd_stars} updated ({ins_stars + upd_stars} aggregated)")
# MAGIC %md
# MAGIC ### Step 6: Reconciliation Check - cumulative_stars vs fact_repo_snapshot.stargazers_count
# MAGIC Reconciles weekly star history against snapshot stargazers count at the exact snapshot date.
# MAGIC Reads snapshot_date_key dynamically from fact_repo_snapshot, sums days array through the snapshot weekday,
# MAGIC and logs measured deltas per repo to gold.audit_gold_reconciliation.

# COMMAND ----------

print("\n" + "=" * 80)
print("EXECUTING STAR HISTORY RECONCILIATION AT SNAPSHOT DATE")
print("=" * 80)

# 1. Read latest snapshot per repo dynamically from fact_repo_snapshot
df_snapshots = (
    spark.table(f"{catalog}.{gold_schema}.fact_repo_snapshot")
    .join(
        spark.table(f"{catalog}.{gold_schema}.fact_repo_snapshot")
        .groupBy("repo_key")
        .agg(F.max("snapshot_date_key").alias("snapshot_date_key")),
        on=["repo_key", "snapshot_date_key"],
        how="inner"
    )
    .select("repo_key", "snapshot_date_key", "stargazers_count")
)

# 2. Derive snapshot date and Sunday week start
df_snapshots_dated = (
    df_snapshots
    .withColumn("snap_date", F.to_date(F.col("snapshot_date_key").cast("string"), "yyyyMMdd"))
    .withColumn("snap_week_start", F.expr("date_sub(snap_date, dayofweek(snap_date) - 1)"))
    .withColumn("snap_weekday_idx", F.dayofweek("snap_date") - 1)  # 0=Sunday, 6=Saturday
)

# 3. Join with silver.stars_weekly for the days array of the snapshot week
# and get repo_full_name mapping via dim_repo
df_dim_r = spark.table(f"{catalog}.{gold_schema}.dim_repo").select("repo_key", "repo_full_name")
df_silver_w = spark.table(f"{catalog}.{silver_schema}.stars_weekly")

df_snap_week_data = (
    df_snapshots_dated
    .join(df_dim_r, on="repo_key", how="inner")
    .join(
        df_silver_w.select(
            F.col("repo_full_name"),
            F.col("week_start_date").alias("snap_week_start"),
            F.col("days"),
            F.col("stars_gained").alias("week_stars_gained"),
        ),
        on=["repo_full_name", "snap_week_start"],
        how="left"
    )
)

# 4. Get prior cumulative stars up to the week before the snapshot week
df_facts = spark.table(f"{catalog}.{gold_schema}.fact_repo_stars_weekly")

# Get cumulative_stars from the fact row matching snap_week_start
df_fact_snap_week = (
    df_facts
    .join(
        df_snapshots_dated.select("repo_key", "snap_week_start"),
        (df_facts.repo_key == df_snapshots_dated.repo_key) &
        (df_facts.week_start_date == df_snapshots_dated.snap_week_start),
        how="inner"
    )
    .select(
        df_facts.repo_key,
        df_facts.cumulative_stars.alias("week_cumulative_stars"),
        df_facts.stars_gained.alias("fact_week_stars_gained")
    )
)

# 5. Calculate cumulative stars through the snapshot day:
# prior_cumulative = week_cumulative_stars - fact_week_stars_gained
# stars_through_snap_day = prior_cumulative + sum(days[0..snap_weekday_idx])
def sum_days_through_weekday(days_arr, weekday_idx):
    if not days_arr:
        return 0
    return sum(days_arr[:weekday_idx + 1])

sum_days_udf = F.udf(sum_days_through_weekday, LongType())

df_reconciled = (
    df_snap_week_data
    .join(df_fact_snap_week, on="repo_key", how="left")
    .withColumn("days_partial_sum", sum_days_udf(F.col("days"), F.col("snap_weekday_idx")))
    .withColumn(
        "prior_cumulative",
        F.coalesce(F.col("week_cumulative_stars") - F.col("fact_week_stars_gained"), F.lit(0))
    )
    .withColumn(
        "reconciled_stars_at_snapshot",
        F.col("prior_cumulative") + F.coalesce(F.col("days_partial_sum"), F.col("fact_week_stars_gained"), F.lit(0))
    )
    .withColumn("delta", F.abs(F.col("stargazers_count") - F.col("reconciled_stars_at_snapshot")))
    .withColumn("status", F.lit("MEASURED"))
)

print("Measured Reconciliation Results (Snapshot vs Cumulative at Snapshot Date):")
df_reconciled.select(
    "repo_key", "snapshot_date_key", "stargazers_count",
    "reconciled_stars_at_snapshot", "delta", "status"
).show(truncate=False)

# Log reconciliation rows to gold.audit_gold_reconciliation
recon_rows = []
for row in df_reconciled.collect():
    recon_rows.append((
        "fact_repo_stars_weekly",
        int(row["reconciled_stars_at_snapshot"]) if row["reconciled_stars_at_snapshot"] is not None else 0,
        int(row["stargazers_count"]) if row["stargazers_count"] is not None else 0,
        int(row["delta"]) if row["delta"] is not None else 0,
        row["status"],
        datetime.now(timezone.utc),
    ))

recon_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("silver_source_rows", LongType(), True),
    StructField("gold_target_rows", LongType(), True),
    StructField("delta_rows", LongType(), True),
    StructField("status", StringType(), False),
    StructField("check_timestamp", TimestampType(), False),
])
df_star_recon = spark.createDataFrame(recon_rows, schema=recon_schema)
df_star_recon.write.format("delta").mode("append").saveAsTable(f"{catalog}.{gold_schema}.audit_gold_reconciliation")
print("Star history reconciliation logged to audit table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Quality Gates Verification
# MAGIC 1. Primary Key Uniqueness
# MAGIC 2. Referential Integrity (No Orphan Foreign Keys)
# MAGIC 3. Row Count Parity Reconciliation vs. Silver

# COMMAND ----------

print("\n" + "=" * 80)
print("EXECUTING GOLD QUALITY GATES")
print("=" * 80)

# Gate 1: Primary Key Uniqueness
pk_definitions = {
    "dim_repo": ["repo_key"],
    "dim_date": ["date_key"],
    "dim_actor": ["actor_key"],
    "fact_commits": ["repo_key", "commit_sha"],
    "fact_issues": ["repo_key", "issue_number"],
    "fact_pull_requests": ["repo_key", "pr_number"],
    "fact_releases": ["repo_key", "release_id"],
    "fact_repo_snapshot": ["repo_key", "snapshot_date_key"],
    "fact_repo_daily_activity": ["repo_key", "date_key"],
    "fact_repo_stars_weekly": ["repo_key", "week_start_date_key"],
}

for tbl, pks in pk_definitions.items():
    df_t = spark.table(f"{catalog}.{gold_schema}.{tbl}")
    tot = df_t.count()
    dist = df_t.select(*pks).distinct().count()
    if tot != dist:
        raise AssertionError(f"Quality Gate 1 Failed: Primary key {pks} not unique in {tbl}! Total: {tot}, Distinct: {dist}")
    print(f"  ✓ [PK Uniqueness] {tbl:<26} verified: {tot:,} unique rows")

# Gate 2: Referential Integrity (Orphan Foreign Keys)
dim_repo_keys = spark.table(f"{catalog}.{gold_schema}.dim_repo").select("repo_key").distinct()
dim_date_keys = spark.table(f"{catalog}.{gold_schema}.dim_date").select("date_key").distinct()
dim_actor_keys = spark.table(f"{catalog}.{gold_schema}.dim_actor").select("actor_key").distinct()

def check_orphans(fact_name, fact_col, dim_df, dim_col, dim_name):
    f_df = spark.table(f"{catalog}.{gold_schema}.{fact_name}")
    orphan_df = (
        f_df.filter(F.col(fact_col).isNotNull())
        .select(F.col(fact_col).alias("k"))
        .join(dim_df.select(F.col(dim_col).alias("k")), on="k", how="left_anti")
    )
    orphans = orphan_df.count()
    if orphans > 0:
        # Compute min/max to pinpoint the out-of-range key for fast debugging
        stats = orphan_df.agg(F.min("k").alias("min_k"), F.max("k").alias("max_k")).first()
        raise AssertionError(
            f"Quality Gate 2 Failed: {orphans} orphan keys found in "
            f"{fact_name}.{fact_col} referencing {dim_name}.{dim_col}. "
            f"Orphan key range: min={stats['min_k']}, max={stats['max_k']}"
        )
    print(f"  ✓ [Referential Integrity] {fact_name}.{fact_col} -> {dim_name}.{dim_col}: 0 orphans")

check_orphans("fact_commits", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_commits", "date_key", dim_date_keys, "date_key", "dim_date")
check_orphans("fact_commits", "actor_key", dim_actor_keys, "actor_key", "dim_actor")

check_orphans("fact_issues", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_issues", "created_date_key", dim_date_keys, "date_key", "dim_date")
check_orphans("fact_issues", "actor_key", dim_actor_keys, "actor_key", "dim_actor")

check_orphans("fact_pull_requests", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_pull_requests", "created_date_key", dim_date_keys, "date_key", "dim_date")
check_orphans("fact_pull_requests", "actor_key", dim_actor_keys, "actor_key", "dim_actor")

check_orphans("fact_releases", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_releases", "published_date_key", dim_date_keys, "date_key", "dim_date")

check_orphans("fact_repo_snapshot", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_repo_snapshot", "snapshot_date_key", dim_date_keys, "date_key", "dim_date")

check_orphans("fact_repo_daily_activity", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_repo_daily_activity", "date_key", dim_date_keys, "date_key", "dim_date")

check_orphans("fact_repo_stars_weekly", "repo_key", dim_repo_keys, "repo_key", "dim_repo")
check_orphans("fact_repo_stars_weekly", "week_start_date_key", dim_date_keys, "date_key", "dim_date")

# Gate 3: Row Count Parity Reconciliation vs. Silver
reconciliation_pairs = [
    ("fact_commits", "commits"),
    ("fact_issues", "issues"),
    ("fact_pull_requests", "pull_requests"),
    ("fact_releases", "releases"),
    ("fact_repo_snapshot", "repo_metadata"),
]

reconciliation_rows = []
all_reconciled = True
check_ts = datetime.now(timezone.utc)

for gold_tbl, silver_tbl in reconciliation_pairs:
    silver_cnt = spark.table(f"{catalog}.{silver_schema}.{silver_tbl}").count()
    gold_cnt = spark.table(f"{catalog}.{gold_schema}.{gold_tbl}").count()
    delta = gold_cnt - silver_cnt
    is_pass = (delta == 0)
    if not is_pass:
        all_reconciled = False
    status = "PASS" if is_pass else "FAIL"
    reconciliation_rows.append((gold_tbl, silver_cnt, gold_cnt, delta, status, check_ts))
    print(f"  ✓ [Reconciliation] {silver_schema}.{silver_tbl:<15} ({silver_cnt:,}) == {gold_schema}.{gold_tbl:<20} ({gold_cnt:,})  Delta: {delta} [{status}]")

# Write audit log to gold.audit_gold_reconciliation
recon_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("silver_source_rows", LongType(), True),
    StructField("gold_target_rows", LongType(), True),
    StructField("delta_rows", LongType(), True),
    StructField("status", StringType(), False),
    StructField("check_timestamp", TimestampType(), False),
])
df_recon_audit = spark.createDataFrame(reconciliation_rows, schema=recon_schema)
df_recon_audit.write.format("delta").mode("append").saveAsTable(f"{catalog}.{gold_schema}.audit_gold_reconciliation")

if not all_reconciled:
    raise AssertionError(f"Quality Gate 3 Failed: Row count disparity detected between Silver and Gold! Review gold.audit_gold_reconciliation.")

print("All Quality Gates PASSED successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Liquid Clustering OPTIMIZE & Storage Inspection

# COMMAND ----------

if run_optimize:
    print("\n" + "=" * 80)
    print("RUNNING LIQUID CLUSTERING OPTIMIZE ON GOLD TABLES")
    print("=" * 80)
    for tbl in pk_definitions.keys():
        print(f"Optimizing {catalog}.{gold_schema}.{tbl}...")
        try:
            spark.sql(f"OPTIMIZE {catalog}.{gold_schema}.{tbl}")
            detail_df = (
                spark.sql(f"DESCRIBE DETAIL {catalog}.{gold_schema}.{tbl}")
                .select("format", "numFiles", "sizeInBytes", "clusteringColumns")
            )
            detail_row = detail_df.first()
            clustering = detail_row["clusteringColumns"] if "clusteringColumns" in detail_df.columns else "N/A"
            print(f"  ✓ {tbl}: numFiles={detail_row['numFiles']}, sizeInBytes={detail_row['sizeInBytes']}, clustering={clustering}")
        except Exception as opt_err:
            print(f"  [Info] OPTIMIZE skipped or failed for {tbl}: {opt_err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: Completion & Exit

# COMMAND ----------

exit_summary = {
    "batch_id": batch_id,
    "status": "SUCCESS",
    "gold_mode": gold_mode,
    "tables_processed": list(pk_definitions.keys()),
    "quality_gates": "ALL_PASSED",
    "reconciliation": [
        {"table": r[0], "silver_rows": r[1], "gold_rows": r[2], "delta": r[3], "status": r[4]}
        for r in reconciliation_rows
    ]
}

print("\n" + "=" * 80)
print("GOLD LAYER PIPELINE RUN COMPLETED SUCCESSFULLY")
print(json.dumps(exit_summary, indent=2, default=str))
print("=" * 80)

try:
    dbutils.notebook.exit(json.dumps(exit_summary, default=str))
except Exception:
    pass
