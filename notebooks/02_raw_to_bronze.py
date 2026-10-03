# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02 - Ingest Raw JSON into Bronze Delta Tables
# MAGIC
# MAGIC **Capabilities & Governance:**
# MAGIC 1. **Zero Inference:** Reads Volume raw JSON with explicit schemas from `src.schemas` using `PERMISSIVE` mode and `_corrupt_record`.
# MAGIC 2. **Quarantine Routing:** Rows with non-null `_corrupt_record` or null primary keys are routed to `ops.silver_quarantine` with `layer='bronze'`.
# MAGIC 3. **Lineage Enrichment:** Adds `repo_full_name`, `load_timestamp`, `_source_file`, and `batch_id`.
# MAGIC 4. **Schema Drift Detection:** Detects unexpected top-level fields in incoming JSON, logs them, and evolves the table with `mergeSchema`.
# MAGIC 5. **Idempotency & Dedup:** Deduplicates the incoming batch by primary key (keeping latest `updated_at`) and merges into `bronze.<entity>` on `(repo_full_name, primary_key)`.
# MAGIC 6. **Window Filtering:** Respects `since` and `until` parameters for `incremental` or `backfill` run modes.
# MAGIC 7. **Execution Audit:** Wrapped with `log_run` logging row counts and status to `ops.pipeline_execution_logs`.
# MAGIC 8. **Multi-Repo Batch:** Supports `repo='ALL'` to process all 10 repos x 5 entities sequentially.

# COMMAND ----------
dbutils.widgets.text("catalog",   "workspace",                         "Unity Catalog Name")
dbutils.widgets.text("repo",      "surrealdb/surrealdb",               "Repository (owner/repo) or 'ALL'")
dbutils.widgets.dropdown("entity", "commits", ["commits", "issues", "pulls", "releases", "repo_metadata", "ALL"], "Entity Name")
dbutils.widgets.text("base_path", "/Volumes/workspace/bronze_data/raw", "Volume Storage Base Path")  # must match 04_run_pipeline base_path widget
dbutils.widgets.text("since",     "",                                   "Since Filter Timestamp (ISO-8601, empty = no lower bound)")
dbutils.widgets.text("until",     "",                                   "Until Filter Timestamp (ISO-8601, empty = no upper bound)")
dbutils.widgets.text("batch_id",  "",                                   "Batch Run ID (empty = auto-generated)")
dbutils.widgets.dropdown("run_mode", "full", ["full", "incremental", "backfill"], "Execution Run Mode")

# COMMAND ----------
import sys
import os
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Ensure project src/ is importable
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.schemas import BRONZE_SCHEMAS, PRIMARY_KEYS
from src.common import (
    get_params,
    log_run,
    add_metadata,
    merge_delta,
    quarantine_records,
    parse_iso_timestamp,
    read_raw_records,
    parse_records,
    discover_drift_keys,
    evolve_table_schema,
    DATE_COLUMN_MAP,
)

# Parse parameters with safe fallbacks
params = get_params(dbutils)
catalog = params["catalog"]
target_repo = params["repo"]
target_entity = params["entity"]
base_path = params["base_path"]
since_str = params["since"]
until_str = params["until"]
batch_id = params["batch_id"]
run_mode = params["run_mode"]

print(f"Target Catalog:  {catalog}")
print(f"Repository:      {target_repo}")
print(f"Entity:          {target_entity}")
print(f"Base Path:       {base_path}")
print(f"Run Mode:        {run_mode}")
print(f"Batch ID:        {batch_id}")
print(f"Window:          {since_str} -> {until_str}")

# COMMAND ----------
# 10 Tracked Database Repositories
ALL_REPOS = [
    ("postgres", "postgres"),
    ("cockroachdb", "cockroach"),
    ("mongodb", "mongo"),
    ("surrealdb", "surrealdb"),
    ("redis", "redis"),
    ("apache", "cassandra"),
    ("facebookresearch", "faiss"),
    ("qdrant", "qdrant"),
    ("milvus-io", "milvus"),
    ("chroma-core", "chroma"),
]

ALL_ENTITIES = ["repo_metadata", "commits", "issues", "pulls", "releases"]

TABLE_NAME_MAP = {
    "commits": "commits",
    "issues": "issues",
    "pulls": "pull_requests",
    "releases": "releases",
    "repo_metadata": "repo_metadata",
}

# COMMAND ----------
def process_entity(
    repo_slug: str,
    entity_name: str,
    catalog: str,
    base_path: str,
    since_str: str,
    until_str: str,
    batch_id: str,
    run_mode: str
) -> dict:
    """
    Ingests one JSON file into the corresponding Bronze Delta table.
    Enforces explicit schema, quarantine routing, deduplication, time filtering,
    and Delta MERGE INTO. Wrapped in log_run for audit compliance.
    """
    repo_full_name = repo_slug if "/" in repo_slug else repo_slug.replace("_", "/", 1)
    dir_slug = repo_full_name.replace("/", "_")
    file_path = f"{base_path.rstrip('/')}/{dir_slug}/{entity_name}.json"
    target_table_name = TABLE_NAME_MAP.get(entity_name, entity_name)
    target_table = f"{catalog}.bronze.{target_table_name}"
    pk_field = PRIMARY_KEYS.get(entity_name, "id")
    explicit_schema = BRONZE_SCHEMAS.get(entity_name)

    if explicit_schema is None:
        raise ValueError(f"Unknown entity '{entity_name}'. Supported: {list(BRONZE_SCHEMAS.keys())}")

    param_desc = f"repo={repo_full_name},entity={entity_name},mode={run_mode},file={file_path}"

    with log_run(spark, layer="BRONZE", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
        # Check source file existence and non-empty status
        file_size = 0
        try:
            file_stats = dbutils.fs.ls(file_path)
            file_size = file_stats[0].size if file_stats else 0
            if not file_stats or file_size <= 2:
                print(f"  [Empty] {file_path} contains 0 records. Skipping merge.")
                run_ctx["status"] = "SUCCESS"
                run_ctx["message"] = "NO_DATA"
                return {"status": "SUCCESS", "inserted": 0, "updated": 0, "drift": []}
        except Exception as ls_err:
            print(f"  [Notice] Accessing {file_path}: {ls_err}")

        # 1. Read raw records independently
        raw_df = read_raw_records(spark, file_path)
        if raw_df.limit(1).count() == 0:
            if file_size > 2:
                raise RuntimeError(f"Invalid JSON in {file_path}: file size {file_size} bytes but yielded 0 records")
            print(f"  [Empty] No rows parsed from {file_path}.")
            run_ctx["status"] = "SUCCESS"
            run_ctx["message"] = "NO_DATA"
            return {"status": "SUCCESS", "inserted": 0, "updated": 0, "drift": []}

        # 2. Discover Schema Drift before reading
        drifted_keys = discover_drift_keys(raw_df, explicit_schema)
        if drifted_keys:
            print(f"  [Schema Drift] Detected {len(drifted_keys)} unmapped fields in raw JSON: {drifted_keys}")
            run_ctx["parameter"] = f"{param_desc},drift_keys={','.join(drifted_keys)}"

        # 3. Parse records independently with PERMISSIVE mode
        parsed = parse_records(raw_df, explicit_schema, drifted_keys)

        # 4. Add lineage and audit metadata
        enriched_df = (
            parsed
            .withColumn("repo_full_name", F.lit(repo_full_name))
            .withColumn("_source_file", F.lit(file_path))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("load_timestamp", F.current_timestamp())
        )

        # 5. Quarantine Check: corrupt records or null primary key
        is_corrupt = F.col("_corrupt_record").isNotNull()
        is_null_pk = F.col(pk_field).isNull()
        rejected_cond = is_corrupt | is_null_pk

        # Silent-loss guard: single aggregation for counts
        counts = enriched_df.agg(
            F.count("*").alias("total"),
            F.count(F.when(~rejected_cond, 1)).alias("valid_n"),
            F.count(F.when(rejected_cond, 1)).alias("rejected_n"),
        ).first()
        total = counts["total"]
        valid_n = counts["valid_n"]
        rejected_n = counts["rejected_n"]
        if valid_n + rejected_n != total:
            raise RuntimeError(f"Silent loss detected in {file_path}: total={total}, valid={valid_n}, rejected={rejected_n}")

        rejection_expr = F.when(is_corrupt, F.lit("Corrupt or type-mismatched JSON record")).otherwise(
            F.lit(f"Null primary key ({pk_field})")
        )
        rejected_df = enriched_df.filter(rejected_cond).withColumn("rejection_reason", rejection_expr)
        valid_df = enriched_df.filter(~rejected_cond).drop("_corrupt_record")

        if rejected_n > 0:
            q_count = quarantine_records(
                spark=spark,
                catalog=catalog,
                df_rejected=rejected_df,
                layer="bronze",
                entity=entity_name,
                rejection_reason="Quarantined during Bronze ingestion",
                batch_id=batch_id
            )
            print(f"  [Quarantine] Routed {q_count} records to {catalog}.ops.silver_quarantine.")

        if valid_n == 0:
            print(f"  [Notice] Zero valid records remaining after quarantine check.")
            run_ctx["status"] = "FAILURE"
            run_ctx["message"] = "ALL_RECORDS_QUARANTINED"
            return {"status": "FAILURE", "inserted": 0, "updated": 0, "drift": drifted_keys}

        if rejected_n > 0:
            run_ctx["message"] = f"PARTIAL_QUARANTINE: {rejected_n}/{total} records quarantined"

        # 6. Respect since / until time filter for incremental or backfill runs
        date_col = DATE_COLUMN_MAP.get(entity_name)
        if run_mode in ("incremental", "backfill") and date_col:
            date_expr = parse_iso_timestamp(date_col)
            if since_str:
                valid_df = valid_df.filter(date_expr >= F.to_timestamp(F.lit(since_str)))
            if until_str:
                valid_df = valid_df.filter(date_expr <= F.to_timestamp(F.lit(until_str)))

        # 7. Deduplicate incoming batch on primary key, keeping latest date, tie-breaking on sha2(_raw_record)
        if date_col:
            dedup_window = (
                Window.partitionBy("repo_full_name", pk_field)
                .orderBy(
                    F.coalesce(parse_iso_timestamp(date_col), F.to_timestamp(F.lit("1970-01-01"))).desc(),
                    F.sha2(F.col("_raw_record"), 256).asc(),
                )
            )
        else:
            dedup_window = (
                Window.partitionBy("repo_full_name", pk_field)
                .orderBy(
                    F.col("load_timestamp").desc(),
                    F.sha2(F.col("_raw_record"), 256).asc(),
                )
            )

        deduped_df = (
            valid_df
            .withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num", "_raw_record")
        )

        # 8. Evolve table schema and MERGE INTO bronze.<entity>
        added = evolve_table_schema(spark, target_table, drifted_keys)
        merge_keys = ["repo_full_name", pk_field]
        inserted, updated = merge_delta(
            spark=spark,
            df=deduped_df,
            target_table=target_table,
            keys=merge_keys,
            update_all=True
        )

        run_ctx["rows_inserted"] = inserted
        run_ctx["rows_updated"] = updated
        run_ctx["parameter"] = f"{param_desc},drift_keys={drifted_keys[:10]},evolved={added}"

        print(f"  [Merge Complete] {target_table} -> Inserted: {inserted}, Updated: {updated}")
        return {"status": "SUCCESS", "inserted": inserted, "updated": updated, "drift": drifted_keys}

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execution Loop (Single Entity or ALL Repositories)

# COMMAND ----------
# Determine execution plan
plan = []

if target_repo.upper() == "ALL":
    repos_to_run = [f"{owner}_{repo}" for owner, repo in ALL_REPOS]
else:
    repos_to_run = [target_repo]

if target_entity.upper() == "ALL":
    entities_to_run = ALL_ENTITIES
else:
    entities_to_run = [target_entity]

print(f"Execution Plan: {len(repos_to_run)} repository/repositories x {len(entities_to_run)} entity/entities")

# Execute
total_inserted = 0
total_updated = 0
batch_results = []

for r_slug in repos_to_run:
    print(f"\n==================================================")
    print(f"Processing Repository: {r_slug}")
    print(f"==================================================")
    for e_name in entities_to_run:
        print(f"> Entity: {e_name}")
        try:
            res = process_entity(
                repo_slug=r_slug,
                entity_name=e_name,
                catalog=catalog,
                base_path=base_path,
                since_str=since_str,
                until_str=until_str,
                batch_id=batch_id,
                run_mode=run_mode
            )
            total_inserted += res.get("inserted", 0)
            total_updated += res.get("updated", 0)
            batch_results.append({
                "repo": r_slug,
                "entity": e_name,
                "status": res.get("status"),
                "inserted": res.get("inserted", 0),
                "updated": res.get("updated", 0),
            })
        except Exception as e:
            print(f"  ❌ FAILED {r_slug} / {e_name}: {e}")
            batch_results.append({
                "repo": r_slug,
                "entity": e_name,
                "status": f"FAILED: {str(e)[:100]}",
                "inserted": 0,
                "updated": 0,
            })

print(f"\n==================================================")
print(f"BRONZE INGESTION RUN FINISHED: {batch_id}")
print(f"Total Rows Inserted: {total_inserted}")
print(f"Total Rows Updated:  {total_updated}")
print(f"==================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Operational Audit Log Summary for this Batch

# COMMAND ----------
try:
    log_tbl = f"{catalog}.ops.pipeline_execution_logs"
    display(
        spark.sql(f"""
            SELECT 
                log_id,
                layer,
                parameter,
                status,
                rows_inserted,
                rows_updated,
                start_time,
                end_time,
                error_message
            FROM {log_tbl}
            WHERE batch_id = '{batch_id}'
            ORDER BY start_time DESC
        """)
    )
except Exception as log_err:
    print(f"Could not query operational execution logs: {log_err}")

failed_entities = [r for r in batch_results if str(r.get("status", "")).startswith("FAILED")]
if failed_entities:
    raise RuntimeError(f"{len(failed_entities)} entity run(s) failed: {failed_entities}")

summary = {
    "batch_id": batch_id,
    "total_inserted": total_inserted,
    "total_updated": total_updated,
    "results": batch_results,
}
try:
    dbutils.notebook.exit(json.dumps(summary))
except Exception:
    pass

