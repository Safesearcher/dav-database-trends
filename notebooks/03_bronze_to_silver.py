# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03 - Bronze to Silver Transformation & Quality Cleansing
# MAGIC
# MAGIC **Transformations & Business Rules (docs/data_dictionary.md):**
# MAGIC 1. **Issue vs. PR Disambiguation:** Excludes pull requests from issues (`pull_request IS NULL`).
# MAGIC    * *Note on Cassandra & MongoDB:* 100% of records in their GitHub `issues.json` are PRs because true issues live on external ASF/MongoDB JIRA instances. Their Silver issues count is strictly 0.
# MAGIC 2. **PII Masking Protocol:** Salting and SHA-256 hashing for all committer and author emails. Drops personal names and raw emails.
# MAGIC 3. **Commit Headline Sanitization:** Truncates commit messages to the first line and scrubs `Signed-off-by` / `Co-authored-by` trailers.
# MAGIC 4. **Actor & Bot Classification (`is_bot`):** Flags CI/CD bots based on `[bot]` usernames, type `'Bot'`, or known bot accounts (`bors`, `cockroach-teamcity`, `mongodb-evergreen`, `dependabot`, `renovate`, `github-actions`).
# MAGIC 5. **Strict Typing:** Casts timestamps to `TimestampType` (UTC), IDs to `LongType`, counts to `IntegerType`.
# MAGIC 6. **Quarantine Routing:** Type-mismatched rows, unparseable dates, or missing primary keys are routed to `ops.silver_quarantine` with `layer='silver'` and rejection reasons.
# MAGIC 7. **Delta Idempotency:** Executes `MERGE INTO` keyed on `(repo_full_name, primary_key)` with `mergeSchema` enabled.
# MAGIC 8. **Auditing & Reconciliation:** Every table run is logged via `log_run` to `ops.pipeline_execution_logs`. At the end, a reconciliation report (Bronze vs. Silver + Quarantined + Excluded) is printed.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace", "Unity Catalog Name")
dbutils.widgets.text("repo", "surrealdb/surrealdb", "Repository Name ('owner/repo' or 'ALL')")
dbutils.widgets.text("since", "", "Since Filter Timestamp (empty = no lower bound)")
dbutils.widgets.text("until", "", "Until Filter Timestamp (empty = no upper bound)")
dbutils.widgets.text("batch_id", "", "Batch Run ID (Empty for auto-generated)")
dbutils.widgets.dropdown("run_mode", "full", ["full", "incremental", "backfill"], "Execution Run Mode")
dbutils.widgets.text("salt", "", "PII Salt (Optional fallback)")

# COMMAND ----------
import sys
import os
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
    ArrayType,
)
from delta.tables import DeltaTable

# Ensure project src/ is importable
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.schemas import SILVER_SCHEMAS
from src.common import (
    get_params,
    get_salt,
    hash_email,
    bot_flag,
    sanitize_commit_message,
    parse_iso_timestamp,
    log_run,
    merge_delta,
    quarantine_records,
    DATE_COLUMN_MAP,
)

# Parse parameters
params = get_params(dbutils)
catalog = params["catalog"]
target_repo = params["repo"]
since_str = params["since"]
until_str = params["until"]
batch_id = params["batch_id"]
run_mode = params["run_mode"]

# Retrieve Salt (from secrets scope 'dbtrends' key 'salt' or widget)
salt = get_salt(dbutils)

print(f"Target Catalog:  {catalog}")
print(f"Repository:      {target_repo}")
print(f"Window:          {since_str} -> {until_str}")
print(f"Batch ID:        {batch_id}")
print(f"Run Mode:        {run_mode}")
print(f"Salt Configured: {'Yes (Length: ' + str(len(salt)) + ')' if salt else 'No'}")

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

# COMMAND ----------
def transform_commits(bronze_df, repo_full_name, salt):
    """Transforms raw bronze commits to typed, PII-masked silver commits."""
    # Compute dates and hashes
    parsed_date = parse_iso_timestamp("commit.author.date")
    raw_date_str = F.col("commit.author.date")

    transformed = bronze_df.select(
        F.lit(repo_full_name).alias("repo_full_name"),
        F.col("sha").alias("commit_sha"),
        F.col("author.login").alias("author_login"),
        F.col("author.id").cast(LongType()).alias("author_id"),
        hash_email("commit.author.email", salt).alias("author_email_hash"),
        hash_email("commit.committer.email", salt).alias("committer_email_hash"),
        parsed_date.alias("commit_date_utc"),
        sanitize_commit_message("commit.message").alias("commit_headline"),
        F.coalesce(F.col("commit.comment_count"), F.lit(0)).cast(IntegerType()).alias("comment_count"),
        bot_flag("author.login", "author.type").alias("is_bot"),
        F.col("_source_file"),
        raw_date_str.alias("_raw_date"),
        F.current_timestamp().alias("load_timestamp")
    )

    # Quarantine checks: missing commit SHA or unparseable date
    missing_pk = F.col("commit_sha").isNull() | (F.length(F.trim(F.col("commit_sha"))) == 0)
    unparseable_dt = F.col("_raw_date").isNotNull() & F.col("commit_date_utc").isNull()
    rejected_cond = missing_pk | unparseable_dt

    rejection_reason = F.when(missing_pk, F.lit("Missing commit SHA primary key")).otherwise(
        F.lit("Unparseable commit_date_utc timestamp")
    )

    valid_df = transformed.filter(~rejected_cond).drop("_raw_date")
    rejected_df = transformed.filter(rejected_cond).withColumn("rejection_reason", rejection_reason).drop("_raw_date")

    return valid_df, rejected_df, ["repo_full_name", "commit_sha"]


def transform_issues(bronze_df, repo_full_name):
    """
    Transforms raw bronze issues to typed silver issues.
    CRITICAL: Excludes PRs (pull_request IS NOT NULL).
    Cassandra and MongoDB issues are 100% PRs, so their valid issues count will be 0.
    """
    # 1. Separate embedded Pull Requests from true issues
    is_pr_row = (
        F.col("pull_request").isNotNull() &
        (F.col("pull_request.url").isNotNull() | F.col("pull_request.html_url").isNotNull())
    )
    true_issues_bronze = bronze_df.filter(~is_pr_row)
    excluded_prs_count = bronze_df.filter(is_pr_row).count()

    raw_created_str = F.col("created_at")
    parsed_created = parse_iso_timestamp("created_at")

    transformed = true_issues_bronze.select(
        F.lit(repo_full_name).alias("repo_full_name"),
        F.col("number").cast(IntegerType()).alias("issue_number"),
        F.col("id").cast(LongType()).alias("issue_id"),
        F.coalesce(F.col("title"), F.lit("[NO TITLE]")).alias("title"),
        F.lower(F.col("state")).alias("state"),
        F.col("user.login").alias("author_login"),
        F.col("user.id").cast(LongType()).alias("author_id"),
        bot_flag("user.login", "user.type").alias("is_bot"),
        parsed_created.alias("created_at_utc"),
        parse_iso_timestamp("updated_at").alias("updated_at_utc"),
        parse_iso_timestamp("closed_at").alias("closed_at_utc"),
        F.coalesce(F.col("comments"), F.lit(0)).cast(IntegerType()).alias("comments_count"),
        F.expr("transform(labels, x -> x.name)").alias("labels_list"),
        F.col("_source_file"),
        raw_created_str.alias("_raw_date"),
        F.current_timestamp().alias("load_timestamp")
    )

    missing_pk = F.col("issue_number").isNull() | F.col("issue_id").isNull()
    unparseable_dt = F.col("_raw_date").isNotNull() & F.col("created_at_utc").isNull()
    rejected_cond = missing_pk | unparseable_dt

    rejection_reason = F.when(missing_pk, F.lit("Missing issue_number or issue_id")).otherwise(
        F.lit("Unparseable created_at_utc timestamp")
    )

    valid_df = transformed.filter(~rejected_cond).drop("_raw_date")
    rejected_df = transformed.filter(rejected_cond).withColumn("rejection_reason", rejection_reason).drop("_raw_date")

    return valid_df, rejected_df, ["repo_full_name", "issue_number"], excluded_prs_count


def transform_pull_requests(bronze_df, repo_full_name):
    """Transforms raw bronze pull requests into typed silver pull requests."""
    raw_created_str = F.col("created_at")
    parsed_created = parse_iso_timestamp("created_at")
    parsed_merged = parse_iso_timestamp("merged_at")

    transformed = bronze_df.select(
        F.lit(repo_full_name).alias("repo_full_name"),
        F.col("number").cast(IntegerType()).alias("pr_number"),
        F.col("id").cast(LongType()).alias("pr_id"),
        F.coalesce(F.col("title"), F.lit("[NO TITLE]")).alias("title"),
        F.lower(F.col("state")).alias("state"),
        F.col("user.login").alias("author_login"),
        bot_flag("user.login", "user.type").alias("is_bot"),
        parsed_created.alias("created_at_utc"),
        parse_iso_timestamp("updated_at").alias("updated_at_utc"),
        parse_iso_timestamp("closed_at").alias("closed_at_utc"),
        parsed_merged.alias("merged_at_utc"),
        parsed_merged.isNotNull().alias("is_merged"),
        F.col("merge_commit_sha"),
        F.col("head.ref").alias("head_branch"),
        F.col("base.ref").alias("base_branch"),
        F.col("_source_file"),
        raw_created_str.alias("_raw_date"),
        F.current_timestamp().alias("load_timestamp")
    )

    missing_pk = F.col("pr_number").isNull() | F.col("pr_id").isNull()
    unparseable_dt = F.col("_raw_date").isNotNull() & F.col("created_at_utc").isNull()
    rejected_cond = missing_pk | unparseable_dt

    rejection_reason = F.when(missing_pk, F.lit("Missing pr_number or pr_id")).otherwise(
        F.lit("Unparseable created_at_utc timestamp")
    )

    valid_df = transformed.filter(~rejected_cond).drop("_raw_date")
    rejected_df = transformed.filter(rejected_cond).withColumn("rejection_reason", rejection_reason).drop("_raw_date")

    return valid_df, rejected_df, ["repo_full_name", "pr_number"]


def transform_releases(bronze_df, repo_full_name):
    """Transforms raw bronze releases into typed silver releases."""
    raw_pub_str = F.col("published_at")
    parsed_pub = parse_iso_timestamp("published_at")

    transformed = bronze_df.select(
        F.lit(repo_full_name).alias("repo_full_name"),
        F.col("id").cast(LongType()).alias("release_id"),
        F.col("tag_name"),
        F.coalesce(F.col("name"), F.col("tag_name")).alias("release_name"),
        F.coalesce(F.col("draft"), F.lit(False)).alias("is_draft"),
        F.coalesce(F.col("prerelease"), F.lit(False)).alias("is_prerelease"),
        parsed_pub.alias("published_at_utc"),
        F.size(F.coalesce(F.col("assets"), F.array())).cast(IntegerType()).alias("assets_count"),
        F.col("_source_file"),
        raw_pub_str.alias("_raw_date"),
        F.current_timestamp().alias("load_timestamp")
    )

    missing_pk = F.col("release_id").isNull() | F.col("tag_name").isNull()
    unparseable_dt = F.col("_raw_date").isNotNull() & F.col("published_at_utc").isNull()
    rejected_cond = missing_pk | unparseable_dt

    rejection_reason = F.when(missing_pk, F.lit("Missing release_id or tag_name")).otherwise(
        F.lit("Unparseable published_at_utc timestamp")
    )

    valid_df = transformed.filter(~rejected_cond).drop("_raw_date")
    rejected_df = transformed.filter(rejected_cond).withColumn("rejection_reason", rejection_reason).drop("_raw_date")

    return valid_df, rejected_df, ["repo_full_name", "release_id"]


def transform_metadata(bronze_df, repo_full_name):
    """Transforms raw bronze repo metadata into typed silver repo metadata."""
    raw_created_str = F.col("created_at")
    parsed_created = parse_iso_timestamp("created_at")

    transformed = bronze_df.select(
        F.col("id").cast(LongType()).alias("repo_id"),
        F.lit(repo_full_name).alias("repo_full_name"),
        F.col("name").alias("repo_name"),
        F.col("owner.login").alias("owner_login"),
        F.col("description"),
        F.col("default_branch"),
        F.col("license.spdx_id").alias("license_spdx_id"),
        F.coalesce(F.col("stargazers_count"), F.lit(0)).cast(IntegerType()).alias("stargazers_count"),
        F.coalesce(F.col("forks_count"), F.lit(0)).cast(IntegerType()).alias("forks_count"),
        F.coalesce(F.col("open_issues_count"), F.lit(0)).cast(IntegerType()).alias("open_issues_count"),
        parsed_created.alias("created_at_utc"),
        parse_iso_timestamp("updated_at").alias("updated_at_utc"),
        parse_iso_timestamp("pushed_at").alias("pushed_at_utc"),
        F.col("topics"),
        F.col("_source_file"),
        raw_created_str.alias("_raw_date"),
        F.current_timestamp().alias("load_timestamp")
    )

    missing_pk = F.col("repo_id").isNull() | F.col("repo_full_name").isNull()
    unparseable_dt = F.col("_raw_date").isNotNull() & F.col("created_at_utc").isNull()
    rejected_cond = missing_pk | unparseable_dt

    rejection_reason = F.when(missing_pk, F.lit("Missing repo_id or repo_full_name")).otherwise(
        F.lit("Unparseable created_at_utc timestamp")
    )

    valid_df = transformed.filter(~rejected_cond).drop("_raw_date")
    rejected_df = transformed.filter(rejected_cond).withColumn("rejection_reason", rejection_reason).drop("_raw_date")

    return valid_df, rejected_df, ["repo_id", "repo_full_name"]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Silver Transformation Runner Function

# COMMAND ----------
def run_silver_entity(
    spark,
    catalog: str,
    repo_full_name: str,
    entity: str,
    since_str: str,
    until_str: str,
    batch_id: str,
    run_mode: str,
    salt: str
) -> dict:
    """
    Executes transformation for a single repo and entity from bronze.<entity> to silver.<entity>.
    Wraps execution in log_run for audit logging to ops.pipeline_execution_logs.
    """
    target_table_map = {
        "commits": f"{catalog}.silver.commits",
        "issues": f"{catalog}.silver.issues",
        "pulls": f"{catalog}.silver.pull_requests",
        "releases": f"{catalog}.silver.releases",
        "repo_metadata": f"{catalog}.silver.repo_metadata",
    }
    bronze_table_map = {
        "commits": f"{catalog}.bronze.commits",
        "issues": f"{catalog}.bronze.issues",
        "pulls": f"{catalog}.bronze.pull_requests",
        "releases": f"{catalog}.bronze.releases",
        "repo_metadata": f"{catalog}.bronze.repo_metadata",
    }

    target_table = target_table_map[entity]
    bronze_table = bronze_table_map[entity]
    repo_slug = repo_full_name.replace("/", "_")

    param_desc = f"repo={repo_full_name},entity={entity},mode={run_mode}"

    with log_run(spark, layer="SILVER", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
        if not spark.catalog.tableExists(bronze_table):
            print(f"  [Skip] Bronze table {bronze_table} does not exist.")
            return {"bronze_count": 0, "silver_inserted": 0, "silver_updated": 0, "quarantined": 0, "excluded_prs": 0}

        # Read bronze source for this repository
        bronze_raw = spark.table(bronze_table).filter(
            (F.col("repo_full_name") == repo_full_name) |
            (F.col("repo_full_name") == repo_slug)
        )

        date_col = DATE_COLUMN_MAP.get(entity)
        if run_mode in ("incremental", "backfill") and date_col:
            date_expr = parse_iso_timestamp(date_col)
            if since_str:
                bronze_raw = bronze_raw.filter(date_expr >= F.to_timestamp(F.lit(since_str)))
            if until_str:
                bronze_raw = bronze_raw.filter(date_expr <= F.to_timestamp(F.lit(until_str)))

        bronze_count = bronze_raw.count()
        if bronze_count == 0:
            print(f"  [Empty] 0 records found in {bronze_table} for {repo_full_name}.")
            return {"bronze_count": 0, "silver_inserted": 0, "silver_updated": 0, "quarantined": 0, "excluded_prs": 0}

        excluded_prs = 0

        # Execute entity-specific transformation
        if entity == "commits":
            valid_df, rejected_df, pk_keys = transform_commits(bronze_raw, repo_full_name, salt)
        elif entity == "issues":
            valid_df, rejected_df, pk_keys, excluded_prs = transform_issues(bronze_raw, repo_full_name)
        elif entity == "pulls":
            valid_df, rejected_df, pk_keys = transform_pull_requests(bronze_raw, repo_full_name)
        elif entity == "releases":
            valid_df, rejected_df, pk_keys = transform_releases(bronze_raw, repo_full_name)
        elif entity == "repo_metadata":
            valid_df, rejected_df, pk_keys = transform_metadata(bronze_raw, repo_full_name)

        # Route invalid / malformed records to ops.silver_quarantine
        quarantined_count = 0
        if rejected_df.limit(1).count() > 0:
            quarantined_count = quarantine_records(
                spark=spark,
                catalog=catalog,
                df_rejected=rejected_df,
                layer="silver",
                entity=entity,
                rejection_reason=f"Constraint violation / null required field in silver_{entity}",
                batch_id=batch_id
            )
            print(f"  [Quarantined] {quarantined_count} invalid records routed to {catalog}.ops.silver_quarantine.")

        # Idempotent MERGE INTO Silver Delta table
        inserted = 0
        updated = 0
        if valid_df.limit(1).count() > 0:
            inserted, updated = merge_delta(
                spark=spark,
                df=valid_df,
                target_table=target_table,
                keys=pk_keys,
                update_all=True
            )

        run_ctx["rows_inserted"] = inserted
        run_ctx["rows_updated"] = updated

        print(f"  [Silver Merged] {target_table} -> Inserted: {inserted}, Updated: {updated}")

        return {
            "bronze_count": bronze_count,
            "silver_inserted": inserted,
            "silver_updated": updated,
            "quarantined": quarantined_count,
            "excluded_prs": excluded_prs,
        }

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execution & Reconciliation Loop

# COMMAND ----------
# Determine repositories to process
if target_repo.upper() == "ALL":
    repos_to_process = [f"{owner}/{repo}" for owner, repo in ALL_REPOS]
else:
    repos_to_process = [target_repo.replace("_", "/", 1) if "_" in target_repo and "/" not in target_repo else target_repo]

ENTITIES = ["commits", "issues", "pulls", "releases", "repo_metadata"]

reconciliation_records = []

for r_name in repos_to_process:
    print(f"\n==================================================")
    print(f"Transforming Repository: {r_name}")
    print(f"==================================================")

    for e_name in ENTITIES:
        print(f"> Transforming Entity: {e_name}")
        try:
            metrics = run_silver_entity(
                spark=spark,
                catalog=catalog,
                repo_full_name=r_name,
                entity=e_name,
                since_str=since_str,
                until_str=until_str,
                batch_id=batch_id,
                run_mode=run_mode,
                salt=salt
            )
            reconciliation_records.append({
                "repository": r_name,
                "entity": e_name,
                "bronze_rows": metrics["bronze_count"],
                "silver_inserted": metrics["silver_inserted"],
                "silver_updated": metrics["silver_updated"],
                "quarantined_rows": metrics["quarantined"],
                "excluded_prs": metrics["excluded_prs"],
                "reconciled": (
                    metrics["bronze_count"] == (metrics["silver_inserted"] + metrics["silver_updated"] + metrics["quarantined"] + metrics["excluded_prs"])
                )
            })
        except Exception as err:
            print(f"  ❌ Error transforming {r_name} [{e_name}]: {err}")
            reconciliation_records.append({
                "repository": r_name,
                "entity": e_name,
                "bronze_rows": 0,
                "silver_inserted": 0,
                "silver_updated": 0,
                "quarantined_rows": 0,
                "excluded_prs": 0,
                "reconciled": False,
                "error": str(err),
            })

# COMMAND ----------
# MAGIC %md
# MAGIC ### Reconciliation Audit Report
# MAGIC Verifies that: `Bronze Rows == (Silver Rows + Quarantined Rows + Excluded PRs)`

# COMMAND ----------
print("\n" + "=" * 105)
print(f"{'REPOSITORY':<25} {'ENTITY':<15} {'BRONZE':<8} {'INSERTED':<10} {'UPDATED':<9} {'QUARANTINED':<13} {'EXCL_PRS':<10} {'STATUS'}")
print("=" * 105)

for rec in reconciliation_records:
    status_label = "BALANCED" if rec["reconciled"] else "CHECK"
    print(
        f"{rec['repository']:<25} "
        f"{rec['entity']:<15} "
        f"{rec['bronze_rows']:<8} "
        f"{rec['silver_inserted']:<10} "
        f"{rec['silver_updated']:<9} "
        f"{rec['quarantined_rows']:<13} "
        f"{rec['excluded_prs']:<10} "
        f"{status_label}"
    )

print("=" * 105)

failed_entities = [r for r in reconciliation_records if "error" in r]
if failed_entities:
    raise RuntimeError(f"{len(failed_entities)} entity run(s) failed: {failed_entities}")

summary = {
    "batch_id": batch_id,
    "records": reconciliation_records,
}
try:
    dbutils.notebook.exit(json.dumps(summary))
except Exception:
    pass
