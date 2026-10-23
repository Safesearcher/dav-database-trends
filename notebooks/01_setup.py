# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Pipeline Environment & Catalog Setup
# MAGIC Initializes Unity Catalog schemas (`ops`, `bronze`, `silver`), Unity Catalog Volume for raw JSON storage,
# MAGIC and creates explicit DDL definitions for:
# MAGIC - `ops.pipeline_execution_logs`
# MAGIC - `ops.silver_quarantine`
# MAGIC - Bronze Delta tables (`bronze.<entity>`)
# MAGIC - Silver Delta tables (`silver.<entity>`)
# MAGIC
# MAGIC **Constraints:** Serverless-compatible, explicit DDL, parameterized catalog/schema widgets, Delta format.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("volume_schema", "bronze_data", "Volume Schema Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema Name")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema Name")
dbutils.widgets.text("ops_schema", "ops", "Ops Schema Name")
dbutils.widgets.text("bronze_volume", "raw", "Bronze Volume Name")
dbutils.widgets.dropdown("reset_tables", "false", ["false", "true"], "Drop Tables (Clean Reset)")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
volume_schema = dbutils.widgets.get("volume_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
ops_schema = dbutils.widgets.get("ops_schema").strip()
bronze_volume = dbutils.widgets.get("bronze_volume").strip()
reset_tables = dbutils.widgets.get("reset_tables").strip().lower() == "true"

print(f"Catalog:       {catalog}")
print(f"Bronze Schema: {bronze_schema}")
print(f"Volume Schema: {volume_schema}")
print(f"Silver Schema: {silver_schema}")
print(f"Gold Schema:   {gold_schema}")
print(f"Ops Schema:    {ops_schema}")
print(f"Bronze Volume: {bronze_volume}")
print(f"Reset Tables:  {reset_tables}")

# COMMAND ----------

import sys
import os
from datetime import datetime, timezone

# Ensure project src/ is importable
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from src.schemas import BRONZE_SCHEMAS
from src.common import log_run, start_run, end_run

batch_id = datetime.now(timezone.utc).strftime("setup_%Y%m%d_%H%M%S")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Unity Catalog Schemas & Volumes

# COMMAND ----------

with log_run(spark, layer="SETUP", parameter=f"catalog={catalog}", batch_id=batch_id, catalog=catalog) as run_ctx:
    # 1. Schemas DDL
    for s in [bronze_schema, volume_schema, silver_schema, gold_schema, ops_schema]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")
        print(f"Verified schema: {catalog}.{s}")

    # 2. Volume DDL
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{volume_schema}.{bronze_volume}")
    print(f"Verified Volume: /Volumes/{catalog}/{volume_schema}/{bronze_volume}")

    # 3. Optional Table Clean Reset
    if reset_tables:
        print("Reset flag is true. Dropping existing tables...")
        for tbl in [
            f"{ops_schema}.pipeline_execution_logs",
            f"{ops_schema}.silver_quarantine",
            f"{silver_schema}.commits",
            f"{silver_schema}.issues",
            f"{silver_schema}.pull_requests",
            f"{silver_schema}.releases",
            f"{silver_schema}.repo_metadata",
            f"{bronze_schema}.commits",
            f"{bronze_schema}.issues",
            f"{bronze_schema}.pull_requests",
            f"{bronze_schema}.releases",
            f"{bronze_schema}.repo_metadata",
            f"{gold_schema}.dim_repo",
            f"{gold_schema}.dim_date",
            f"{gold_schema}.dim_actor",
            f"{gold_schema}.fact_commits",
            f"{gold_schema}.fact_issues",
            f"{gold_schema}.fact_pull_requests",
            f"{gold_schema}.fact_releases",
            f"{gold_schema}.fact_repo_snapshot",
            f"{gold_schema}.fact_repo_daily_activity",
            f"{gold_schema}.audit_gold_reconciliation",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{tbl}")

    # 4. Explicit DDL for ops.pipeline_execution_logs (Hard Rule 6)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{ops_schema}.pipeline_execution_logs (
            log_id STRING NOT NULL,
            layer STRING NOT NULL,
            parameter STRING,
            batch_id STRING,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status STRING NOT NULL,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            rows_aggregated BIGINT,
            error_message STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)

    # Idempotent column check: ALTER TABLE to add rows_aggregated if table pre-existed without it
    try:
        log_tbl = f"{catalog}.{ops_schema}.pipeline_execution_logs"
        if spark.catalog.tableExists(log_tbl):
            existing_cols = [c.name.lower() for c in spark.catalog.listColumns(log_tbl)]
            if "rows_aggregated" not in existing_cols:
                spark.sql(f"ALTER TABLE {log_tbl} ADD COLUMNS (rows_aggregated BIGINT COMMENT 'Source rows read and aggregated')")
                print(f"Added column rows_aggregated to {log_tbl}")
    except Exception as alt_err:
        print(f"  [Info] ALTER TABLE check on {ops_schema}.pipeline_execution_logs: {alt_err}")

    print(f"Verified table: {catalog}.{ops_schema}.pipeline_execution_logs")

    # 5. Explicit DDL for ops.silver_quarantine (Hard Rule 7)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{ops_schema}.silver_quarantine (
            quarantine_id STRING NOT NULL,
            layer STRING NOT NULL,
            entity STRING NOT NULL,
            repo_full_name STRING,
            rejection_reason STRING NOT NULL,
            raw_payload STRING,
            batch_id STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)
    print(f"Verified table: {catalog}.{ops_schema}.silver_quarantine")

    # 6. Explicit DDL for Silver Layer Tables
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.commits (
            repo_full_name STRING NOT NULL,
            commit_sha STRING NOT NULL,
            author_login STRING,
            author_id BIGINT,
            author_email_hash STRING,
            committer_email_hash STRING,
            commit_date_utc TIMESTAMP,
            commit_headline STRING,
            comment_count INT,
            is_bot BOOLEAN NOT NULL,
            _source_file STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.issues (
            repo_full_name STRING NOT NULL,
            issue_number INT NOT NULL,
            issue_id BIGINT NOT NULL,
            title STRING,
            state STRING,
            author_login STRING,
            author_id BIGINT,
            is_bot BOOLEAN NOT NULL,
            created_at_utc TIMESTAMP,
            updated_at_utc TIMESTAMP,
            closed_at_utc TIMESTAMP,
            comments_count INT,
            labels_list ARRAY<STRING>,
            _source_file STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.pull_requests (
            repo_full_name STRING NOT NULL,
            pr_number INT NOT NULL,
            pr_id BIGINT NOT NULL,
            title STRING,
            state STRING,
            author_login STRING,
            is_bot BOOLEAN NOT NULL,
            created_at_utc TIMESTAMP,
            updated_at_utc TIMESTAMP,
            closed_at_utc TIMESTAMP,
            merged_at_utc TIMESTAMP,
            is_merged BOOLEAN NOT NULL,
            merge_commit_sha STRING,
            head_branch STRING,
            base_branch STRING,
            _source_file STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.releases (
            repo_full_name STRING NOT NULL,
            release_id BIGINT NOT NULL,
            tag_name STRING,
            release_name STRING,
            is_draft BOOLEAN NOT NULL,
            is_prerelease BOOLEAN NOT NULL,
            published_at_utc TIMESTAMP,
            assets_count INT,
            _source_file STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.repo_metadata (
            repo_id BIGINT NOT NULL,
            repo_full_name STRING NOT NULL,
            repo_name STRING,
            owner_login STRING,
            description STRING,
            default_branch STRING,
            license_spdx_id STRING,
            stargazers_count INT,
            forks_count INT,
            open_issues_count INT,
            created_at_utc TIMESTAMP,
            updated_at_utc TIMESTAMP,
            pushed_at_utc TIMESTAMP,
            topics ARRAY<STRING>,
            _source_file STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)
    print(f"Verified all 5 Silver tables in {catalog}.{silver_schema}")

    # 7. Explicit Creation of Bronze Tables from BRONZE_SCHEMAS
    TABLE_NAME_MAP = {
        "commits": "commits",
        "issues": "issues",
        "pulls": "pull_requests",
        "releases": "releases",
        "repo_metadata": "repo_metadata",
    }
    LINEAGE = [
        StructField("repo_full_name", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
    for entity, schema in BRONZE_SCHEMAS.items():
        fields = [f for f in schema.fields if f.name != "_corrupt_record"] + LINEAGE
        (spark.createDataFrame([], StructType(fields)).write.format("delta").mode("ignore")
             .saveAsTable(f"{catalog}.{bronze_schema}.{TABLE_NAME_MAP[entity]}"))
    print(f"Verified all 5 Bronze tables in {catalog}.{bronze_schema}")

    # 8. Explicit DDL for Gold Layer Star Schema Tables (Liquid Clustering + Comments)
    # Dimensions
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_repo (
            repo_key BIGINT NOT NULL COMMENT 'Deterministic surrogate key xxhash64(repo_full_name)',
            repo_id BIGINT NOT NULL COMMENT 'Upstream GitHub repository ID',
            repo_full_name STRING NOT NULL COMMENT 'Full repository name owner/repo',
            repo_name STRING COMMENT 'Repository name',
            owner_login STRING COMMENT 'Organization or owner login',
            description STRING COMMENT 'Repository description',
            default_branch STRING COMMENT 'Default git branch',
            license_spdx_id STRING COMMENT 'Open-source or commercial license SPDX identifier',
            created_at_utc TIMESTAMP COMMENT 'Repository creation timestamp in UTC',
            db_type STRING NOT NULL COMMENT 'Database paradigm: SQL, NoSQL, or Vector',
            db_category STRING NOT NULL COMMENT 'Database architectural category',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key)
        COMMENT 'SCD Type 1 repository dimension with classified database paradigm and architectural category';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_date (
            date_key INT NOT NULL COMMENT 'Integer date key yyyymmdd',
            full_date DATE NOT NULL COMMENT 'Calendar date',
            year INT NOT NULL COMMENT 'Calendar year',
            quarter INT NOT NULL COMMENT 'Calendar quarter 1-4',
            month INT NOT NULL COMMENT 'Calendar month 1-12',
            month_name STRING NOT NULL COMMENT 'Full English month name',
            week_of_year INT NOT NULL COMMENT 'ISO week of year 1-53',
            day_of_month INT NOT NULL COMMENT 'Day of month 1-31',
            day_of_week INT NOT NULL COMMENT 'Day of week 1 (Sun) to 7 (Sat)',
            is_weekend BOOLEAN NOT NULL COMMENT 'True if Saturday or Sunday',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if within standard 6-month analysis window (2026-04-01 to 2026-09-30)',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (date_key)
        COMMENT 'Continuous calendar dimension spine with temporal grains and analysis window tagging';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_actor (
            actor_key BIGINT NOT NULL COMMENT 'Deterministic surrogate key xxhash64(actor_identity)',
            actor_identity STRING NOT NULL COMMENT 'Natural contributor identity: login, hash:author_email_hash, or unknown',
            author_login STRING COMMENT 'Public GitHub login if linked',
            author_id BIGINT COMMENT 'Public GitHub user ID if linked',
            author_email_hash STRING COMMENT 'Salted SHA-256 hash of author email (no raw PII)',
            identity_source STRING NOT NULL COMMENT 'Identity resolution source: login, email_hash, or unknown',
            is_bot BOOLEAN NOT NULL COMMENT 'True if identified as automation bot account',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (actor_key)
        COMMENT 'Contributor dimension resolving logins, salted email hashes, bot tags, and unknown fallback';
    """)

    # Facts
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_commits (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            date_key INT NOT NULL COMMENT 'FK to dim_date.date_key (committed_at_utc)',
            actor_key BIGINT NOT NULL COMMENT 'FK to dim_actor.actor_key',
            commit_sha STRING NOT NULL COMMENT 'Unique 40-character Git commit SHA',
            comment_count INT COMMENT 'Commit comments count',
            is_bot BOOLEAN NOT NULL COMMENT 'True if commit authored by a known automation bot',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if within standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, date_key)
        COMMENT 'Commit fact table linking repository, calendar date, and contributor dimensions';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_issues (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            created_date_key INT NOT NULL COMMENT 'FK to dim_date.date_key (created_at_utc)',
            closed_date_key INT COMMENT 'FK to dim_date.date_key (closed_at_utc, nullable if open)',
            actor_key BIGINT NOT NULL COMMENT 'FK to dim_actor.actor_key',
            issue_number INT NOT NULL COMMENT 'GitHub issue number',
            state STRING NOT NULL COMMENT 'Issue state: open or closed',
            is_open BOOLEAN NOT NULL COMMENT 'True if issue is currently open',
            comments_count INT COMMENT 'Total comments on issue',
            label_count INT COMMENT 'Count of labels attached',
            time_to_close_hours DOUBLE COMMENT 'Cycle time in hours from creation to closure (null if open, never negative)',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if within standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, created_date_key)
        COMMENT 'True issues fact table tracking defect and feature lifecycle cycle times';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_pull_requests (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            created_date_key INT NOT NULL COMMENT 'FK to dim_date.date_key (created_at_utc)',
            closed_date_key INT COMMENT 'FK to dim_date.date_key (closed_at_utc, nullable)',
            merged_date_key INT COMMENT 'FK to dim_date.date_key (merged_at_utc, nullable if unmerged)',
            actor_key BIGINT NOT NULL COMMENT 'FK to dim_actor.actor_key',
            pr_number INT NOT NULL COMMENT 'GitHub pull request number',
            is_merged BOOLEAN NOT NULL COMMENT 'True if pull request was merged',
            is_bot BOOLEAN NOT NULL COMMENT 'True if pull request authored by automation bot',
            hours_to_merge DOUBLE COMMENT 'Hours elapsed from PR open to merge (null if unmerged, never negative)',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if within standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, created_date_key)
        COMMENT 'Pull request lifecycle fact table tracking developer throughput and merge latency';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_releases (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            published_date_key INT NOT NULL COMMENT 'FK to dim_date.date_key (published_at_utc)',
            release_id BIGINT NOT NULL COMMENT 'Upstream GitHub release ID',
            tag_name STRING COMMENT 'Git tag associated with release',
            is_prerelease BOOLEAN NOT NULL COMMENT 'True if marked as pre-release / beta',
            is_draft BOOLEAN NOT NULL COMMENT 'True if marked as draft',
            assets_count INT COMMENT 'Count of downloadable binary assets attached',
            days_since_previous_release DOUBLE COMMENT 'Days elapsed since preceding release for the same repo (null for first release)',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if within standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, published_date_key)
        COMMENT 'Release cadence fact table measuring interval cadences between consecutive releases';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_repo_snapshot (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            snapshot_date_key INT NOT NULL COMMENT 'FK to dim_date.date_key (snapshot calendar date)',
            stargazers_count INT COMMENT 'Total stargazers count as of snapshot date',
            forks_count INT COMMENT 'Total forks count as of snapshot date',
            open_issues_count INT COMMENT 'Total open issues count as of snapshot date',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if snapshot falls in standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, snapshot_date_key)
        COMMENT 'Periodic repository community metrics snapshot tracking popularity and adoption growth';
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_repo_daily_activity (
            repo_key BIGINT NOT NULL COMMENT 'FK to dim_repo.repo_key',
            date_key INT NOT NULL COMMENT 'FK to dim_date.date_key',
            commits_total BIGINT NOT NULL COMMENT 'Total commits authored on calendar day',
            commits_human BIGINT NOT NULL COMMENT 'Human-authored commits on calendar day (excluding bots)',
            commits_bot BIGINT NOT NULL COMMENT 'Bot-authored commits on calendar day',
            distinct_human_authors BIGINT NOT NULL COMMENT 'Count of unique human authors active on calendar day',
            issues_opened BIGINT NOT NULL COMMENT 'True issues created on calendar day',
            issues_closed BIGINT NOT NULL COMMENT 'Issues closed on calendar day',
            prs_opened BIGINT NOT NULL COMMENT 'Pull requests opened on calendar day',
            prs_merged BIGINT NOT NULL COMMENT 'Pull requests merged on calendar day',
            releases_published BIGINT NOT NULL COMMENT 'Releases published on calendar day',
            commits_human_4wk BIGINT NOT NULL COMMENT 'Rolling 28-day human commit velocity sum over dense calendar spine',
            commits_human_12wk BIGINT NOT NULL COMMENT 'Rolling 84-day human commit velocity sum over dense calendar spine',
            in_analysis_window BOOLEAN NOT NULL COMMENT 'True if calendar day is in standard analysis window',
            load_timestamp TIMESTAMP NOT NULL COMMENT 'Medallion ETL load timestamp'
        ) USING DELTA
        CLUSTER BY (repo_key, date_key)
        COMMENT 'Daily aggregate fact with dense calendar spine per repo and rolling 4-week / 12-week velocity metrics';
    """)
    print(f"Verified all 9 Gold tables in {catalog}.{gold_schema}")

    # 9. Informational Unity Catalog Primary Key & Foreign Key Constraints
    constraints = [
        f"ALTER TABLE {catalog}.{gold_schema}.dim_repo ADD CONSTRAINT pk_dim_repo PRIMARY KEY (repo_key) RELY",
        f"ALTER TABLE {catalog}.{gold_schema}.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key) RELY",
        f"ALTER TABLE {catalog}.{gold_schema}.dim_actor ADD CONSTRAINT pk_dim_actor PRIMARY KEY (actor_key) RELY",
        f"ALTER TABLE {catalog}.{gold_schema}.fact_commits ADD CONSTRAINT fk_fc_repo FOREIGN KEY (repo_key) REFERENCES {catalog}.{gold_schema}.dim_repo(repo_key)",
        f"ALTER TABLE {catalog}.{gold_schema}.fact_commits ADD CONSTRAINT fk_fc_date FOREIGN KEY (date_key) REFERENCES {catalog}.{gold_schema}.dim_date(date_key)",
        f"ALTER TABLE {catalog}.{gold_schema}.fact_commits ADD CONSTRAINT fk_fc_actor FOREIGN KEY (actor_key) REFERENCES {catalog}.{gold_schema}.dim_actor(actor_key)",
    ]
    for c_sql in constraints:
        try:
            spark.sql(c_sql)
        except Exception as c_err:
            print(f"  [Info] Unity Catalog PK/FK constraint skipped (informational): {c_err}")

    # 10. Gold Layer Audit & Ops Objects
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.audit_gold_reconciliation (
            table_name STRING NOT NULL COMMENT 'Gold target table name',
            silver_source_rows BIGINT COMMENT 'Source row count in Silver layer',
            gold_target_rows BIGINT COMMENT 'Target row count in Gold layer',
            delta_rows BIGINT COMMENT 'Difference between Gold and Silver row counts',
            status STRING NOT NULL COMMENT 'Reconciliation test status: PASS or FAIL',
            check_timestamp TIMESTAMP NOT NULL COMMENT 'Execution timestamp of data quality check'
        ) USING DELTA
        COMMENT 'Audit reconciliation gate tracking row count parity between Silver and Gold layers';
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{gold_schema}.audit_pipeline_runs AS
        SELECT
            log_id,
            CAST(start_time AS DATE) AS run_date,
            layer,
            parameter,
            status,
            ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 2) AS duration_seconds,
            rows_inserted,
            rows_updated,
            rows_aggregated,
            error_message
        FROM {catalog}.{ops_schema}.pipeline_execution_logs;
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{gold_schema}.audit_quarantine_summary AS
        SELECT
            CAST(load_timestamp AS DATE) AS quarantine_date,
            layer,
            entity,
            repo_full_name,
            rejection_reason,
            COUNT(*) AS quarantine_count
        FROM {catalog}.{ops_schema}.silver_quarantine
        GROUP BY 1, 2, 3, 4, 5;
    """)
    print(f"Verified Gold audit views and reconciliation table in {catalog}.{gold_schema}")

    # Set metrics in context
    run_ctx["rows_inserted"] = 22  # 2 ops + 5 silver + 5 bronze + 9 gold tables + 1 reconciliation table
    run_ctx["rows_updated"] = 0

print("\nSetup notebook finished successfully. All schemas and tables initialized.")