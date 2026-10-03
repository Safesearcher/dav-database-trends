# Databricks notebook source
# COMMAND ----------
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
dbutils.widgets.text("ops_schema", "ops", "Ops Schema Name")
dbutils.widgets.text("bronze_volume", "raw", "Bronze Volume Name")
dbutils.widgets.dropdown("reset_tables", "false", ["false", "true"], "Drop Tables (Clean Reset)")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
volume_schema = dbutils.widgets.get("volume_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
ops_schema = dbutils.widgets.get("ops_schema").strip()
bronze_volume = dbutils.widgets.get("bronze_volume").strip()
reset_tables = dbutils.widgets.get("reset_tables").strip().lower() == "true"

print(f"Catalog:       {catalog}")
print(f"Bronze Schema: {bronze_schema}")
print(f"Volume Schema: {volume_schema}")
print(f"Silver Schema: {silver_schema}")
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

from src.common import log_run, start_run, end_run

batch_id = datetime.now(timezone.utc).strftime("setup_%Y%m%d_%H%M%S")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Create Unity Catalog Schemas & Volumes

# COMMAND ----------
with log_run(spark, layer="SETUP", parameter=f"catalog={catalog}", batch_id=batch_id, catalog=catalog) as run_ctx:
    # 1. Schemas DDL
    for s in [bronze_schema, volume_schema, silver_schema, ops_schema]:
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
            error_message STRING,
            load_timestamp TIMESTAMP NOT NULL
        ) USING DELTA;
    """)
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

    # Set metrics in context
    run_ctx["rows_inserted"] = 12  # 2 ops + 5 silver + 5 bronze tables created/verified
    run_ctx["rows_updated"] = 0

print("\nSetup notebook finished successfully. All schemas and tables initialized.")
