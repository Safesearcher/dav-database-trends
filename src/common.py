"""
Reusable pipeline functions for Databricks Medallion architecture (Bronze -> Silver).
Serverless-compatible (no RDDs, no sparkContext, no .cache()/.persist(), no custom Spark configs).

Includes:
- get_params: Parameter widget parser with safe defaults and run_mode validation
- start_run / end_run / log_run: Operational audit logging context manager to ops.pipeline_execution_logs
- add_metadata: Injects load_timestamp, _source_file, and batch_id
- merge_delta: Idempotent Delta MERGE INTO with operationMetrics parsing
- get_salt: Resolves salt from dbutils.secrets (scope "dbtrends", key "salt") with widget fallback
- hash_email: Salted SHA-256 PII masking
- bot_flag: Bot classification rules from progress.md section 4.3
"""

import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StringType,
        TimestampType,
        LongType,
    )
except ImportError:
    # Stubs for local testing outside Databricks
    DataFrame = object
    SparkSession = object

    class _FStub:
        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    F = _FStub()
    StringType = object
    TimestampType = object
    LongType = object

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = object


# Known CI/CD bot accounts from progress.md section 4.3
KNOWN_BOT_ACCOUNTS = [
    "bors",
    "cockroach-teamcity",
    "dependabot",
    "mongodb-evergreen",
    "renovate",
    "github-actions",
]

VALID_RUN_MODES = {"incremental", "backfill", "full"}
EMPTY_ALLOWED = {"since", "until"}

DATE_COLUMN_MAP = {
    "commits": "commit.committer.date",
    "issues": "updated_at",
    "pulls": "updated_at",
    "releases": "published_at",
    "repo_metadata": "updated_at",
}


# -----------------------------------------------------------------------------
# 1. Parameter Widget Parser
# -----------------------------------------------------------------------------

def get_params(dbutils=None) -> dict:
    """
    Reads notebook widgets with safe defaults.
    Default base_path: /Volumes/workspace/bronze_data/raw.
    Empty values are preserved for since/until to allow unbounded windows.
    """
    default_batch = datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")
    defaults = {
        "catalog": "workspace",
        "repo": "surrealdb/surrealdb",
        "entity": "commits",
        "base_path": "/Volumes/workspace/bronze_data/raw",
        "since": "",
        "until": "",
        "batch_id": default_batch,
        "run_mode": "full",
        "salt": "",
    }

    if dbutils is None:
        return defaults

    params = {}
    for key, default_val in defaults.items():
        try:
            val = dbutils.widgets.get(key)            # widget already defined by the notebook -> never re-declare
        except Exception:
            try:
                if key == "run_mode":
                    dbutils.widgets.dropdown(key, default_val, ["full", "incremental", "backfill"], key)
                else:
                    dbutils.widgets.text(key, str(default_val), key)
                val = dbutils.widgets.get(key)
            except Exception:
                val = None
        if val is None:
            params[key] = default_val
        elif key in EMPTY_ALLOWED:
            params[key] = val.strip()                 # keep "" as ""
        else:
            params[key] = val.strip() or default_val  # blank catalog/repo/batch_id -> default

    if params["run_mode"].lower() not in VALID_RUN_MODES:
        params["run_mode"] = "full"
    else:
        params["run_mode"] = params["run_mode"].lower()

    if params["batch_id"] in (None, ""):
        params["batch_id"] = default_batch

    return params


# -----------------------------------------------------------------------------
# 2. Salt & PII Hashing
# -----------------------------------------------------------------------------

def get_salt(dbutils=None, scope: str = "dbtrends", key: str = "salt", widget_name: str = "salt") -> str:
    """Resolve salt from dbutils secrets, widget fallback, then environment variable."""
    if dbutils is not None:
        try:
            val = dbutils.secrets.get(scope=scope, key=key)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

        try:
            val = dbutils.widgets.get(widget_name)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

    env_val = os.environ.get("SALT")
    if env_val and str(env_val).strip():
        return str(env_val).strip()

    raise ValueError(
        "No salt configured. Set dbutils.secrets.get('dbtrends', 'salt'), add widget 'salt', or export SALT."
    )


def hash_email(col, salt: str):
    """
    Computes sha256(lower(trim(email)) + salt).
    Returns null if email column is null or blank.
    """
    target_col = F.col(col) if isinstance(col, str) else col
    return F.when(
        target_col.isNotNull() & (F.length(F.trim(target_col)) > 0),
        F.sha2(F.concat(F.lower(F.trim(target_col)), F.lit(salt)), 256)
    ).otherwise(F.lit(None).cast(StringType()))


# -----------------------------------------------------------------------------
# 3. Bot Classification
# -----------------------------------------------------------------------------

def bot_flag(login_col, type_col=None):
    """
    Flags bot accounts using:
    1. login contains '[bot]'
    2. user.type = 'Bot'
    3. known accounts in KNOWN_BOT_ACCOUNTS
    4. login ends with '-bot' or '_bot' or 'bot' as final token
    """
    login = F.lower(F.coalesce(F.col(login_col) if isinstance(login_col, str) else login_col, F.lit("")))
    cond = login.like("%[bot]%") | login.isin([b.lower() for b in KNOWN_BOT_ACCOUNTS])
    cond = cond | login.rlike(r"(?i)(^|[-_])bot$")

    if type_col is not None:
        t_col = F.lower(F.coalesce(F.col(type_col) if isinstance(type_col, str) else type_col, F.lit("")))
        cond = cond | (t_col == "bot")

    return F.when(cond, F.lit(True)).otherwise(F.lit(False))


# -----------------------------------------------------------------------------
# 4. Lineage & Metadata Enrichment
# -----------------------------------------------------------------------------

def add_metadata(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Adds mandatory lineage columns to DataFrame:
    - load_timestamp: current_timestamp()
    - _source_file: input_file_name() fallback to provided source_file parameter
    - batch_id: execution run identifier
    """
    file_col = F.when(
        F.input_file_name() != "", F.input_file_name()
    ).otherwise(F.lit(source_file))

    return (
        df
        .withColumn("_source_file", file_col)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_timestamp", F.current_timestamp())
    )


# -----------------------------------------------------------------------------
# 5. Delta Idempotency MERGE INTO Helper
# -----------------------------------------------------------------------------

def merge_delta(spark: SparkSession, df: DataFrame, target_table: str, keys: list, update_all: bool = True) -> tuple:
    """
    Creates target Delta table if it doesn't exist; otherwise executes a MERGE INTO
    using composite keys. Returns (rows_inserted, rows_updated) parsed from operationMetrics.
    """
    if not spark.catalog.tableExists(target_table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        inserted = df.count()
        return (inserted, 0)

    delta_target = DeltaTable.forName(spark, target_table)
    match_expr = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    merge_builder = delta_target.alias("target").merge(
        df.alias("source"),
        match_expr
    )

    if update_all:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder = merge_builder.whenNotMatchedInsertAll()
    merge_builder.execute()

    # Extract metrics from Delta commit history
    history = delta_target.history(1).collect()
    num_inserted = 0
    num_updated = 0
    if history and "operationMetrics" in history[0]:
        metrics = history[0]["operationMetrics"]
        num_inserted = int(metrics.get("numTargetRowsInserted", 0))
        num_updated = int(metrics.get("numTargetRowsUpdated", 0))
    else:
        num_inserted = df.count()

    return (num_inserted, num_updated)


# -----------------------------------------------------------------------------
# 6. Operational Execution Logging (ops.pipeline_execution_logs)
# -----------------------------------------------------------------------------

def start_run(spark: SparkSession, layer: str, parameter: str, batch_id: str, catalog: str = "workspace") -> dict:
    """Initializes start timestamp and unique run tracking context."""
    return {
        "log_id": str(uuid.uuid4()),
        "layer": layer,
        "parameter": parameter,
        "batch_id": batch_id,
        "catalog": catalog,
        "start_time": datetime.now(timezone.utc),
        "rows_inserted": 0,
        "rows_updated": 0,
    }


def end_run(
    spark: SparkSession,
    log_id: str,
    layer: str,
    parameter: str,
    batch_id: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    error_message: str = None,
    catalog: str = "workspace",
):
    """Writes execution audit metrics into Delta table <catalog>.ops.pipeline_execution_logs."""
    from src.schemas import PIPELINE_EXECUTION_LOGS_SCHEMA

    table_name = f"{catalog}.ops.pipeline_execution_logs"
    now_ts = datetime.now(timezone.utc)

    log_row = [(
        log_id,
        layer,
        parameter if parameter is not None else None,
        batch_id if batch_id is not None else None,
        start_time,
        end_time,
        status,
        int(rows_inserted) if rows_inserted is not None else 0,
        int(rows_updated) if rows_updated is not None else 0,
        str(error_message) if error_message is not None else None,
        now_ts,
    )]

    log_df = spark.createDataFrame(log_row, schema=PIPELINE_EXECUTION_LOGS_SCHEMA)

    try:
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("target").merge(
            log_df.alias("source"),
            "target.log_id = source.log_id"
        ).whenNotMatchedInsertAll().execute()
    except Exception:
        log_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)


@contextmanager
def log_run(spark: SparkSession, layer: str, parameter: str, batch_id: str, catalog: str = "workspace"):
    """Context manager that writes a log row to ops.pipeline_execution_logs."""
    run_ctx = start_run(spark, layer, parameter, batch_id, catalog)
    try:
        yield run_ctx
        end_time = datetime.now(timezone.utc)
        run_ctx["status"] = "SUCCESS"
        end_run(
            spark=spark,
            log_id=run_ctx["log_id"],
            layer=layer,
            parameter=run_ctx.get("parameter", parameter),
            batch_id=batch_id,
            start_time=run_ctx["start_time"],
            end_time=end_time,
            status=run_ctx.get("status", "SUCCESS"),
            rows_inserted=run_ctx.get("rows_inserted", 0),
            rows_updated=run_ctx.get("rows_updated", 0),
            error_message=None,
            catalog=catalog,
        )
    except Exception as exc:
        end_time = datetime.now(timezone.utc)
        run_ctx["status"] = "FAILURE"
        try:
            end_run(
                spark=spark,
                log_id=run_ctx["log_id"],
                layer=layer,
                parameter=run_ctx.get("parameter", parameter),
                batch_id=batch_id,
                start_time=run_ctx["start_time"],
                end_time=end_time,
                status=run_ctx.get("status", "FAILURE"),
                rows_inserted=run_ctx.get("rows_inserted", 0),
                rows_updated=run_ctx.get("rows_updated", 0),
                error_message=str(exc),
                catalog=catalog,
            )
        except Exception as log_err:
            print(f"[Warning] Failed to log run failure to Delta: {log_err}")
        raise exc


# -----------------------------------------------------------------------------
# 7. Quarantine Helper
# -----------------------------------------------------------------------------

def quarantine_records(
    spark: SparkSession,
    catalog: str,
    df_rejected: DataFrame,
    layer: str,
    entity: str,
    rejection_reason: str,
    batch_id: str = "unknown"
) -> int:
    """Idempotent per (batch_id, payload, reason) quarantine sink for rejected rows."""
    try:
        if df_rejected.limit(1).count() == 0:
            return 0
    except Exception:
        return 0

    table_name = f"{catalog}.ops.silver_quarantine"
    cols = df_rejected.columns
    volatile = {"load_timestamp", "batch_id", "rejection_reason", "_raw_record"}
    payload_cols = [c for c in cols if c not in volatile]
    payload = F.coalesce(
        F.col("_raw_record") if "_raw_record" in cols else F.lit(None),
        F.to_json(F.struct(*[F.col(c) for c in payload_cols]))
    )
    reason_expr = F.coalesce(
        F.col("rejection_reason") if "rejection_reason" in cols else F.lit(None),
        F.lit(rejection_reason)
    )
    repo_expr = F.col("repo_full_name") if "repo_full_name" in cols else F.lit("UNKNOWN")

    quarantine_df = (
        df_rejected.select(
            repo_expr.alias("repo_full_name"),
            reason_expr.alias("rejection_reason"),
            payload.alias("raw_payload")
        )
        .withColumn("layer", F.lit(layer))
        .withColumn("entity", F.lit(entity))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn(
            "quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("layer"),
                    F.col("entity"),
                    F.col("repo_full_name"),
                    F.col("batch_id"),
                    F.col("rejection_reason"),
                    F.col("raw_payload"),
                ),
                256,
            )
        )
        .withColumn("load_timestamp", F.current_timestamp())
        .dropDuplicates(["quarantine_id"]) 
        .select("quarantine_id", "layer", "entity", "repo_full_name", "rejection_reason", "raw_payload", "batch_id", "load_timestamp")
    )

    count = quarantine_df.count()
    if not spark.catalog.tableExists(table_name):
        quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    else:
        try:
            delta_table = DeltaTable.forName(spark, table_name)
            delta_table.alias("target").merge(
                quarantine_df.alias("source"),
                "target.quarantine_id = source.quarantine_id"
            ).whenNotMatchedInsertAll().execute()
        except Exception:
            quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    return count


# Aliases for backward compatibility
compute_salted_hash = hash_email
build_is_bot_column = bot_flag
merge_delta_table = merge_delta
log_pipeline_execution = end_run


def sanitize_commit_message(message_col: str):
    """Extracts the first line of a commit message, scrubs trailers, and masks e-mail addresses."""
    target_col = F.col(message_col) if isinstance(message_col, str) else message_col
    first_line = F.split(target_col, r"\r?\n").getItem(0)
    clean_line = F.trim(F.regexp_replace(first_line, r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", "[EMAIL]"))
    sanitized = F.when(
        clean_line.rlike(r"(?i)^(signed-off-by|co-authored-by):"),
        F.lit("[TRUNCATED_TRAILER]")
    ).otherwise(clean_line)
    return sanitized


_ISO_FORMATS = ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"]

def parse_iso_timestamp(col_name: str):
    """ANSI-safe: never raises, returns NULL for unparseable input.
    col_name is a SQL column reference string; dotted nested paths are fine."""
    if not isinstance(col_name, str):
        raise TypeError("parse_iso_timestamp expects a column-name string")
    exprs = [F.expr(f'try_to_timestamp({col_name}, "{fmt}")') for fmt in _ISO_FORMATS]
    exprs.append(F.expr(f"try_to_timestamp({col_name})"))
    return F.coalesce(*exprs)
