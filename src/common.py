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
        StructType,
        StructField,
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
    StructType = object
    StructField = object
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

EMAIL_REGEX = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"


def scrub_emails(col):
    """Masks email addresses in a string Column or string expression with [EMAIL]."""
    target_col = F.col(col) if isinstance(col, str) else col
    return F.regexp_replace(target_col, EMAIL_REGEX, "[EMAIL]")


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

def align_to_target(df: DataFrame, target_schema: StructType) -> tuple:
    """
    Pure function aligning source DataFrame columns to target_schema:
    - columns_to_add: list of (col_name, type_string) for source-only columns
    - target-only columns: added to df as F.lit(None).cast(<target type>)
    - result DataFrame selected in target's column order
    """
    target_fields = {f.name: f for f in target_schema.fields}
    target_names = [f.name for f in target_schema.fields]
    source_fields = {f.name: f for f in df.schema.fields}

    columns_to_add = []
    for f in df.schema.fields:
        if f.name not in target_fields:
            type_str = f.dataType.simpleString() if hasattr(f.dataType, "simpleString") else str(f.dataType)
            columns_to_add.append((f.name, type_str))

    aligned_df = df
    for f in target_schema.fields:
        if f.name not in source_fields:
            aligned_df = aligned_df.withColumn(f.name, F.lit(None).cast(f.dataType))

    aligned_df = aligned_df.select(*target_names)
    return aligned_df, columns_to_add


def merge_delta(spark: SparkSession, df: DataFrame, target_table: str, keys: list, update_all: bool = True) -> tuple:
    """
    Creates target Delta table if it doesn't exist; otherwise aligns schema and executes a MERGE INTO
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

    target_schema = spark.table(target_table).schema
    aligned_df, columns_to_add = align_to_target(df, target_schema)

    if columns_to_add:
        for col_name, type_str in columns_to_add:
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (`{col_name}` {type_str})")
        target_schema = spark.table(target_table).schema
        aligned_df, _ = align_to_target(df, target_schema)

    delta_target = DeltaTable.forName(spark, target_table)
    match_expr = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    merge_builder = delta_target.alias("target").merge(
        aligned_df.alias("source"),
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
        num_inserted = aligned_df.count()

    return (num_inserted, num_updated)


def read_raw_records(spark, file_path: str):
    """One row per JSON record (as a JSON string) in column _raw_record."""
    txt = spark.read.text(file_path, wholetext=True)
    arr = F.when(F.ltrim("value").startswith("["), F.col("value")) \
           .otherwise(F.concat(F.lit("["), F.col("value"), F.lit("]")))
    return txt.select(F.explode(F.from_json(arr, "array<string>")).alias("_raw_record"))


def parse_records(raw_df, schema, extra_string_cols=()):
    """Parse each record independently; schema must contain _corrupt_record."""
    from pyspark.sql.types import StructType, StructField, StringType
    have = set(schema.fieldNames())
    read_schema = StructType(list(schema.fields) +
        [StructField(c, StringType(), True) for c in extra_string_cols if c not in have])
    parsed = raw_df.select(
        "_raw_record",
        F.from_json("_raw_record", read_schema,
                    {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}).alias("p"))
    return parsed.select("_raw_record", "p.*")


def discover_drift_keys(raw_df, schema) -> list:
    """Top-level keys present in ANY record but absent from the explicit schema."""
    expected = {f.name for f in schema.fields if f.name != "_corrupt_record"}
    keys = raw_df.select(F.explode(F.expr("json_object_keys(_raw_record)")).alias("k")).distinct()
    return sorted(r["k"] for r in keys.collect() if r["k"] not in expected)


def evolve_table_schema(spark, target_table: str, new_cols) -> list:
    if not new_cols or not spark.catalog.tableExists(target_table):
        return []
    existing = {c.lower() for c in spark.table(target_table).columns}
    added = []
    for c in new_cols:
        if c.lower() not in existing:
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (`{c}` STRING)")
            added.append(c)
    return added


# -----------------------------------------------------------------------------
# 6. Operational Execution Logging (ops.pipeline_execution_logs)
# -----------------------------------------------------------------------------

VALID_LOG_STATUSES = {"SUCCESS", "FAILURE"}


def build_log_row(
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
    load_timestamp: datetime = None,
) -> tuple:
    """Builds a validated log row tuple matching PIPELINE_EXECUTION_LOGS_SCHEMA.

    Status must strictly be 'SUCCESS' or 'FAILURE'.
    """
    if status not in VALID_LOG_STATUSES:
        raise ValueError(
            f"Invalid log status '{status}'. Must be one of {sorted(VALID_LOG_STATUSES)}."
        )
    if load_timestamp is None:
        load_timestamp = datetime.now(timezone.utc)
    return (
        str(log_id),
        str(layer),
        str(parameter) if parameter is not None else None,
        str(batch_id) if batch_id is not None else None,
        start_time,
        end_time,
        status,
        int(rows_inserted) if rows_inserted is not None else 0,
        int(rows_updated) if rows_updated is not None else 0,
        str(error_message) if error_message is not None else None,
        load_timestamp,
    )


def start_run(spark: SparkSession, layer: str, parameter: str, batch_id: str, catalog: str = "workspace") -> dict:
    """Initializes start timestamp and unique run tracking context."""
    return {
        "log_id": str(uuid.uuid4()),
        "layer": layer,
        "parameter": parameter,
        "batch_id": batch_id,
        "catalog": catalog,
        "start_time": datetime.now(timezone.utc),
        "status": "SUCCESS",
        "rows_inserted": 0,
        "rows_updated": 0,
        "message": None,
        "error_message": None,
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
    row = build_log_row(
        log_id=log_id,
        layer=layer,
        parameter=parameter,
        batch_id=batch_id,
        start_time=start_time,
        end_time=end_time,
        status=status,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        error_message=error_message,
    )

    log_df = spark.createDataFrame([row], schema=PIPELINE_EXECUTION_LOGS_SCHEMA)

    try:
        merge_delta(spark=spark, df=log_df, target_table=table_name, keys=["log_id"], update_all=False)
    except Exception:
        log_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)


@contextmanager
def log_run(spark: SparkSession, layer: str, parameter: str, batch_id: str, catalog: str = "workspace"):
    """Context manager that writes a log row to ops.pipeline_execution_logs."""
    run_ctx = start_run(spark, layer, parameter, batch_id, catalog)
    try:
        yield run_ctx
        end_time = datetime.now(timezone.utc)
        status = run_ctx.get("status", "SUCCESS")
        if status not in VALID_LOG_STATUSES:
            status = "SUCCESS" if status not in ("FAILURE", "FAILED", "ERROR") else "FAILURE"
        # Preserve run_ctx['message'] or run_ctx['error_message'] even on SUCCESS
        msg = run_ctx.get("error_message") or run_ctx.get("message")
        end_run(
            spark=spark,
            log_id=run_ctx["log_id"],
            layer=layer,
            parameter=run_ctx.get("parameter", parameter),
            batch_id=batch_id,
            start_time=run_ctx["start_time"],
            end_time=end_time,
            status=status,
            rows_inserted=run_ctx.get("rows_inserted", 0),
            rows_updated=run_ctx.get("rows_updated", 0),
            error_message=msg,
            catalog=catalog,
        )
    except Exception as exc:
        end_time = datetime.now(timezone.utc)
        run_ctx["status"] = "FAILURE"
        msg = str(exc)
        if run_ctx.get("message"):
            msg = f"{run_ctx.get('message')}: {msg}"
        try:
            end_run(
                spark=spark,
                log_id=run_ctx["log_id"],
                layer=layer,
                parameter=run_ctx.get("parameter", parameter),
                batch_id=batch_id,
                start_time=run_ctx["start_time"],
                end_time=end_time,
                status="FAILURE",
                rows_inserted=run_ctx.get("rows_inserted", 0),
                rows_updated=run_ctx.get("rows_updated", 0),
                error_message=msg,
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
    try:
        merge_delta(spark=spark, df=quarantine_df, target_table=table_name, keys=["quarantine_id"], update_all=False)
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
    clean_line = F.trim(scrub_emails(first_line))
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
