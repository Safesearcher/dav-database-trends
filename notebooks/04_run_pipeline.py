# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Pipeline Orchestrator
# MAGIC
# MAGIC Drives the full **Bronze → Silver** pipeline by calling notebooks 02 and 03
# MAGIC via `dbutils.notebook.run`.  Three clearly-labelled sections:
# MAGIC
# MAGIC | Section | Purpose |
# MAGIC |---------|---------|
# MAGIC | **A — STANDARD INCREMENTAL RUN** | Normal nightly cadence for all repos / all entities |
# MAGIC | **B — BACKFILL RUN** | Re-process an explicit time window for one repo |
# MAGIC | **C — DRIFT TEST** | Exercises schema-drift detection and Silver quarantine |
# MAGIC
# MAGIC Each section ends with a **verification cell** that prints row counts and the
# MAGIC last 10 rows of `ops.pipeline_execution_logs`.
# MAGIC
# MAGIC A dedicated **idempotency proof** cell (Section D) runs the *same* batch twice
# MAGIC and asserts that Silver and Bronze counts are unchanged after the second pass.

# COMMAND ----------
# MAGIC %md ## ── Global Widgets & Helpers ──

# COMMAND ----------
dbutils.widgets.text("catalog",        "workspace",                         "Unity Catalog Name")
dbutils.widgets.text("base_path",      "/Volumes/workspace/bronze_data/raw", "Volume Base Path")
dbutils.widgets.text("batch_id",       "",                                   "Batch ID (empty = auto)")
dbutils.widgets.text("nb_02_path",     "./02_raw_to_bronze",                 "Path to notebook 02")
dbutils.widgets.text("nb_03_path",     "./03_bronze_to_silver",              "Path to notebook 03")
dbutils.widgets.text("timeout_s",      "3600",                               "Notebook timeout (seconds)")
# Section A/D date window — override these to change which window is fetched
dbutils.widgets.text("incr_since",     "2026-09-25T00:00:00Z",               "Section A+D: incremental since")
dbutils.widgets.text("incr_until",     "",                                   "Section A+D: incremental until (empty = unbounded)")
# Section B backfill parameters
dbutils.widgets.text("backfill_repo",  "surrealdb/surrealdb",                "Section B: repo for backfill")
dbutils.widgets.text("backfill_since", "2026-04-01T00:00:00Z",               "Section B: backfill window start")
dbutils.widgets.text("backfill_until", "2026-06-30T23:59:59Z",               "Section B: backfill window end")

# COMMAND ----------
import uuid
from datetime import datetime, timezone

CATALOG        = dbutils.widgets.get("catalog")
BASE_PATH      = dbutils.widgets.get("base_path")
NB_02          = dbutils.widgets.get("nb_02_path")
NB_03          = dbutils.widgets.get("nb_03_path")
TIMEOUT        = int(dbutils.widgets.get("timeout_s"))
BATCH_ID       = dbutils.widgets.get("batch_id") or f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
# Section-scoped date params (read from widgets — override in the widget bar)
INCR_SINCE     = dbutils.widgets.get("incr_since")
INCR_UNTIL     = dbutils.widgets.get("incr_until")
BACKFILL_REPO  = dbutils.widgets.get("backfill_repo")
BACKFILL_SINCE = dbutils.widgets.get("backfill_since")
BACKFILL_UNTIL = dbutils.widgets.get("backfill_until")

ALL_REPOS = [
    "postgres/postgres",
    "cockroachdb/cockroach",
    "mongodb/mongo",
    "surrealdb/surrealdb",
    "redis/redis",
    "apache/cassandra",
    "facebookresearch/faiss",
    "qdrant/qdrant",
    "milvus-io/milvus",
    "chroma-core/chroma",
]
ALL_ENTITIES = ["repo_metadata", "commits", "issues", "pulls", "releases"]

print(f"Orchestrator ready")
print(f"  catalog   : {CATALOG}")
print(f"  base_path : {BASE_PATH}")
print(f"  batch_id  : {BATCH_ID}")
print(f"  NB_02     : {NB_02}")
print(f"  NB_03     : {NB_03}")
print(f"  timeout   : {TIMEOUT}s")

# COMMAND ----------
# Helper: run a single notebook, return its exit value, and print timing
def run_nb(path, params, timeout=TIMEOUT):
    """Thin wrapper around dbutils.notebook.run with timing and error surfacing."""
    started = datetime.now(timezone.utc)
    result = dbutils.notebook.run(path, timeout, params)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"  ✓ {path.split('/')[-1]}  → {result!r}  ({elapsed:.0f}s)")
    return result

# COMMAND ----------
# Helper: show row counts for all Silver tables + Bronze tables
def show_counts(label=""):
    print(f"\n{'='*70}")
    print(f"ROW COUNTS  {label}")
    print(f"{'='*70}")
    layers = [
        (f"{CATALOG}.bronze",  ["commits","issues","pull_requests","releases","repo_metadata"]),
        (f"{CATALOG}.silver",  ["commits","issues","pull_requests","releases","repo_metadata"]),
        (f"{CATALOG}.ops",     ["silver_quarantine"]),
    ]
    for schema, tables in layers:
        for tbl in tables:
            try:
                n = spark.table(f"{schema}.{tbl}").count()
                print(f"  {schema}.{tbl:<30} {n:>8,} rows")
            except Exception as e:
                print(f"  {schema}.{tbl:<30}  [not found: {e}]")

# COMMAND ----------
# Helper: print last 10 rows of ops.pipeline_execution_logs
def show_logs(label="", n=10):
    print(f"\n{'─'*70}")
    print(f"Last {n} rows of ops.pipeline_execution_logs  {label}")
    print(f"{'─'*70}")
    try:
        (spark.table(f"{CATALOG}.ops.pipeline_execution_logs")
              .orderBy("start_time", ascending=False)
              .limit(n)
              .show(truncate=80))
    except Exception as e:
        print(f"  [logs table not found or empty: {e}]")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pre-flight: Validate Salt Configuration
# MAGIC Ensures that PII salt is resolvable (via secrets, widget, or environment variable) before running pipeline notebooks.

# COMMAND ----------
import sys
import os

workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.common import get_salt

try:
    _configured_salt = get_salt(dbutils)
    print(f"  ✓ Salt verified and active: length = {len(_configured_salt)} characters")
except Exception as _salt_err:
    raise ValueError(f"Pre-flight failed: {_salt_err}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC ## SECTION A — STANDARD INCREMENTAL RUN
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC **Purpose:** Nightly incremental ingest — picks up everything updated since
# MAGIC `since` for all 10 repos and all 5 entities, then promotes to Silver.

# COMMAND ----------
# ── A-1 : Configure incremental window ──────────────────────────────────────
# Dates driven by widgets 'incr_since' and 'incr_until' set in the widget bar above.
A_SINCE     = INCR_SINCE          # widget: incr_since  (e.g. 2026-09-25T00:00:00Z)
A_UNTIL     = INCR_UNTIL          # widget: incr_until  (empty = no upper bound)
A_RUN_MODE  = "incremental"
A_BATCH_ID  = f"{BATCH_ID}_incr"

print(f"[Section A] STANDARD INCREMENTAL RUN")
print(f"  since     : {A_SINCE}")
print(f"  until     : {A_UNTIL or '(none)'}")
print(f"  run_mode  : {A_RUN_MODE}")
print(f"  batch_id  : {A_BATCH_ID}")

# COMMAND ----------
# ── A-2 : Bronze pass — all repos × all entities ─────────────────────────────
print(f"\n[A-2] Bronze — all repos")
for repo in ALL_REPOS:
    for entity in ALL_ENTITIES:
        run_nb(NB_02, {
            "catalog":   CATALOG,
            "repo":      repo,
            "entity":    entity,
            "base_path": BASE_PATH,
            "since":     A_SINCE,
            "until":     A_UNTIL,
            "batch_id":  A_BATCH_ID,
            "run_mode":  A_RUN_MODE,
        })

# COMMAND ----------
# ── A-3 : Silver pass — promote all repos ────────────────────────────────────
print(f"\n[A-3] Silver — all repos")
for repo in ALL_REPOS:
    run_nb(NB_03, {
        "catalog":  CATALOG,
        "repo":     repo,
        "since":    A_SINCE,
        "until":    A_UNTIL,
        "batch_id": A_BATCH_ID,
        "run_mode": A_RUN_MODE,
    })

# COMMAND ----------
# ── A-4 : Verification ───────────────────────────────────────────────────────
show_counts("after SECTION A — STANDARD INCREMENTAL RUN")
show_logs("after SECTION A", n=10)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC ## SECTION B — BACKFILL RUN
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC **Purpose:** Re-process a specific historical window for a single repo.
# MAGIC Useful after fixing a bug in the transformation logic or after receiving
# MAGIC delayed data from the API.

# COMMAND ----------
# ── B-1 : Configure backfill window ─────────────────────────────────────────
# All three values driven by widgets: backfill_repo, backfill_since, backfill_until.
B_REPO      = BACKFILL_REPO        # widget: backfill_repo
B_SINCE     = BACKFILL_SINCE       # widget: backfill_since
B_UNTIL     = BACKFILL_UNTIL       # widget: backfill_until
B_RUN_MODE  = "backfill"
B_BATCH_ID  = f"{BATCH_ID}_backfill"

print(f"[Section B] BACKFILL RUN")
print(f"  repo      : {B_REPO}")
print(f"  since     : {B_SINCE}")
print(f"  until     : {B_UNTIL}")
print(f"  run_mode  : {B_RUN_MODE}")
print(f"  batch_id  : {B_BATCH_ID}")

# COMMAND ----------
# ── B-2 : Bronze pass — one repo, all entities ───────────────────────────────
print(f"\n[B-2] Bronze backfill — {B_REPO}")
for entity in ALL_ENTITIES:
    run_nb(NB_02, {
        "catalog":   CATALOG,
        "repo":      B_REPO,
        "entity":    entity,
        "base_path": BASE_PATH,
        "since":     B_SINCE,
        "until":     B_UNTIL,
        "batch_id":  B_BATCH_ID,
        "run_mode":  B_RUN_MODE,
    })

# COMMAND ----------
# ── B-3 : Silver pass — one repo ─────────────────────────────────────────────
print(f"\n[B-3] Silver backfill — {B_REPO}")
run_nb(NB_03, {
    "catalog":  CATALOG,
    "repo":     B_REPO,
    "since":    B_SINCE,
    "until":    B_UNTIL,
    "batch_id": B_BATCH_ID,
    "run_mode": B_RUN_MODE,
})

# COMMAND ----------
# ── B-4 : Verification ───────────────────────────────────────────────────────
show_counts(f"after SECTION B — BACKFILL {B_REPO} [{B_SINCE} → {B_UNTIL}]")
show_logs("after SECTION B", n=10)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC ## SECTION C — DRIFT TEST
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC **Purpose:** Validate that the pipeline correctly handles schema drift:
# MAGIC - **Anomaly A:** New unexpected top-level field `new_field_test` on every record.
# MAGIC   → Bronze absorbs it via schema evolution (evolve_table_schema), adding a new STRING column.
# MAGIC - **Anomaly B:** `comments` field set to string `"many"` on 3 issue records.
# MAGIC   → Bronze isolates and quarantines those 3 rows in `ops.silver_quarantine` with layer='bronze'.
# MAGIC - **Anomaly C:** `created_at = "not-a-date"` on 1 true issue record (index 4).
# MAGIC   → Silver quarantines that 1 row in `ops.silver_quarantine` with layer='silver'.
# MAGIC
# MAGIC **Pre-requisite:** Run `python scripts/make_drift_sample.py` locally and upload
# MAGIC `samples/drift/` to the Volume at `{base_path}/drift/driftlab_surrealdb/`.

# COMMAND ----------
# ── C-1 : Configure drift test ───────────────────────────────────────────────
C_REPO        = "driftlab/surrealdb"
C_SINCE       = "2026-01-01T00:00:00Z"   # wide window — catches all drift records
C_UNTIL       = ""
C_RUN_MODE    = "full"
C_BATCH_ID    = f"{BATCH_ID}_drift"
C_DRIFT_PATH  = f"{BASE_PATH}/drift"     # Volume path where drift samples are staged

print(f"[Section C] DRIFT TEST")
print(f"  repo              : {C_REPO}")
print(f"  drift_path        : {C_DRIFT_PATH}")
print(f"  batch_id          : {C_BATCH_ID}")
print()
print("  Pre-requisite check — drift files must exist on the Volume:")
for entity in ("issues", "commits"):
    path = f"{C_DRIFT_PATH}/driftlab_surrealdb/{entity}.json"
    try:
        dbutils.fs.ls(path)
        print(f"    ✓ {path}")
    except Exception:
        print(f"    ✗ MISSING: {path}  ← run make_drift_sample.py then upload")

# COMMAND ----------
# ── C-2 : Bronze pass using drift data ──────────────────────────────────────
# Point base_path to the drift folder so notebook 02 reads the injected files
print(f"\n[C-2] Bronze (drift) — issues & commits for {C_REPO}")
for entity in ("issues", "commits"):
    run_nb(NB_02, {
        "catalog":   CATALOG,
        "repo":      C_REPO,
        "entity":    entity,
        "base_path": C_DRIFT_PATH,          # ← drift Volume path
        "since":     C_SINCE,
        "until":     C_UNTIL,
        "batch_id":  C_BATCH_ID,
        "run_mode":  C_RUN_MODE,
    })

# COMMAND ----------
# ── C-3 : Silver pass — expect quarantine rows ───────────────────────────────
print(f"\n[C-3] Silver (drift) — {C_REPO}")
run_nb(NB_03, {
    "catalog":  CATALOG,
    "repo":     C_REPO,
    "since":    C_SINCE,
    "until":    C_UNTIL,
    "batch_id": C_BATCH_ID,
    "run_mode": C_RUN_MODE,
})

# COMMAND ----------
# ── C-4 : Verification ───────────────────────────────────────────────────────
show_counts("after SECTION C — DRIFT TEST")
show_logs("after SECTION C", n=10)

# ── C-4b : Drift-specific assertions ────────────────────────────────────────
print("\n[C-4b] Drift-specific assertions")

# 1. Bronze table must have evolved to include new_field_test
bronze_issues_cols = [c.name for c in spark.table(f"{CATALOG}.bronze.issues").schema]
bronze_commits_cols = [c.name for c in spark.table(f"{CATALOG}.bronze.commits").schema]
assert "new_field_test" in bronze_issues_cols, \
    "FAIL: 'new_field_test' not found in bronze.issues — schema evolution did not add the column"
assert "new_field_test" in bronze_commits_cols, \
    "FAIL: 'new_field_test' not found in bronze.commits — schema evolution did not add the column"
print(f"  ✓ bronze.issues and bronze.commits have 'new_field_test' column  (schema evolution confirmed)")

# 2. Quarantine rows for this batch with layer='bronze' and entity='issues' == 3
bronze_q = (
    spark.table(f"{CATALOG}.ops.silver_quarantine")
         .filter(f"batch_id = '{C_BATCH_ID}' AND layer = 'bronze' AND entity = 'issues'")
)
bronze_q_count = bronze_q.count()
assert bronze_q_count == 3, \
    f"FAIL: Expected == 3 bronze issues quarantine rows for batch {C_BATCH_ID}, got {bronze_q_count}"
print(f"  ✓ ops.silver_quarantine has {bronze_q_count} row(s) with layer='bronze' and entity='issues' (== 3 expected)")

# 3. Quarantine rows for this batch with layer='silver' and entity='issues' >= 1
silver_q = (
    spark.table(f"{CATALOG}.ops.silver_quarantine")
         .filter(f"batch_id = '{C_BATCH_ID}' AND layer = 'silver' AND entity = 'issues'")
)
silver_q_count = silver_q.count()
assert silver_q_count >= 1, \
    f"FAIL: Expected >= 1 silver issues quarantine rows for batch {C_BATCH_ID}, got {silver_q_count}"
print(f"  ✓ ops.silver_quarantine has {silver_q_count} row(s) with layer='silver' and entity='issues' (>= 1 expected)")

# 4. Log entry must record drift detection in parameter
drift_logs = (
    spark.table(f"{CATALOG}.ops.pipeline_execution_logs")
         .filter(f"batch_id = '{C_BATCH_ID}' AND layer = 'BRONZE'")
         .filter("parameter LIKE '%drift_keys=%' AND parameter LIKE '%new_field_test%'")
)
assert drift_logs.count() > 0, \
    f"FAIL: ops.pipeline_execution_logs row for batch {C_BATCH_ID} does not contain drift_keys with 'new_field_test'"
print("  ✓ ops.pipeline_execution_logs has Bronze log entry with drift_keys containing 'new_field_test'")

print("\n  All drift assertions passed.")
silver_q.select(
    "batch_id", "entity", "rejection_reason", "raw_payload"
).show(10, truncate=100)

# COMMAND ----------
# ── C-5 : Cleanup drift data from bronze/silver tables ───────────────────────
print(f"\n[C-5] Cleanup {C_REPO} rows from Bronze and Silver")
for tbl in ["issues", "commits"]:
    try:
        spark.sql(f"DELETE FROM {CATALOG}.bronze.{tbl} WHERE repo_full_name = '{C_REPO}'")
    except Exception as e:
        print(f"  Could not delete from bronze.{tbl}: {e}")

for tbl in ["issues", "commits", "pull_requests"]:
    try:
        spark.sql(f"DELETE FROM {CATALOG}.silver.{tbl} WHERE repo_full_name = '{C_REPO}'")
    except Exception as e:
        print(f"  Could not delete from silver.{tbl}: {e}")

print(f"  ✓ Cleaned up any rows for {C_REPO} from bronze and silver (quarantine and execution logs retained)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC ## SECTION D — IDEMPOTENCY PROOF
# MAGIC ## ══════════════════════════════════════════════════════
# MAGIC **Purpose:** Run the **same batch** twice (same `batch_id`) and assert that
# MAGIC row counts in Bronze, Silver, and the quarantine table are **identical** after
# MAGIC both passes.  This proves the `MERGE INTO` logic is truly idempotent.

# COMMAND ----------
# ── D-1 : Configure idempotency test ────────────────────────────────────────
# Reuses the same incremental window so the proof runs on real fetched data.
# D_REPO is intentionally hardcoded to a single repo to keep the proof fast.
D_REPO      = "redis/redis"        # fixed: single repo for the proof
D_SINCE     = INCR_SINCE           # widget: incr_since (same window as Section A)
D_UNTIL     = INCR_UNTIL           # widget: incr_until
D_RUN_MODE  = "incremental"
D_BATCH_ID  = f"{BATCH_ID}_idempotency_proof"   # fixed ID — MUST be the same on both runs

def _bronze_silver_counts():
    """Collect current row counts from all Bronze + Silver tables and quarantine."""
    counts = {}
    for schema, tables in [
        (f"{CATALOG}.bronze", ["commits","issues","pull_requests","releases","repo_metadata"]),
        (f"{CATALOG}.silver", ["commits","issues","pull_requests","releases","repo_metadata"]),
        (f"{CATALOG}.ops",    ["silver_quarantine"]),
    ]:
        for tbl in tables:
            key = f"{schema}.{tbl}"
            try:
                counts[key] = spark.table(key).count()
            except Exception:
                counts[key] = None   # table doesn't exist yet
    return counts

print(f"[Section D] IDEMPOTENCY PROOF")
print(f"  repo      : {D_REPO}")
print(f"  since     : {D_SINCE}")
print(f"  batch_id  : {D_BATCH_ID}  (same for both runs)")

# COMMAND ----------
# ── D-2 : FIRST pass ─────────────────────────────────────────────────────────
print("\n[D-2] First pass — Bronze")
for entity in ALL_ENTITIES:
    run_nb(NB_02, {
        "catalog":   CATALOG,
        "repo":      D_REPO,
        "entity":    entity,
        "base_path": BASE_PATH,
        "since":     D_SINCE,
        "until":     D_UNTIL,
        "batch_id":  D_BATCH_ID,
        "run_mode":  D_RUN_MODE,
    })

print("\n[D-2] First pass — Silver")
run_nb(NB_03, {
    "catalog":  CATALOG,
    "repo":     D_REPO,
    "since":    D_SINCE,
    "until":    D_UNTIL,
    "batch_id": D_BATCH_ID,
    "run_mode": D_RUN_MODE,
})

counts_after_first = _bronze_silver_counts()
print("\nCounts after first pass:")
for k, v in counts_after_first.items():
    print(f"  {k:<50} {v}")

# COMMAND ----------
# ── D-3 : SECOND pass (identical params) ────────────────────────────────────
print("\n[D-3] Second pass — Bronze  (same batch_id, same data)")
for entity in ALL_ENTITIES:
    run_nb(NB_02, {
        "catalog":   CATALOG,
        "repo":      D_REPO,
        "entity":    entity,
        "base_path": BASE_PATH,
        "since":     D_SINCE,
        "until":     D_UNTIL,
        "batch_id":  D_BATCH_ID,
        "run_mode":  D_RUN_MODE,
    })

print("\n[D-3] Second pass — Silver  (same batch_id, same data)")
run_nb(NB_03, {
    "catalog":  CATALOG,
    "repo":     D_REPO,
    "since":    D_SINCE,
    "until":    D_UNTIL,
    "batch_id": D_BATCH_ID,
    "run_mode": D_RUN_MODE,
})

counts_after_second = _bronze_silver_counts()
print("\nCounts after second pass:")
for k, v in counts_after_second.items():
    print(f"  {k:<50} {v}")

# COMMAND ----------
# ── D-4 : Assert idempotency ─────────────────────────────────────────────────
print("\n[D-4] Idempotency assertion")
all_pass = True
for key in counts_after_first:
    v1 = counts_after_first[key]
    v2 = counts_after_second[key]
    if v1 is None and v2 is None:
        continue
    if v1 != v2:
        print(f"  ✗ FAIL: {key}  first={v1}  second={v2}  DELTA={v2 - (v1 or 0)}")
        all_pass = False
    else:
        print(f"  ✓ PASS: {key}  count={v2} (unchanged)")

if all_pass:
    print("\n  ✓ IDEMPOTENCY CONFIRMED — all row counts are identical after second pass.")
    print("    The MERGE INTO strategy correctly upserts without duplicates.\n")
else:
    raise AssertionError(
        "Idempotency check FAILED. One or more table row counts changed on the second pass. "
        "Review the MERGE keys in notebooks/02_raw_to_bronze.py and 03_bronze_to_silver.py."
    )

show_counts("after SECTION D — IDEMPOTENCY PROOF")
show_logs("after SECTION D", n=10)
