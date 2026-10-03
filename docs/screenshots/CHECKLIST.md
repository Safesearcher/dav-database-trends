# docs/screenshots/CHECKLIST.md
# Databricks Screenshots Checklist — DAV Database Trends

This file describes **exactly** what to do and capture for each screenshot.
Save each file as `docs/screenshots/<filename>.png` and tick the box when done.
Screenshots are used for the project submission and README illustration.

> **Tip:** Maximize your browser window and set Databricks to the **Light** theme
> for cleaner screenshots. Zoom the browser to 80% to fit more content.

---

## Legend

- **⭐ Must** — Required for project submission. Do not skip.
- **★ Nice** — Strengthens the report but not strictly required.

---

## ⭐ Must-Have Screenshots

---

### 01 — Unity Catalog Schemas Overview
**File:** `01_unity_catalog_schemas.png`  
**Where:** Databricks → left sidebar → **Catalog** icon → expand your catalog

**Steps:**
1. Open the Catalog Explorer
2. Expand your catalog (e.g. `workspace`)
3. Confirm `bronze`, `silver`, and `ops` schemas are visible
4. Expand `bronze` so its 5 tables are visible in the tree

**Capture:** The full Catalog Explorer panel showing the three schemas and their child tables.

---

### 02 — Volumes Directory Structure
**File:** `02_volumes_structure.png`  
**Where:** Catalog Explorer → Volumes → your volume → `raw/`

**Steps:**
1. In Catalog Explorer, click **Volumes** under your catalog
2. Open the volume (e.g. `bronze_data`) → `raw/`
3. Expand 2–3 repo folders so the entity files are visible

**Capture:** Volume tree showing `raw/apache_cassandra/`, `raw/redis_redis/` etc.,
with `commits.json`, `issues.json`, `pulls.json`, `releases.json`, `repo_metadata.json` inside.

---

### 03 — Setup Notebook Successful Run
**File:** `03_setup_notebook_success.png`  
**Where:** `notebooks/01_setup.py`

**Steps:**
1. Open `01_setup.py` in Databricks
2. Click **Run All**
3. Wait for all cells to turn green

**Capture:** The full notebook with all cells completed, last cell output showing
`Created tables: bronze.commits, bronze.issues, ...`

---

### 04 — Bronze MERGE Output
**File:** `04_bronze_merge_output.png`  
**Where:** `notebooks/02_raw_to_bronze.py` — run for one repo

**Steps:**
1. Set widgets: `repo = surrealdb/surrealdb`, `entity = commits`, `run_mode = full`
2. Run All
3. Scroll to the final cell output

**Capture:** Cell output showing:
```
rows_inserted: 414   rows_updated: 0   batch_id: run_...
```

---

### 05 — Silver Reconciliation Report
**File:** `05_silver_merge_output.png`  
**Where:** `notebooks/03_bronze_to_silver.py` — run for one repo

**Steps:**
1. Set widgets: `repo = surrealdb/surrealdb`, `run_mode = full`
2. Run All
3. Scroll to the reconciliation table printed at the end

**Capture:** The printed reconciliation table with columns:
`REPOSITORY | ENTITY | BRONZE | INSERTED | UPDATED | QUARANTINED | EXCL_PRS | STATUS`

---

### 06 — Pipeline Execution Logs
**File:** `06_pipeline_execution_logs.png`  
**Where:** Databricks SQL Editor or a notebook cell

**SQL to run:**
```sql
SELECT log_id, layer, parameter, batch_id, start_time, end_time,
       status, rows_inserted, rows_updated, error_message
FROM   ops.pipeline_execution_logs
ORDER  BY start_time DESC
LIMIT  20;
```

**Capture:** Full result table with at least 5 rows visible. Confirm `status = 'SUCCESS'`
for at least one Bronze and one Silver row.

---

### 07 — Silver Quarantine Sample
**File:** `07_silver_quarantine_sample.png`  
**Where:** Databricks SQL Editor or notebook cell

**SQL to run:**
```sql
SELECT quarantine_id, layer, entity, repo_full_name,
       rejection_reason, LEFT(raw_payload, 200) AS raw_payload_preview,
       batch_id, load_timestamp
FROM   ops.silver_quarantine
LIMIT  10;
```

> If the quarantine table is empty after a clean run, first run the **Drift Test**
> (Section C of `04_run_pipeline.py`) to populate it.

**Capture:** At least one row with a visible `rejection_reason` (e.g. `type_mismatch: comments`).

---

### 08 — Bronze Table Schema
**File:** `08_bronze_table_schema.png`  
**Where:** Catalog Explorer → `bronze` schema → `commits` table → **Schema** tab

**Capture:** The Schema tab showing all columns including `sha`, `commit` (nested struct),
`author`, `committer`, `repo_full_name`, `load_timestamp`, `_corrupt_record`.

---

### 09 — Silver Table Schema (PII proof)
**File:** `09_silver_table_schema.png`  
**Where:** Catalog Explorer → `silver` schema → `commits` table → **Schema** tab

**Capture:** Schema tab confirming:
- `author_email_hash` column is present (STRING)
- `committer_email_hash` column is present (STRING)
- No column named `email`, `author_email`, or `committer_email`

---

### 10 — Row Counts All Tables
**File:** `10_row_counts_all_tables.png`  
**Where:** Databricks SQL Editor

**SQL to run:**
```sql
SELECT 'bronze.commits'          AS tbl, COUNT(*) AS rows FROM bronze.commits         UNION ALL
SELECT 'bronze.issues'           AS tbl, COUNT(*) AS rows FROM bronze.issues           UNION ALL
SELECT 'bronze.pull_requests'    AS tbl, COUNT(*) AS rows FROM bronze.pull_requests    UNION ALL
SELECT 'bronze.releases'         AS tbl, COUNT(*) AS rows FROM bronze.releases         UNION ALL
SELECT 'bronze.repo_metadata'    AS tbl, COUNT(*) AS rows FROM bronze.repo_metadata    UNION ALL
SELECT 'silver.commits'          AS tbl, COUNT(*) AS rows FROM silver.commits          UNION ALL
SELECT 'silver.issues'           AS tbl, COUNT(*) AS rows FROM silver.issues           UNION ALL
SELECT 'silver.pull_requests'    AS tbl, COUNT(*) AS rows FROM silver.pull_requests    UNION ALL
SELECT 'silver.releases'         AS tbl, COUNT(*) AS rows FROM silver.releases         UNION ALL
SELECT 'silver.repo_metadata'    AS tbl, COUNT(*) AS rows FROM silver.repo_metadata    UNION ALL
SELECT 'ops.silver_quarantine'   AS tbl, COUNT(*) AS rows FROM ops.silver_quarantine   UNION ALL
SELECT 'ops.pipeline_exec_logs'  AS tbl, COUNT(*) AS rows FROM ops.pipeline_execution_logs
ORDER BY tbl;
```

**Capture:** All 12 rows visible showing non-zero counts for the active tables.

---

### 11 — Incremental Run Widgets
**File:** `11_incremental_run_widgets.png`  
**Where:** `notebooks/04_run_pipeline.py` — widget bar at top

**Widget values to set:**

| Widget | Value |
|---|---|
| `catalog` | `workspace` |
| `base_path` | `/Volumes/workspace/bronze_data/raw` |
| `repo` | `ALL` |
| `since` | `2026-09-25T00:00:00Z` |
| `until` | *(empty)* |
| `run_mode` | `incremental` |
| `batch_id` | *(empty)* |

**Capture:** The widget bar before running, with all values filled as above.

---

### 12 — Backfill Run Widgets
**File:** `12_backfill_run_widgets.png`  
**Where:** `notebooks/04_run_pipeline.py` — widget bar

**Widget values to set:**

| Widget | Value |
|---|---|
| `catalog` | `workspace` |
| `base_path` | `/Volumes/workspace/bronze_data/raw` |
| `repo` | `surrealdb/surrealdb` |
| `since` | `2026-04-01T00:00:00Z` |
| `until` | `2026-06-30T23:59:59Z` |
| `run_mode` | `backfill` |
| `batch_id` | `backfill_surrealdb_q1fy26` |

**Capture:** Widget bar showing the backfill configuration.

---

### 13 — Idempotency Proof
**File:** `13_idempotency_proof.png`  
**Where:** `notebooks/04_run_pipeline.py` — Section D output cells

**Steps:**
1. Configure widgets for Section D (`repo = redis/redis`, `run_mode = incremental`)
2. Run only cells in **Section D — IDEMPOTENCY PROOF**
3. Scroll to the D-4 assertion output

**Capture:** Console output showing all lines like:
```
  ✓ PASS: workspace.bronze.commits        count=414 (unchanged)
  ✓ PASS: workspace.silver.commits        count=414 (unchanged)
  ...
  ✓ IDEMPOTENCY CONFIRMED — all row counts are identical after second pass.
```

---

### 14 — Drift Test: Schema Evolved in Bronze
**File:** `14_drift_test_bronze_schema_evolved.png`  
**Where:** Catalog Explorer → `bronze` schema → `issues` table → **Schema** tab

**Pre-requisite:** Run Section C (Drift Test) of `04_run_pipeline.py` first.

**Capture:** Schema tab showing `new_field_test` (STRING) column at the bottom of
the column list — proving `mergeSchema` added the unexpected field without crashing.

---

### 15 — Drift Test: Quarantine Rows
**File:** `15_drift_test_quarantine_rows.png`  
**Where:** Databricks SQL Editor

**SQL to run** (replace `%_drift` with your actual drift batch_id if needed):
```sql
SELECT entity, rejection_reason, LEFT(raw_payload, 300) AS raw_preview
FROM   ops.silver_quarantine
WHERE  batch_id LIKE '%_drift'
ORDER  BY rejection_reason;
```

**Capture:** Result showing at least 4 rows:
- 3 rows with `rejection_reason = 'type_mismatch: comments cannot cast to int'`
- 1 row with `rejection_reason = 'unparseable_date: created_at'`

---

## ★ Nice-to-Have Screenshots

---

### 16 — Secrets Scope
**File:** `16_secrets_scope.png`  
**Where:** Terminal / Databricks CLI

**Command:**
```bash
databricks secrets list-secrets --scope dbtrends
```

**Capture:** CLI output listing the `salt` key (value is hidden automatically).

---

### 17 — Git Folder Import
**File:** `17_git_folder_import.png`  
**Where:** Databricks Workspace → Git Folders

**Capture:** The Git Folder view showing `notebooks/` with the 4 notebooks listed
as Databricks notebook icons (not plain `.py` files).

---

### 18 — Delta MERGE History
**File:** `18_bronze_delta_history.png`  
**Where:** Databricks SQL Editor

**SQL to run:**
```sql
DESCRIBE HISTORY bronze.commits;
```

**Capture:** Result table showing at least 2 MERGE operations with `operationParameters`
visible (including `predicate` showing the merge key).

---

### 19 — Cassandra Zero Issues Proof
**File:** `19_silver_issues_cassandra_zero.png`  
**Where:** Databricks SQL Editor

**SQL to run:**
```sql
SELECT repo_full_name, COUNT(*) AS issue_count
FROM   silver.issues
WHERE  repo_full_name IN ('apache/cassandra', 'mongodb/mongo')
GROUP  BY repo_full_name;
```

**Capture:** Result showing `0` for both Cassandra and MongoDB, confirming the
`pull_request IS NULL` filter and external JIRA caveat.

---

### 20 — PostgreSQL Zero Issues Proof
**File:** `20_silver_issues_postgres_zero.png`  
**Where:** Databricks SQL Editor

**SQL to run:**
```sql
SELECT 'issues'        AS entity, COUNT(*) AS rows FROM silver.issues         WHERE repo_full_name = 'postgres/postgres' UNION ALL
SELECT 'pull_requests' AS entity, COUNT(*) AS rows FROM silver.pull_requests   WHERE repo_full_name = 'postgres/postgres' UNION ALL
SELECT 'releases'      AS entity, COUNT(*) AS rows FROM silver.releases        WHERE repo_full_name = 'postgres/postgres';
```

**Capture:** All three rows showing `0`, confirming the GitHub mirror caveat.

---

## Progress Tracker

Copy and update as you take each screenshot:

```
[ ] 01_unity_catalog_schemas.png
[ ] 02_volumes_structure.png
[ ] 03_setup_notebook_success.png
[ ] 04_bronze_merge_output.png
[ ] 05_silver_merge_output.png
[ ] 06_pipeline_execution_logs.png
[ ] 07_silver_quarantine_sample.png
[ ] 08_bronze_table_schema.png
[ ] 09_silver_table_schema.png
[ ] 10_row_counts_all_tables.png
[ ] 11_incremental_run_widgets.png
[ ] 12_backfill_run_widgets.png
[ ] 13_idempotency_proof.png
[ ] 14_drift_test_bronze_schema_evolved.png
[ ] 15_drift_test_quarantine_rows.png
[ ] 16_secrets_scope.png          (nice to have)
[ ] 17_git_folder_import.png      (nice to have)
[ ] 18_bronze_delta_history.png   (nice to have)
[ ] 19_silver_issues_cassandra_zero.png  (nice to have)
[ ] 20_silver_issues_postgres_zero.png   (nice to have)
```
