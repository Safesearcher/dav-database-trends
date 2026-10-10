# Pipeline Proof of Execution & Grading Rubric Mapping

This directory holds the artifacts proving end-to-end execution of the PySpark Medallion pipeline on Databricks.

> **Submission Instructions:**
> Export the executed notebooks (**File → Export → IPython Notebook** or **HTML**) into this folder, or add a video link here.
> Graders evaluate what the pipeline actually demonstrates on Databricks.

---

## Rubric Requirement Mapping

| Rubric Requirement | Demonstrated In | Notebook Section / Cell | Table / Query / Proof to Show |
|---|---|---|---|
| **Explicit `StructType` Schemas**<br>*(Zero schema inference, no `inferSchema`, proper Silver types)* | [`src/schemas.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/src/schemas.py), [`notebooks/01_setup.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/01_setup.py), [`notebooks/02_raw_to_bronze.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py), [`notebooks/03_bronze_to_silver.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/03_bronze_to_silver.py) | `01_setup.py` (Cell DDL setup); `02_raw_to_bronze.py` (`process_entity`, explicit `BRONZE_SCHEMAS`); `03_bronze_to_silver.py` (`transform_*`) | `DESCRIBE TABLE EXTENDED bronze.commits;`<br>`DESCRIBE TABLE EXTENDED silver.commits;`<br>Unit test suite: `tests/test_schemas.py` |
| **Lineage Enrichment**<br>*(Source file, batch ID, repo name on every row)* | [`notebooks/02_raw_to_bronze.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py), [`src/common.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/src/common.py) (`add_metadata`) | `02_raw_to_bronze.py` (Cell 2 `process_entity` Step 3) | `SELECT _source_file, batch_id, repo_full_name, load_timestamp FROM bronze.commits LIMIT 5;` |
| **`load_timestamp` Tracking**<br>*(Present on every Bronze and Silver row)* | [`src/schemas.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/src/schemas.py), [`notebooks/02_raw_to_bronze.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py), [`notebooks/03_bronze_to_silver.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/03_bronze_to_silver.py) | `02_raw_to_bronze.py` (lineage injection); `03_bronze_to_silver.py` (`current_timestamp().alias("load_timestamp")`) | `SELECT repo_full_name, load_timestamp FROM bronze.issues LIMIT 5;`<br>`SELECT repo_full_name, load_timestamp FROM silver.issues LIMIT 5;` |
| **Silver Data Cleansing, PII & Quarantine**<br>*(PII hashing, bot flags, email scrub, bad records quarantined)* | [`notebooks/03_bronze_to_silver.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/03_bronze_to_silver.py), [`src/common.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/src/common.py) | `03_bronze_to_silver.py` (Cells 2–6: `transform_commits`, `transform_issues`, etc.) | `SELECT commit_sha, author_email_hash, is_bot FROM silver.commits LIMIT 5;`<br>`SELECT quarantine_id, layer, entity, rejection_reason FROM ops.silver_quarantine WHERE layer = 'silver' LIMIT 10;` |
| **MERGE INTO & Idempotency**<br>*(Re-running identical batch produces zero duplicates)* | [`notebooks/04_run_pipeline.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/04_run_pipeline.py) | **Section D — IDEMPOTENCY PROOF** (Cells D-1 through D-4) | Assertion output in Cell D-4 verifying identical row counts across all Bronze and Silver tables:<br>`✓ IDEMPOTENCY CONFIRMED — all row counts are identical after second pass.` |
| **Parameters & Backfills**<br>*(Re-process historical period for single repo without code changes)* | [`notebooks/04_run_pipeline.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/04_run_pipeline.py), [`notebooks/02_raw_to_bronze.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py), [`notebooks/03_bronze_to_silver.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/03_bronze_to_silver.py) | **Section B — BACKFILL RUN** (Cells B-1 through B-4); Widgets: `backfill_repo`, `backfill_since`, `backfill_until` | Query logs for backfill batch:<br>`SELECT log_id, layer, parameter, rows_inserted, rows_updated, status FROM ops.pipeline_execution_logs WHERE parameter LIKE '%mode=backfill%' ORDER BY start_time DESC;` |
| **Schema Drift Handling**<br>*(Absorb new columns via mergeSchema, quarantine malformed types, no crashes)* | [`notebooks/04_run_pipeline.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/04_run_pipeline.py), [`notebooks/02_raw_to_bronze.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py), [`scripts/make_drift_sample.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/scripts/make_drift_sample.py) | **Section C — DRIFT TEST** (Cells C-1 through C-6) | Cell C-4b assertions passing:<br>1. `new_field_test` absorbed into `bronze.issues` and `bronze.commits` schemas.<br>2. 3 Bronze quarantine rows for type-corrupted comments.<br>3. 1 Silver quarantine row for unparseable `created_at`.<br>4. Drift keys logged in execution logs. |
| **Execution Log Table**<br>*(Audit log tracking layer, parameters, run time, rows inserted/updated, status)* | [`notebooks/01_setup.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/01_setup.py), [`src/common.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/src/common.py) (`log_run`), [`notebooks/04_run_pipeline.py`](file:///C:/Users/dell/Pictures/dav-database-trends-main/notebooks/04_run_pipeline.py) | `04_run_pipeline.py` (Cells A-4, B-4, C-4, D-4: `show_logs()`) | `SELECT log_id, layer, parameter, status, rows_inserted, rows_updated, start_time, end_time, error_message FROM ops.pipeline_execution_logs ORDER BY start_time DESC LIMIT 20;` |

---

## Exported Notebook Placeholders / Video Links

- **Video Demonstration Link:** *(Add video walkthrough link here if applicable)*
- **Exported Executed Notebooks:**
  - `01_setup.html` / `01_setup.ipynb`
  - `02_raw_to_bronze.html` / `02_raw_to_bronze.ipynb`
  - `03_bronze_to_silver.html` / `03_bronze_to_silver.ipynb`
  - `04_run_pipeline.html` / `04_run_pipeline.ipynb`
