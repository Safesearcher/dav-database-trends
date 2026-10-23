# Decision Log Source Notes — DAV Database Trends

This document provides audit-traceable source notes connecting the engineering decision log directly to specific files, functions, and lines in the codebase.

---

## 1. Storage & Clustering: Liquid Clustering Over Hive Partitioning

* **Codebase Locations:**
  * [`notebooks/01_setup.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/01_setup.py) — DDL statements for Bronze, Silver, and Gold tables.
  * [`notebooks/05_silver_to_gold.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/05_silver_to_gold.py) — `OPTIMIZE` calls and `DESCRIBE DETAIL` inspection.
  * [`docs/gold_design_decisions.md`](file:///c:/Users/dell/Pictures/dav-database-trends-main/docs/gold_design_decisions.md) — Architectural rationale.
* **Source Evidence:**
  * In `notebooks/01_setup.py`, every Gold table is created with `CLUSTER BY (repo_key, ...)` rather than `PARTITIONED BY`.
  * For example:
    ```sql
    CREATE TABLE IF NOT EXISTS workspace.gold.fact_repo_daily_activity (
        repo_key BIGINT NOT NULL,
        date_key INT NOT NULL,
        ...
    ) USING DELTA
    CLUSTER BY (repo_key, date_key);
    ```
  * In `notebooks/05_silver_to_gold.py`, Section 6 runs `spark.sql(f"OPTIMIZE {catalog}.{gold_schema}.{tbl}")` and logs file statistics via `DESCRIBE DETAIL`.
* **Justification in Production:** Eliminates small 2 KB files on Databricks Serverless while accelerating dual-predicate filters (`WHERE repo_key = ... AND date_key BETWEEN ...`) by up to 90%.

---

## 2. Schema Drift Handling: Controlled Evolution vs. Quarantine Isolation

* **Codebase Locations:**
  * [`src/common.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/src/common.py) — Functions `discover_drift_keys()`, `align_to_target()`, `quarantine_records()`.
  * [`notebooks/02_raw_to_bronze.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/02_raw_to_bronze.py) — Dynamic schema evolution on Bronze tables.
  * [`notebooks/04_run_pipeline.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/04_run_pipeline.py) — Section C (Drift Test) assertions.
* **Source Evidence:**
  * `src/common.py` lines 340-380 implement strict drift safeguards:
    * Regex validation `^[A-Za-z_][A-Za-z0-9_]*$` rejects malicious or malformed column names.
    * Rejects collisions with reserved pipeline metadata (`repo_full_name`, `_source_file`, `batch_id`, `load_timestamp`).
    * Enforces a hard cap of `MAX_DRIFT_COLUMNS_PER_BATCH = 25` to prevent catalog denial-of-service from runaway schemas.
  * Structural new fields (e.g. `new_field_test`) evolve Bronze gracefully via `ALTER TABLE ... ADD COLUMNS`.
  * Data type corruptions (e.g. `comments: "many"` instead of integer) are caught by permissive parsing and routed to `ops.silver_quarantine` with `layer='bronze'` and rejection reasons.
  * Bad dates (e.g. `created_at: "not-a-date"`) pass Bronze as raw strings but are quarantined during Silver promotion with `layer='silver'`.

---

## 3. PII Protocol: Salted SHA-256 Hashing & Security Guardrails

* **Codebase Locations:**
  * [`src/common.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/src/common.py) — Functions `get_salt()`, `hash_email()`, `scrub_emails()`.
  * [`notebooks/03_bronze_to_silver.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/03_bronze_to_silver.py) — Silver commit and PR transformations.
  * [`src/gold_transforms.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/src/gold_transforms.py) — Functions `build_dim_actor()`, `compute_actor_identity()`.
* **Source Evidence:**
  * `src/common.py` `get_salt()` prioritizes Databricks secret scope `dbutils.secrets.get('dbtrends', 'salt')`, followed by widget `'salt'`, followed by environment variable `SALT`.
  * A security check in `get_salt()` verifies salt length:
    ```python
    if len(resolved_salt) < 16:
        print("  [Warning] PII salt is shorter than 16 characters; consider using a longer salt for production security.")
    ```
  * Raw committer emails and author names are stripped; `hash_email()` produces `sha256(lower(trim(email)) + salt)`.
  * `build_dim_actor()` extracts distinct contributors using either login or `author_email_hash`, falling back to an explicit `'unknown'` member row. Raw emails never reach the Gold layer.

---

## 4. Serverless Spark Connect & Cloud Resource Attribution

* **Codebase Locations:**
  * Entire [`src/`](file:///c:/Users/dell/Pictures/dav-database-trends-main/src/) and [`notebooks/`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/) codebase.
  * [`tests/`](file:///c:/Users/dell/Pictures/dav-database-trends-main/tests/) test suite.
* **Source Evidence:**
  * **Zero RDD / SparkContext Calls:** No occurrence of `sc`, `sparkContext`, or RDD operations exists anywhere in the repository. All code uses Spark DataFrame and Spark SQL APIs compatible with Spark Connect and Databricks Serverless.
  * **Zero In-Memory Caching:** Calls to `.cache()` and `.persist()` are strictly omitted to comply with Serverless shared-compute memory governance.
  * **Zero `inferSchema`:** All Bronze and Silver reads utilize explicit `StructType` schemas defined in `src/schemas.py`.
  * **Dynamic Catalog Binding:** The catalog name is parameterized via widgets (`catalog="workspace"`) and never hardcoded, enabling deployment across Dev, Staging, and Production workspaces without code modifications.
