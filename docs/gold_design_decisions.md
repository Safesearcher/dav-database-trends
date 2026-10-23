# Gold Layer Architectural Design Decisions — DAV Database Trends

This document details the core architectural and dimensional design decisions implemented in Phase 3 (Gold Layer) for the DAV Database Trends PySpark pipeline running on Databricks Serverless with Unity Catalog.

---

## 1. Deterministic Surrogate Keys (`xxhash64`)

### Context
Star schemas traditionally assign integer surrogate keys to dimension records. In distributed computing (Spark / Databricks), generating auto-incrementing integer keys (e.g. `monotonically_increasing_id()` or stateful identity columns) causes severe drawbacks:
1. **Non-deterministic re-computation:** Keys change across pipeline re-runs or backfills, invalidating downstream foreign keys.
2. **Cluster synchronization bottleneck:** Monotonic counters require global driver coordination or shuffle stages.
3. **Partition skew:** Sequential IDs do not distribute uniformly across storage files.

### Decision
All dimensional and fact surrogate keys in the Gold layer are computed using deterministic 64-bit hashing:
* **`dim_repo.repo_key`:** `xxhash64(lower(trim(repo_full_name)))`
* **`dim_actor.actor_key`:** `xxhash64(actor_identity)`
* **`dim_date.date_key`:** Integer representation of the calendar date formatted as `YYYYMMDD` (e.g., `20260401`).

### Benefits
* **Complete Determinism:** An identity token (e.g., `'surrealdb/surrealdb'` or an author login) consistently resolves to the exact same 64-bit integer across every batch, partition, or backfill pass.
* **Zero Driver Coordination:** Workers compute surrogate keys locally in parallel without requiring stateful sequence lookups.
* **Storage Efficiency:** 64-bit `LongType` (`BIGINT`) occupies 8 bytes, offering negligible collision probability for open-source repository populations while optimizing join performance in Delta and Power BI.

---

## 2. Slowly Changing Dimension (SCD) Type 1 Strategy

### Context
Dimension attributes (e.g., repository description, default branch, database classification, actor bot status) may evolve over time. Dimensional modeling offers several strategies, primarily SCD Type 1 (in-place overwrite) and SCD Type 2 (row versioning with validity intervals `valid_from` / `valid_to`).

### Decision
Both `gold.dim_repo` and `gold.dim_actor` are implemented as **SCD Type 1**:
* Upstream changes in Silver trigger a Delta `MERGE INTO ... WHEN MATCHED THEN UPDATE SET *`.
* Only the latest state is retained for each repository and contributor.

### Rationale
1. **Analytical Goal:** The business questions in this project focus on repository adoption, velocity trends, and ecosystem momentum across 2026. Slicing historical commits by an older repository description or a transient bot status flag introduces unnecessary complexity without analytical value.
2. **Simplified Downstream BI:** SCD 1 allows Power BI and SQL consumers to create simple, direct 1-to-many relationships without complex point-in-time temporal joins.
3. **Auditability Preserved:** True historical changes are already preserved immutably in Bronze and Silver Delta transaction logs (`DESCRIBE HISTORY`), making SCD 2 in Gold redundant.

---

## 3. Liquid Clustering (`CLUSTER BY`) vs. Hive-Style Partitioning

### Context
Databricks historically partitioned tables using directory-based Hive partitioning (e.g., `/year=2026/month=04/repo=postgres/`). 

### Problem with Hive Partitioning
1. **Small File Problem:** Partitioning by high-cardinality keys (such as `date_key` or `(repo, date)`) creates tens of thousands of subdirectories containing tiny 2 KB Delta files. This severely degrades query performance and increases metadata overhead.
2. **Over-Partitioning Risk:** Repositories have wildly uneven activity (Postgres and CockroachDB have high commit volumes, while vector DBs have fewer commits). Static partitioning causes severe data skew.
3. **Inflexibility:** Changing partition columns in Delta requires a complete, expensive table rewrite.

### Decision
All 9 Gold tables reject Hive partitioning entirely in favor of Delta **Liquid Clustering** (`CLUSTER BY`):
* `dim_repo`: `CLUSTER BY (repo_key)`
* `dim_date`: `CLUSTER BY (date_key)`
* `dim_actor`: `CLUSTER BY (actor_key)`
* `fact_commits`: `CLUSTER BY (repo_key, date_key)`
* `fact_issues`: `CLUSTER BY (repo_key, created_date_key)`
* `fact_pull_requests`: `CLUSTER BY (repo_key, created_date_key)`
* `fact_releases`: `CLUSTER BY (repo_key, published_date_key)`
* `fact_repo_snapshot`: `CLUSTER BY (repo_key, snapshot_date_key)`
* `fact_repo_daily_activity`: `CLUSTER BY (repo_key, date_key)`

### Advantages on Databricks Serverless
* **Zero Directory Overhead:** Files reside in a single flat directory structure; clustering organizes rows inside Parquet files using Hilbert space-filling curves.
* **Predictable File Sizes:** Liquid clustering automatically sizes files to target 128 MB – 1 GB blocks, eliminating small-file fragmentation.
* **Fast Multi-Dimensional Skipping:** Queries filtering on either `repo_key`, `date_key`, or both skip up to 90% of file reads instantly.
* **Maintenance Automation:** Running `OPTIMIZE {table}` dynamically re-clusters data without requiring manual partition re-writes.

---

## 4. Dense Calendar Spine for Rolling Activity Metrics

### Context
In software analytics, development activity is naturally sparse: weekends, holidays, or release freezes produce zero commits or pull requests for days or weeks.

### Problem with Sparse Aggregation
If daily activity is aggregated solely from logged events (`GROUP BY repo, date`), inactive calendar days are omitted from the resulting table. 
Applying rolling window functions (such as `rowsBetween(-27, 0)`) over a sparse table counts the **previous 28 active days**, not the previous 28 calendar days! A repo inactive for 3 months would sum commits from 9 months prior, fatally distorting 4-week and 12-week velocity metrics.

### Decision
`gold.fact_repo_daily_activity` enforces a **dense calendar spine**:
1. Cross-joins `dim_repo` with `dim_date` to materialize every calendar day for each repository.
2. Aggregates commits, issues, PRs, and releases by `(repo_key, date_key)`.
3. Left-joins the spine with activity aggregates, filling missing measures with `0`.
4. Executes rolling 28-day (`commits_human_4wk`) and 84-day (`commits_human_12wk`) window functions partitioned by `repo_key` and ordered by `full_date`.

### Result
Days with zero activity are explicitly recorded with `0`, guaranteeing 100% mathematical precision for velocity metrics across the analysis window.

---

## 5. Salted SHA-256 PII Protection & Security Safeguards

### Threat Model
Committer and author emails are personal data (PII) under GDPR and privacy regulations. Direct hashing (plain SHA-256) is vulnerable to dictionary and rainbow table attacks because email address syntax is highly predictable.

### Implementation Protocol
1. **Dynamic Secret Retrieval:** The pipeline queries Databricks Secrets scope `dbtrends` key `salt` (falling back to the notebook widget `salt` or `SALT` environment variable in local development).
2. **Length Enforcing & Warning:** If the resolved salt is shorter than 16 characters, `get_salt()` outputs a non-fatal security warning advising production teams to lengthen the salt.
3. **Secret Confidentiality:** Raw salts and plaintext email addresses are strictly prohibited from being printed to notebook cell outputs, stored in logs, or written to Delta tables.
4. **Surrogate Identity Masking:** Contributor identity in Gold resolves to `dim_actor` where emails appear exclusively as salted SHA-256 digests (`author_email_hash`). Contributor personal names are discarded.

---

## 6. `dim_date` Range Extension to 2008 & Activity Spine Guard

### Context
Quality Gate 2 (Referential Integrity) failed in production with 556 orphan `fact_issues.created_date_key` rows (dates 2011-09-18 to 2019-12-26) and 12 orphan `fact_releases.published_date_key` rows (dates 2018-03-01 to 2019-12-07). The root cause is the GitHub API's `since` query parameter: it filters by an item's **last-updated** timestamp, not its creation date. An issue opened in 2012 that received a new comment in 2026 will appear in an incremental load, but its `created_at_utc` still resolves to a 2012 `date_key` that did not exist in `dim_date`.

### Decision
1. **`DIM_DATE_START = "2008-01-01"`** — Extend the lower bound of `dim_date` to GitHub's founding year (April 2008). This makes `dim_date` cover every possible `created_at_utc` / `published_at_utc` timestamp that the API can return. The full spine is 7,305 rows (2008-01-01 to 2027-12-31 inclusive).
2. **`DIM_DATE_END = "2027-12-31"`** — End boundary updated to a proper year-end (was `2027-01-01`); reflects the current analysis planning horizon.
3. **`DIM_DATE_ACTIVITY_SPINE_START = "2020-01-01"`** — A separate constant that guards `build_fact_repo_daily_activity`. The dense cross-join (`dim_repo × dim_date`) filters `dim_date` rows to `>= 2020-01-01` before the join, so extending the full calendar spine back to 2008 does **not** add ~43,800 zero-activity rows (10 repos × ~4,383 days) to the aggregate fact table.

### Why 2020 as the Activity Spine Floor?
Pre-2020 activity is outside the project's analytical scope (the analysis window is 2026-04-01 to 2026-09-30). Rolling window velocity metrics are meaningless for years without any ingested data. Writing zero-rows back to 2008 would increase storage and slow Power BI refresh without any analytical gain.

### Named Constants
All three bounds are module-level string constants in `src/gold_transforms.py`, imported directly by `notebooks/05_silver_to_gold.py`. Hardcoded literal dates have been eliminated from both files so a future range adjustment requires a single-file change.
