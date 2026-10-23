# Spark DAG Inspection Guide: `fact_repo_daily_activity` — DAV Database Trends

This guide explains how to capture, read, and interpret the physical execution plan and Spark DAG for the Gold layer's central aggregate table, `gold.fact_repo_daily_activity`.

---

## 1. Where to Capture the DAG in Databricks

When running [`notebooks/05_silver_to_gold.py`](file:///c:/Users/dell/Pictures/dav-database-trends-main/notebooks/05_silver_to_gold.py):

1. **In-Notebook Explain Plan:** Cell 4 automatically prints the formatted plan via `df_fact_daily.explain(mode="formatted")`.
2. **Databricks Spark UI:**
   * Open the executed notebook run in Databricks.
   * Expand the cell execution details for Step 4 (`fact_repo_daily_activity`).
   * Click **View Spark UI** (or click the active cluster → **Spark UI** → **Jobs** / **SQL/Dataframe** tab).
   * Select the query matching `Delta MergeInto` or the latest SQL query execution.
   * Click the **Details** / **DAG Visualization** toggle to render the graphical Spark stage DAG.

---

## 2. Key Stages in the Physical Execution DAG

The calculation of `fact_repo_daily_activity` combines four distinct query processing phases:

```
[ dim_repo (10 rows) ]    [ dim_date (2,557 days) ]
          │                         │
          └───────────┬─────────────┘
                      ▼
         [ BroadcastNestedLoopJoin ]
                      │
                      ▼ (Dense Spine: 25,570 rows)
     [ Left Outer Joins with Daily Aggregates ]
     ├── agg_commits  (groupBy repo_key, date_key)
     ├── agg_issues_o (groupBy repo_key, created_date_key)
     ├── agg_issues_c (groupBy repo_key, closed_date_key)
     ├── agg_prs_o    (groupBy repo_key, created_date_key)
     ├── agg_prs_m    (groupBy repo_key, merged_date_key)
     └── agg_releases (groupBy repo_key, published_date_key)
                      │
                      ▼
            [ Coalesce 0-Fills ]
                      │
                      ▼
         [ Exchange hashpartitioning(repo_key) ]
                      │
                      ▼
            [ Sort(repo_key ASC, full_date ASC) ]
                      │
                      ▼
           [ WindowExec: rowsBetween(-27, 0) ]  ── 4-Week Velocity
           [ WindowExec: rowsBetween(-83, 0) ]  ── 12-Week Velocity
                      │
                      ▼
             [ Delta MERGE INTO ]
```

### Stage 1: Dense Calendar Spine Materialization
* **Operator:** `BroadcastNestedLoopJoin` or `CartesianProduct`.
* **Execution:** Because `dim_repo` is tiny (10 rows) and `dim_date` is small (~2,557 days for 7 years), Spark broadcasts `dim_repo` to all executors. The join materializes a complete Cartesian grid of 25,570 repository-day combinations in a few milliseconds.
* **Why it matters:** Ensures every calendar day exists in the stream before applying window functions.

### Stage 2: Event Aggregations
* **Operator:** `HashAggregate` (Partial) → `Exchange (hashpartitioning)` → `HashAggregate` (Final).
* **Execution:** Base facts (`fact_commits`, `fact_issues`, `fact_pull_requests`, `fact_releases`) are aggregated to `(repo_key, date_key)` in parallel across worker nodes.

### Stage 3: Left Outer Joins & Zero-Filling
* **Operator:** `SortMergeJoin` or `ShuffledHashJoin`.
* **Execution:** The dense spine is left-joined with each of the daily aggregate streams. Days without activity produce `NULL`, which is converted to `0` via `coalesce(count, 0)`.

### Stage 4: Window Functions (`WindowExec`)
* **Operator:** `Exchange (hashpartitioning(repo_key))` → `Sort (repo_key, full_date)` → `WindowExec`.
* **Execution:** Rows are shuffled by `repo_key` so all dates for a given repository arrive at the same worker. Within each partition, rows are sorted chronologically by `full_date`.
* Two consecutive `WindowExec` operators evaluate the rolling sums:
  * `rowsBetween(-27, 0)`: Computes the 28-day human commit velocity (`commits_human_4wk`).
  * `rowsBetween(-83, 0)`: Computes the 84-day human commit velocity (`commits_human_12wk`).

---

## 3. Interpreting the `explain("formatted")` Output

When reviewing the cell output in Databricks, verify these key execution markers:

1. **`BroadcastExchange`:** Confirms dimension tables are broadcasted efficiently without large network shuffles.
2. **`Filter (in_analysis_window#...)`:** Verifies analysis window flags are evaluated as pushdown predicates.
3. **`Window [sum(...) ... rowsBetween(-27, 0)]`:** Confirms the 28-row rolling frame executes within Spark's optimized C++ vectorized window engine.
4. **`AdaptiveSparkPlan (AQE)`:** Confirms Adaptive Query Execution dynamically coalesces shuffle partitions to prevent skew.
