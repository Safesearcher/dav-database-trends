# Power BI Developer Handoff Guide — DAV Database Trends

This document serves as the implementation contract and developer handoff guide for building the Power BI dashboard over the **Gold Layer** (`workspace.gold`).

---

## 1. Connecting Power BI to Databricks Unity Catalog

1. Open **Power BI Desktop**.
2. Select **Get Data → Databricks**.
3. Enter connection coordinates:
   * **Server Hostname:** Your Databricks workspace URL (e.g., `adb-xxxxxxxx.xx.azuredatabricks.net`).
   * **HTTP Path:** Located under SQL Warehouse → Connection Details (`/sql/1.0/warehouses/xxxxxx`).
4. In the Navigator:
   * Expand Catalog: `workspace`
   * Select Schema: `gold`
   * Select the 9 Gold star schema tables and 2 audit views:
     * `dim_repo`, `dim_date`, `dim_actor`
     * `fact_commits`, `fact_issues`, `fact_pull_requests`, `fact_releases`, `fact_repo_snapshot`, `fact_repo_daily_activity`
     * `audit_pipeline_runs`, `audit_gold_reconciliation`
5. **Connectivity Mode:**
   * **Import Mode (Recommended):** Ingests all tables into Power BI's in-memory VertiPaq engine. The dataset is compact (~35,000 total rows across all tables) and Import mode enables instantaneous slicing, complex DAX measures, and offline development.
   * **DirectQuery Mode (Optional):** DirectQuery can be used if live serverless query passthrough is required.

---

## 2. Star Schema Relationship Modeling

In the **Model View**, establish the following relationships. All relationship cross-filter directions must be **Single** (Dimension filters Fact):

| Dimension (1) | Column | Fact (Many) | Column | Cardinality | Active |
|---|---|---|---|---|---|
| `dim_repo` | `repo_key` | `fact_commits` | `repo_key` | 1 : Many | Active |
| `dim_repo` | `repo_key` | `fact_issues` | `repo_key` | 1 : Many | Active |
| `dim_repo` | `repo_key` | `fact_pull_requests` | `repo_key` | 1 : Many | Active |
| `dim_repo` | `repo_key` | `fact_releases` | `repo_key` | 1 : Many | Active |
| `dim_repo` | `repo_key` | `fact_repo_snapshot` | `repo_key` | 1 : Many | Active |
| `dim_repo` | `repo_key` | `fact_repo_daily_activity` | `repo_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_commits` | `date_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_issues` | `created_date_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_issues` | `closed_date_key` | 1 : Many | Inactive |
| `dim_date` | `date_key` | `fact_pull_requests` | `created_date_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_pull_requests` | `merged_date_key` | 1 : Many | Inactive |
| `dim_date` | `date_key` | `fact_releases` | `published_date_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_repo_snapshot` | `snapshot_date_key` | 1 : Many | Active |
| `dim_date` | `date_key` | `fact_repo_daily_activity` | `date_key` | 1 : Many | Active |
| `dim_actor` | `actor_key` | `fact_commits` | `actor_key` | 1 : Many | Active |
| `dim_actor` | `actor_key` | `fact_issues` | `actor_key` | 1 : Many | Active |
| `dim_actor` | `actor_key` | `fact_pull_requests` | `actor_key` | 1 : Many | Active |

---

## 3. Date Dimension Configuration

1. In the **Fields** pane, right-click `dim_date` → **Mark as Date Table**.
2. Select `full_date` as the Date column.
3. Sort `month_name` by column `month` to ensure correct chronological ordering on visuals.
4. Sort `day_name` by column `day_of_week`.

---

## 4. Key Metrics & DAX Measures Reference

Create a dedicated DAX measure table `_Measures`:

```dax
// 1. Human Commit Velocity
Human Commits = 
CALCULATE(
    COUNTROWS(fact_commits),
    fact_commits[is_bot] = FALSE()
)

// 2. Bot Commit Percentage
Bot Commit Ratio = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_commits), fact_commits[is_bot] = TRUE()),
    COUNTROWS(fact_commits),
    0
)

// 3. Average Issue Closure Time (Hours)
Avg Issue Close Time Hours = 
AVERAGE(fact_issues[hours_to_close])

// 4. PR Merge Rate
PR Merge Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_pull_requests), fact_pull_requests[is_merged] = TRUE()),
    COUNTROWS(fact_pull_requests),
    0
)

// 5. Average PR Lead Time (Hours)
Avg Hours to Merge = 
AVERAGE(fact_pull_requests[hours_to_merge])

// 6. Release Cadence (Average Days Between Releases)
Avg Days Between Releases = 
AVERAGE(fact_releases[days_since_previous_release])

// 7. Active Human Contributors (Distinct Count)
Active Contributors = 
CALCULATE(
    DISTINCTCOUNT(fact_commits[actor_key]),
    fact_commits[is_bot] = FALSE()
)
```

---

## 5. Critical Domain Quirks & Visual Slicing Guidelines

When configuring dashboard visuals and report filters, keep these empirical characteristics in mind:

1. **Analysis Window Filter:**
   * Standard analysis window is **2026-04-01 to 2026-09-30**.
   * Add a page-level or report-level filter on `dim_date[in_analysis_window] = TRUE` to ensure all visualizations align with the primary assignment scope.
2. **PostgreSQL Is Commits-Only:**
   * PostgreSQL (`postgres/postgres`) is an upstream mirror of `git.postgresql.org`.
   * It has 0 issues, 0 PRs, and 0 releases on GitHub. Visualizations for PR merge rate or issue closure must gracefully handle or document this expected zero.
3. **Cassandra & MongoDB Issues Count Is Strictly Zero:**
   * Apache Cassandra and MongoDB manage their issue tracking externally in JIRA.
   * 100% of records in their GitHub issues API are pull requests (which the pipeline accurately filtered out). Issue metrics for these two engines are legitimately 0.
4. **CockroachDB High Bot Ratio:**
   * Over 80% of CockroachDB commits are generated by CI/CD automation bots (`cockroach-teamcity`, `bors`).
   * When comparing developer velocity, **always use the `Human Commits` measure** rather than total commits, or slice by `is_bot = FALSE`.
5. **Vector vs. Established Database Categories:**
   * `dim_repo[db_category]` divides the 10 repos into `'Established'` (Postgres, Redis, Cassandra, MongoDB, CockroachDB) and `'Emerging'` (SurrealDB, Qdrant, Milvus, Chroma, FAISS).
   * Use this column as a visual category filter on scatter plots and comparative bar charts.
