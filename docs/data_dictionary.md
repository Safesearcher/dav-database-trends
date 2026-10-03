# Data Dictionary — Medallion Architecture (Bronze & Silver)

**Project:** Cross-paradigm Database Ecosystem Trend Analytics (`dav-database-trends`)  
**Platform:** Databricks Serverless, Unity Catalog, Delta Lake  
**Timestamp:** 2026-10-03  

---

## 1. Architecture Overview

| Layer | Storage Format | Unity Catalog Namespace | Description |
| :--- | :--- | :--- | :--- |
| **Bronze** | Delta Lake | `<catalog>.bronze.<entity>` | Raw GitHub API responses preserved as typed structs with lineage metadata (`_source_file`, `batch_id`, `load_timestamp`). Idempotently merged. |
| **Silver** | Delta Lake | `<catalog>.silver.<entity>` | Cleaned, deduplicated, typed, bot-flagged, and PII-sanitized entity tables across the 10 database engines. |
| **Quarantine**| Delta Lake | `<catalog>.silver.quarantine` | Captures malformed, schema-violating, or unparseable records with original payload and rejection reason. |
| **Operations**| Delta Lake | `<catalog>.ops.pipeline_execution_logs` | Audit trail of every pipeline batch run with status, timing, rows inserted, and rows updated. |

---

## 2. Silver Layer Tables

### 2.1 `silver.commits`
Represents development velocity, code churn, and contributor engagement across repos.

| Column | Type | Nullable | Primary Key | Description & Transformation |
| :--- | :--- | :---: | :---: | :--- |
| `repo_full_name` | `STRING` | No | PK (Composite) | GitHub repository identifier (e.g. `surrealdb/surrealdb`). |
| `commit_sha` | `STRING` | No | PK (Composite) | Unique 40-character Git commit hash. |
| `author_login` | `STRING` | Yes | No | Contributor GitHub username; falls back to committer login if null. |
| `author_id` | `BIGINT` | Yes | No | Contributor GitHub account ID. |
| `author_email_hash` | `STRING` | Yes | No | Salted SHA-256 hash: `sha256(lowercase(author.email) + salt)`. |
| `committer_email_hash` | `STRING` | Yes | No | Salted SHA-256 hash: `sha256(lowercase(committer.email) + salt)`. |
| `commit_date_utc` | `TIMESTAMP` | Yes | No | UTC timestamp parsed from commit author date. |
| `commit_headline` | `STRING` | Yes | No | First line of commit message; Signed-off-by trailers stripped. |
| `comment_count` | `INT` | Yes | No | Number of commit discussion comments on GitHub. |
| `is_bot` | `BOOLEAN` | No | No | True if user type is Bot, login contains `[bot]`, or matches known CI accounts. |
| `_source_file` | `STRING` | Yes | No | Lineage path to raw Bronze source in Unity Catalog Volume. |
| `load_timestamp` | `TIMESTAMP` | No | No | UTC timestamp when record was merged into Silver. |

---

### 2.2 `silver.issues`
Represents user bug reports, feature requests, and issue resolution cadence.

> **CRITICAL CLEANING RULE:** GitHub's `/issues` endpoint returns both PRs and true issues. All records where `pull_request` is not null are excluded. True issue count for `apache/cassandra` and `mongodb/mongo` is 0.

| Column | Type | Nullable | Primary Key | Description & Transformation |
| :--- | :--- | :---: | :---: | :--- |
| `repo_full_name` | `STRING` | No | PK (Composite) | GitHub repository identifier. |
| `issue_number` | `INT` | No | PK (Composite) | Repository issue number. |
| `issue_id` | `BIGINT` | No | No | Global GitHub issue entity ID. |
| `title` | `STRING` | Yes | No | Issue title. |
| `state` | `STRING` | Yes | No | Issue state: `open` or `closed`. |
| `author_login` | `STRING` | Yes | No | Issue creator username. |
| `author_id` | `BIGINT` | Yes | No | Issue creator ID. |
| `is_bot` | `BOOLEAN` | No | No | True if issue author is an automated bot. |
| `created_at_utc` | `TIMESTAMP` | Yes | No | Issue creation timestamp (ISO-8601 UTC). |
| `updated_at_utc` | `TIMESTAMP` | Yes | No | Last update timestamp (ISO-8601 UTC). |
| `closed_at_utc` | `TIMESTAMP` | Yes | No | Issue close timestamp (null if open). |
| `comments_count` | `INT` | Yes | No | Total comment count on issue. |
| `labels_list` | `ARRAY<STRING>` | Yes | No | Array of issue label names (e.g. `['bug', 'enhancement']`). |
| `_source_file` | `STRING` | Yes | No | Lineage source Volume file path. |
| `load_timestamp` | `TIMESTAMP` | No | No | UTC timestamp of ingestion. |

---

### 2.3 `silver.pull_requests`
Tracks code review efficiency, contribution throughput, and branch merge lifecycles.

| Column | Type | Nullable | Primary Key | Description & Transformation |
| :--- | :--- | :---: | :---: | :--- |
| `repo_full_name` | `STRING` | No | PK (Composite) | GitHub repository identifier. |
| `pr_number` | `INT` | No | PK (Composite) | Pull request number. |
| `pr_id` | `BIGINT` | No | No | Global GitHub pull request entity ID. |
| `title` | `STRING` | Yes | No | PR title. |
| `state` | `STRING` | Yes | No | State: `open`, `closed`. |
| `author_login` | `STRING` | Yes | No | PR submitter username. |
| `is_bot` | `BOOLEAN` | No | No | True if submitter is an automated bot. |
| `created_at_utc` | `TIMESTAMP` | Yes | No | Creation timestamp (UTC). |
| `updated_at_utc` | `TIMESTAMP` | Yes | No | Last updated timestamp (UTC). |
| `closed_at_utc` | `TIMESTAMP` | Yes | No | PR close timestamp (UTC, nullable). |
| `merged_at_utc` | `TIMESTAMP` | Yes | No | PR merge timestamp (UTC, null if closed unmerged). |
| `is_merged` | `BOOLEAN` | No | No | True if `merged_at_utc` is not null. |
| `merge_commit_sha` | `STRING` | Yes | No | SHA of the resulting merge commit on target branch. |
| `head_branch` | `STRING` | Yes | No | Source branch reference. |
| `base_branch` | `STRING` | Yes | No | Target branch reference (e.g. `main`, `master`). |
| `_source_file` | `STRING` | Yes | No | Lineage source file path. |
| `load_timestamp` | `TIMESTAMP` | No | No | Ingestion timestamp. |

---

### 2.4 `silver.releases`
Tracks project release cadence, versioning tags, and release distribution assets.

| Column | Type | Nullable | Primary Key | Description & Transformation |
| :--- | :--- | :---: | :---: | :--- |
| `repo_full_name` | `STRING` | No | PK (Composite) | GitHub repository identifier. |
| `release_id` | `BIGINT` | No | PK (Composite) | Global GitHub release ID. |
| `tag_name` | `STRING` | Yes | No | Git release tag (e.g. `v2.1.0`). |
| `release_name` | `STRING` | Yes | No | Human-readable title of the release. |
| `is_draft` | `BOOLEAN` | No | No | True if release is a draft. |
| `is_prerelease` | `BOOLEAN` | No | No | True if release is marked pre-release/alpha/beta. |
| `published_at_utc` | `TIMESTAMP` | Yes | No | UTC timestamp when release was published. |
| `assets_count` | `INT` | Yes | No | Count of binary assets attached to the release. |
| `_source_file` | `STRING` | Yes | No | Lineage source path. |
| `load_timestamp` | `TIMESTAMP` | No | No | Ingestion timestamp. |

---

### 2.5 `silver.repo_metadata`
High-level repository health indicators, community adoption, and licensing governance.

| Column | Type | Nullable | Primary Key | Description & Transformation |
| :--- | :--- | :---: | :---: | :--- |
| `repo_id` | `BIGINT` | No | PK (Composite) | Global repository ID. |
| `repo_full_name` | `STRING` | No | PK (Composite) | Repository name (`owner/repo`). |
| `repo_name` | `STRING` | Yes | No | Repository name without owner prefix. |
| `owner_login` | `STRING` | Yes | No | Organization or owner account name. |
| `description` | `STRING` | Yes | No | Repository description. |
| `default_branch` | `STRING` | Yes | No | Default branch (e.g. `main`). |
| `license_spdx_id` | `STRING` | Yes | No | Normalized SPDX license identifier (e.g. `Apache-2.0`). |
| `stargazers_count` | `INT` | Yes | No | Total stars at time of snapshot. |
| `forks_count` | `INT` | Yes | No | Total forks at time of snapshot. |
| `open_issues_count` | `INT` | Yes | No | Open issues count from repository metadata. |
| `created_at_utc` | `TIMESTAMP` | Yes | No | Repository creation timestamp. |
| `updated_at_utc` | `TIMESTAMP` | Yes | No | Repository last updated timestamp. |
| `pushed_at_utc` | `TIMESTAMP` | Yes | No | Last code push timestamp. |
| `topics` | `ARRAY<STRING>` | Yes | No | List of repository topical tags. |
| `_source_file` | `STRING` | Yes | No | Lineage source path. |
| `load_timestamp` | `TIMESTAMP` | No | No | Ingestion timestamp. |

---

## 3. Operations & Quarantine Tables

### 3.1 `silver.quarantine`
Records rejected during Bronze ingestion or Silver transformation due to constraint breaks, missing primary keys, or unparseable timestamps.

| Column | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `quarantine_id` | `STRING` | No | Unique UUID generated for the rejection event. |
| `layer` | `STRING` | No | Pipeline layer where rejection occurred (`BRONZE` or `SILVER`). |
| `entity` | `STRING` | No | Target entity name (`commits`, `issues`, etc.). |
| `repo_full_name` | `STRING` | Yes | Repository identifier. |
| `rejection_reason` | `STRING` | No | Explanation of why record was rejected. |
| `raw_payload` | `STRING` | Yes | Complete serialized JSON representation of the rejected row. |
| `load_timestamp` | `TIMESTAMP` | No | Rejection logging timestamp. |

### 3.2 `ops.pipeline_execution_logs`
Operational execution log tracking every notebook run across full and incremental batches.

| Column | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `log_id` | `STRING` | No | Unique UUID generated for the execution run. |
| `layer` | `STRING` | No | Execution layer (`SETUP`, `BRONZE`, `SILVER`, `ORCHESTRATOR`). |
| `parameter_file` | `STRING` | No | Parameters, widgets, and target file for the run. |
| `start_time` | `TIMESTAMP` | No | Batch start UTC timestamp. |
| `end_time` | `TIMESTAMP` | Yes | Batch finish UTC timestamp. |
| `status` | `STRING` | No | Status: `SUCCESS` or `FAILED`. |
| `rows_inserted` | `BIGINT` | Yes | Total rows inserted into target Delta table. |
| `rows_updated` | `BIGINT` | Yes | Total rows updated via Delta MERGE. |
| `error_message` | `STRING` | Yes | Traceback/error summary if status is `FAILED`. |
| `load_timestamp` | `TIMESTAMP` | No | Log insertion timestamp. |
