# Bronze Layer Data Quality Audit Report
**Dataset:** Cross-paradigm database ecosystem trends (10 GitHub Repositories)  
**Medallion Target:** Bronze (Raw JSON) → Silver (Cleaned Delta/Parquet) → Gold (Star Schema)  
**Execution Timestamp:** 2026-09-30 16:53:15 UTC  
**Auditor Engine:** `scripts/audit_bronze.py` (Pandas & Stdlib, Read-Only on `bronze/`)

---

## 1. Executive Summary

1. **Dataset Overview:** Audited 50 files across 10 repositories (~585 MB raw JSON) spanning relational, document, key-value, and vector database engines.
2. **File Integrity:** 100% of files exist, parse as valid JSON, match expected top-level types (dict for metadata, array for entities), and reconcile perfectly with load summary record counts.
3. **Truncation Reality:** 9 of 10 repos hit the 2,000-record pagination cap across commits, issues, and pulls; commit histories cover 2.5 months (MongoDB) to 6 years (FAISS), rather than entire repository lifespans.
4. **Issue-PR Overlap Discovery:** GitHub's `/issues` endpoint returned 100% PRs for Cassandra (2,000 PRs) and MongoDB (1,783 PRs); true issues count is exactly **0** for both because issue tracking is hosted on external ASF/MongoDB JIRA instances.
5. **PostgreSQL Mirror Asymmetry:** PostgreSQL is an official git read-only mirror of git.postgresql.org with GitHub Issues, PRs, and Releases disabled (0 records captured for issues, pulls, and releases).
6. **Zero Primary Key Duplication:** 0 duplicate commit SHAs, issue IDs, PR IDs, or release IDs were detected across any of the 10 repositories.
7. **Pagination Resilience Verified:** Milvus (Page 15) and Chroma (Pages 6 & 9) transient error retries resulted in zero duplicate IDs and maintained strict chronological monotonicity.
8. **Heavy Bot Automation:** Bot activity accounts for **41.2%** of CockroachDB commits (TeamCity/bors) and **13.4%** of its PRs, requiring mandatory bot filtering in Silver to prevent distorted developer velocity metrics.
9. **PII Inventory:** Identified 20,000 author/committer emails and names in `commits.json` plus embedded `Signed-off-by` trailers in commit messages; salted SHA-256 hashing is required in Silver.
10. **Final Verdict:** **READY WITH CAVEATS** — The raw Bronze data is technically sound and structurally valid, but requires explicit filtering and window standardization in Silver.

---

## 2. Comprehensive Quality Scorecard

| Check # | Audit Dimension | cassandra | chroma | cockroach | faiss | milvus | mongo | postgres | qdrant | redis | surrealdb |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 1. File Integrity | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟡 WARN | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **2** | 2. Completeness & Coverage | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN |
| **3** | 3. Uniqueness & Deduplication | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **4** | 4. Issues vs Pulls Overlap | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | ⚪ SKIP | 🟡 WARN | 🟡 WARN | 🟡 WARN |
| **5** | 5. Schema Profile & Types | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **6** | 6. Value Validity & Dates | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **7** | 7. Actors, Bots & Unlinked | 🟢 PASS | 🟢 PASS | 🟡 WARN | 🟡 WARN | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟡 WARN | 🟢 PASS | 🟢 PASS |
| **8** | 8. PII Inventory & Privacy | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **9** | 9. Temporal Shape & Spikes | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN | 🟡 WARN |
| **10** | 10. Cross-Repo Comparability | 🟡 WARN | 🟢 PASS | 🟡 WARN | 🟢 PASS | 🟢 PASS | 🟡 WARN | 🟡 WARN | 🟢 PASS | 🟢 PASS | 🟢 PASS |
| **11** | 11. Repository Metadata | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟢 PASS | 🟡 WARN | 🟢 PASS | 🟢 PASS | 🟢 PASS |

> **Scorecard Legend:**  
> - 🟢 **PASS:** Metric completely validated with zero data quality defects.  
> - 🟡 **WARN:** Architectural anomaly or structural limitation detected (truncation, external JIRA, heavy bots, empty release feed) requiring Silver transformation logic.  
> - 🔴 **FAIL:** Corrupt file, unparseable schema, invalid primary key duplicate, or hard constraint break (None encountered).  
> - ⚪ **SKIP:** Check omitted because entity is natively non-existent (e.g. Postgres issues/pulls).

---

## 3. Detailed Audit Findings & Supporting Metrics

### 3.1 Check 1 — File Integrity & Verification
- **Existence & Validity:** All 50 files across all 10 repositories exist, are non-empty, and parse cleanly as UTF-8 JSON.
- **Top-Level Structures:** Every `repo_metadata.json` is a JSON Object (`dict`); all `commits.json`, `issues.json`, `pulls.json`, and `releases.json` are JSON Arrays (`list`).
- **Record Count Reconciliation:** All files match the full-load summary counts with 0 discrepancies:
  - 10/10 repos have 2,000 commits.
  - 8/10 repos have 2,000 issues; MongoDB has 1,783 (exhausted API); PostgreSQL has 0 (`[]`).
  - 8/10 repos have 2,000 pulls; MongoDB has 1,804 (exhausted API); PostgreSQL has 0 (`[]`).
  - Releases: Cassandra (0), Chroma (137), Cockroach (0), Faiss (29), Milvus (176), Mongo (0), Postgres (0), Qdrant (117), Redis (155), SurrealDB (150).
- *Artifact:* [`audit/file_integrity.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/file_integrity.csv)

### 3.2 Check 2 — Completeness, Coverage & Live API Comparison
Comparing Bronze captured records against live GitHub API totals (queried 2026-09-30 16:53:15 UTC):

| Repository | Commits Captured | Live Commits | Commit % | Issues Captured | Live Issues | Pulls Captured | Live Pulls | Truncation Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `postgres_postgres` | 2,000 | ~62,000 | 3.2% | 0 | 0 | 0 | 0 | Commits Truncated (3.2%) |
| `cockroachdb_cockroach` | 2,000 | ~142,000 | 1.4% | 2,000 | ~19,000 | 2,000 | ~157,000 | Heavily Truncated (<2%) |
| `mongodb_mongo` | 2,000 | ~168,000 | 1.2% | 1,783 | 24 | 1,804 | 1,804 | 100% PRs, Commits Truncated |
| `surrealdb_surrealdb` | 2,000 | ~7,900 | 25.3% | 2,000 | 780 | 2,000 | ~6,800 | Truncated (25.3%) |
| `redis_redis` | 2,000 | ~13,600 | 14.7% | 2,000 | ~2,500 | 2,000 | ~13,400 | Truncated (14.7%) |
| `apache_cassandra` | 2,000 | ~31,000 | 6.5% | 2,000 | 0 | 2,000 | ~5,200 | 100% PRs in issues |
| `facebookresearch_faiss` | 2,000 | ~2,400 | 83.3% | 2,000 | ~500 | 2,000 | ~3,200 | Substantial Coverage (83.3%) |
| `qdrant_qdrant` | 2,000 | ~6,900 | 29.0% | 2,000 | ~2,000 | 2,000 | ~8,300 | Truncated (29.0%) |
| `milvus-io_milvus` | 2,000 | ~29,500 | 6.8% | 2,000 | ~4,200 | 2,000 | ~49,700 | Truncated (6.8%) |
| `chroma-core_chroma` | 2,000 | ~4,400 | 45.5% | 2,000 | ~200 | 2,000 | ~7,600 | Truncated (45.5%) |

- **Issue/PR Number Continuity & Disparities:**
  - `mongodb_mongo`: Range 1 to 1829. Missing 25 numbers total (natural deleted/spam gaps).
  - `chroma-core_chroma`: Range 5566 to 7819. Missing 54 numbers across 38 small gaps.
  - `apache_cassandra`: Range 3222 to 5223. **0 missing numbers in range**.
  - `cockroachdb_cockroach`: Range 167840 to 175961. 4,188 missing numbers due to divergence between the 2,000 issue pagination window and 2,000 PR pagination window.
- **Pagination Boundary Recoveries:**
  - Milvus Page 15 (Index 1400): 0 duplicate IDs; dates strictly descending.
  - Chroma Page 6 (Index 500) and Page 9 (Index 800): 0 duplicate IDs; dates strictly descending.
- *Artifacts:* [`audit/coverage.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/coverage.csv), [`audit/number_gaps.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/number_gaps.csv), [`audit/pagination_boundaries.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/pagination_boundaries.csv)

### 3.3 Check 3 — Uniqueness & Duplicate Profile
- **Primary Key Uniqueness:** Verified 0 duplicate keys across all entities:
  - Commits (`sha`): 0 duplicates across 20,000 records.
  - Issues (`id`): 0 duplicates across 17,783 records.
  - Pulls (`id`): 0 duplicates across 17,804 records.
  - Releases (`id`): 0 duplicates across 764 records.
- **Whole-Record Duplication:** Checked full JSON canonical SHA-256 hashes: **0 whole-record duplicate rows found**.
- *Artifact:* [`audit/duplicates.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/duplicates.csv)

### 3.4 Check 4 — Issues vs Pull Requests Overlap
The GitHub REST API returns Pull Requests inside the `/issues` endpoint marked with a `pull_request` key. Audit reveals dramatic ecosystem differences:

| Repository | Issues File Total | Embedded PRs | True Issues Count | Pulls File Total | Overlap Count | Overlap % of Issue-PRs | Overlap % of Pulls |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `apache_cassandra` | 2,000 | 2,000 | **0** | 2,000 | 1,998 | 99.9% | 99.9% |
| `mongodb_mongo` | 1,783 | 1,783 | **0** | 1,804 | 1,783 | 100.0% | 98.8% |
| `chroma-core_chroma` | 2,000 | 1,800 | **200** | 2,000 | 1,800 | 100.0% | 90.0% |
| `qdrant_qdrant` | 2,000 | 1,725 | **275** | 2,000 | 1,725 | 100.0% | 86.2% |
| `facebookresearch_faiss` | 2,000 | 1,558 | **442** | 2,000 | 1,558 | 100.0% | 77.9% |
| `redis_redis` | 2,000 | 1,524 | **476** | 2,000 | 1,524 | 100.0% | 76.2% |
| `milvus-io_milvus` | 2,000 | 1,347 | **653** | 2,000 | 1,347 | 100.0% | 67.3% |
| `surrealdb_surrealdb` | 2,000 | 1,244 | **756** | 2,000 | 1,244 | 100.0% | 62.2% |
| `cockroachdb_cockroach` | 2,000 | 66 | **1,934** | 2,000 | 66 | 100.0% | 3.3% |
| `postgres_postgres` | 0 | 0 | **0** | 0 | 0 | 0.0% | 0.0% |

- **Critical Insight:** For Cassandra and MongoDB, `issues.json` contains ZERO true bug reports or feature requests; it is 100% duplicate PR metadata.
- *Artifact:* [`audit/issues_pulls_overlap.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/issues_pulls_overlap.csv)

### 3.5 Check 5 — Schema Profile & Silver Target Fields Reliability
Profiled 24 core fields targeted for the Silver layer:
- **Core Keys & Timestamps:** `sha`, `created_at`, `state`, `number`, `user.login` have **100% presence** and **0% nulls** across active repos.
- **Resolution Timestamps:** `closed_at` and `merged_at` have expected null rates (~15–30% in PRs; ~20–40% in issues) representing currently open items.
- **Commit Author Logins:** `author.login` is null in **27.8%** of FAISS commits and **6.7%** of Chroma commits because internal corporate emails (`@meta.com`) were not linked to public GitHub profiles.
- **Repo Metadata:** Stars, forks, language, license, and pushed_at are 100% populated.
- *Artifacts:* [`audit/schema_profile.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/schema_profile.csv), [`audit/silver_fields_reliability.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/silver_fields_reliability.csv)

### 3.6 Check 6 — Value Validity & Integrity Constraints
- **Timestamps:** 100% of dates across all 50 files parse as valid ISO-8601 UTC.
- **Future Dates:** Exactly **0** dates in the future.
- **Chronological Coherence:**
  - `closed_at >= created_at`: 0 violations across 17,783 issues and 17,804 PRs.
  - `merged_at >= created_at`: 0 violations across all merged PRs.
- **Numeric Fields:** Stars, forks, comments count are all non-negative integers; 0 negative or impossible values detected.
- *Artifact:* [`audit/value_validity.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/value_validity.csv)

### 3.7 Check 7 — Actors, Bot Activity & Ghost Accounts
Bot activity significantly impacts activity metrics if left unfiltered:

| Repository | Commit Bot % | Unlinked Commit Authors % | PR Bot % | Ghost/Deleted Users |
| :--- | :---: | :---: | :---: | :---: |
| `cockroachdb_cockroach` | **41.2%** (bors, TeamCity) | 0.6% | **13.4%** | 0 |
| `mongodb_mongo` | **8.4%** (mongodb-evergreen) | 0.0% | 0.0% | 22 (1.2%) |
| `chroma-core_chroma` | 0.4% | 6.7% | 0.4% | 1 |
| `facebookresearch_faiss` | 0.0% | **27.8%** (Meta internal) | 0.2% | 2 |
| `apache_cassandra` | 0.0% | 2.6% | 0.1% | 2 |
| `milvus-io_milvus` | 0.1% | 0.1% | 0.2% | 0 |
| `qdrant_qdrant` | 0.0% | 0.0% | 0.1% | 0 |
| `redis_redis` | 0.0% | 0.0% | 0.0% | 1 |
| `surrealdb_surrealdb` | 0.0% | 0.0% | 0.0% | 0 |
| `postgres_postgres` | 0.0% | 0.0% | N/A | N/A |

- *Artifacts:* [`audit/actors_summary.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/actors_summary.csv), [`audit/top_actors.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/top_actors.csv)

### 3.8 Check 8 — PII Inventory & Privacy Assessment
Identified direct PII requiring masking/hashing prior to Gold/BI consumption:
- `commit.author.email`: 20,000 occurrences (e.g. `l***r@n***.edu.pk`, `m***e@m***a.com`, `t***m@g***l.com`).
- `commit.committer.email`: 20,000 occurrences (e.g. `n***e@g***b.com`).
- `commit.author.name`: 20,000 occurrences (e.g. `M***d A***n`, `B***a P***r`).
- `commit.message` Signed-off-by: 1,482 occurrences of embedded emails in commit trailers.
- *Silver Mandate:* Salted SHA-256 hash on author and committer emails; truncate commit messages to subject line or scrub regex email patterns.
- *Artifact:* [`audit/pii_inventory.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/pii_inventory.csv)

### 3.9 Check 9 — Temporal Shape, Anomalies & Staleness
- **Span Differences:**
  - FAISS 2,000 commits span August 2020 to September 2026 (~73 months).
  - CockroachDB 2,000 commits span April 2026 to September 2026 (~5.5 months).
  - MongoDB 2,000 commits span July 2026 to September 2026 (~2.5 months).
- **Spike Months (>5x median monthly volume):**
  - Detected in MongoDB (September 2026: automated branch synchronization).
  - Detected in SurrealDB (early alpha launch period).
- **Staleness:** All 10 repositories have pushed commits within September 2026 (zero stale repos).
- *Artifacts:* [`audit/monthly_counts.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/monthly_counts.csv), [`audit/temporal_anomalies.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/temporal_anomalies.csv)

### 3.10 Check 10 — Cross-Repo Comparability & Time Windowing
- **Strict Common Window:** The longest unbroken window where ALL 10 repos have captured commits is bounded by the oldest commit of the most active repo:
  - **Start:** `2026-07-13T11:50:22Z` (MongoDB oldest commit)
  - **End:** `2026-09-29T16:31:20Z` (Latest commit in snapshot)
  - **Duration:** Exactly **2.5 months**.
  - In this strict window, FAISS only has 104 commits (5.2% of its data), whereas MongoDB has 2,000 (100%).
- **Recommended Balanced Analysis Window:**
  - **Window:** **2026-04-01 to 2026-09-30 (Last 6 Months)**.
  - Retains **100%** of CockroachDB and MongoDB commits, **92%** of Milvus, **78%** of Chroma, **64%** of Qdrant, and **22%** of FAISS.
- **Releases Gap:** 4 repos (Cassandra, CockroachDB, MongoDB, Postgres) have 0 records in `releases.json`. To compare release cadences across all 10 repos, Git tags must be extracted or release cadences restricted to the 6 repos with GitHub release data.
- *Artifact:* [`audit/cross_repo_comparability.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/cross_repo_comparability.csv)

### 3.11 Check 11 — Repository Metadata Profile
- All 10 repositories are primary upstream projects (fork = False, archived = False).
- Licensing represents the cross-paradigm shift:
  - PostgreSQL (PostgreSQL license), Redis (RSALv2/SSPLv1 dual license after fork), CockroachDB (BSL-1.1 commercial core), MongoDB (SSPL).
  - Vector DBs (Chroma, Qdrant, Milvus, Faiss) maintain permissive Apache-2.0 / MIT licenses.
- *Artifact:* [`audit/repo_metadata.csv`](file:///C:/Users/dell/Documents/Dav_project/audit/repo_metadata.csv)

---

## 4. Known Limitations for Proposal & README

When documenting the dataset in the project proposal and `README.md`, state these transparently:
1. **Truncation to 2,000 Records:** Due to the API pagination budget, high-velocity repositories (CockroachDB, MongoDB, PostgreSQL) cover only recent 2026 history, while moderate-velocity projects (FAISS, Redis) cover multiple years. Comparisons across repos must be normalized per unit time (e.g. weekly velocity) rather than all-time totals.
2. **Issue Tracking Disparity (External JIRA):** Apache Cassandra and MongoDB do not use GitHub Issues. Their `issues.json` files contain exclusively Pull Request entries. Bug resolution time metrics cannot be computed for Cassandra or MongoDB from GitHub data alone.
3. **PostgreSQL Mirror Inactivity:** PostgreSQL on GitHub is an official mirror; development happens on the `pgsql-hackers` mailing list and commitfest app. Commits are fully populated, but issues and PRs are disabled (0 records).
4. **Release Feed Omissions:** Cassandra, CockroachDB, MongoDB, and Postgres do not use GitHub's Releases endpoint to publish version assets.
5. **Bot Activity Distortion:** Over 41% of CockroachDB commits are generated by CI/CD automation bots. Unfiltered analysis would dramatically overestimate human contributor numbers.

---

## 5. Silver Design Implications

The Bronze audit enforces the following non-negotiable transformation requirements for `bronze_to_silver`:

1. **Issue Cleaning Rule:** Filter `issues.json` with `WHERE pull_request IS NULL`. Do NOT count records containing `pull_request` as issues. For Cassandra and MongoDB, flag that true GitHub issue count is 0.
2. **Bot Tagging Column:** Add `is_bot` boolean column in `silver_commits` and `silver_pulls` using regex pattern:
   `user.login LIKE '%[bot]%' OR user.type = 'Bot' OR user.login IN ('dependabot', 'bors', 'cockroach-teamcity', 'mongodb-evergreen', 'renovate')`.
3. **PII Masking Rule:** 
   - `author_email_hash`: `sha256(concat(commit.author.email, salt))`
   - `committer_email_hash`: `sha256(concat(commit.committer.email, salt))`
   - Truncate `commit_message` to the first newline or strip lines starting with `Signed-off-by:`, `Co-authored-by:`.
4. **Unlinked Author Fallback:** When `author.login` is NULL (27.8% of FAISS), fall back to `commit.author.name` or `author_email_hash` as the unique contributor identifier.
5. **Time-Window Standardization:** In `silver_to_gold`, compute rolling 4-week and 12-week velocity metrics, and filter cross-repo aggregations to the standard 6-month window (`>= 2026-04-01`).
6. **Deduplication Strategy:** Dedup using primary keys `sha` (commits), `id` (issues), `id` (pulls), `id` (releases).

---

## 6. Final Verdict

### 🟢 READY WITH CAVEATS

The Bronze layer raw data is **structurally valid, 100% uncorrupted, has 0 primary key duplicates, and is verified against live GitHub API endpoints**. It is ready for Bronze-to-Silver transformation subject to the following required caveats:

1. **No Data Re-fetch Needed:** The 2,000-record caps and empty Postgres/Cassandra issue sets are natural characteristics of the targeted ecosystems and API limits. Re-fetching would not resolve external JIRA usage or Postgres mirror settings.
2. **Mandatory Silver Transformation Rules:**
   - Exclude PRs from `issues.json` (`pull_request IS NULL`).
   - Implement the `is_bot` flag column.
   - Salt and hash all commit author and committer emails.
   - Align cross-repo comparative metrics to a standardized post-April 2026 analysis window.
