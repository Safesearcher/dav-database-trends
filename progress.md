# Project Progress — DAV Database Trends
# PySpark Medallion Pipeline (Bronze → Silver → Gold)
# University Data Analysis & Visualization — 2026

**Last updated:** 2026-10-03  
**Current branch:** `main`  
**Last commit:** `6f8e17d` — *"phase 2 progress"*  
**Test suite:** 8/8 tests passing ✅

---

## Phase Overview

| Phase | Title | Status | Complete |
|---|---|---|---|
| **Phase 1** | Data Collection & Bronze Layer Setup | ✅ Done | 100% |
| **Phase 2** | Silver Layer — Transformation, PII, Quality | ✅ Done | 100% |
| **Phase 3** | Gold Layer — Star Schema & BI Analytics | 🔲 Not started | 0% |
| **Phase 4** | Visualizations & Final Report | 🔲 Not started | 0% |

---

## ✅ Phase 1 — Data Collection & Bronze Layer (COMPLETE)

### 1.1 GitHub API Ingestion
- [x] **`full_load.py`** — Full historical ingest for all 10 repos (2026-04-01 → present)
- [x] **`scripts/fetch_incremental.py`** — Incremental fetch with `--since`, `--repos`, `--dry-run`, rate-limit handling, `updated_at` stop for pulls
- [x] **`fetch_samples.py`** — 5-record slice samples for local testing
- [x] **10 repos × 5 entities** = 50 JSON files collected in `bronze/`
- [x] Pagination resilience verified (Milvus page 15, Chroma pages 6 & 9 retried successfully)

### 1.2 Bronze Data (Local `bronze/`)

| Entity | Total Records | Notes |
|---|---|---|
| `commits.json` | **14,942** | Varies by repo activity level |
| `issues.json` | **27,863** | Includes embedded PRs (filtered in Silver) |
| `pulls.json` | **10,681** | Postgres = 0 (mirror); Cassandra/MongoDB = all PRs |
| `releases.json` | **764** | 4 repos have 0 (no GitHub releases) |
| `repo_metadata.json` | **10** | 1 dict per repo |
| **Total** | **54,260** | **Across 50 files / 10 repos** |

### 1.3 Bronze Data Quality Audit
- [x] **`scripts/audit_bronze.py`** — Full automated DQ audit (11 check dimensions)
- [x] **`audit/bronze_quality_report.md`** — 230-line report with scorecards
- [x] **19 audit CSV artefacts** in `audit/` (coverage, duplicates, PII inventory, etc.)
- [x] Zero duplicate primary keys across all 56,361 records
- [x] 100% of dates parse as valid ISO-8601 UTC
- [x] Issue-PR overlap quantified per repo (Cassandra: 100% PRs, Postgres: 0 issues)

### 1.4 Samples
- [x] `samples/` — 5-record slice for each of the 5 entities
- [x] `samples/full_load/` — larger reference samples
- [x] `samples/incremental_load/` — incremental API response samples
- [x] `samples/drift/` — schema-drift test data (anomalies A, B, C injected)
- [x] `samples/incremental/` — placeholder for `fetch_incremental.py` output

---

## ✅ Phase 2 — Silver Layer (COMPLETE)

### 2.1 Core Library (`src/`)

| File | Status | Contents |
|---|---|---|
| `src/schemas.py` | ✅ Done | 5 Bronze schemas, 5 Silver schemas, Quarantine schema, Execution Log schema — all explicit `StructType`, zero `inferSchema` |
| `src/common.py` | ✅ Done | `get_params`, `log_run`, `add_metadata`, `merge_delta`, `get_salt`, `hash_email`, `bot_flag`, `sanitize_commit_message`, `quarantine_records` |

### 2.2 Notebooks (`notebooks/`)

| Notebook | Status | What it does |
|---|---|---|
| `01_setup.py` | ✅ Done | Creates Unity Catalog schemas (`bronze`, `silver`, `ops`) and all Delta tables |
| `02_raw_to_bronze.py` | ✅ Done | Reads Volume JSON → Bronze Delta via `MERGE INTO`; PERMISSIVE mode; quarantine; drift detection; `log_run` |
| `03_bronze_to_silver.py` | ✅ Done | Bronze → Silver for all 5 entities; PII hashing; `is_bot`; type casting; quarantine; reconciliation report; `log_run` |
| `04_run_pipeline.py` | ✅ Done | Orchestrator with 4 sections: Incremental, Backfill, Drift Test, Idempotency Proof |

### 2.3 Pipeline Business Rules Implemented

| Rule | Status | Location |
|---|---|---|
| No `inferSchema` — all explicit `StructType` | ✅ | `src/schemas.py` |
| `load_timestamp` on every Bronze & Silver row | ✅ | `src/common.py::add_metadata`, `notebooks/03` |
| `MERGE INTO` for all writes (no append-only) | ✅ | `src/common.py::merge_delta` |
| Widgets — no hardcoded paths or dates in logic cells | ✅ | All 4 notebooks |
| Quarantine: corrupt JSON, null PK, type mismatch, bad date | ✅ | `src/common.py`, `notebooks/03` |
| Schema drift detection + `mergeSchema` absorption | ✅ | `notebooks/02`, `src/common.py` |
| FAILURE logged to `ops.pipeline_execution_logs` + re-raised | ✅ | `src/common.py::log_run` |
| PII: salted SHA-256 hash on author/committer emails | ✅ | `src/common.py::hash_email`, `notebooks/03` |
| Raw emails/names never written to Silver | ✅ | `notebooks/03` Silver schemas |
| Commit headline only — Signed-off-by/Co-authored-by stripped | ✅ | `src/common.py::sanitize_commit_message` |
| `is_bot` flag (commits, issues, PRs) | ✅ | `src/common.py::bot_flag`, `notebooks/03` |
| Issues: `pull_request IS NULL` filter | ✅ | `notebooks/03::transform_issues` |
| Cassandra/MongoDB → 0 Silver issues (documented) | ✅ | `notebooks/03` comment + README |
| Postgres → 0 Silver issues/PRs/releases (documented) | ✅ | README §12 |
| Backfill: `since`/`until` filter by entity date column | ✅ | `notebooks/02`, `notebooks/03` |
| Idempotency proof (run twice, assert counts unchanged) | ✅ | `notebooks/04` Section D |

### 2.4 Scripts
- [x] `scripts/fetch_incremental.py` — Incremental GitHub fetcher (reuses `full_load.py` pagination/retry)
- [x] `scripts/make_drift_sample.py` — Injects 3 anomalies into sample data for drift testing

### 2.5 Tests (`tests/`)

| File | Tests | Status |
|---|---|---|
| `tests/test_schemas.py` | 4 tests: schemas exist, `load_timestamp`, PII masking, `is_bot` presence | ✅ 4/4 pass |
| `tests/test_common.py` | 4 tests: `get_params`, `get_salt`, `bot_flag`, `start_run` | ✅ 4/4 pass |
| `tests/check_schemas.py` | Schema vs real JSON field coverage reporter | ✅ Runnable |
| **Total** | **8 tests** | **✅ 8/8 pass** |

### 2.6 Documentation
- [x] `README.md` — 630+ lines covering all 13 sections (overview, architecture, data dicts, execution guide, idempotency, PII, caveats, screenshots checklist)
- [x] `docs/data_dictionary.md` — Extended data dictionary
- [x] `docs/screenshots/CHECKLIST.md` — 20 screenshots with exact SQL + widget values
- [x] `audit/bronze_quality_report.md` — Full DQ audit
- [x] `.gitignore` — Covers `.salt`, `.env`, `silver/`, `gold/`, `logs/`, `__pycache__/`

### 2.7 Phase 2 Requirements Audit (2026-10-03)
All 24 checks pass. See audit report.

| Check | Result |
|---|---|
| No `inferSchema` | ✅ |
| `load_timestamp` in all schemas | ✅ |
| `MERGE INTO` everywhere | ✅ |
| No hardcoded paths/dates in logic | ✅ |
| Schema drift detection & absorption | ✅ |
| FAILURE logging + re-raise | ✅ |
| Data dictionary in README | ✅ |
| Execution guide in README | ✅ |
| No secrets committed | ✅ |

---

## 🔲 Phase 3 — Gold Layer (NOT STARTED)

The Gold layer will transform Silver Delta tables into a star schema optimized
for BI queries and cross-repo trend analysis.

### Planned work:

- [ ] **`notebooks/05_silver_to_gold.py`** — Build fact and dimension tables
- [ ] **Fact tables:**
  - `gold.fact_commits` — daily commit velocity per repo, filtered `WHERE NOT is_bot`
  - `gold.fact_issues` — issue open/close cycle times
  - `gold.fact_pull_requests` — PR merge rate, time-to-merge
  - `gold.fact_releases` — release cadence per repo
- [ ] **Dimension tables:**
  - `gold.dim_repo` — repo metadata snapshot
  - `gold.dim_date` — calendar spine (day, week, month, quarter)
  - `gold.dim_actor` — contributor identity (login + email hash)
- [ ] **Standard 6-month analysis window** (2026-04-01 → 2026-09-30)
- [ ] **Rolling metrics:** 4-week and 12-week commit velocity
- [ ] **Cross-repo normalization:** per-unit-time metrics to handle pagination-cap differences
- [ ] **`src/gold_transforms.py`** — Reusable Gold aggregation helpers
- [ ] **Update `01_setup.py`** to create Gold schema and tables
- [ ] **Update `04_run_pipeline.py`** to orchestrate Bronze → Silver → Gold

### Known Gold-layer caveats to handle:
- Postgres: commits only (no issues/PRs/releases)
- Cassandra + MongoDB: 0 true issues → exclude from issue metrics
- CockroachDB: filter `is_bot=true` commits (41.2% of records)
- FAISS: `author_login` null for 27.8% → use `author_email_hash` as identity

---

## 🔲 Phase 4 — Visualizations & Final Report (NOT STARTED)

### Planned work:

- [ ] **Databricks SQL Dashboard** — interactive charts using Gold tables
- [ ] **Visualization 1:** Commit velocity trend (all 10 repos, 6-month window, line chart)
- [ ] **Visualization 2:** PR merge rate comparison (bar chart, human contributors only)
- [ ] **Visualization 3:** Issue resolution time distribution (boxplot per repo)
- [ ] **Visualization 4:** Release cadence heatmap (6 repos with GitHub releases)
- [ ] **Visualization 5:** Star growth vs commit activity scatter
- [ ] **Visualization 6:** Bot vs human contributor ratio (stacked bar)
- [ ] **Final report / notebook** with narrative, methodology, findings, limitations
- [ ] **Take all 20 screenshots** in `docs/screenshots/CHECKLIST.md`
- [ ] **Presentation slides** (if required)

---

## Files Created This Session (Phase 2)

| File | Size | Description |
|---|---|---|
| `src/schemas.py` | 20 KB | All explicit StructType schemas |
| `src/common.py` | 15 KB | Shared pipeline helpers |
| `notebooks/01_setup.py` | 8 KB | UC schema + table initialization |
| `notebooks/02_raw_to_bronze.py` | 14 KB | Raw JSON → Bronze Delta |
| `notebooks/03_bronze_to_silver.py` | 21 KB | Bronze → Silver transformation |
| `notebooks/04_run_pipeline.py` | 21 KB | Orchestrator (4 sections) |
| `scripts/fetch_incremental.py` | 18 KB | Incremental GitHub fetcher |
| `scripts/make_drift_sample.py` | 8 KB | Drift anomaly injector |
| `tests/test_schemas.py` | 3 KB | Schema unit tests |
| `tests/test_common.py` | 2 KB | Common helper unit tests |
| `tests/check_schemas.py` | 6 KB | Schema vs JSON field reporter |
| `README.md` | 32 KB | Full project README (13 sections) |
| `docs/data_dictionary.md` | 10 KB | Extended data dictionary |
| `docs/screenshots/CHECKLIST.md` | 11 KB | 20-screenshot checklist with SQL |
| `samples/drift/issues.json` | 38 KB | Drift test data (anomalies A+B+C) |
| `samples/drift/commits.json` | 25 KB | Drift test data (anomaly A) |

---

## What Still Needs to Happen Before Submission

### Immediate (before running on Databricks)
- [ ] Upload `bronze/` folder to Unity Catalog Volume at `/Volumes/workspace/bronze_data/raw/`
- [ ] Create Databricks Secrets scope: `databricks secrets create-scope --scope dbtrends`
- [ ] Store PII salt: `databricks secrets put --scope dbtrends --key salt --string-value <random-hex>`
- [ ] Import notebooks into Databricks via Git Folder
- [ ] Run `01_setup.py` once to create all Delta tables
- [ ] Run `04_run_pipeline.py` Section A (standard incremental run)
- [ ] Run `04_run_pipeline.py` Section C (drift test) after uploading `samples/drift/`

### Screenshots (15 required, 5 optional)
- [ ] 01–15 from `docs/screenshots/CHECKLIST.md` — must-have
- [ ] 16–20 from `docs/screenshots/CHECKLIST.md` — nice-to-have

### Phase 3 + 4
- [ ] Build Gold layer (`05_silver_to_gold.py`)
- [ ] Build 6 visualizations
- [ ] Write final report / narrative notebook
