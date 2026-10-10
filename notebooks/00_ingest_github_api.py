# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00 - Ingest Raw JSON from GitHub REST API to Unity Catalog Volume
# MAGIC
# MAGIC **Capabilities & Governance:**
# MAGIC 1. **In-Databricks Extraction:** Directly pulls GitHub REST API data inside serverless compute into Unity Catalog Volumes.
# MAGIC 2. **Target File Isolation:** Default full-mode output root is `/Volumes/workspace/bronze_data/raw_api` (never overwriting verified `/Volumes/workspace/bronze_data/raw`).
# MAGIC 3. **Incremental/Backfill Staging:** Lands batch runs into `<landing_root>/<batch_id>/<owner>_<repo>/<entity>.json`.
# MAGIC 4. **Pre-flight Health & Rate-Limit Check:** Verifies outbound network access to `api.github.com` and checks remaining API quota vs. scope estimate before commencing.
# MAGIC 5. **Audit Logging:** Every (repo, entity) is logged to `ops.pipeline_execution_logs` with `layer='INGEST'`, recording inserted record counts and API call statistics.
# MAGIC 6. **Resilience & Resumption:** Atomic write (`.tmp` -> replace/move) prevents truncated files. `skip_existing=true` enables resuming interrupted jobs.
# MAGIC 7. **Security:** Tokens are resolved securely (Secret -> Widget -> Env) and never printed, logged, or exposed in exception messages.

# COMMAND ----------
dbutils.widgets.text("catalog",        "workspace",                                  "Unity Catalog Name")
dbutils.widgets.text("repo",           "ALL",                                        "Repository (owner/repo or 'ALL')")
dbutils.widgets.dropdown("entity",     "ALL", ["ALL", "commits", "issues", "pulls", "releases", "repo_metadata"], "Entity Name")
dbutils.widgets.dropdown("run_mode",   "full", ["full", "incremental", "backfill"], "Execution Run Mode")
dbutils.widgets.text("since",          "",                                           "Since Timestamp (ISO-8601, empty = default 2026-04-01 in full mode)")
dbutils.widgets.text("until",          "",                                           "Until Timestamp (ISO-8601, empty = unbounded)")
dbutils.widgets.text("batch_id",       "",                                           "Batch ID (empty = auto-generated)")
dbutils.widgets.text("out_root",       "/Volumes/workspace/bronze_data/raw_api",     "Full Mode Output Root Volume Path")
dbutils.widgets.text("landing_root",   "/Volumes/workspace/bronze_data/incremental", "Incremental/Backfill Landing Root Volume Path")
dbutils.widgets.text("github_token",   "",                                           "GitHub Token (fallback to secret scope 'dbtrends' or env)")
dbutils.widgets.dropdown("skip_existing", "false", ["false", "true"],                "Skip already downloaded files (resumption)")

# COMMAND ----------
import sys
import os
import json
import urllib.request
from datetime import datetime, timezone

# Ensure project root is in sys.path
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.common import log_run
from src.ingestion.github_client import (
    TRACKED_REPOS,
    ALL_ENTITIES,
    DEFAULT_SINCE,
    resolve_token,
    resolve_repos,
    normalise_since,
    repo_dir_slug,
    fetch_entity,
    write_json_atomic,
)

# 1. Parse widget inputs with fallbacks
catalog = dbutils.widgets.get("catalog").strip() or "workspace"
target_repo = dbutils.widgets.get("repo").strip() or "ALL"
target_entity = dbutils.widgets.get("entity").strip() or "ALL"
run_mode = dbutils.widgets.get("run_mode").strip().lower() or "full"
since_raw = dbutils.widgets.get("since").strip()
until_raw = dbutils.widgets.get("until").strip()
batch_id_raw = dbutils.widgets.get("batch_id").strip()
out_root = dbutils.widgets.get("out_root").strip() or f"/Volumes/{catalog}/bronze_data/raw_api"
landing_root = dbutils.widgets.get("landing_root").strip() or f"/Volumes/{catalog}/bronze_data/incremental"
skip_existing = dbutils.widgets.get("skip_existing").strip().lower() == "true"

batch_id = batch_id_raw or datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")

# Normalise window
if run_mode == "full" and not since_raw:
    since_iso = DEFAULT_SINCE
elif since_raw:
    since_iso = normalise_since(since_raw)
else:
    since_iso = None

until_iso = normalise_since(until_raw) if until_raw else None

# Resolve repos and entities
repos_to_run = resolve_repos(target_repo)
if target_entity.upper() == "ALL":
    entities_to_run = list(ALL_ENTITIES)
else:
    if target_entity not in ALL_ENTITIES:
        raise ValueError(f"Unknown entity '{target_entity}'. Must be 'ALL' or one of {ALL_ENTITIES}")
    entities_to_run = [target_entity]

print(f"Ingestion Configuration:")
print(f"  Catalog:         {catalog}")
print(f"  Repos ({len(repos_to_run)}):     {[f'{o}/{r}' for o, r in repos_to_run]}")
print(f"  Entities ({len(entities_to_run)}):  {entities_to_run}")
print(f"  Run Mode:        {run_mode}")
print(f"  Since:           {since_iso}")
print(f"  Until:           {until_iso or '(unbounded)'}")
print(f"  Batch ID:        {batch_id}")
print(f"  Out Root:        {out_root}")
print(f"  Landing Root:    {landing_root}")
print(f"  Skip Existing:   {skip_existing}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pre-Flight: Outbound Connectivity & Rate-Limit Quota Verification

# COMMAND ----------
# Resolve GitHub Token securely
token = resolve_token(dbutils, scope="dbtrends", key="github_token", widget="github_token")

# Test outbound network access and fetch rate limit quota
rate_limit_url = "https://api.github.com/rate_limit"
try:
    req = urllib.request.Request(rate_limit_url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("User-Agent", "dav-database-trends-ingest")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    with urllib.request.urlopen(req, timeout=15.0) as resp:
        rate_json = json.loads(resp.read().decode("utf-8"))
        core_rate = rate_json.get("resources", {}).get("core", {})
        remaining_quota = int(core_rate.get("remaining", 0))
        limit_quota = int(core_rate.get("limit", 0))
        reset_epoch = int(core_rate.get("reset", 0))
        reset_time_utc = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
except Exception as preflight_err:
    exc_type = type(preflight_err).__name__
    raise RuntimeError(
        f"Pre-flight failed: Databricks workspace appears to block outbound access to api.github.com "
        f"or the network connection timed out (Exception Type: {exc_type})."
    ) from None

# Estimate required API calls
def estimate_calls(num_repos: int, entities: list, mode: str) -> int:
    est = 0
    per_repo_mode = {
        "repo_metadata": 1,
        "releases": 2 if mode == "full" else 1,
        "commits": 30 if mode == "full" else 5,
        "issues": 30 if mode == "full" else 5,
        "pulls": 30 if mode == "full" else 5,
    }
    for e in entities:
        est += per_repo_mode.get(e, 10) * num_repos
    return est

estimated_calls = estimate_calls(len(repos_to_run), entities_to_run, run_mode)

print(f"Pre-flight Rate Limit Status:")
print(f"  GitHub Rate Limit:      {remaining_quota} / {limit_quota} remaining")
print(f"  Quota Resets At:        {reset_time_utc}")
print(f"  Estimated Scope Calls:  ~{estimated_calls} API calls")

if estimated_calls > remaining_quota:
    raise RuntimeError(
        f"Pre-flight rejected: Estimated {estimated_calls} calls exceed remaining quota of {remaining_quota}. "
        f"Quota resets at {reset_time_utc}. Reduce scope or wait for rate limit reset."
    )

print(f"  ✓ Pre-flight passed: Outbound API connectivity active and quota sufficient.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execution Plan

# COMMAND ----------
dest_base = out_root if run_mode == "full" else f"{landing_root.rstrip('/')}/{batch_id}"

print(f"==================================================")
print(f"EXECUTION PLAN: {len(repos_to_run)} repos x {len(entities_to_run)} entities = {len(repos_to_run) * len(entities_to_run)} tasks")
print(f"Run Mode:         {run_mode}")
print(f"Window:           {since_iso} -> {until_iso or '(unbounded)'}")
print(f"Destination Base: {dest_base}")
print(f"==================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Main Ingestion Loop

# COMMAND ----------
batch_results = []
total_records_written = 0

for owner, repo in repos_to_run:
    repo_full = f"{owner}/{repo}"
    repo_slug = repo_dir_slug(repo_full)
    repo_dest_dir = os.path.join(dest_base, repo_slug)
    os.makedirs(repo_dest_dir, exist_ok=True)

    print(f"\n==================================================")
    print(f"Ingesting Repository: {repo_full}")
    print(f"==================================================")

    for entity in entities_to_run:
        dest_file = os.path.join(repo_dest_dir, f"{entity}.json")
        param_desc = f"repo={repo_full},entity={entity},mode={run_mode},since={since_iso},until={until_iso},file={dest_file}"
        print(f" > [{entity}] -> {dest_file}")

        if skip_existing and os.path.exists(dest_file):
            print(f"   [Skipped] File already exists and skip_existing=true")
            with log_run(spark, layer="INGEST", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
                run_ctx["status"] = "SUCCESS"
                run_ctx["message"] = "SKIPPED_EXISTING"
                run_ctx["rows_inserted"] = 0
                run_ctx["rows_updated"] = 0

            batch_results.append({
                "repo": repo_full,
                "entity": entity,
                "status": "SKIPPED_EXISTING",
                "records": 0,
                "file": dest_file,
            })
            continue

        try:
            with log_run(spark, layer="INGEST", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
                records, stats = fetch_entity(
                    owner=owner,
                    repo=repo,
                    entity=entity,
                    run_mode=run_mode,
                    since_iso=since_iso,
                    until_iso=until_iso,
                    token=token,
                )

                # Atomically write to destination file
                write_json_atomic(dest_file, records)

                rec_count = 1 if isinstance(records, dict) else len(records)
                total_records_written += rec_count

                run_ctx["rows_inserted"] = rec_count
                run_ctx["rows_updated"] = 0
                run_ctx["message"] = f"pages={stats['pages']} api_calls={stats['api_calls']}"
                run_ctx["status"] = "SUCCESS"

                print(f"   ✓ Success: {rec_count} records (pages={stats['pages']}, api_calls={stats['api_calls']})")

                batch_results.append({
                    "repo": repo_full,
                    "entity": entity,
                    "status": "SUCCESS",
                    "records": rec_count,
                    "file": dest_file,
                    "pages": stats["pages"],
                    "api_calls": stats["api_calls"],
                })

        except Exception as exc:
            err_name = type(exc).__name__
            print(f"   ❌ FAILED: {err_name} - {str(exc)[:150]}")
            batch_results.append({
                "repo": repo_full,
                "entity": entity,
                "status": f"FAILED: {err_name}",
                "records": 0,
                "file": dest_file,
            })

# COMMAND ----------
# MAGIC %md
# MAGIC ### Ingestion Audit & Summary

# COMMAND ----------
print(f"\n==================================================")
print(f"INGESTION RUN FINISHED: {batch_id}")
print(f"Destination Root:     {dest_base}")
print(f"Total Records Landed: {total_records_written}")
print(f"==================================================")
print(f"{'REPOSITORY':<30} {'ENTITY':<15} {'RECORDS':<10} STATUS")
print(f"{'-'*70}")
for res in batch_results:
    print(f"{res['repo']:<30} {res['entity']:<15} {res['records']:<10} {res['status']}")
print(f"{'-'*70}")

# Display recent execution log records
try:
    log_tbl = f"{catalog}.ops.pipeline_execution_logs"
    display(
        spark.sql(f"""
            SELECT 
                log_id,
                layer,
                parameter,
                status,
                rows_inserted,
                rows_updated,
                start_time,
                end_time,
                error_message
            FROM {log_tbl}
            WHERE batch_id = '{batch_id}' AND layer = 'INGEST'
            ORDER BY start_time DESC
        """)
    )
except Exception as log_err:
    print(f"Could not query operational execution logs: {log_err}")

# Check for failures and report
failed_items = [r for r in batch_results if str(r.get("status", "")).startswith("FAILED")]

summary = {
    "batch_id": batch_id,
    "destination_path": dest_base,
    "total_records": total_records_written,
    "results": batch_results,
    "failures": len(failed_items),
}

if failed_items:
    try:
        dbutils.notebook.exit(json.dumps(summary))
    except Exception:
        pass
    raise RuntimeError(f"{len(failed_items)} entity ingestion task(s) failed: {failed_items}")

try:
    dbutils.notebook.exit(json.dumps(summary))
except Exception:
    pass
