# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00 - Ingest Raw GitHub Data into Unity Catalog Volume
# MAGIC
# MAGIC **Capabilities & Architecture:**
# MAGIC 1. **Serverless & In-Databricks Execution:** Ingests raw JSON directly from GitHub REST API into Unity Catalog Volumes.
# MAGIC 2. **Pre-Flight Validation:** Probes outbound connectivity to api.github.com and checks rate-limit quota against estimated consumption.
# MAGIC 3. **Configurable Run Modes:**
# MAGIC    - `full`: Uses target window (>= 2026-04-01T00:00:00Z) and lands at `<out_root>/<owner>_<repo>/<entity>.json`
# MAGIC    - `incremental` / `backfill`: Respects `since` and `until` boundaries and lands at `<landing_root>/<batch_id>/<owner>_<repo>/<entity>.json`
# MAGIC 4. **Resilience & Resumption:** `skip_existing=true` safely skips completed entities on rerun.
# MAGIC 5. **Audit Governance:** Every entity run logs to `ops.pipeline_execution_logs` with layer `'INGEST'`.
# MAGIC 6. **Credential Secrecy:** Token resolved via secret scope `dbtrends` or widget; never leaked in logs, code, or outputs.

# COMMAND ----------
# ── Global Widgets ──────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "workspace", "Unity Catalog Name")
dbutils.widgets.text("repo", "ALL", "Repository (owner/repo or 'ALL')")
dbutils.widgets.dropdown("entity", "ALL", ["ALL", "commits", "issues", "pulls", "releases", "repo_metadata"], "Entity Name")
dbutils.widgets.dropdown("run_mode", "full", ["full", "incremental", "backfill"], "Execution Run Mode")
dbutils.widgets.text("since", "", "Since Timestamp (ISO-8601, empty = default)")
dbutils.widgets.text("until", "", "Until Timestamp (ISO-8601, empty = unbounded)")
dbutils.widgets.text("batch_id", "", "Batch Run ID (empty = auto-generated)")
dbutils.widgets.text("out_root", "/Volumes/workspace/bronze_data/raw_api", "Full Load Volume Storage Root")
dbutils.widgets.text("landing_root", "/Volumes/workspace/bronze_data/incremental", "Incremental/Backfill Storage Root")
dbutils.widgets.text("github_token", "", "GitHub API Token (optional fallback; secret scope preferred)")
dbutils.widgets.dropdown("skip_existing", "false", ["false", "true"], "Skip already downloaded entities")

# COMMAND ----------
import json
import os
import sys
from datetime import datetime, timezone

# Ensure project root is in sys.path
workspace_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

from src.common import log_run
from src.ingestion.github_client import (
    DEFAULT_SINCE,
    fetch_entity,
    fetch_page,
    normalise_since,
    repo_dir_slug,
    resolve_repo,
    resolve_token,
    write_json_atomic,
)

# Parse parameters from widgets
catalog = dbutils.widgets.get("catalog").strip() or "workspace"
repo_widget = dbutils.widgets.get("repo").strip() or "ALL"
entity_widget = dbutils.widgets.get("entity").strip() or "ALL"
run_mode = dbutils.widgets.get("run_mode").strip().lower() or "full"
since_raw = dbutils.widgets.get("since").strip()
until_raw = dbutils.widgets.get("until").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")
out_root = dbutils.widgets.get("out_root").strip() or "/Volumes/workspace/bronze_data/raw_api"
landing_root = dbutils.widgets.get("landing_root").strip() or "/Volumes/workspace/bronze_data/incremental"
skip_existing = dbutils.widgets.get("skip_existing").strip().lower() in ("true", "1", "yes")

# Validate date window
if since_raw:
    since_iso = normalise_since(since_raw)
elif run_mode == "full":
    since_iso = DEFAULT_SINCE
else:
    since_iso = None

until_iso = normalise_since(until_raw) if until_raw else None

# Resolve target repositories
target_repos = resolve_repo(repo_widget)

# Resolve target entities
ALL_ENTITIES = ["repo_metadata", "commits", "issues", "pulls", "releases"]
if entity_widget.upper() == "ALL":
    target_entities = list(ALL_ENTITIES)
elif entity_widget in ALL_ENTITIES:
    target_entities = [entity_widget]
else:
    raise ValueError(f"Unknown entity {entity_widget!r}. Supported: 'ALL' or {ALL_ENTITIES}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pre-Flight: Outbound Connectivity & Rate-Limit Quota Validation

# COMMAND ----------
# Resolve GitHub API token securely
try:
    token = resolve_token(dbutils=dbutils)
    print("✓ GitHub API token resolved successfully (credentials masked)")
except Exception as tok_err:
    raise ValueError(f"Authentication setup failed: {tok_err}")

# Check outbound network connectivity to api.github.com
RATE_LIMIT_URL = "https://api.github.com/rate_limit"
try:
    rate_data, rate_headers = fetch_page(RATE_LIMIT_URL, token, timeout=15.0)
except Exception as preflight_err:
    err_type = type(preflight_err).__name__
    raise RuntimeError(
        f"Workspace appears to block outbound access to api.github.com (error: {err_type}). "
        "Verify network security group (NSG) and egress firewall rules."
    )

core_limits = rate_data.get("resources", {}).get("core", {})
remaining_quota = int(core_limits.get("remaining", 0))
total_limit = int(core_limits.get("limit", 5000))
reset_epoch = int(core_limits.get("reset", 0))
reset_str = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if reset_epoch else "unknown"

print("Pre-flight status: 200 OK")
print(f"GitHub API Rate Limit: {remaining_quota} / {total_limit} remaining (resets at {reset_str})")

# Estimate required API calls
calls_per_repo_full = {
    "repo_metadata": 1,
    "releases": 2,
    "commits": 20,
    "issues": 30,
    "pulls": 15,
}
calls_per_repo_incr = {
    "repo_metadata": 1,
    "releases": 2,
    "commits": 5,
    "issues": 5,
    "pulls": 5,
}

call_weights = calls_per_repo_full if run_mode == "full" else calls_per_repo_incr
estimated_calls = len(target_repos) * sum(call_weights.get(e, 5) for e in target_entities)

print(f"Estimated API calls for this run: ~{estimated_calls} calls ({len(target_repos)} repos x {len(target_entities)} entities in {run_mode} mode)")

if estimated_calls > remaining_quota:
    raise RuntimeError(
        f"Pre-flight quota check failed: estimated requirement (~{estimated_calls} calls) "
        f"exceeds remaining GitHub quota ({remaining_quota} calls). "
        "Reduce scope (target fewer repos/entities) or wait until rate-limit reset."
    )
print("✓ Quota validation passed: sufficient rate limit quota available.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execution Plan

# COMMAND ----------
# Determine target output root directory
if run_mode == "full":
    destination_root = out_root.rstrip("/")
else:
    destination_root = f"{landing_root.rstrip('/')}/{batch_id}"

print("======================================================================")
print("GITHUB API INGESTION EXECUTION PLAN")
print("======================================================================")
print(f"Run Mode:          {run_mode}")
print(f"Batch ID:          {batch_id}")
print(f"Catalog:           {catalog}")
print(f"Destination Root:  {destination_root}")
print(f"Since Window:      {since_iso or '(none)'}")
print(f"Until Window:      {until_iso or '(none)'}")
print(f"Skip Existing:     {skip_existing}")
print(f"Target Repos ({len(target_repos)}):     {', '.join([f'{o}/{r}' for o, r in target_repos])}")
print(f"Target Entities ({len(target_entities)}):  {', '.join(target_entities)}")
print("======================================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Main Ingestion Loop

# COMMAND ----------
batch_results = []
total_records_ingested = 0

for owner, repo in target_repos:
    repo_full = f"{owner}/{repo}"
    slug = repo_dir_slug(owner, repo)
    repo_dir = f"{destination_root}/{slug}"

    print(f"\n=== Ingesting {repo_full} ===")

    for entity in target_entities:
        target_file = f"{repo_dir}/{entity}.json"
        param_desc = f"repo={repo_full},entity={entity},mode={run_mode},since={since_iso or ''},until={until_iso or ''},file={target_file}"

        file_already_exists = False
        try:
            if os.path.exists(target_file):
                file_already_exists = True
            elif target_file.startswith("/Volumes"):
                try:
                    stats = dbutils.fs.ls(target_file)
                    if stats and stats[0].size > 0:
                        file_already_exists = True
                except Exception:
                    pass
        except Exception:
            pass

        if skip_existing and file_already_exists:
            print(f"  ⏭ [{entity}] File exists ({target_file}). Skipping.")
            with log_run(spark, layer="INGEST", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
                run_ctx["status"] = "SUCCESS"
                run_ctx["rows_inserted"] = 0
                run_ctx["rows_updated"] = 0
                run_ctx["message"] = "SKIPPED_EXISTING"
            batch_results.append({
                "repo": repo_full,
                "entity": entity,
                "status": "SUCCESS (SKIPPED_EXISTING)",
                "records": 0,
                "file": target_file,
            })
            continue

        with log_run(spark, layer="INGEST", parameter=param_desc, batch_id=batch_id, catalog=catalog) as run_ctx:
            try:
                records, stats = fetch_entity(
                    owner=owner,
                    repo=repo,
                    entity=entity,
                    run_mode=run_mode,
                    since_iso=since_iso,
                    until_iso=until_iso,
                    token=token,
                )

                write_json_atomic(target_file, records)

                rec_count = 1 if entity == "repo_metadata" and isinstance(records, dict) else len(records)
                total_records_ingested += rec_count

                run_ctx["rows_inserted"] = rec_count
                run_ctx["rows_updated"] = 0
                run_ctx["message"] = f"pages={stats['pages']} api_calls={stats['api_calls']}"
                run_ctx["status"] = "SUCCESS"

                print(f"  ✓ [{entity}] {rec_count} records ({stats['pages']} pages, {stats['api_calls']} api calls) -> {target_file}")
                batch_results.append({
                    "repo": repo_full,
                    "entity": entity,
                    "status": "SUCCESS",
                    "records": rec_count,
                    "file": target_file,
                })
            except Exception as e:
                run_ctx["status"] = "FAILURE"
                clean_err = f"{type(e).__name__}: {str(e)[:150]}"
                run_ctx["message"] = clean_err
                print(f"  ❌ [{entity}] FAILED: {clean_err}")
                batch_results.append({
                    "repo": repo_full,
                    "entity": entity,
                    "status": f"FAILED: {clean_err}",
                    "records": 0,
                    "file": target_file,
                })

print("\n======================================================================")
print(f"INGESTION RUN FINISHED: batch_id={batch_id}")
print(f"Total Records Ingested: {total_records_ingested}")
print("======================================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Summary Table and Notebook Exit

# COMMAND ----------
failed_entities = [r for r in batch_results if str(r.get("status", "")).startswith("FAILED")]

summary_payload = {
    "batch_id": batch_id,
    "destination_root": destination_root,
    "total_records": total_records_ingested,
    "results": batch_results,
}

if failed_entities:
    print(f"❌ {len(failed_entities)} entity ingestion(s) failed out of {len(batch_results)} attempted.")
    raise RuntimeError(f"{len(failed_entities)} entity ingestion(s) failed: {failed_entities}")

try:
    dbutils.notebook.exit(json.dumps(summary_payload))
except Exception:
    pass
