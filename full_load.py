"""
Phase 2 Time-Bounded Full Ingestion Script
Overwrites raw Bronze data in `bronze/` with a balanced 6-month historical window:
Target Window: 2026-04-01T00:00:00Z to Present

Usage:
    python full_load.py YOUR_TOKEN
"""

import http.client
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request

# Ensure UTF-8 stdout on Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

TOKEN = sys.argv[1] if len(sys.argv) > 1 else None
if not TOKEN:
    token_file = os.path.join(os.path.dirname(__file__), "github_api.txt")
    if os.path.exists(token_file):
        with open(token_file, "r", encoding="utf-8") as f:
            TOKEN = f.read().strip()
    elif os.environ.get("GITHUB_TOKEN"):
        TOKEN = os.environ.get("GITHUB_TOKEN").strip()

if not TOKEN:
    print("Error: GitHub Token required. Usage: python full_load.py YOUR_TOKEN")
    sys.exit(1)

REPOS = [
    ("postgres", "postgres"),
    ("cockroachdb", "cockroach"),
    ("mongodb", "mongo"),
    ("surrealdb", "surrealdb"),
    ("redis", "redis"),
    ("apache", "cassandra"),
    ("facebookresearch", "faiss"),
    ("qdrant", "qdrant"),
    ("milvus-io", "milvus"),
    ("chroma-core", "chroma"),
]

SINCE_DATE = "2026-04-01T00:00:00Z"
PER_PAGE = 100
OUT_DIR = "bronze"
LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def check_rate_limit(headers):
    remaining = int(headers.get("X-RateLimit-Remaining", "1"))
    reset_at = int(headers.get("X-RateLimit-Reset", "0"))
    if remaining <= 2:
        wait = max(reset_at - int(time.time()), 0) + 2
        print(f"    [Rate Limit Warning] Sleeping {wait}s until reset...")
        time.sleep(wait)


def fetch_page(url):
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("User-Agent", "dav-database-trends")
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        headers = dict(resp.headers)
        link_header = headers.get("Link", "")
        return data, headers, link_header


def paginate_standard(base_url, retries=3):
    """Standard Link-header pagination for endpoints supporting filtering."""
    all_records = []
    url = f"{base_url}{'&' if '?' in base_url else '?'}per_page={PER_PAGE}"
    page = 0
    while url:
        page += 1
        for attempt in range(retries + 1):
            try:
                data, headers, link_header = fetch_page(url)
                break
            except urllib.error.HTTPError as e:
                body = e.read().decode(errors="replace")
                if e.code == 403 and "rate limit" in body.lower():
                    check_rate_limit(dict(e.headers))
                    continue
                print(f"    Page {page} HTTP {e.code} — {body[:100]}")
                return all_records
            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
            ) as e:
                print(
                    f"    Page {page} transient error ({type(e).__name__}), retrying ({attempt + 1}/{retries})..."
                )
                time.sleep(2)
                if attempt == retries:
                    return all_records
        else:
            break

        if not isinstance(data, list):
            all_records.append(data)
            break

        all_records.extend(data)
        check_rate_limit(headers)

        m = LINK_RE.search(link_header)
        url = m.group(1) if m else None
        if not data:
            break

    return all_records


def paginate_pulls_timebounded(base_url, since_iso, retries=3):
    """Time-bounded pagination for Pull Requests endpoint using created_at date cutoff."""
    all_records = []
    url = f"{base_url}?state=all&sort=created&direction=desc&per_page={PER_PAGE}"
    page = 0
    stop_fetching = False

    while url and not stop_fetching:
        page += 1
        for attempt in range(retries + 1):
            try:
                data, headers, link_header = fetch_page(url)
                break
            except urllib.error.HTTPError as e:
                body = e.read().decode(errors="replace")
                if e.code == 403 and "rate limit" in body.lower():
                    check_rate_limit(dict(e.headers))
                    continue
                print(f"    Pulls page {page} HTTP {e.code}")
                return all_records
            except (urllib.error.URLError, http.client.IncompleteRead, ConnectionError):
                time.sleep(2)
                if attempt == retries:
                    return all_records
        else:
            break

        if not isinstance(data, list) or not data:
            break

        for pr in data:
            created_at = pr.get("created_at", "")
            if created_at and created_at < since_iso:
                stop_fetching = True
                break
            all_records.append(pr)

        check_rate_limit(headers)
        m = LINK_RE.search(link_header)
        url = m.group(1) if m else None

    return all_records


def main():
    print(f"Starting Bronze Ingestion — Target Window: >= {SINCE_DATE}")

    # Clean and overwrite bronze directory
    if os.path.exists(OUT_DIR):
        print(f"Purging existing '{OUT_DIR}' directory for clean overwrite...")
        shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR, exist_ok=True)

    for owner, repo in REPOS:
        base = f"https://api.github.com/repos/{owner}/{repo}"
        folder = os.path.join(OUT_DIR, f"{owner}_{repo}")
        os.makedirs(folder, exist_ok=True)
        print(f"\n=== Fetching: {owner}/{repo} ===", flush=True)

        # 1. Repo Metadata
        try:
            meta, _, _ = fetch_page(base)
            with open(
                os.path.join(folder, "repo_metadata.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(meta, f, indent=2)
            print("  ✓ repo_metadata.json", flush=True)
        except Exception as e:
            print(f"  ✗ Metadata failed: {e}")

        # 2. Commits (Time-Bounded)
        commits = paginate_standard(f"{base}/commits?since={SINCE_DATE}")
        with open(os.path.join(folder, "commits.json"), "w", encoding="utf-8") as f:
            json.dump(commits, f, indent=2)
        print(f"  ✓ commits.json ({len(commits)} records)", flush=True)

        # 3. Issues (Time-Bounded)
        issues = paginate_standard(f"{base}/issues?state=all&since={SINCE_DATE}")
        with open(os.path.join(folder, "issues.json"), "w", encoding="utf-8") as f:
            json.dump(issues, f, indent=2)
        print(f"  ✓ issues.json ({len(issues)} records)", flush=True)

        # 4. Pull Requests (Time-Bounded Cutoff)
        pulls = paginate_pulls_timebounded(f"{base}/pulls", SINCE_DATE)
        with open(os.path.join(folder, "pulls.json"), "w", encoding="utf-8") as f:
            json.dump(pulls, f, indent=2)
        print(f"  ✓ pulls.json ({len(pulls)} records)", flush=True)

        # 5. Releases
        releases = paginate_standard(f"{base}/releases")
        with open(os.path.join(folder, "releases.json"), "w", encoding="utf-8") as f:
            json.dump(releases, f, indent=2)
        print(f"  ✓ releases.json ({len(releases)} records)", flush=True)

    print("\nIngestion complete. Updated Bronze data written to bronze/")


if __name__ == "__main__":
    main()
