"""
Phase 2 full-load extractor — pulls REAL historical depth (not a 30-record sample)
across all 10 tracked repositories, using proper Link-header pagination.

This is different from fetch_samples.py (which deliberately capped at 30 records
per endpoint for the Phase 1 sample files). This script is meant to feed the
actual Bronze layer.

Usage:
    python full_load.py YOUR_TOKEN

Output:
    bronze/<owner>_<repo>/<entity>.json   (one folder per repo)

Tune MAX_PAGES_PER_ENDPOINT below if you want more/less history. At 100 records
per page, MAX_PAGES_PER_ENDPOINT=20 means up to 2,000 records per endpoint per
repo — plenty for weekly/monthly trend aggregation without burning your whole
5,000/hour rate limit budget across all 10 repos in one run.
"""

import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

TOKEN = sys.argv[1] if len(sys.argv) > 1 else None
if not TOKEN:
    print("Usage: python full_load.py YOUR_TOKEN")
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

MAX_PAGES_PER_ENDPOINT = 20  # 20 pages x 100/page = up to 2,000 records/endpoint/repo
PER_PAGE = 100
OUT_DIR = "bronze"

LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def check_rate_limit(headers):
    remaining = int(headers.get("X-RateLimit-Remaining", "1"))
    reset_at = int(headers.get("X-RateLimit-Reset", "0"))
    if remaining <= 1:
        wait = max(reset_at - int(time.time()), 0) + 2
        print(f"    rate limit nearly exhausted — sleeping {wait}s until reset")
        time.sleep(wait)


def fetch_page(url):
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {TOKEN}")
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        headers = dict(resp.headers)
        link_header = headers.get("Link", "")
        return data, headers, link_header


def paginate(base_url, max_pages=MAX_PAGES_PER_ENDPOINT, retries=3):
    """Follow the Link header until exhausted or max_pages reached."""
    all_records = []
    url = f"{base_url}{'&' if '?' in base_url else '?'}per_page={PER_PAGE}"
    page = 0
    while url and page < max_pages:
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
                print(f"    page {page} FAILED: HTTP {e.code} — {body[:150]}")
                return all_records
            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
            ) as e:
                print(
                    f"    page {page} transient error ({type(e).__name__}), retrying..."
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


def main():
    for owner, repo in REPOS:
        base = f"https://api.github.com/repos/{owner}/{repo}"
        folder = os.path.join(OUT_DIR, f"{owner}_{repo}")
        os.makedirs(folder, exist_ok=True)
        print(f"\n=== {owner}/{repo} ===")

        meta, _, _ = fetch_page(base)
        with open(os.path.join(folder, "repo_metadata.json"), "w") as f:
            json.dump(meta, f, indent=2)
        print("  repo_metadata.json (1 record)")

        for entity, path in [
            ("commits", f"{base}/commits"),
            ("issues", f"{base}/issues?state=all"),
            ("pulls", f"{base}/pulls?state=all"),
            ("releases", f"{base}/releases"),
        ]:
            records = paginate(path)
            with open(os.path.join(folder, f"{entity}.json"), "w") as f:
                json.dump(records, f, indent=2)
            print(f"  {entity}.json ({len(records)} records)")

    print("\nDone. Real historical data is now in bronze/<owner>_<repo>/")


if __name__ == "__main__":
    main()
