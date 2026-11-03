"""
scripts/fetch_stars_history.py — Weekly Stargazers History Extractor
===================================================================
Extracts weekly star counts and daily arrays from the GitHub API:
    GET /repos/{owner}/{repo}/stargazers/history

Replaces the obsolete /stargazers (star+json) endpoint with GitHub's pre-aggregated
weekly star endpoint (returns ~30 weeks per page, up to 100 pages, with zero PII).

Modes:
  --mode full        : Fetches all pages via Link pagination (all-time lifetime history).
                       Takes exactly 204 requests total across all 10 repositories.
  --mode incremental : Fetches page 1 only (30 trailing weeks buffer).
                       Takes exactly 10 requests total across all 10 repositories.

Authentication:
  Reads GITHUB_TOKEN from environment variable. If not set, warns and attempts
  unauthenticated requests (subject to 60 req/hr limit).

Usage:
  uv run python scripts/fetch_stars_history.py --mode full --out-dir bronze
  uv run python scripts/fetch_stars_history.py --mode incremental --out-dir bronze
  uv run python scripts/fetch_stars_history.py --repos qdrant/qdrant --mode full
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ALL_REPOS = [
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

LINK_RE = re.compile(r'<([^>]+)>;\s*rel="([^"]+)"')


def parse_next_link(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        m = LINK_RE.search(part)
        if m:
            url, rel = m.group(1), m.group(2).strip()
            rels = [r.strip().lower() for r in rel.split()]
            if "next" in rels:
                return url
    return None


def fetch_stargazers_history_repo(
    owner: str,
    repo: str,
    token: Optional[str] = None,
    mode: str = "full",
    max_retries: int = 4,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    base_url = f"https://api.github.com/repos/{owner}/{repo}/stargazers/history"
    current_url: Optional[str] = base_url
    all_weeks: List[Dict[str, Any]] = []
    page_count = 0
    repo_full_name = f"{owner}/{repo}"

    last_headers: Dict[str, str] = {}

    while current_url:
        page_count += 1
        req = urllib.request.Request(current_url)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "dav-database-trends-stars")
        if token:
            req.add_header("Authorization", f"Bearer {token}")

        resp_data = None
        for attempt in range(1, max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=20.0) as resp:
                    last_headers = {k: v for k, v in resp.headers.items()}
                    raw_body = resp.read().decode("utf-8")
                    resp_data = json.loads(raw_body)
                    break
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                remaining = e.headers.get("X-RateLimit-Remaining", "0")
                if e.code in (403, 429) and remaining == "0":
                    reset_time = int(e.headers.get("X-RateLimit-Reset", str(int(time.time()) + 60)))
                    sleep_sec = max(reset_time - int(time.time()) + 2, 5)
                    print(f"    [Rate limit reached] Sleeping {sleep_sec}s until reset...")
                    time.sleep(sleep_sec)
                    continue
                elif e.code in (500, 502, 503, 504):
                    backoff = 2 ** attempt
                    print(f"    [HTTP {e.code}] Retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue
                else:
                    raise RuntimeError(f"HTTP {e.code} on {current_url}: {body[:200]}") from e
            except urllib.error.URLError as e:
                backoff = 2 ** attempt
                print(f"    [Network error] {e.reason}, retrying in {backoff}s...")
                time.sleep(backoff)

        if resp_data is None:
            raise RuntimeError(f"Failed to fetch {current_url} after {max_retries} retries")

        if isinstance(resp_data, list):
            for item in resp_data:
                if isinstance(item, dict):
                    all_weeks.append({
                        "week": item.get("week"),
                        "total": item.get("total", 0),
                        "days": item.get("days", []),
                        "repo_full_name": repo_full_name,
                    })

        if mode == "incremental":
            break

        current_url = parse_next_link(last_headers.get("Link"))

    stats = {
        "pages": page_count,
        "weeks": len(all_weeks),
        "rate_limit": last_headers.get("X-RateLimit-Limit", "unknown"),
        "rate_remaining": last_headers.get("X-RateLimit-Remaining", "unknown"),
        "rate_reset": last_headers.get("X-RateLimit-Reset", "unknown"),
    }
    return all_weeks, stats


def write_json_atomic(dest_path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
    tmp_path = dest_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, dest_path)


def main():
    parser = argparse.ArgumentParser(description="Fetch weekly stargazers history from GitHub API")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="full (all pages) or incremental (page 1 only)")
    parser.add_argument("--repos", nargs="+", default=["ALL"], help="List of repos (owner/repo) or ALL")
    parser.add_argument("--out-dir", default="bronze", help="Destination root directory (default: bronze)")
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("[Warning] GITHUB_TOKEN not set in environment. Running unauthenticated (60 req/hr limit).")

    if args.repos == ["ALL"] or "ALL" in args.repos:
        target_repos = ALL_REPOS
    else:
        target_repos = []
        for r in args.repos:
            parts = r.split("/")
            if len(parts) == 2:
                target_repos.append((parts[0], parts[1]))
            else:
                print(f"[Error] Invalid repo format {r!r}, expected owner/repo")
                sys.exit(1)

    print("=" * 80)
    print(f"FETCHING STARGAZERS HISTORY (mode={args.mode}, repos={len(target_repos)})")
    print("=" * 80)

    summary_rows = []
    for owner, repo in target_repos:
        slug = f"{owner}_{repo}"
        out_file = os.path.join(args.out_dir, slug, "stars_history.json")
        print(f"--> [{owner}/{repo}] fetching stargazers history...")
        try:
            weeks, stats = fetch_stargazers_history_repo(owner, repo, token=token, mode=args.mode)
            write_json_atomic(out_file, weeks)
            print(f"    [OK] {len(weeks)} weeks across {stats['pages']} page(s) -> {out_file}")
            print(f"         RateLimit Remaining: {stats['rate_remaining']}/{stats['rate_limit']}")
            summary_rows.append((f"{owner}/{repo}", stats["pages"], len(weeks), stats["rate_remaining"], "SUCCESS"))
        except Exception as e:
            print(f"    [FAIL] Error for {owner}/{repo}: {e}")
            summary_rows.append((f"{owner}/{repo}", 0, 0, "N/A", f"FAIL: {e}"))

    print("\n" + "=" * 80)
    print(f"{'Repository':<25} {'Pages':<8} {'Weeks':<8} {'Rate Remaining':<16} {'Status':<10}")
    print("-" * 80)
    for repo, pages, weeks, remaining, status in summary_rows:
        print(f"{repo:<25} {pages:<8} {weeks:<8} {remaining:<16} {status:<10}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()

