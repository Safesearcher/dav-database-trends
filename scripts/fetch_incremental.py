"""
scripts/fetch_incremental.py — Incremental GitHub API Fetcher
=================================================================
Fetches updated records for one or more repos since a given timestamp
and writes them into samples/incremental/<owner>_<repo>/<entity>.json,
mirroring the same file layout as bronze/.

Pagination & retry logic is taken directly from full_load.py:
  - paginate_standard  : Link-header pagination with exponential back-off
  - paginate_pulls_updated_desc : fetch PRs sorted by updated desc, stop
    when updated_at < since (GitHub REST does not natively filter PRs
    by 'since', so this is the canonical approach)

Rate-limit handling: reads X-RateLimit-Remaining / X-RateLimit-Reset
headers and sleeps until reset when fewer than 3 calls remain.

Token: read ONLY from the GITHUB_TOKEN environment variable.
       No CLI flag, no file fallback. Set it before running:
           export GITHUB_TOKEN=ghp_...      (Linux/macOS)
           $env:GITHUB_TOKEN = "ghp_..."   (PowerShell)

Usage:
    python scripts/fetch_incremental.py --repos redis/redis qdrant/qdrant --since 2026-09-25
    python scripts/fetch_incremental.py --repos surrealdb/surrealdb --since 2026-09-25T00:00:00Z --out-dir samples/incremental
    python scripts/fetch_incremental.py --repos ALL --since 2026-09-25

Optional flags:
    --out-dir   Destination folder (default: samples/incremental)
    --dry-run   Print the API URLs that would be called; do not fetch
"""

import argparse
import http.client
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

# Ensure UTF-8 output on Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# All 10 tracked database repositories (owner, repo) pairs
# ---------------------------------------------------------------------------
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

PER_PAGE = 100
LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


# ---------------------------------------------------------------------------
# Token resolution (GITHUB_TOKEN env var ONLY — no CLI flags, no file fallback)
# ---------------------------------------------------------------------------
def get_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set.", file=sys.stderr)
        print("       export GITHUB_TOKEN=ghp_...  (bash/zsh)", file=sys.stderr)
        print("       $env:GITHUB_TOKEN = 'ghp_...' (PowerShell)", file=sys.stderr)
        sys.exit(1)
    return token


# ---------------------------------------------------------------------------
# Core HTTP helpers — taken from full_load.py and parameterised by token
# ---------------------------------------------------------------------------
def check_rate_limit(headers: dict, token: str) -> None:
    """Sleep until the rate-limit window resets when fewer than 3 calls remain."""
    remaining = int(headers.get("X-RateLimit-Remaining", "60"))
    reset_at = int(headers.get("X-RateLimit-Reset", str(int(time.time()) + 60)))
    if remaining <= 2:
        wait = max(reset_at - int(time.time()), 0) + 2
        print(f"    [Rate Limit] {remaining} calls left — sleeping {wait}s until reset...", flush=True)
        time.sleep(wait)


def fetch_page(url: str, token: str) -> tuple:
    """
    GETs one API page. Returns (data, headers_dict, link_header_str).
    Raises urllib.error.HTTPError on 4xx/5xx that should propagate.
    """
    import urllib.request
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("User-Agent", "dav-database-trends-incremental")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        headers = dict(resp.headers)
        return data, headers, headers.get("Link", "")


def paginate_standard(base_url: str, token: str, retries: int = 3) -> list:
    """
    Standard Link-header pagination (directly from full_load.py).
    Follows rel="next" links until exhausted. Handles rate-limit 403
    and transient network errors with exponential back-off.
    """
    import urllib.error
    all_records = []
    url = f"{base_url}{'&' if '?' in base_url else '?'}per_page={PER_PAGE}"
    page = 0

    while url:
        page += 1
        data = None
        for attempt in range(retries + 1):
            try:
                data, headers, link_header = fetch_page(url, token)
                break
            except urllib.error.HTTPError as exc:
                body = exc.read().decode(errors="replace")
                if exc.code == 403 and ("rate limit" in body.lower() or "secondary rate" in body.lower()):
                    check_rate_limit(dict(exc.headers), token)
                    continue
                print(f"    Page {page} HTTP {exc.code} on {url} — {body[:120]}", flush=True)
                return all_records
            except (urllib.error.URLError, http.client.IncompleteRead, ConnectionError) as exc:
                wait = 2 ** attempt
                print(f"    Page {page} transient {type(exc).__name__} (attempt {attempt + 1}/{retries}) — retry in {wait}s", flush=True)
                time.sleep(wait)
                if attempt == retries:
                    return all_records
        else:
            break   # all retries exhausted for this page

        if data is None:
            break

        # repo_metadata returns a dict, not a list
        if not isinstance(data, list):
            all_records.append(data)
            break

        all_records.extend(data)
        check_rate_limit(headers, token)

        m = LINK_RE.search(link_header)
        url = m.group(1) if m else None

        if not data:
            break

    return all_records


def paginate_pulls_updated_desc(base_url: str, since_iso: str, token: str, retries: int = 3) -> list:
    """
    Fetches pull requests sorted by updated desc and stops when updated_at < since.

    GitHub's Pulls API does not support a 'since' query parameter (unlike Issues/Commits).
    The canonical approach is to sort by updated:desc and early-exit as soon as we see
    a PR whose updated_at pre-dates our window — all subsequent pages are definitionally
    older, so we can safely discard them.
    """
    import urllib.error
    all_records = []
    url = (
        f"{base_url}?state=all"
        f"&sort=updated&direction=desc"
        f"&per_page={PER_PAGE}"
    )
    page = 0
    stop_fetching = False

    while url and not stop_fetching:
        page += 1
        data = None
        for attempt in range(retries + 1):
            try:
                data, headers, link_header = fetch_page(url, token)
                break
            except urllib.error.HTTPError as exc:
                body = exc.read().decode(errors="replace")
                if exc.code == 403 and ("rate limit" in body.lower() or "secondary rate" in body.lower()):
                    check_rate_limit(dict(exc.headers), token)
                    continue
                print(f"    Pulls page {page} HTTP {exc.code} — {body[:120]}", flush=True)
                return all_records
            except (urllib.error.URLError, http.client.IncompleteRead, ConnectionError) as exc:
                wait = 2 ** attempt
                print(f"    Pulls page {page} transient {type(exc).__name__} (attempt {attempt + 1}/{retries}) — retry in {wait}s", flush=True)
                time.sleep(wait)
                if attempt == retries:
                    return all_records
        else:
            break

        if data is None or not isinstance(data, list) or not data:
            break

        for pr in data:
            pr_updated = pr.get("updated_at", "")
            # Stop when updated_at < since — all later pages are older
            if pr_updated and pr_updated < since_iso:
                stop_fetching = True
                print(f"    [Pulls] updated_at {pr_updated!r} < since {since_iso!r} — stopping pagination.", flush=True)
                break
            all_records.append(pr)

        check_rate_limit(headers, token)
        m = LINK_RE.search(link_header)
        url = m.group(1) if m else None

    return all_records


# ---------------------------------------------------------------------------
# ISO date normalisation
# ---------------------------------------------------------------------------
def normalise_since(raw: str) -> str:
    """
    Accepts '2026-09-25', '2026-09-25T00:00:00', or '2026-09-25T00:00:00Z'.
    Always returns a full ISO-8601 UTC string ending in 'Z'.
    """
    raw = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw + "T00:00:00Z"
    if raw.endswith("Z"):
        return raw
    if "+" in raw:
        # Strip offset, treat as UTC approximation
        return raw.split("+")[0] + "Z"
    return raw + "Z"


# ---------------------------------------------------------------------------
# Per-repo fetch logic
# ---------------------------------------------------------------------------
def fetch_repo(owner: str, repo: str, since_iso: str, out_dir: str, token: str, dry_run: bool) -> dict:
    """Fetches all 5 entities for one repository and writes them to disk."""
    base = f"https://api.github.com/repos/{owner}/{repo}"
    slug = f"{owner}_{repo}"
    folder = os.path.join(out_dir, slug)

    if not dry_run:
        os.makedirs(folder, exist_ok=True)

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}=== {owner}/{repo} ===", flush=True)

    results = {}

    # ------------------------------------------------------------------
    # 1. repo_metadata  (single dict, no since filter)
    # ------------------------------------------------------------------
    url_meta = base
    if dry_run:
        print(f"  [DRY-RUN] GET {url_meta}")
    else:
        try:
            meta, meta_hdrs, _ = fetch_page(url_meta, token)
            check_rate_limit(meta_hdrs, token)
            _write(folder, "repo_metadata.json", meta)
            results["repo_metadata"] = 1
            print(f"  + repo_metadata.json (1 record)", flush=True)
        except Exception as exc:
            print(f"  ! repo_metadata failed: {exc}", flush=True)
            results["repo_metadata"] = 0

    # ------------------------------------------------------------------
    # 2. commits — GitHub Commits API supports ?since= natively
    # ------------------------------------------------------------------
    url_commits = f"{base}/commits?since={since_iso}"
    if dry_run:
        print(f"  [DRY-RUN] GET {url_commits}&per_page={PER_PAGE}")
    else:
        commits = paginate_standard(url_commits, token)
        _write(folder, "commits.json", commits)
        results["commits"] = len(commits)
        print(f"  + commits.json ({len(commits)} records)", flush=True)

    # ------------------------------------------------------------------
    # 3. issues — GitHub Issues API supports ?since= (filters by updated_at)
    # ------------------------------------------------------------------
    url_issues = f"{base}/issues?state=all&since={since_iso}&sort=updated&direction=desc"
    if dry_run:
        print(f"  [DRY-RUN] GET {url_issues}&per_page={PER_PAGE}")
    else:
        issues = paginate_standard(url_issues, token)
        _write(folder, "issues.json", issues)
        results["issues"] = len(issues)
        print(f"  + issues.json ({len(issues)} records)", flush=True)

    # ------------------------------------------------------------------
    # 4. pulls — no native 'since'; paginate updated:desc and early-exit
    # ------------------------------------------------------------------
    url_pulls = f"{base}/pulls"
    if dry_run:
        print(f"  [DRY-RUN] GET {url_pulls}?state=all&sort=updated&direction=desc&per_page={PER_PAGE}")
    else:
        pulls = paginate_pulls_updated_desc(url_pulls, since_iso, token)
        _write(folder, "pulls.json", pulls)
        results["pulls"] = len(pulls)
        print(f"  + pulls.json ({len(pulls)} records)", flush=True)

    # ------------------------------------------------------------------
    # 5. releases — no since filter available; fetch all and caller can trim
    # ------------------------------------------------------------------
    url_releases = f"{base}/releases"
    if dry_run:
        print(f"  [DRY-RUN] GET {url_releases}&per_page={PER_PAGE}")
    else:
        releases = paginate_standard(url_releases, token)
        # Filter to releases published >= since
        releases_filtered = [
            r for r in releases
            if r.get("published_at", "") >= since_iso
        ]
        _write(folder, "releases.json", releases_filtered)
        results["releases"] = len(releases_filtered)
        print(f"  + releases.json ({len(releases_filtered)} records, trimmed from {len(releases)} total)", flush=True)

    return results


def _write(folder: str, filename: str, data) -> None:
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental GitHub REST API fetcher — mirrors bronze/ layout into samples/incremental/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/fetch_incremental.py --repos redis/redis qdrant/qdrant --since 2026-09-25
  python scripts/fetch_incremental.py --repos ALL --since 2026-09-25T00:00:00Z
  python scripts/fetch_incremental.py --repos surrealdb/surrealdb --since 2026-09-25 --out-dir /tmp/incremental
  python scripts/fetch_incremental.py --repos redis/redis --since 2026-09-25 --dry-run

Token must be set via:
  export GITHUB_TOKEN=ghp_...       (bash/zsh)
  $env:GITHUB_TOKEN = 'ghp_...'    (PowerShell)
        """,
    )
    parser.add_argument(
        "--repos",
        nargs="+",
        required=True,
        metavar="OWNER/REPO",
        help="One or more 'owner/repo' slugs, or 'ALL' for all 10 tracked repos.",
    )
    parser.add_argument(
        "--since",
        required=True,
        metavar="ISO_DATE",
        help="Fetch records updated/created on or after this date. Accepts YYYY-MM-DD or full ISO-8601.",
    )
    parser.add_argument(
        "--out-dir",
        default="samples/incremental",
        metavar="PATH",
        help="Destination directory (default: samples/incremental).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the API URLs that would be called; do not fetch or write anything.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    since_iso = normalise_since(args.since)
    token = get_token()

    # Resolve repo list
    if len(args.repos) == 1 and args.repos[0].upper() == "ALL":
        repos = ALL_REPOS
    else:
        repos = []
        for slug in args.repos:
            if "/" not in slug:
                print(f"Error: '{slug}' is not in 'owner/repo' format.", file=sys.stderr)
                sys.exit(1)
            owner, repo = slug.split("/", 1)
            repos.append((owner, repo))

    out_dir = args.out_dir
    if not args.dry_run:
        os.makedirs(out_dir, exist_ok=True)

    print(f"Incremental Fetch")
    print(f"  Since:    {since_iso}")
    print(f"  Out dir:  {out_dir}")
    print(f"  Repos:    {[f'{o}/{r}' for o, r in repos]}")
    print(f"  Dry run:  {args.dry_run}")

    run_start = datetime.now(timezone.utc)
    grand_total = 0
    summary = []

    for owner, repo in repos:
        try:
            counts = fetch_repo(
                owner=owner,
                repo=repo,
                since_iso=since_iso,
                out_dir=out_dir,
                token=token,
                dry_run=args.dry_run,
            )
            row_total = sum(counts.values())
            grand_total += row_total
            summary.append((f"{owner}/{repo}", counts, "OK"))
        except Exception as exc:
            print(f"  ! FAILED {owner}/{repo}: {exc}", flush=True)
            summary.append((f"{owner}/{repo}", {}, f"FAILED: {exc}"))

    elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()

    print(f"\n{'=' * 70}")
    print(f"INCREMENTAL FETCH SUMMARY — since={since_iso}")
    print(f"{'=' * 70}")
    print(f"{'REPO':<30} {'COMMITS':<9} {'ISSUES':<8} {'PULLS':<7} {'RELEASES':<10} STATUS")
    print("-" * 70)
    for repo_name, counts, status in summary:
        print(
            f"{repo_name:<30} "
            f"{counts.get('commits', '-'):<9} "
            f"{counts.get('issues', '-'):<8} "
            f"{counts.get('pulls', '-'):<7} "
            f"{counts.get('releases', '-'):<10} "
            f"{status}"
        )
    print("-" * 70)
    print(f"Total records fetched: {grand_total}  |  Elapsed: {elapsed:.1f}s")
    print(f"Output written to:     {out_dir}/")


if __name__ == "__main__":
    main()
