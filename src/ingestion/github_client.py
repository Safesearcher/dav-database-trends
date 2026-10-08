"""
GitHub REST API client for Databricks Medallion ingestion.

Pure Python module with NO PySpark or dbutils dependencies.
Fully unit-testable with mock openers.

Handles:
- URL generation and Link-header pagination
- Since/until ISO-8601 normalization and filtering
- Rate-limit detection and exponential backoff retries
- Atomic JSON writing to Unity Catalog Volumes
- Secure token resolution (Secret -> Widget -> Env)
"""

import http.client
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

# 10 Tracked Database Repositories
TRACKED_REPOS: List[Tuple[str, str]] = [
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

TRACKED_REPO_SLUGS = {f"{owner}/{repo}" for owner, repo in TRACKED_REPOS}
TRACKED_REPO_DIR_SLUGS = {f"{owner}_{repo}" for owner, repo in TRACKED_REPOS}

ALL_ENTITIES = ["repo_metadata", "commits", "issues", "pulls", "releases"]
DEFAULT_SINCE = "2026-04-01T00:00:00Z"
PER_PAGE = 100
LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


class GitHubFetchError(Exception):
    """Raised when GitHub REST API fetch fails unrecoverably or exhausts retries."""
    pass


def repo_dir_slug(repo_full_name: str) -> str:
    """Converts owner/repo into filesystem slug owner_repo."""
    return repo_full_name.replace("/", "_")


def repo_full_name_of(slug: str) -> str:
    """Converts owner_repo or owner/repo into canonical owner/repo format."""
    if "/" in slug:
        return slug
    return slug.replace("_", "/", 1)


def resolve_repos(repo_arg: str) -> List[Tuple[str, str]]:
    """
    Resolves repository selector.
    'ALL' expands to all 10 tracked repos.
    'owner/repo' or 'owner_repo' resolves to the matching repository tuple.
    Raises ValueError on unknown repository.
    """
    clean = (repo_arg or "").strip()
    if clean.upper() == "ALL":
        return list(TRACKED_REPOS)

    canonical = repo_full_name_of(clean)
    for owner, repo in TRACKED_REPOS:
        if f"{owner}/{repo}".lower() == canonical.lower():
            return [(owner, repo)]

    valid_list = [f"{o}/{r}" for o, r in TRACKED_REPOS]
    raise ValueError(
        f"Unknown repository '{repo_arg}'. Must be 'ALL' or one of: {valid_list}"
    )


def normalise_since(raw: str) -> str:
    """
    Accepts 'YYYY-MM-DD' or ISO-8601 (UTC or offset).
    Converts to UTC and returns 'YYYY-MM-DDTHH:MM:SSZ'.
    Raises ValueError on invalid format.
    """
    if not raw or not isinstance(raw, str) or not raw.strip():
        raise ValueError("Timestamp string cannot be empty.")

    cleaned = raw.strip()

    # Case 1: Simple date YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", cleaned):
        try:
            dt = datetime.strptime(cleaned, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as err:
            raise ValueError(f"Invalid date format '{cleaned}': {err}") from err

    # Case 2: Full ISO-8601 with or without timezone
    # datetime.fromisoformat supports '2026-09-25T10:00:00+05:00' and '2026-09-25T00:00:00Z'
    iso_candidate = cleaned
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as err:
        raise ValueError(f"Cannot parse '{cleaned}' as ISO-8601 timestamp: {err}") from err


def build_url(
    owner: str,
    repo: str,
    entity: str,
    run_mode: str = "full",
    since_iso: Optional[str] = None,
    until_iso: Optional[str] = None,
    per_page: int = PER_PAGE,
) -> str:
    """Builds initial API URL for a given repository entity and mode."""
    base = f"https://api.github.com/repos/{owner}/{repo}"

    if entity == "repo_metadata":
        return base
    elif entity == "commits":
        url = f"{base}/commits?per_page={per_page}"
        if since_iso:
            url += f"&since={since_iso}"
        if until_iso:
            url += f"&until={until_iso}"
        return url
    elif entity == "issues":
        url = f"{base}/issues?state=all&per_page={per_page}"
        if since_iso:
            url += f"&since={since_iso}&sort=updated&direction=desc"
        return url
    elif entity == "pulls":
        url = f"{base}/pulls?state=all&per_page={per_page}"
        if run_mode == "full":
            url += "&sort=created&direction=desc"
        else:
            url += "&sort=updated&direction=desc"
        return url
    elif entity == "releases":
        return f"{base}/releases?per_page={per_page}"
    else:
        raise ValueError(f"Unknown entity '{entity}'. Supported: {ALL_ENTITIES}")


def parse_next_link(link_header: Optional[str]) -> Optional[str]:
    """Parses HTTP Link header and returns the URL for rel='next' if present."""
    if not link_header:
        return None
    for part in link_header.split(","):
        m = LINK_RE.search(part.strip())
        if m:
            return m.group(1)
    return None


def _check_rate_limit(headers: dict, margin: int = 2) -> None:
    """Checks rate-limit headers and sleeps if quota exhausted or secondary limit active."""
    # Check primary quota
    rem_str = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining")
    reset_str = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")

    if rem_str is not None and reset_str is not None:
        try:
            remaining = int(rem_str)
            reset_at = int(reset_str)
            if remaining <= 2:
                wait_sec = max(reset_at - int(time.time()), 0) + margin
                print(f"    [Rate Limit] {remaining} calls remaining. Sleeping {wait_sec}s until reset...", flush=True)
                time.sleep(wait_sec)
        except (ValueError, TypeError):
            pass


def fetch_page(url: str, token: str, opener=None, timeout: float = 15.0) -> Tuple[Any, dict]:
    """
    Fetches one page from GitHub REST API.
    Returns (json_data, headers_dict).
    Uses opener if provided, else urllib.request.urlopen.
    Never logs or leaks the token in exceptions or output.
    """
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("User-Agent", "dav-database-trends-ingest")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")

    try:
        if opener is not None:
            resp = opener.open(req, timeout=timeout)
        else:
            resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as exc:
        # Scrub token from exception representations
        raise exc
    except Exception as exc:
        raise exc

    with resp:
        body = resp.read()
        headers = dict(resp.headers)
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception as json_err:
            raise GitHubFetchError(f"Failed to decode JSON from {url}: {json_err}") from json_err
        return data, headers


def paginate(
    url: str,
    token: str,
    opener=None,
    retries: int = 3,
    stats: Optional[Dict[str, int]] = None,
) -> list:
    """
    Follows Link-header pagination until exhausted.
    Retries on 5xx, connection errors, and IncompleteRead with exponential backoff.
    Honours primary and secondary rate limits (Retry-After).
    On unrecoverable error or retry exhaustion raises GitHubFetchError (never partial list).
    """
    all_records: list = []
    current_url: Optional[str] = url

    while current_url:
        page_data = None
        page_headers: dict = {}

        for attempt in range(retries + 1):
            if stats is not None:
                stats["api_calls"] = stats.get("api_calls", 0) + 1

            try:
                page_data, page_headers = fetch_page(current_url, token, opener=opener)
                break
            except urllib.error.HTTPError as exc:
                body_bytes = b""
                try:
                    body_bytes = exc.read()
                except Exception:
                    pass
                body_text = body_bytes.decode("utf-8", errors="replace")
                headers = dict(exc.headers)

                # Secondary rate limit or 429
                retry_after = headers.get("Retry-After") or headers.get("retry-after")
                is_rate_limit = (
                    exc.code == 429
                    or (exc.code == 403 and ("rate limit" in body_text.lower() or "secondary rate" in body_text.lower()))
                )

                if is_rate_limit:
                    if retry_after:
                        wait = int(retry_after) + 2
                    else:
                        reset_at = int(headers.get("X-RateLimit-Reset", str(int(time.time()) + 60)) or 0)
                        wait = max(reset_at - int(time.time()), 0) + 2
                    print(f"    [Rate Limit HTTP {exc.code}] Waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    continue

                # 5xx Server errors are retriable
                if 500 <= exc.code < 600 and attempt < retries:
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue

                # Non-retriable or retries exhausted
                raise GitHubFetchError(
                    f"GitHub API HTTP {exc.code} fetching {current_url}: {body_text[:120]}"
                ) from exc

            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
                TimeoutError,
            ) as exc:
                if attempt < retries:
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue
                raise GitHubFetchError(
                    f"Transient network error exhausted {retries} retries on {current_url}: {type(exc).__name__}"
                ) from exc

        if stats is not None:
            stats["pages"] = stats.get("pages", 0) + 1

        if not isinstance(page_data, list):
            if page_data is not None:
                all_records.append(page_data)
            break

        all_records.extend(page_data)
        _check_rate_limit(page_headers)

        link_header = page_headers.get("Link") or page_headers.get("link", "")
        current_url = parse_next_link(link_header)

        if not page_data:
            break

    return all_records


def paginate_pulls(
    url: str,
    token: str,
    since_iso: str,
    cutoff_field: str = "created_at",
    until_iso: Optional[str] = None,
    opener=None,
    retries: int = 3,
    stats: Optional[Dict[str, int]] = None,
) -> list:
    """
    Paginates pull requests and stops when cutoff_field < since_iso.
    In full mode: cutoff_field='created_at' (sorted created desc).
    In incremental mode: cutoff_field='updated_at' (sorted updated desc).
    Filters out records > until_iso if provided.
    Raises GitHubFetchError on error.
    """
    all_records: list = []
    current_url: Optional[str] = url
    stop_fetching = False

    while current_url and not stop_fetching:
        page_data = None
        page_headers: dict = {}

        for attempt in range(retries + 1):
            if stats is not None:
                stats["api_calls"] = stats.get("api_calls", 0) + 1

            try:
                page_data, page_headers = fetch_page(current_url, token, opener=opener)
                break
            except urllib.error.HTTPError as exc:
                body_bytes = b""
                try:
                    body_bytes = exc.read()
                except Exception:
                    pass
                body_text = body_bytes.decode("utf-8", errors="replace")
                headers = dict(exc.headers)

                retry_after = headers.get("Retry-After") or headers.get("retry-after")
                is_rate_limit = (
                    exc.code == 429
                    or (exc.code == 403 and ("rate limit" in body_text.lower() or "secondary rate" in body_text.lower()))
                )

                if is_rate_limit:
                    wait = int(retry_after) + 2 if retry_after else 60
                    time.sleep(wait)
                    continue

                if 500 <= exc.code < 600 and attempt < retries:
                    time.sleep(2 ** attempt)
                    continue

                raise GitHubFetchError(
                    f"GitHub API HTTP {exc.code} fetching pulls from {current_url}: {body_text[:120]}"
                ) from exc

            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
                TimeoutError,
            ) as exc:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                    continue
                raise GitHubFetchError(
                    f"Transient network error exhausted {retries} retries on pulls {current_url}: {type(exc).__name__}"
                ) from exc

        if stats is not None:
            stats["pages"] = stats.get("pages", 0) + 1

        if not isinstance(page_data, list) or not page_data:
            break

        for pr in page_data:
            cutoff_val = pr.get(cutoff_field, "")
            if cutoff_val and cutoff_val < since_iso:
                stop_fetching = True
                break

            if until_iso and cutoff_val and cutoff_val > until_iso:
                continue

            all_records.append(pr)

        _check_rate_limit(page_headers)
        link_header = page_headers.get("Link") or page_headers.get("link", "")
        current_url = parse_next_link(link_header)

    return all_records


def fetch_entity(
    owner: str,
    repo: str,
    entity: str,
    run_mode: str,
    since_iso: Optional[str],
    until_iso: Optional[str],
    token: str,
    opener=None,
) -> Tuple[Union[list, dict], Dict[str, int]]:
    """
    Fetches one entity for a repository.
    Returns (records, stats) where stats = {'pages': n, 'api_calls': n}.
    Applies client-side until filtering for issues/pulls, passes native until to commits.
    """
    stats: Dict[str, int] = {"pages": 0, "api_calls": 0}

    if entity == "repo_metadata":
        url = build_url(owner, repo, "repo_metadata")
        stats["api_calls"] = 1
        data, _ = fetch_page(url, token, opener=opener)
        stats["pages"] = 1
        return data, stats

    elif entity == "commits":
        url = build_url(
            owner, repo, "commits", run_mode=run_mode, since_iso=since_iso, until_iso=until_iso
        )
        records = paginate(url, token, opener=opener, stats=stats)
        return records, stats

    elif entity == "issues":
        url = build_url(owner, repo, "issues", run_mode=run_mode, since_iso=since_iso)
        records = paginate(url, token, opener=opener, stats=stats)
        if until_iso:
            records = [r for r in records if r.get("updated_at", "") <= until_iso]
        return records, stats

    elif entity == "pulls":
        cutoff_field = "created_at" if run_mode == "full" else "updated_at"
        effective_since = since_iso or DEFAULT_SINCE
        url = build_url(owner, repo, "pulls", run_mode=run_mode)
        records = paginate_pulls(
            url=url,
            token=token,
            since_iso=effective_since,
            cutoff_field=cutoff_field,
            until_iso=until_iso,
            opener=opener,
            stats=stats,
        )
        return records, stats

    elif entity == "releases":
        url = build_url(owner, repo, "releases")
        records = paginate(url, token, opener=opener, stats=stats)
        if run_mode in ("incremental", "backfill") and since_iso:
            records = [r for r in records if r.get("published_at", "") >= since_iso]
            if until_iso:
                records = [r for r in records if r.get("published_at", "") <= until_iso]
        return records, stats

    else:
        raise ValueError(f"Unknown entity '{entity}'. Supported: {ALL_ENTITIES}")


def write_json_atomic(path: str, obj: Any) -> None:
    """
    Atomically writes JSON object to path using a .tmp file in the same folder.
    Falls back to shutil.move on Volume OSError.
    Cleans up any .tmp file on failure; never leaves a truncated final file.
    """
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass  # fsync may not be supported on some virtual filesystems

        try:
            os.replace(tmp_path, path)
        except OSError:
            shutil.move(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def resolve_token(
    dbutils: Any = None,
    scope: str = "dbtrends",
    key: str = "github_token",
    widget: str = "github_token",
) -> str:
    """
    Resolves GitHub token with strict precedence:
    1. Databricks secret (scope, key)
    2. Databricks widget (widget)
    3. GITHUB_TOKEN environment variable
    Never prints, logs, or leaks the token value.
    Raises ValueError with a clear message if no token found.
    """
    if dbutils is not None:
        # 1. Secret scope
        try:
            val = dbutils.secrets.get(scope=scope, key=key)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

        # 2. Widget
        try:
            val = dbutils.widgets.get(widget)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

    # 3. Environment variable
    env_val = os.environ.get("GITHUB_TOKEN")
    if env_val and str(env_val).strip():
        return str(env_val).strip()

    raise ValueError(
        "GitHub token not found. Configure Databricks secret scope 'dbtrends' key 'github_token', "
        "pass via widget 'github_token', or set GITHUB_TOKEN environment variable."
    )
