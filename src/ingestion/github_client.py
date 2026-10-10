"""
Pure Python GitHub REST API Client for Databricks Medallion Ingestion.
No PySpark or dbutils dependencies; fully unit-testable locally with mocked HTTP.

Provides:
- build_url / normalise_since
- parse_next_link
- fetch_page / paginate / paginate_pulls
- fetch_entity
- write_json_atomic
- resolve_token / resolve_repo / repo_dir_slug / repo_full_name_of
"""

import http.client
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


class GitHubFetchError(Exception):
    """Raised when an unrecoverable HTTP or network error occurs during ingestion."""
    pass


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

PER_PAGE = 100
DEFAULT_SINCE = "2026-04-01T00:00:00Z"
LINK_RE = re.compile(r'<([^>]+)>;\s*rel="([^"]+)"')


def _sanitize(text: str, token: Optional[str]) -> str:
    """Removes token occurrences from error strings and messages."""
    if not token or not text:
        return text
    return text.replace(token, "[REDACTED]")


def normalise_since(raw: Optional[str]) -> str:
    """
    Normalises a date string to 'YYYY-MM-DDTHH:MM:SSZ' (UTC).
    Accepts 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SSZ', or ISO-8601 with offset.
    Raises ValueError on invalid or empty input.
    """
    if not raw or not isinstance(raw, str):
        raise ValueError(f"Invalid timestamp: {raw!r}")
    raw_str = raw.strip()
    if not raw_str:
        raise ValueError("Timestamp cannot be blank")

    # If it is purely 'YYYY-MM-DD', append T00:00:00+00:00 for parsing
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw_str):
        try:
            dt = datetime.strptime(raw_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            raise ValueError(f"Invalid date {raw_str!r}: {e}")

    # Replace terminal 'Z' with '+00:00' for fromisoformat compatibility across Python versions
    iso_candidate = raw_str
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(iso_candidate)
    except Exception as e:
        raise ValueError(f"Could not parse ISO timestamp {raw_str!r}: {e}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_next_link(link_header: Optional[str]) -> Optional[str]:
    """
    Extracts the 'rel="next"' URL from a GitHub Link header.
    Returns None if not present or if link_header is empty.
    """
    if not link_header:
        return None

    # Link header can contain comma-separated entries
    for part in link_header.split(","):
        part = part.strip()
        m = LINK_RE.search(part)
        if m:
            url, rel = m.group(1), m.group(2).strip()
            rels = [r.strip().lower() for r in rel.split()]
            if "next" in rels:
                return url
    return None


def fetch_page(
    url: str,
    token: str,
    opener: Optional[Any] = None,
    timeout: float = 15.0,
) -> Tuple[Any, Dict[str, str]]:
    """
    Performs a single HTTP GET request to the GitHub API.
    Returns (json_data, headers_dict).
    Accepts an injectable `opener` for unit testing.
    """
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("User-Agent", "dav-database-trends")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")

    open_func = opener.open if opener is not None else urllib.request.urlopen

    with open_func(req, timeout=timeout) if opener is None else open_func(req) as resp:
        body = resp.read()
        if isinstance(body, (bytes, bytearray)):
            body = body.decode("utf-8")
        data = json.loads(body)
        headers = dict(resp.headers)
        return data, headers


def _check_rate_limit(headers: Dict[str, str], token: str) -> None:
    """Honours X-RateLimit-Remaining and sleeps when <= 2 calls remain."""
    remaining = int(headers.get("X-RateLimit-Remaining", "100"))
    reset_at = int(headers.get("X-RateLimit-Reset", "0"))
    if remaining <= 2:
        wait = max(reset_at - int(time.time()), 0) + 2
        print(f"    [Rate Limit Warning] {remaining} calls remaining. Sleeping {wait}s until reset...", flush=True)
        time.sleep(wait)


def paginate(
    url: str,
    token: str,
    opener: Optional[Any] = None,
    max_retries: int = 3,
) -> Tuple[List[Any], Dict[str, int]]:
    """
    Standard Link-header pagination with exponential backoff on transient errors
    and strict rate-limit handling.
    Returns (all_records, stats) where stats = {"pages": n, "api_calls": n}.
    Raises GitHubFetchError on unrecoverable failure or retry exhaustion.
    """
    all_records: List[Any] = []
    current_url: Optional[str] = url
    page_count = 0
    api_call_count = 0

    while current_url:
        page_data = None
        resp_headers: Dict[str, str] = {}
        fetched = False

        for attempt in range(max_retries + 1):
            api_call_count += 1
            try:
                page_data, resp_headers = fetch_page(current_url, token, opener=opener)
                fetched = True
                break
            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass

                # Check secondary rate limit or standard 403/429
                is_rate_limit = (
                    exc.code == 429
                    or (
                        exc.code == 403
                        and ("rate limit" in body.lower() or "secondary rate limit" in body.lower())
                    )
                )
                retry_after = exc.headers.get("Retry-After") if exc.headers else None

                if retry_after or is_rate_limit:
                    if attempt < max_retries:
                        if retry_after:
                            try:
                                wait = int(retry_after) + 2
                            except ValueError:
                                wait = 2 ** attempt
                        else:
                            reset_at = int(exc.headers.get("X-RateLimit-Reset", "0")) if exc.headers else 0
                            wait = max(reset_at - int(time.time()), 0) + 2
                        print(f"    [Rate Limit {exc.code}] Backing off for {wait}s...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
                    raise GitHubFetchError(clean_msg) from None

                if exc.code >= 500:
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        print(f"    [HTTP {exc.code}] Transient server error, retrying in {wait}s ({attempt + 1}/{max_retries})...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"HTTP {exc.code} on {current_url}: {body[:200]}", token)
                    raise GitHubFetchError(clean_msg) from None

                clean_msg = _sanitize(f"HTTP {exc.code} on {current_url}: {body[:200]}", token)
                raise GitHubFetchError(clean_msg) from None

            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
                TimeoutError,
            ) as exc:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    print(f"    [Transient Network Error] {type(exc).__name__}, retrying in {wait}s ({attempt + 1}/{max_retries})...", flush=True)
                    time.sleep(wait)
                    continue
                clean_msg = _sanitize(f"Network error after {max_retries} retries on {current_url}: {type(exc).__name__}", token)
                raise GitHubFetchError(clean_msg) from None
        else:
            clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
            raise GitHubFetchError(clean_msg)

        if not fetched or page_data is None:
            clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
            raise GitHubFetchError(clean_msg)

        page_count += 1

        if not isinstance(page_data, list):
            all_records.append(page_data)
            break

        all_records.extend(page_data)
        _check_rate_limit(resp_headers, token)

        link_header = resp_headers.get("Link", "")
        current_url = parse_next_link(link_header)
        if not page_data:
            break

    return all_records, {"pages": page_count, "api_calls": api_call_count}


def paginate_pulls(
    url: str,
    token: str,
    since_iso: Optional[str],
    cutoff_field: str = "created_at",
    until_iso: Optional[str] = None,
    opener: Optional[Any] = None,
    max_retries: int = 3,
) -> Tuple[List[Any], Dict[str, int]]:
    """
    Time-bounded pagination for Pull Requests endpoint using descending sort.
    Stops paginating when record[cutoff_field] < since_iso.
    If until_iso is provided, filters records to those with updated_at <= until_iso.
    """
    all_records: List[Any] = []
    current_url: Optional[str] = url
    page_count = 0
    api_call_count = 0
    stop_fetching = False

    while current_url and not stop_fetching:
        page_data = None
        resp_headers: Dict[str, str] = {}
        fetched = False

        for attempt in range(max_retries + 1):
            api_call_count += 1
            try:
                page_data, resp_headers = fetch_page(current_url, token, opener=opener)
                fetched = True
                break
            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass

                is_rate_limit = (
                    exc.code == 429
                    or (
                        exc.code == 403
                        and ("rate limit" in body.lower() or "secondary rate limit" in body.lower())
                    )
                )
                retry_after = exc.headers.get("Retry-After") if exc.headers else None

                if retry_after or is_rate_limit:
                    if attempt < max_retries:
                        if retry_after:
                            try:
                                wait = int(retry_after) + 2
                            except ValueError:
                                wait = 2 ** attempt
                        else:
                            reset_at = int(exc.headers.get("X-RateLimit-Reset", "0")) if exc.headers else 0
                            wait = max(reset_at - int(time.time()), 0) + 2
                        print(f"    [Rate Limit {exc.code}] Backing off for {wait}s...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
                    raise GitHubFetchError(clean_msg) from None

                if exc.code >= 500:
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        print(f"    [HTTP {exc.code}] Transient server error, retrying in {wait}s ({attempt + 1}/{max_retries})...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"HTTP {exc.code} on {current_url}: {body[:200]}", token)
                    raise GitHubFetchError(clean_msg) from None

                clean_msg = _sanitize(f"HTTP {exc.code} on {current_url}: {body[:200]}", token)
                raise GitHubFetchError(clean_msg) from None

            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
                TimeoutError,
            ) as exc:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    print(f"    [Transient Network Error] {type(exc).__name__}, retrying in {wait}s ({attempt + 1}/{max_retries})...", flush=True)
                    time.sleep(wait)
                    continue
                clean_msg = _sanitize(f"Network error after {max_retries} retries on {current_url}: {type(exc).__name__}", token)
                raise GitHubFetchError(clean_msg) from None
        else:
            clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
            raise GitHubFetchError(clean_msg)

        if not fetched or page_data is None:
            clean_msg = _sanitize(f"Rate limit / retries exhausted on {current_url}", token)
            raise GitHubFetchError(clean_msg)

        page_count += 1

        if not isinstance(page_data, list) or not page_data:
            break

        for pr in page_data:
            cutoff_val = pr.get(cutoff_field, "")
            if since_iso and cutoff_val and cutoff_val < since_iso:
                stop_fetching = True
                break

            # Client-side until filtering on updated_at
            if until_iso:
                pr_updated = pr.get("updated_at", "")
                if pr_updated and pr_updated > until_iso:
                    continue

            all_records.append(pr)

        _check_rate_limit(resp_headers, token)
        link_header = resp_headers.get("Link", "")
        current_url = parse_next_link(link_header)

    return all_records, {"pages": page_count, "api_calls": api_call_count}


def build_url(
    owner: str,
    repo: str,
    entity: str,
    since_iso: Optional[str] = None,
    until_iso: Optional[str] = None,
    run_mode: str = "full",
) -> str:
    """Constructs the canonical GitHub API URL for a given entity and run mode."""
    base = f"https://api.github.com/repos/{owner}/{repo}"

    if entity == "repo_metadata":
        return base
    elif entity == "commits":
        params = [f"per_page={PER_PAGE}"]
        if since_iso:
            params.append(f"since={urllib.parse.quote(since_iso)}")
        if until_iso:
            params.append(f"until={urllib.parse.quote(until_iso)}")
        return f"{base}/commits?{'&'.join(params)}"
    elif entity == "issues":
        params = ["state=all", f"per_page={PER_PAGE}"]
        if since_iso:
            params.append(f"since={urllib.parse.quote(since_iso)}")
        return f"{base}/issues?{'&'.join(params)}"
    elif entity == "pulls":
        if run_mode == "full":
            return f"{base}/pulls?state=all&sort=created&direction=desc&per_page={PER_PAGE}"
        else:
            return f"{base}/pulls?state=all&sort=updated&direction=desc&per_page={PER_PAGE}"
    elif entity == "releases":
        return f"{base}/releases?per_page={PER_PAGE}"
    else:
        raise ValueError(f"Unknown entity {entity!r}")


def fetch_entity(
    owner: str,
    repo: str,
    entity: str,
    run_mode: str,
    since_iso: Optional[str],
    until_iso: Optional[str],
    token: str,
    opener: Optional[Any] = None,
) -> Tuple[Union[List[Any], Dict[str, Any]], Dict[str, int]]:
    """
    Fetches all records for an entity applying native and client-side filters.
    Returns (records, stats) where stats = {"pages": n, "api_calls": n}.
    """
    if entity == "repo_metadata":
        url = build_url(owner, repo, entity)
        max_attempts = 3
        api_call_count = 0
        last_exc = None
        for attempt in range(max_attempts):
            api_call_count += 1
            try:
                data, _ = fetch_page(url, token, opener=opener)
                return data, {"pages": 1, "api_calls": api_call_count}
            except urllib.error.HTTPError as exc:
                last_exc = exc
                body = ""
                try:
                    body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass

                is_rate_limit = (
                    exc.code == 429
                    or (
                        exc.code == 403
                        and ("rate limit" in body.lower() or "secondary rate limit" in body.lower())
                    )
                )
                retry_after = exc.headers.get("Retry-After") if exc.headers else None

                if retry_after or is_rate_limit:
                    if attempt < max_attempts - 1:
                        if retry_after:
                            try:
                                wait = int(retry_after) + 2
                            except ValueError:
                                wait = 2 ** attempt
                        else:
                            reset_at = int(exc.headers.get("X-RateLimit-Reset", "0")) if exc.headers else 0
                            wait = max(reset_at - int(time.time()), 0) + 2
                        print(f"    [Rate Limit {exc.code}] Backing off for {wait}s...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"Rate limit / retries exhausted on {url}", token)
                    raise GitHubFetchError(clean_msg) from None

                if exc.code >= 500:
                    if attempt < max_attempts - 1:
                        wait = 2 ** attempt
                        print(f"    [HTTP {exc.code}] Transient server error, retrying in {wait}s ({attempt + 1}/{max_attempts})...", flush=True)
                        time.sleep(wait)
                        continue
                    clean_msg = _sanitize(f"HTTP {exc.code} on {url}: {body[:200]}", token)
                    raise GitHubFetchError(clean_msg) from None

                clean_msg = _sanitize(f"HTTP {exc.code} on {url}: {body[:200]}", token)
                raise GitHubFetchError(clean_msg) from None

            except (
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionError,
                TimeoutError,
            ) as exc:
                last_exc = exc
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    print(f"    [Transient Network Error] {type(exc).__name__}, retrying in {wait}s ({attempt + 1}/{max_attempts})...", flush=True)
                    time.sleep(wait)
                    continue
                clean_msg = _sanitize(f"Network error after {max_attempts} attempts on {url}: {type(exc).__name__}", token)
                raise GitHubFetchError(clean_msg) from None
            except Exception as exc:
                last_exc = exc
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue
                clean_msg = _sanitize(f"Failed to fetch metadata for {owner}/{repo}: {exc}", token)
                raise GitHubFetchError(clean_msg) from None
        else:
            clean_msg = _sanitize(f"Rate limit / retries exhausted on {url}", token)
            raise GitHubFetchError(clean_msg)

    elif entity == "pulls":
        cutoff_field = "created_at" if run_mode == "full" else "updated_at"
        url = build_url(owner, repo, entity, since_iso, until_iso, run_mode=run_mode)
        return paginate_pulls(
            url=url,
            token=token,
            since_iso=since_iso,
            cutoff_field=cutoff_field,
            until_iso=until_iso,
            opener=opener,
        )

    elif entity == "issues":
        url = build_url(owner, repo, entity, since_iso, until_iso, run_mode=run_mode)
        records, stats = paginate(url, token, opener=opener)
        if until_iso:
            records = [
                rec for rec in records
                if not rec.get("updated_at") or rec.get("updated_at") <= until_iso
            ]
        return records, stats

    else:
        # commits (native since and until) or releases (no window)
        url = build_url(owner, repo, entity, since_iso, until_iso, run_mode=run_mode)
        return paginate(url, token, opener=opener)


def write_json_atomic(path: str, obj: Any) -> None:
    """
    Writes JSON data atomically to a file:
    Writes to `<path>.tmp` first, flushes and syncs to disk, then renames via os.replace.
    If os.replace fails with OSError (e.g. on Databricks Volumes), falls back to shutil.move.
    Cleans up any `.tmp` file on failure so no partial or corrupted file remains.
    """
    parent_dir = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent_dir, exist_ok=True)
    tmp_path = f"{path}.tmp"

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

        try:
            os.replace(tmp_path, path)
        except OSError:
            shutil.move(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def resolve_token(
    dbutils: Optional[Any] = None,
    scope: str = "dbtrends",
    key: str = "github_token",
    widget: str = "github_token",
) -> str:
    """
    Resolves GitHub API token in strict priority order:
    1. Databricks secret (dbutils.secrets.get(scope, key))
    2. Notebook widget (dbutils.widgets.get(widget))
    3. GITHUB_TOKEN environment variable
    Raises ValueError if none configured.
    Never exposes token in exception messages.
    """
    if dbutils is not None:
        try:
            val = dbutils.secrets.get(scope=scope, key=key)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

        try:
            val = dbutils.widgets.get(widget)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

    env_val = os.environ.get("GITHUB_TOKEN")
    if env_val and str(env_val).strip():
        return str(env_val).strip()

    raise ValueError(
        "No GitHub token found. Provide via Databricks secret (scope 'dbtrends', key 'github_token'), "
        "notebook widget 'github_token', or GITHUB_TOKEN environment variable."
    )


def repo_dir_slug(owner_or_fullname: str, repo: Optional[str] = None) -> str:
    """Generates filesystem slug like 'postgres_postgres' from 'postgres/postgres'."""
    if repo is not None:
        return f"{owner_or_fullname}_{repo}"
    if "/" in owner_or_fullname:
        parts = owner_or_fullname.split("/", 1)
        return f"{parts[0]}_{parts[1]}"
    return owner_or_fullname


def repo_full_name_of(slug_or_tuple: Union[str, Tuple[str, str]]) -> str:
    """Converts ('postgres', 'postgres') or 'postgres_postgres' to 'postgres/postgres'."""
    if isinstance(slug_or_tuple, (tuple, list)):
        return f"{slug_or_tuple[0]}/{slug_or_tuple[1]}"
    if "/" in slug_or_tuple:
        return slug_or_tuple
    # Replace the separating underscore for known repos
    for owner, repo in TRACKED_REPOS:
        if slug_or_tuple == f"{owner}_{repo}":
            return f"{owner}/{repo}"
    return slug_or_tuple.replace("_", "/", 1)


def resolve_repo(repo_input: str) -> List[Tuple[str, str]]:
    """
    Resolves repository parameter:
    - 'ALL' -> returns all 10 tracked repositories
    - 'owner/repo' or 'owner_repo' -> returns [('owner', 'repo')]
    Raises ValueError if unknown.
    """
    cleaned = repo_input.strip()
    if cleaned.upper() == "ALL":
        return list(TRACKED_REPOS)

    # Check exact match in tracked repos
    for owner, repo in TRACKED_REPOS:
        if cleaned in (f"{owner}/{repo}", f"{owner}_{repo}"):
            return [(owner, repo)]

    valid_repos = [f"{o}/{r}" for o, r in TRACKED_REPOS]
    raise ValueError(
        f"Unknown repository {repo_input!r}. Supported repositories: 'ALL' or one of {valid_repos}"
    )
