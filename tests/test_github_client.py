"""
Unit tests for src/ingestion/github_client.py.
Validates:
1. parse_next_link (present / absent / multiple rels)
2. paginate across 3 mocked pages (all records, page/api_call counts)
3. normalise_since ("2026-09-25", "2026-09-25T00:00:00Z", "2026-09-25T10:00:00+05:00" -> UTC, invalid -> ValueError)
4. pulls cutoff mode (full mode created_at vs incremental mode updated_at)
5. until filtering for issues and pulls
6. rate limit handling (remaining<=2 sleep, 403 secondary limit with Retry-After)
7. retry logic (transient 500 then 200, persistent failure -> GitHubFetchError and target file untouched)
8. write_json_atomic (success leaves 1 file and no .tmp; failure cleans .tmp and leaves target intact)
9. token secrecy (token never leaks to logs, stdout/stderr, or exception messages)
10. resolve_token precedence and missing-token error
11. repo selection (ALL, owner/repo, owner_repo, unknown -> ValueError)
12. integration test with Spark (real schemas, drift, null PK reject, empty [] files)
"""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock
import urllib.error

from src.ingestion.github_client import (
    GitHubFetchError,
    TRACKED_REPOS,
    build_url,
    normalise_since,
    parse_next_link,
    fetch_page,
    paginate,
    paginate_pulls,
    fetch_entity,
    write_json_atomic,
    resolve_token,
    resolve_repo,
    repo_dir_slug,
    repo_full_name_of,
)


class MockHTTPResponse:
    def __init__(self, data, headers=None, status=200):
        self._data = data if isinstance(data, (bytes, bytearray)) else json.dumps(data).encode("utf-8")
        self.headers = headers or {}
        self.status = status

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockOpener:
    def __init__(self, handler_fn):
        self.handler_fn = handler_fn

    def open(self, req):
        return self.handler_fn(req)


class TestGithubClient(unittest.TestCase):

    def test_build_url(self):
        # repo_metadata
        self.assertEqual(
            build_url("postgres", "postgres", "repo_metadata"),
            "https://api.github.com/repos/postgres/postgres"
        )
        # commits
        self.assertEqual(
            build_url("postgres", "postgres", "commits", since_iso="2026-04-01T00:00:00Z", until_iso="2026-05-01T00:00:00Z"),
            "https://api.github.com/repos/postgres/postgres/commits?per_page=100&since=2026-04-01T00%3A00%3A00Z&until=2026-05-01T00%3A00%3A00Z"
        )
        # issues
        self.assertEqual(
            build_url("postgres", "postgres", "issues", since_iso="2026-04-01T00:00:00Z"),
            "https://api.github.com/repos/postgres/postgres/issues?state=all&per_page=100&since=2026-04-01T00%3A00%3A00Z"
        )
        # pulls full mode (created desc) vs incremental mode (updated desc)
        self.assertEqual(
            build_url("postgres", "postgres", "pulls", run_mode="full"),
            "https://api.github.com/repos/postgres/postgres/pulls?state=all&sort=created&direction=desc&per_page=100"
        )
        self.assertEqual(
            build_url("postgres", "postgres", "pulls", run_mode="incremental"),
            "https://api.github.com/repos/postgres/postgres/pulls?state=all&sort=updated&direction=desc&per_page=100"
        )
        # releases
        self.assertEqual(
            build_url("postgres", "postgres", "releases"),
            "https://api.github.com/repos/postgres/postgres/releases?per_page=100"
        )
        # invalid entity
        with self.assertRaises(ValueError):
            build_url("postgres", "postgres", "invalid_entity")

    def test_fetch_page_with_mock_opener(self):
        def handler(req):
            self.assertEqual(req.headers.get("Authorization"), "Bearer test_tok")
            self.assertEqual(req.headers.get("Accept"), "application/vnd.github+json")
            return MockHTTPResponse({"hello": "world"}, {"X-RateLimit-Remaining": "4999"})

        data, headers = fetch_page("https://api.github.com/test", "test_tok", opener=MockOpener(handler))
        self.assertEqual(data, {"hello": "world"})
        self.assertEqual(headers.get("X-RateLimit-Remaining"), "4999")

    def test_parse_next_link(self):

        # 1. Present
        h1 = '<https://api.github.com/repositories/123/commits?page=2>; rel="next"'
        self.assertEqual(parse_next_link(h1), "https://api.github.com/repositories/123/commits?page=2")

        # 2. Absent
        self.assertIsNone(parse_next_link(None))
        self.assertIsNone(parse_next_link(""))
        h_prev = '<https://api.github.com/repositories/123/commits?page=1>; rel="prev"'
        self.assertIsNone(parse_next_link(h_prev))

        # 3. Multiple rels
        h_multi = (
            '<https://api.github.com/repositories/123/commits?page=1>; rel="prev", '
            '<https://api.github.com/repositories/123/commits?page=3>; rel="next", '
            '<https://api.github.com/repositories/123/commits?page=10>; rel="last"'
        )
        self.assertEqual(parse_next_link(h_multi), "https://api.github.com/repositories/123/commits?page=3")

    def test_normalise_since(self):
        # "YYYY-MM-DD" -> "YYYY-MM-DDT00:00:00Z"
        self.assertEqual(normalise_since("2026-09-25"), "2026-09-25T00:00:00Z")

        # "YYYY-MM-DDTHH:MM:SSZ" -> unchanged
        self.assertEqual(normalise_since("2026-09-25T00:00:00Z"), "2026-09-25T00:00:00Z")

        # Offset "+05:00" -> converted to UTC
        # 10:00 +05:00 = 05:00 UTC
        self.assertEqual(normalise_since("2026-09-25T10:00:00+05:00"), "2026-09-25T05:00:00Z")

        # Invalid strings must raise ValueError
        for invalid in ["", "   ", "not-a-date", "2026-13-45", "yesterday", "2026/09/25"]:
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalise_since(invalid)

    def test_paginate_three_mocked_pages(self):
        page1 = [{"id": 1}, {"id": 2}]
        page2 = [{"id": 3}, {"id": 4}]
        page3 = [{"id": 5}]

        calls = []

        def handler(req):
            url = req.full_url
            calls.append(url)
            if "page=2" in url:
                headers = {"Link": '<https://api.github.com/items?page=3>; rel="next"'}
                return MockHTTPResponse(page2, headers)
            elif "page=3" in url:
                return MockHTTPResponse(page3, {})
            else:
                headers = {"Link": '<https://api.github.com/items?page=2>; rel="next"'}
                return MockHTTPResponse(page1, headers)

        opener = MockOpener(handler)
        records, stats = paginate("https://api.github.com/items", "secret-token", opener=opener)

        self.assertEqual(len(records), 5)
        self.assertEqual([r["id"] for r in records], [1, 2, 3, 4, 5])
        self.assertEqual(stats["pages"], 3)
        self.assertEqual(stats["api_calls"], 3)

    def test_pulls_cutoff_modes_created_vs_updated(self):
        # Two fixtures where created_at and updated_at differ
        # Window: since = 2026-04-01T00:00:00Z
        # Item A: created 2026-05-01 (>= since), updated 2026-03-01 (< since)
        # Item B: created 2026-03-01 (< since), updated 2026-05-01 (>= since)
        fixture_a = {"id": 101, "created_at": "2026-05-01T00:00:00Z", "updated_at": "2026-03-01T00:00:00Z"}
        fixture_b = {"id": 102, "created_at": "2026-03-01T00:00:00Z", "updated_at": "2026-05-01T00:00:00Z"}

        since_iso = "2026-04-01T00:00:00Z"

        # Full mode: sorted by created desc, cutoff on created_at
        # Page has [fixture_a, fixture_b]. fixture_a passes, fixture_b triggers cutoff.
        def handler_full(req):
            return MockHTTPResponse([fixture_a, fixture_b], {})

        full_records, _ = paginate_pulls(
            "https://api.github.com/repos/test/test/pulls",
            token="tok",
            since_iso=since_iso,
            cutoff_field="created_at",
            opener=MockOpener(handler_full)
        )
        self.assertEqual([r["id"] for r in full_records], [101])

        # Incremental mode: sorted by updated desc, cutoff on updated_at
        # Page has [fixture_b, fixture_a]. fixture_b passes, fixture_a triggers cutoff.
        def handler_incr(req):
            return MockHTTPResponse([fixture_b, fixture_a], {})

        incr_records, _ = paginate_pulls(
            "https://api.github.com/repos/test/test/pulls",
            token="tok",
            since_iso=since_iso,
            cutoff_field="updated_at",
            opener=MockOpener(handler_incr)
        )
        self.assertEqual([r["id"] for r in incr_records], [102])

    def test_until_filtering_for_issues_and_pulls(self):
        until_iso = "2026-05-01T00:00:00Z"
        rec_old = {"id": 1, "updated_at": "2026-04-15T00:00:00Z"}
        rec_exact = {"id": 2, "updated_at": "2026-05-01T00:00:00Z"}
        rec_future = {"id": 3, "updated_at": "2026-05-15T00:00:00Z"}

        def handler(req):
            return MockHTTPResponse([rec_future, rec_exact, rec_old], {})

        opener = MockOpener(handler)
        records, _ = fetch_entity(
            owner="surrealdb",
            repo="surrealdb",
            entity="issues",
            run_mode="incremental",
            since_iso="2026-04-01T00:00:00Z",
            until_iso=until_iso,
            token="tok",
            opener=opener,
        )
        # rec_future is > until_iso, so filtered out client-side
        self.assertEqual([r["id"] for r in records], [2, 1])

    @mock.patch("time.sleep")
    @mock.patch("time.time")
    def test_rate_limit_remaining_triggers_sleep(self, mock_time, mock_sleep):
        mock_time.return_value = 1000.0
        # remaining <= 2, reset_at = 1020 -> sleep(20 + margin)
        headers = {
            "X-RateLimit-Remaining": "2",
            "X-RateLimit-Reset": "1020",
        }

        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            return MockHTTPResponse([{"id": 1}], headers)

        opener = MockOpener(handler)
        records, stats = paginate("https://api.github.com/test", "tok", opener=opener)

        self.assertEqual(len(records), 1)
        mock_sleep.assert_called()
        slept_duration = mock_sleep.call_args[0][0]
        # 1020 - 1000 + margin(2) = 22
        self.assertGreaterEqual(slept_duration, 20)

    @mock.patch("time.sleep")
    def test_secondary_rate_limit_403_with_retry_after(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                fp = io.BytesIO(b'{"message": "You have exceeded a secondary rate limit."}')
                hdrs = {"Retry-After": "15"}
                raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", hdrs, fp)
            return MockHTTPResponse([{"id": 1}], {})

        opener = MockOpener(handler)
        records, stats = paginate("https://api.github.com/test", "tok", opener=opener)

        self.assertEqual(len(records), 1)
        self.assertEqual(call_count, 2)
        mock_sleep.assert_called()
        self.assertGreaterEqual(mock_sleep.call_args_list[0][0][0], 15)

    @mock.patch("time.sleep")
    def test_retry_on_500_then_success(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                fp = io.BytesIO(b"Internal Server Error")
                raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, fp)
            return MockHTTPResponse([{"id": 42}], {})

        opener = MockOpener(handler)
        records, stats = paginate("https://api.github.com/test", "tok", opener=opener)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 42)
        self.assertEqual(call_count, 2)

    @mock.patch("time.sleep")
    def test_persistent_failure_raises_github_fetch_error_and_leaves_target_unchanged(self, mock_sleep):
        temp_dir = tempfile.mkdtemp()
        try:
            target_file = os.path.join(temp_dir, "issues.json")
            original_content = b'[{"id": 999, "existing": true}]'
            with open(target_file, "wb") as f:
                f.write(original_content)

            def failing_handler(req):
                fp = io.BytesIO(b"Persistent 500")
                raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, fp)

            opener = MockOpener(failing_handler)

            with self.assertRaises(GitHubFetchError):
                fetch_entity(
                    owner="redis",
                    repo="redis",
                    entity="issues",
                    run_mode="full",
                    since_iso="2026-04-01T00:00:00Z",
                    until_iso=None,
                    token="tok",
                    opener=opener,
                )

            # Pre-existing file must remain byte-identical
            with open(target_file, "rb") as f:
                self.assertEqual(f.read(), original_content)

            # No temporary files created
            self.assertEqual(os.listdir(temp_dir), ["issues.json"])
        finally:
            shutil.rmtree(temp_dir)

    @mock.patch("time.sleep")
    def test_paginate_persistent_429_raises_github_fetch_error(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            fp = io.BytesIO(b'{"message": "Too Many Requests"}')
            hdrs = {"Retry-After": "5"}
            raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", hdrs, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            paginate("https://api.github.com/items", "tok", opener=opener, max_retries=3)

        self.assertIn("Rate limit / retries exhausted", str(ctx.exception))
        self.assertEqual(call_count, 4)  # initial attempt + 3 retries
        self.assertEqual(mock_sleep.call_count, 3)

    @mock.patch("time.sleep")
    def test_paginate_persistent_403_rate_limit_raises_github_fetch_error(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            fp = io.BytesIO(b'{"message": "API rate limit exceeded for user"}')
            hdrs = {"X-RateLimit-Reset": "1000"}
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", hdrs, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            paginate("https://api.github.com/items", "tok", opener=opener, max_retries=3)

        self.assertIn("Rate limit / retries exhausted", str(ctx.exception))
        self.assertEqual(call_count, 4)
        self.assertEqual(mock_sleep.call_count, 3)

    @mock.patch("time.sleep")
    def test_paginate_pulls_persistent_429_raises_github_fetch_error(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            fp = io.BytesIO(b'{"message": "Too Many Requests"}')
            hdrs = {"Retry-After": "5"}
            raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", hdrs, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            paginate_pulls(
                "https://api.github.com/repos/test/test/pulls",
                "tok",
                since_iso="2026-04-01T00:00:00Z",
                opener=opener,
                max_retries=3,
            )

        self.assertIn("Rate limit / retries exhausted", str(ctx.exception))
        self.assertEqual(call_count, 4)
        self.assertEqual(mock_sleep.call_count, 3)

    @mock.patch("time.sleep")
    def test_paginate_pulls_persistent_403_rate_limit_raises_github_fetch_error(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            fp = io.BytesIO(b'{"message": "You have exceeded a secondary rate limit."}')
            hdrs = {}
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", hdrs, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            paginate_pulls(
                "https://api.github.com/repos/test/test/pulls",
                "tok",
                since_iso="2026-04-01T00:00:00Z",
                opener=opener,
                max_retries=3,
            )

        self.assertIn("Rate limit / retries exhausted", str(ctx.exception))
        self.assertEqual(call_count, 4)
        self.assertEqual(mock_sleep.call_count, 3)

    @mock.patch("time.sleep")
    def test_paginate_never_returns_partial_list_on_later_page_failure(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Page 1 succeeds with next link
                headers = {"Link": '<https://api.github.com/items?page=2>; rel="next"'}
                return MockHTTPResponse([{"id": 1}], headers)
            # Page 2 persistently rate limited
            fp = io.BytesIO(b'{"message": "API rate limit exceeded"}')
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            paginate("https://api.github.com/items", "tok", opener=opener, max_retries=2)

        self.assertIn("Rate limit / retries exhausted", str(ctx.exception))

    @mock.patch("time.sleep")
    def test_repo_metadata_retry_success(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                fp = io.BytesIO(b"Internal Server Error")
                raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, fp)
            return MockHTTPResponse({"id": 12345, "name": "postgres"}, {})

        opener = MockOpener(handler)
        data, stats = fetch_entity(
            owner="postgres",
            repo="postgres",
            entity="repo_metadata",
            run_mode="full",
            since_iso=None,
            until_iso=None,
            token="tok",
            opener=opener,
        )

        self.assertEqual(data["id"], 12345)
        self.assertEqual(stats["api_calls"], 2)
        self.assertEqual(mock_sleep.call_count, 1)

    @mock.patch("time.sleep")
    def test_repo_metadata_persistent_failure_raises(self, mock_sleep):
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            fp = io.BytesIO(b'{"message": "Too Many Requests"}')
            raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, fp)

        opener = MockOpener(handler)
        with self.assertRaises(GitHubFetchError) as ctx:
            fetch_entity(
                owner="postgres",
                repo="postgres",
                entity="repo_metadata",
                run_mode="full",
                since_iso=None,
                until_iso=None,
                token="tok",
                opener=opener,
            )

        self.assertEqual(call_count, 3)  # 3 attempts
        self.assertEqual(mock_sleep.call_count, 2)

    def test_common_module_imports_repo_slug_helpers(self):
        from src.common import repo_dir_slug as common_slug, repo_full_name_of as common_fullname
        self.assertEqual(common_slug("postgres/postgres"), "postgres_postgres")
        self.assertEqual(common_fullname("postgres_postgres"), "postgres/postgres")

    def test_write_json_atomic_success_and_failure(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # 1. Success case
            target = os.path.join(temp_dir, "data.json")
            data = {"status": "ok", "count": 10}
            write_json_atomic(target, data)

            self.assertTrue(os.path.exists(target))
            self.assertFalse(os.path.exists(f"{target}.tmp"))
            with open(target, "r", encoding="utf-8") as f:
                self.assertEqual(json.load(f), data)

            # 2. Failure case during write/replace
            target_fail = os.path.join(temp_dir, "fail.json")
            with open(target_fail, "w", encoding="utf-8") as f:
                f.write("intact")

            with mock.patch("os.replace", side_effect=OSError("Disk write error")):
                with mock.patch("shutil.move", side_effect=RuntimeError("Move failed")):
                    with self.assertRaises(RuntimeError):
                        write_json_atomic(target_fail, {"new": "corrupt"})

            # Assert .tmp was cleaned up and target file was not modified or truncated
            self.assertFalse(os.path.exists(f"{target_fail}.tmp"))
            with open(target_fail, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "intact")
        finally:
            shutil.rmtree(temp_dir)

    def test_token_secrecy_never_leaked(self):
        secret_token = "ghp_super_secret_token_12345"
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        def handler(req):
            fp = io.BytesIO(b'{"message": "Bad credentials"}')
            raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, fp)

        opener = MockOpener(handler)

        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = stdout_capture, stderr_capture
        try:
            with self.assertRaises(GitHubFetchError) as ctx:
                paginate("https://api.github.com/secret", secret_token, opener=opener)

            err_msg = str(ctx.exception)
            self.assertNotIn(secret_token, err_msg)
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr

        self.assertNotIn(secret_token, stdout_capture.getvalue())
        self.assertNotIn(secret_token, stderr_capture.getvalue())

    def test_resolve_token_precedence_and_missing(self):
        class FakeSecrets:
            def __init__(self, val=None):
                self.val = val

            def get(self, scope, key):
                if self.val:
                    return self.val
                raise KeyError("no secret")

        class FakeWidgets:
            def __init__(self, val=None):
                self.val = val

            def get(self, key):
                if self.val:
                    return self.val
                raise KeyError("no widget")

        class FakeDbutils:
            def __init__(self, secret=None, widget=None):
                self.secrets = FakeSecrets(secret)
                self.widgets = FakeWidgets(widget)

        # 1. Secret beats widget and env
        db1 = FakeDbutils(secret="secret_tok", widget="widget_tok")
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "env_tok"}):
            self.assertEqual(resolve_token(db1), "secret_tok")

        # 2. Widget beats env when secret is empty
        db2 = FakeDbutils(secret="", widget="widget_tok")
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "env_tok"}):
            self.assertEqual(resolve_token(db2), "widget_tok")

        # 3. Env fallback when no secret and no widget
        db3 = FakeDbutils(secret="", widget="")
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "env_tok"}):
            self.assertEqual(resolve_token(db3), "env_tok")
            self.assertEqual(resolve_token(None), "env_tok")

        # 4. Missing token raises ValueError
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                resolve_token(None)

    def test_repo_selection_and_slugs(self):
        # "ALL" expands to the 10 repos
        all_repos = resolve_repo("ALL")
        self.assertEqual(len(all_repos), 10)
        self.assertEqual(all_repos, TRACKED_REPOS)

        # "owner/repo" and "owner_repo" both resolve
        self.assertEqual(resolve_repo("postgres/postgres"), [("postgres", "postgres")])
        self.assertEqual(resolve_repo("postgres_postgres"), [("postgres", "postgres")])
        self.assertEqual(resolve_repo("milvus-io/milvus"), [("milvus-io", "milvus")])
        self.assertEqual(resolve_repo("milvus-io_milvus"), [("milvus-io", "milvus")])

        # Unknown repo raises clear ValueError
        with self.assertRaises(ValueError):
            resolve_repo("unknown/repo")

        # repo_dir_slug and repo_full_name_of
        self.assertEqual(repo_dir_slug("postgres/postgres"), "postgres_postgres")
        self.assertEqual(repo_dir_slug("postgres", "postgres"), "postgres_postgres")
        self.assertEqual(repo_dir_slug("milvus-io/milvus"), "milvus-io_milvus")
        self.assertEqual(repo_full_name_of("postgres_postgres"), "postgres/postgres")
        self.assertEqual(repo_full_name_of(("postgres", "postgres")), "postgres/postgres")
        self.assertEqual(repo_full_name_of("milvus-io_milvus"), "milvus-io/milvus")


# Integration acceptance check using Spark if available
HAS_SPARK = False
spark = None
try:
    from pyspark.sql import SparkSession
    from src.common import read_raw_records, parse_records, discover_drift_keys
    from src.schemas import BRONZE_SCHEMAS

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_github_client_integration")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    HAS_SPARK = True
except Exception:
    HAS_SPARK = False


class TestGithubClientSparkIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not HAS_SPARK:
            return
        cls.temp_dir = tempfile.mkdtemp()
        cls.repo_dir = os.path.join(cls.temp_dir, "postgres_postgres")
        os.makedirs(cls.repo_dir, exist_ok=True)

        # 1. repo_metadata (single dict)
        cls.metadata_data = {
            "id": 12345,
            "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NQ==",
            "name": "postgres",
            "full_name": "postgres/postgres",
            "owner": {"login": "postgres", "id": 1, "type": "Organization"},
            "private": False,
            "description": "PostgreSQL database",
            "fork": False,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T00:00:00Z",
            "pushed_at": "2026-04-01T00:00:00Z",
            "stargazers_count": 1000,
            "watchers_count": 1000,
            "forks_count": 500,
            "open_issues_count": 10,
            "language": "C",
            "default_branch": "master",
            "archived": False,
            "disabled": False,
            "topics": ["database", "sql"],
        }
        write_json_atomic(os.path.join(cls.repo_dir, "repo_metadata.json"), cls.metadata_data)

        # 2. commits (array)
        cls.commits_data = [
            {
                "sha": "abc1234567890abcdef1234567890abcdef1234",
                "node_id": "C_kwDO...",
                "commit": {
                    "author": {"name": "Alice", "email": "alice@example.com", "date": "2026-04-01T12:00:00Z"},
                    "committer": {"name": "Alice", "email": "alice@example.com", "date": "2026-04-01T12:00:00Z"},
                    "message": "fix: resolve buffer leak",
                    "comment_count": 0,
                },
                "author": {"login": "alice", "id": 10, "type": "User"},
                "committer": {"login": "alice", "id": 10, "type": "User"},
            }
        ]
        write_json_atomic(os.path.join(cls.repo_dir, "commits.json"), cls.commits_data)

        # 3. issues (array with: one normal, one PR embedded, one null id, one extra field)
        cls.issues_data = [
            {
                "id": 1001,
                "node_id": "I_1001",
                "number": 1,
                "title": "Bug in index scan",
                "user": {"login": "bob", "id": 20, "type": "User"},
                "state": "open",
                "comments": 2,
                "created_at": "2026-04-01T10:00:00Z",
                "updated_at": "2026-04-02T10:00:00Z",
            },
            {
                "id": 1002,
                "node_id": "PR_1002",
                "number": 2,
                "title": "Fix index scan",
                "user": {"login": "carol", "id": 30, "type": "User"},
                "state": "closed",
                "comments": 1,
                "created_at": "2026-04-02T10:00:00Z",
                "updated_at": "2026-04-03T10:00:00Z",
                "pull_request": {"url": "https://api.github.com/repos/postgres/postgres/pulls/2"},
            },
            {
                "id": None,
                "node_id": "I_null",
                "number": 3,
                "title": "Issue without id",
                "user": {"login": "dave", "id": 40, "type": "User"},
                "state": "open",
                "comments": 0,
                "created_at": "2026-04-03T10:00:00Z",
                "updated_at": "2026-04-03T10:00:00Z",
            },
            {
                "id": 1004,
                "node_id": "I_1004",
                "number": 4,
                "title": "Issue with drift",
                "user": {"login": "eve", "id": 50, "type": "User"},
                "state": "open",
                "comments": 0,
                "created_at": "2026-04-04T10:00:00Z",
                "updated_at": "2026-04-04T10:00:00Z",
                "extra_community_sentiment": "positive",
            },
        ]
        write_json_atomic(os.path.join(cls.repo_dir, "issues.json"), cls.issues_data)

        # 4. pulls (array)
        cls.pulls_data = [
            {
                "id": 2001,
                "node_id": "PR_2001",
                "number": 2,
                "state": "closed",
                "title": "Fix index scan",
                "user": {"login": "carol", "id": 30, "type": "User"},
                "created_at": "2026-04-02T10:00:00Z",
                "updated_at": "2026-04-03T10:00:00Z",
            }
        ]
        write_json_atomic(os.path.join(cls.repo_dir, "pulls.json"), cls.pulls_data)

        # 5. releases (empty array)
        cls.releases_data = []
        write_json_atomic(os.path.join(cls.repo_dir, "releases.json"), cls.releases_data)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "temp_dir") and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_spark_integration_read_and_parse(self):
        if not HAS_SPARK:
            self.skipTest("PySpark not installed or SparkSession unavailable")

        # 1. Empty '[]' file reads as 0 rows
        rel_path = os.path.join(self.repo_dir, "releases.json")
        rel_raw = read_raw_records(spark, rel_path)
        self.assertEqual(rel_raw.count(), 0)

        # 2. Issues file read
        issues_path = os.path.join(self.repo_dir, "issues.json")
        issues_raw = read_raw_records(spark, issues_path)
        self.assertEqual(issues_raw.count(), 4)

        # 3. Extra field discovered as drift
        drift_keys = discover_drift_keys(issues_raw, BRONZE_SCHEMAS["issues"])
        self.assertIn("extra_community_sentiment", drift_keys)

        # 4. Parse records with explicit schema and drift keys
        parsed_df = parse_records(issues_raw, BRONZE_SCHEMAS["issues"], drift_keys)
        rows = parsed_df.collect()
        self.assertEqual(len(rows), 4)

        # 5. No record is wrongly flagged corrupt (_corrupt_record is None for all valid JSON records)
        for row in rows:
            self.assertIsNone(row["_corrupt_record"], f"Corrupt flag wrongly set for row: {row}")

        # 6. Null id record is the only reject (where id is null)
        null_rows = [r for r in rows if r["id"] is None]
        self.assertEqual(len(null_rows), 1)
        self.assertEqual(null_rows[0]["number"], 3)

        # 7. Valid rows have non-null id
        valid_rows = [r for r in rows if r["id"] is not None]
        self.assertEqual(len(valid_rows), 3)

        # 8. PR embedded in issues parses properly
        pr_rows = [r for r in valid_rows if r["pull_request"] is not None]
        self.assertEqual(len(pr_rows), 1)
        self.assertEqual(pr_rows[0]["pull_request"]["url"], "https://api.github.com/repos/postgres/postgres/pulls/2")


if __name__ == "__main__":
    unittest.main()

