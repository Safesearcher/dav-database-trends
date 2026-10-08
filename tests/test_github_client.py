"""
Unit and integration tests for src/ingestion/github_client.py.

Covers:
1. parse_next_link: present, absent, multiple rels
2. paginate across 3 mocked pages -> all records, page and api_call counts
3. normalise_since: YYYY-MM-DD, ISO-8601 UTC, offset -> UTC, invalid -> ValueError
4. pulls cutoff: created_at in full mode vs updated_at in incremental mode
5. until filtering for issues and pulls
6. rate limit: remaining <= 2 triggers sleep(reset - now + margin); 403 Retry-After honoured
7. 500 retry then 200; persistent 500/connection error -> GitHubFetchError and atomic safety
8. write_json_atomic: success clean file; simulated failure leaves no .tmp and no truncated target
9. token security: token never in exceptions, stdout, or stderr on failure
10. resolve_token: secret -> widget -> env precedence, missing token error
11. repo selection: ALL -> 10 repos, owner/repo and owner_repo resolve, unknown -> clear error
12. Integration with local Spark: BRONZE_SCHEMAS compatibility, drift discovery, null id, empty files
"""

import io
import json
import os
import tempfile
import time
import unittest
import urllib.error
from unittest import mock

from src.ingestion.github_client import (
    TRACKED_REPOS,
    GitHubFetchError,
    build_url,
    fetch_entity,
    normalise_since,
    paginate,
    paginate_pulls,
    parse_next_link,
    repo_dir_slug,
    repo_full_name_of,
    resolve_repos,
    resolve_token,
    write_json_atomic,
)


class MockHTTPResponse:
    def __init__(self, data, headers=None, status=200):
        self._data = json.dumps(data).encode("utf-8") if not isinstance(data, (bytes, bytearray)) else data
        self.headers = headers or {}
        self.status = status

    def read(self):
        return self._data

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockOpener:
    def __init__(self, responses_by_url=None):
        self.responses_by_url = responses_by_url or {}
        self.calls = []

    def open(self, req, timeout=15.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        self.calls.append((url, req.headers))
        if url in self.responses_by_url:
            resp = self.responses_by_url[url]
            if isinstance(resp, Exception):
                raise resp
            if callable(resp):
                return resp(req)
            return resp
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b'{"message":"Not Found"}'))


class TestParseNextLink(unittest.TestCase):
    def test_link_present(self):
        header = '<https://api.github.com/repos/postgres/postgres/commits?page=2&per_page=100>; rel="next"'
        self.assertEqual(
            parse_next_link(header),
            "https://api.github.com/repos/postgres/postgres/commits?page=2&per_page=100",
        )

    def test_link_absent(self):
        header = '<https://api.github.com/repos/postgres/postgres/commits?page=1&per_page=100>; rel="prev"'
        self.assertIsNone(parse_next_link(header))
        self.assertIsNone(parse_next_link(None))
        self.assertIsNone(parse_next_link(""))

    def test_multiple_rels(self):
        header = (
            '<https://api.github.com/repos/org/repo/issues?page=1>; rel="prev", '
            '<https://api.github.com/repos/org/repo/issues?page=3>; rel="next", '
            '<https://api.github.com/repos/org/repo/issues?page=10>; rel="last"'
        )
        self.assertEqual(
            parse_next_link(header),
            "https://api.github.com/repos/org/repo/issues?page=3",
        )


class TestNormaliseSince(unittest.TestCase):
    def test_date_only(self):
        self.assertEqual(normalise_since("2026-09-25"), "2026-09-25T00:00:00Z")

    def test_iso_utc(self):
        self.assertEqual(normalise_since("2026-09-25T00:00:00Z"), "2026-09-25T00:00:00Z")

    def test_iso_with_offset(self):
        # 10:00 +05:00 is 05:00 UTC
        self.assertEqual(normalise_since("2026-09-25T10:00:00+05:00"), "2026-09-25T05:00:00Z")

    def test_invalid_dates_raise(self):
        for invalid in ["not-a-date", "2026-13-45", "", "   ", "2026/09/25"]:
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalise_since(invalid)


class TestPagination(unittest.TestCase):
    def test_paginate_three_pages(self):
        url1 = "https://api.github.com/repos/org/repo/commits?per_page=100"
        url2 = "https://api.github.com/repos/org/repo/commits?page=2&per_page=100"
        url3 = "https://api.github.com/repos/org/repo/commits?page=3&per_page=100"

        opener = MockOpener({
            url1: MockHTTPResponse(
                [{"sha": "c1"}, {"sha": "c2"}],
                headers={"Link": f'<{url2}>; rel="next"'},
            ),
            url2: MockHTTPResponse(
                [{"sha": "c3"}, {"sha": "c4"}],
                headers={"Link": f'<{url3}>; rel="next"'},
            ),
            url3: MockHTTPResponse(
                [{"sha": "c5"}],
                headers={},
            ),
        })

        stats = {"pages": 0, "api_calls": 0}
        records = paginate(url1, token="test_token", opener=opener, stats=stats)
        self.assertEqual(len(records), 5)
        self.assertEqual([r["sha"] for r in records], ["c1", "c2", "c3", "c4", "c5"])
        self.assertEqual(stats["pages"], 3)
        self.assertEqual(stats["api_calls"], 3)


class TestPullsCutoff(unittest.TestCase):
    def test_pulls_cutoff_created_vs_updated(self):
        """
        Fixtures where created_at and updated_at cutoffs produce different record sets.
        PR 1: created=2026-03-15 (before since), updated=2026-05-10 (after since)
        PR 2: created=2026-05-01 (after since), updated=2026-05-02 (after since)
        since = 2026-04-01T00:00:00Z
        """
        url = "https://api.github.com/repos/org/repo/pulls"
        since_iso = "2026-04-01T00:00:00Z"

        # Case A: Full mode (cutoff on created_at, sorted created desc)
        # Sequence encountered: PR2 (created 05-01) -> PR1 (created 03-15 < since -> stop!)
        opener_full = MockOpener({
            url: MockHTTPResponse([
                {"id": 2, "created_at": "2026-05-01T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"},
                {"id": 1, "created_at": "2026-03-15T00:00:00Z", "updated_at": "2026-05-10T00:00:00Z"},
            ])
        })
        records_full = paginate_pulls(
            url, token="test_token", since_iso=since_iso, cutoff_field="created_at", opener=opener_full
        )
        self.assertEqual([r["id"] for r in records_full], [2])

        # Case B: Incremental mode (cutoff on updated_at, sorted updated desc)
        # Sequence: PR1 (updated 05-10 >= since), PR2 (updated 05-02 >= since), PR3 (updated 03-20 < since -> stop)
        opener_incr = MockOpener({
            url: MockHTTPResponse([
                {"id": 1, "created_at": "2026-03-15T00:00:00Z", "updated_at": "2026-05-10T00:00:00Z"},
                {"id": 2, "created_at": "2026-05-01T00:00:00Z", "updated_at": "2026-05-02T00:00:00Z"},
                {"id": 3, "created_at": "2026-02-01T00:00:00Z", "updated_at": "2026-03-20T00:00:00Z"},
            ])
        })
        records_incr = paginate_pulls(
            url, token="test_token", since_iso=since_iso, cutoff_field="updated_at", opener=opener_incr
        )
        self.assertEqual([r["id"] for r in records_incr], [1, 2])


class TestUntilFiltering(unittest.TestCase):
    def test_until_filtering_issues_and_pulls(self):
        url_issues = build_url("org", "repo", "issues", run_mode="incremental", since_iso="2026-04-01T00:00:00Z")
        opener = MockOpener({
            url_issues: MockHTTPResponse([
                {"id": 101, "updated_at": "2026-05-15T00:00:00Z"},  # After until -> excluded
                {"id": 102, "updated_at": "2026-05-01T00:00:00Z"},  # Before until -> kept
                {"id": 103, "updated_at": "2026-04-10T00:00:00Z"},  # Before until -> kept
            ])
        })
        records, stats = fetch_entity(
            owner="org",
            repo="repo",
            entity="issues",
            run_mode="incremental",
            since_iso="2026-04-01T00:00:00Z",
            until_iso="2026-05-05T00:00:00Z",
            token="test_token",
            opener=opener,
        )
        self.assertEqual([r["id"] for r in records], [102, 103])


class TestRateLimiting(unittest.TestCase):
    @mock.patch("time.sleep")
    def test_remaining_le_2_triggers_sleep(self, mock_sleep):
        url = "https://api.github.com/repos/org/repo/commits?per_page=100"
        reset_time = int(time.time()) + 120
        opener = MockOpener({
            url: MockHTTPResponse(
                [{"sha": "c1"}],
                headers={
                    "X-RateLimit-Remaining": "2",
                    "X-RateLimit-Reset": str(reset_time),
                },
            )
        })
        paginate(url, token="test_token", opener=opener)
        self.assertTrue(mock_sleep.called)
        slept_seconds = mock_sleep.call_args[0][0]
        # Should sleep approximately 120 + margin
        self.assertGreaterEqual(slept_seconds, 118)

    @mock.patch("time.sleep")
    def test_403_secondary_rate_limit_with_retry_after(self, mock_sleep):
        url = "https://api.github.com/repos/org/repo/commits?per_page=100"
        call_count = [0]

        def handler(req):
            call_count[0] += 1
            if call_count[0] == 1:
                fp = io.BytesIO(b'{"message":"You have exceeded a secondary rate limit."}')
                raise urllib.error.HTTPError(url, 403, "Forbidden", {"Retry-After": "15"}, fp)
            return MockHTTPResponse([{"sha": "c1"}])

        opener = MockOpener({url: handler})
        records = paginate(url, token="test_token", opener=opener, retries=3)
        self.assertEqual(len(records), 1)
        self.assertTrue(mock_sleep.called)
        # Verify it slept at least 15s
        self.assertGreaterEqual(mock_sleep.call_args[0][0], 15)


class TestRetryAndErrorHandling(unittest.TestCase):
    @mock.patch("time.sleep")
    def test_500_then_200_succeeds(self, mock_sleep):
        url = "https://api.github.com/repos/org/repo/commits?per_page=100"
        call_count = [0]

        def handler(req):
            call_count[0] += 1
            if call_count[0] == 1:
                fp = io.BytesIO(b'{"message":"Internal Server Error"}')
                raise urllib.error.HTTPError(url, 500, "Internal Server Error", {}, fp)
            return MockHTTPResponse([{"sha": "c_success"}])

        opener = MockOpener({url: handler})
        records = paginate(url, token="test_token", opener=opener, retries=3)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["sha"], "c_success")
        self.assertTrue(mock_sleep.called)

    @mock.patch("time.sleep")
    def test_persistent_500_raises_fetch_error_and_preserves_target_file(self, mock_sleep):
        url = "https://api.github.com/repos/org/repo/commits?per_page=100"

        def handler(req):
            fp = io.BytesIO(b'{"message":"Persistent 500"}')
            raise urllib.error.HTTPError(url, 500, "Internal Server Error", {}, fp)

        opener = MockOpener({url: handler})
        with tempfile.TemporaryDirectory() as tmpdir:
            target_file = os.path.join(tmpdir, "commits.json")
            initial_content = '[{"existing": "data"}]\n'
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(initial_content)

            with self.assertRaises(GitHubFetchError):
                records, _ = fetch_entity(
                    "org", "repo", "commits", "full", "2026-04-01T00:00:00Z", None, "test_token", opener=opener
                )
                write_json_atomic(target_file, records)

            # Target file must be byte-identical and no .tmp left behind
            with open(target_file, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), initial_content)
            self.assertFalse(os.path.exists(f"{target_file}.tmp"))


class TestWriteJsonAtomic(unittest.TestCase):
    def test_success_leaves_no_tmp_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "subdir", "output.json")
            data = [{"key": "value", "id": 123}]
            write_json_atomic(target, data)

            self.assertTrue(os.path.exists(target))
            self.assertFalse(os.path.exists(f"{target}.tmp"))
            with open(target, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self.assertEqual(loaded, data)

    def test_simulated_failure_cleans_tmp_and_leaves_no_corrupt_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "output.json")
            # Object that fails during JSON serialization
            bad_obj = {"unserializable": object()}

            with self.assertRaises(Exception):
                write_json_atomic(target, bad_obj)

            self.assertFalse(os.path.exists(target))
            self.assertFalse(os.path.exists(f"{target}.tmp"))


class TestTokenSecurity(unittest.TestCase):
    def test_token_never_printed_or_in_exception(self):
        secret_token = "ghp_SECRET_TOKEN_DO_NOT_LEAK_99999"
        url = "https://api.github.com/repos/org/repo/commits?per_page=100"

        def handler(req):
            fp = io.BytesIO(b'{"message":"Bad credentials"}')
            raise urllib.error.HTTPError(url, 401, "Unauthorized", {}, fp)

        opener = MockOpener({url: handler})

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with mock.patch("sys.stdout", stdout_capture), mock.patch("sys.stderr", stderr_capture):
            try:
                paginate(url, token=secret_token, opener=opener, retries=0)
            except GitHubFetchError as exc:
                err_msg = str(exc)
                self.assertNotIn(secret_token, err_msg)
            except Exception as e:
                self.assertNotIn(secret_token, str(e))

        captured_out = stdout_capture.getvalue()
        captured_err = stderr_capture.getvalue()
        self.assertNotIn(secret_token, captured_out)
        self.assertNotIn(secret_token, captured_err)


class TestResolveToken(unittest.TestCase):
    def test_secret_takes_precedence(self):
        class FakeSecrets:
            def get(self, scope, key):
                return "token_from_secret"

        class FakeWidgets:
            def get(self, name):
                return "token_from_widget"

        class FakeDbutils:
            secrets = FakeSecrets()
            widgets = FakeWidgets()

        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "token_from_env"}):
            token = resolve_token(FakeDbutils())
            self.assertEqual(token, "token_from_secret")

    def test_widget_fallback_when_no_secret(self):
        class FakeSecrets:
            def get(self, scope, key):
                raise RuntimeError("Secret not found")

        class FakeWidgets:
            def get(self, name):
                return "token_from_widget"

        class FakeDbutils:
            secrets = FakeSecrets()
            widgets = FakeWidgets()

        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "token_from_env"}):
            token = resolve_token(FakeDbutils())
            self.assertEqual(token, "token_from_widget")

    def test_env_fallback_when_dbutils_none(self):
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "token_from_env"}):
            token = resolve_token(None)
            self.assertEqual(token, "token_from_env")

    def test_missing_token_raises_value_error(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as ctx:
                resolve_token(None)
            self.assertIn("GitHub token not found", str(ctx.exception))


class TestRepoSelection(unittest.TestCase):
    def test_all_expands_to_ten_repos(self):
        repos = resolve_repos("ALL")
        self.assertEqual(len(repos), 10)
        self.assertEqual(repos, TRACKED_REPOS)

    def test_owner_repo_and_owner_underscore_repo_resolve(self):
        r1 = resolve_repos("surrealdb/surrealdb")
        self.assertEqual(r1, [("surrealdb", "surrealdb")])

        r2 = resolve_repos("surrealdb_surrealdb")
        self.assertEqual(r2, [("surrealdb", "surrealdb")])

    def test_unknown_repo_raises_clear_error(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_repos("unknown/repo")
        self.assertIn("Unknown repository 'unknown/repo'", str(ctx.exception))

    def test_repo_slug_helpers(self):
        self.assertEqual(repo_dir_slug("surrealdb/surrealdb"), "surrealdb_surrealdb")
        self.assertEqual(repo_dir_slug("surrealdb_surrealdb"), "surrealdb_surrealdb")
        self.assertEqual(repo_full_name_of("surrealdb_surrealdb"), "surrealdb/surrealdb")
        self.assertEqual(repo_full_name_of("surrealdb/surrealdb"), "surrealdb/surrealdb")


class TestBronzeIntegrationWithLocalSpark(unittest.TestCase):
    """
    Integration test asserting that JSON written with the new github_client writer
    parses cleanly into Bronze Delta schemas:
    - zero false-positive corrupt records
    - schema drift is correctly discovered
    - null-id record is detected for quarantine
    - embedded PR in issues is parsed
    - empty [] file reads 0 rows.
    """

    @classmethod
    def setUpClass(cls):
        try:
            from pyspark.sql import SparkSession
            cls.spark = (
                SparkSession.builder
                .master("local[1]")
                .appName("test_bronze_integration")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
        except Exception:
            cls.spark = None

    def test_spark_read_and_schema_compatibility(self):
        if self.spark is None:
            self.skipTest("Local Spark not available")

        from src.common import discover_drift_keys, parse_records, read_raw_records
        from src.schemas import BRONZE_SCHEMAS

        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Fake Issues: 1 valid issue, 1 embedded PR, 1 with null id, 1 with drift field
            issues_data = [
                {
                    "id": 1001,
                    "number": 1,
                    "title": "Bug report",
                    "state": "open",
                    "user": {"login": "alice", "id": 11, "type": "User"},
                    "created_at": "2026-05-01T00:00:00Z",
                    "updated_at": "2026-05-02T00:00:00Z",
                },
                {
                    "id": 1002,
                    "number": 2,
                    "title": "PR submission",
                    "state": "open",
                    "pull_request": {"url": "https://api.github.com/repos/org/repo/pulls/2"},
                    "user": {"login": "bob", "id": 12, "type": "User"},
                    "created_at": "2026-05-03T00:00:00Z",
                    "updated_at": "2026-05-04T00:00:00Z",
                },
                {
                    "id": None,  # Null PK
                    "number": 3,
                    "title": "Invalid issue with null id",
                    "created_at": "2026-05-05T00:00:00Z",
                },
                {
                    "id": 1004,
                    "number": 4,
                    "title": "Issue with schema drift",
                    "extra_drift_key": "unexpected_metadata_value",
                    "created_at": "2026-05-06T00:00:00Z",
                },
            ]

            issues_path = os.path.join(tmpdir, "issues.json")
            write_json_atomic(issues_path, issues_data)

            # 2. Empty releases file "[]"
            releases_path = os.path.join(tmpdir, "releases.json")
            write_json_atomic(releases_path, [])

            # Read and verify issues
            raw_issues = read_raw_records(self.spark, issues_path)
            self.assertEqual(raw_issues.count(), 4)

            drift_keys = discover_drift_keys(raw_issues, BRONZE_SCHEMAS["issues"])
            self.assertIn("extra_drift_key", drift_keys)

            parsed_issues = parse_records(raw_issues, BRONZE_SCHEMAS["issues"], drift_keys)
            rows = parsed_issues.collect()

            # Zero corrupt records among valid JSON objects
            corrupt_count = sum(1 for r in rows if r["_corrupt_record"] is not None)
            self.assertEqual(corrupt_count, 0)

            # Null ID record detected
            null_id_count = sum(1 for r in rows if r["id"] is None)
            self.assertEqual(null_id_count, 1)

            # Embedded PR recognized
            pr_embedded = [r for r in rows if r["pull_request"] is not None]
            self.assertEqual(len(pr_embedded), 1)
            self.assertEqual(pr_embedded[0]["pull_request"]["url"], "https://api.github.com/repos/org/repo/pulls/2")

            # Read and verify empty releases
            raw_releases = read_raw_records(self.spark, releases_path)
            self.assertEqual(raw_releases.count(), 0)


if __name__ == "__main__":
    unittest.main()
