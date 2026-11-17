import unittest

from src.pipeline_preflight import (
    ensure_source_layout,
    expected_source_files,
    find_missing_source_files,
    normalize_source_path,
)


class TestPipelinePreflight(unittest.TestCase):
    def test_rejects_nested_raw_api_path_with_actionable_message(self):
        with self.assertRaisesRegex(ValueError, "raw_api must be a sibling"):
            normalize_source_path("/Volumes/workspace/bronze_data/raw/raw_api")

    def test_accepts_valid_raw_roots(self):
        self.assertEqual(
            normalize_source_path("/Volumes/workspace/bronze_data/raw/"),
            "/Volumes/workspace/bronze_data/raw",
        )
        self.assertEqual(
            normalize_source_path("/Volumes/workspace/bronze_data/raw_api"),
            "/Volumes/workspace/bronze_data/raw_api",
        )

    def test_detects_missing_stars_history_without_skipping_it(self):
        repos = ["postgres/postgres", "redis/redis"]
        entities = ["repo_metadata", "stars_history"]
        staged = {
            "/Volumes/workspace/bronze_data/raw/postgres_postgres/repo_metadata.json"
        }

        missing = find_missing_source_files(
            "/Volumes/workspace/bronze_data/raw",
            repos,
            entities,
            staged.__contains__,
        )

        self.assertEqual(
            missing,
            [
                "/Volumes/workspace/bronze_data/raw/postgres_postgres/stars_history.json",
                "/Volumes/workspace/bronze_data/raw/redis_redis/repo_metadata.json",
                "/Volumes/workspace/bronze_data/raw/redis_redis/stars_history.json",
            ],
        )
        self.assertTrue(
            any(path.endswith("/stars_history.json") for path in missing)
        )

    def test_all_staged_files_have_no_missing_sources(self):
        paths = expected_source_files(
            "/Volumes/workspace/bronze_data/raw",
            ["postgres/postgres"],
            ["repo_metadata", "stars_history"],
        )
        self.assertEqual(
            ensure_source_layout(
                "/Volumes/workspace/bronze_data/raw",
                ["postgres/postgres"],
                ["repo_metadata", "stars_history"],
                set(paths).__contains__,
                lambda: self.fail("ingestion should not run for a complete batch"),
            ),
            "/Volumes/workspace/bronze_data/raw",
        )

    def test_present_empty_json_file_is_not_missing(self):
        self.assertEqual(
            find_missing_source_files(
                "/Volumes/workspace/bronze_data/raw",
                ["postgres/postgres"],
                ["issues"],
                lambda _path: True,
            ),
            [],
        )

    def test_missing_stars_history_refreshes_and_redirects_to_new_batch(self):
        repos = ["postgres/postgres"]
        entities = ["repo_metadata", "stars_history"]
        old_paths = {
            "/Volumes/workspace/bronze_data/raw/postgres_postgres/repo_metadata.json"
        }
        new_paths = set(expected_source_files(
            "/Volumes/workspace/bronze_data/incremental/run_incr",
            repos,
            entities,
        ))
        ingestion_calls = []

        def file_exists(path):
            return path in (new_paths if "incremental/run_incr" in path else old_paths)

        result = ensure_source_layout(
            "/Volumes/workspace/bronze_data/raw",
            repos,
            entities,
            file_exists,
            lambda: ingestion_calls.append(True) or
            "/Volumes/workspace/bronze_data/incremental/run_incr",
        )

        self.assertEqual(result, "/Volumes/workspace/bronze_data/incremental/run_incr")
        self.assertEqual(ingestion_calls, [True])


if __name__ == "__main__":
    unittest.main()
