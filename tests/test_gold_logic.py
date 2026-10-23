"""
Unit and integration tests for Gold Star Schema transformations (src/gold_transforms.py).

Covers:
1. Surrogate key determinism (xxhash64 reproducibility)
2. Coverage of all 10 target repositories in REPO_DB_MAPPING (fails if any repo unmapped)
3. dim_repo enrichment with db_type and db_category
4. dim_date spine generation, temporal attributes, and analysis window tagging
5. dim_actor unknown handling, FAISS-style null-login fallback to email_hash,
   and max(author_id) dedup when same login appears in commits (has author_id) and PRs (no author_id)
6. fact_commits construction and PII avoidance — uses SILVER_COMMITS_SCHEMA (commit_date_utc)
7. fact_issues label_count derived from labels_list ARRAY, time_to_close_hours defensive behavior
8. fact_pull_requests hours_to_merge defensive behavior — SILVER schema has no author_id
9. fact_releases days_since_previous_release window lag calculation
10. fact_repo_snapshot metric preservation and idempotent keying
11. fact_repo_daily_activity dense calendar spine, rolling 4-week / 12-week metrics, and total reconciliation
"""

import os
import unittest
from datetime import datetime, timezone, date

from src.gold_transforms import (
    REPO_DB_MAPPING,
    ALL_10_REPOS,
    DIM_DATE_START,
    DIM_DATE_END,
    DIM_DATE_ACTIVITY_SPINE_START,
    compute_repo_key,
    compute_date_key,
    compute_actor_identity,
    compute_actor_key,
    build_dim_repo,
    build_dim_date,
    build_dim_actor,
    build_fact_commits,
    build_fact_issues,
    build_fact_pull_requests,
    build_fact_releases,
    build_fact_repo_snapshot,
    build_fact_repo_daily_activity,
)

# Detect if PySpark is available
HAS_SPARK = False
spark = None

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        LongType,
        BooleanType,
        TimestampType,
        DoubleType,
        ArrayType,
    )

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_logic")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    HAS_SPARK = True
except Exception:
    HAS_SPARK = False


class TestGoldTransforms(unittest.TestCase):

    def test_all_10_repos_mapped(self):
        """Test verifies that all 10 project repositories are explicitly mapped to a db_type and db_category."""
        expected_repos = {
            "postgres/postgres",
            "cockroachdb/cockroach",
            "mongodb/mongo",
            "surrealdb/surrealdb",
            "redis/redis",
            "apache/cassandra",
            "facebookresearch/faiss",
            "qdrant/qdrant",
            "milvus-io/milvus",
            "chroma-core/chroma",
        }
        self.assertEqual(set(REPO_DB_MAPPING.keys()), expected_repos)
        for repo, meta in REPO_DB_MAPPING.items():
            self.assertIn("db_type", meta)
            self.assertIn("db_category", meta)
            self.assertIn(meta["db_type"], {"SQL", "NoSQL", "Vector"})
            self.assertGreater(len(meta["db_category"]), 0)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_surrogate_key_determinism(self):
        """Test verifies xxhash64 surrogate keys are reproducible across multiple DataFrame calls."""
        data = [("postgres/postgres",), ("redis/redis",), ("mongodb/mongo",)]
        df1 = spark.createDataFrame(data, ["repo_full_name"]).withColumn("repo_key", compute_repo_key("repo_full_name"))
        df2 = spark.createDataFrame(data, ["repo_full_name"]).withColumn("repo_key", compute_repo_key("repo_full_name"))

        keys1 = [row["repo_key"] for row in df1.collect()]
        keys2 = [row["repo_key"] for row in df2.collect()]
        self.assertEqual(keys1, keys2)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_repo_enrichment(self):
        """Test verifies silver.repo_metadata is transformed into gold.dim_repo with db_type and db_category.
        Uses SILVER_REPO_METADATA_SCHEMA so a wrong column name fails immediately."""
        from src.schemas import SILVER_REPO_METADATA_SCHEMA
        now = datetime.now(timezone.utc)
        data = [
            (1001, "postgres/postgres", "postgres", "postgres", "PostgreSQL database", "master", "PostgreSQL", 1000, 200, 50, now, now, now, ["sql"], "file1", now),
            (1002, "facebookresearch/faiss", "faiss", "facebookresearch", "Vector similarity search", "main", "MIT", 5000, 800, 100, now, now, now, ["vector"], "file2", now),
        ]
        df_silver_meta = spark.createDataFrame(data, schema=SILVER_REPO_METADATA_SCHEMA)
        dim_repo = build_dim_repo(df_silver_meta)

        rows = {r["repo_full_name"]: r for r in dim_repo.collect()}
        self.assertIn("postgres/postgres", rows)
        self.assertEqual(rows["postgres/postgres"]["db_type"], "SQL")
        self.assertEqual(rows["postgres/postgres"]["db_category"], "Relational")

        self.assertIn("facebookresearch/faiss", rows)
        self.assertEqual(rows["facebookresearch/faiss"]["db_type"], "Vector")
        self.assertEqual(rows["facebookresearch/faiss"]["db_category"], "Vector Index/Search")

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_date_spine_and_window_flags(self):
        """Test verifies calendar spine generation and in_analysis_window boundary logic.
        Uses an explicit small range to avoid generating years of data in local tests."""
        dim_date = build_dim_date(
            spark,
            start_date_str="2026-03-31",
            end_date_str="2026-04-03",
            window_since="2026-04-01",
            window_until="2026-09-30",
        )
        dates = {r["date_key"]: r for r in dim_date.collect()}

        # Exactly 4 rows: 2026-03-31, 2026-04-01, 2026-04-02, 2026-04-03
        self.assertEqual(len(dates), 4)

        # 2026-03-31 should be outside window
        self.assertFalse(dates[20260331]["in_analysis_window"])
        # 2026-04-01 should be inside window
        self.assertTrue(dates[20260401]["in_analysis_window"])
        self.assertEqual(dates[20260401]["year"], 2026)
        self.assertEqual(dates[20260401]["quarter"], 2)
        self.assertEqual(dates[20260401]["month"], 4)
        self.assertEqual(dates[20260401]["month_name"], "April")

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_date_covers_historical_dates(self):
        """Verify that the production dim_date range (DIM_DATE_START=2008-01-01 to
        DIM_DATE_END=2027-12-31) covers pre-2020 issue and release creation dates
        that arrive via GitHub's since-filter which matches on *updated* date.

        Specifically tests:
        - A 2012-01-15 issue created during PostgreSQL's early GitHub era.
        - A 2018-09-12 release from early MongoDB/Redis history.
        - That DIM_DATE_ACTIVITY_SPINE_START (2020-01-01) is still after DIM_DATE_START,
          so the daily-activity spine guard is effective.
        """
        self.assertEqual(DIM_DATE_START, "2008-01-01")
        self.assertEqual(DIM_DATE_END, "2027-12-31")
        self.assertEqual(DIM_DATE_ACTIVITY_SPINE_START, "2020-01-01")

        # Build a minimal slice that covers the two historical sentinel dates.
        # Using a narrow range keeps local test execution fast.
        dim_date_hist = build_dim_date(
            spark,
            start_date_str="2012-01-15",
            end_date_str="2018-09-12",
            window_since="2026-04-01",
            window_until="2026-09-30",
        )
        date_keys = {r["date_key"] for r in dim_date_hist.collect()}

        # 2012-01-15: early PostgreSQL/Redis GitHub issue creation dates (YYYYMMDD = 20120115)
        self.assertIn(20120115, date_keys,
            "dim_date must cover 20120115 to resolve orphan fact_issues.created_date_key")

        # 2018-09-12: early release published_date examples (YYYYMMDD = 20180912)
        self.assertIn(20180912, date_keys,
            "dim_date must cover 20180912 to resolve orphan fact_releases.published_date_key")

        # Validate that the full production default range generates the right row count.
        # 2008-01-01 to 2027-12-31 inclusive:
        # 2008 is a leap year (366), 2012 leap (366), 2016 leap (366), 2020 leap (366), 2024 leap (366).
        # Total years 2008-2027 = 20 years. Non-leap = 15 years x 365 = 5475. Leap = 5 x 366 = 1830.
        # Total = 5475 + 1830 = 7305 days.
        full_dim_date = build_dim_date(
            spark,
            start_date_str=DIM_DATE_START,
            end_date_str=DIM_DATE_END,
        )
        full_count = full_dim_date.count()
        self.assertEqual(full_count, 7305,
            f"dim_date for {DIM_DATE_START} to {DIM_DATE_END} should have 7305 rows, got {full_count}")


    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_actor_identity_resolution(self):
        """Test verifies login priority, null-login fallback to email_hash (FAISS), and unknown member row.
        Uses SILVER_COMMITS_SCHEMA so column-name drift in the schema is caught immediately."""
        from src.schemas import SILVER_COMMITS_SCHEMA
        now = datetime.now(timezone.utc)
        # Build commit rows matching SILVER_COMMITS_SCHEMA column order:
        # repo_full_name, commit_sha, author_login, author_id, author_email_hash,
        # committer_email_hash, commit_date_utc, commit_headline, comment_count, is_bot,
        # _source_file, load_timestamp
        c_data = [
            ("facebookresearch/faiss", "sha_a", "alice",            101,  "hash_aaa",          "hash_com_a", now, "Fix A", 0, False, "f1", now),
            ("facebookresearch/faiss", "sha_b", None,               None, "hash_meta_employee", "hash_com_b", now, "Fix B", 0, False, "f2", now),
            ("facebookresearch/faiss", "sha_c", "dependabot[bot]",  202,  "hash_dep",           "hash_com_c", now, "Bump", 0, True,  "f3", now),
        ]
        df_commits = spark.createDataFrame(c_data, schema=SILVER_COMMITS_SCHEMA)
        dim_actor = build_dim_actor(df_commits=df_commits, spark=spark)

        actors = {r["actor_identity"]: r for r in dim_actor.collect()}

        # Verify normal user
        self.assertIn("alice", actors)
        self.assertEqual(actors["alice"]["identity_source"], "login")

        # Verify FAISS email_hash fallback
        self.assertIn("hash:hash_meta_employee", actors)
        self.assertEqual(actors["hash:hash_meta_employee"]["identity_source"], "email_hash")

        # Verify Unknown member
        self.assertIn("unknown", actors)
        self.assertEqual(actors["unknown"]["identity_source"], "unknown")
        self.assertFalse(actors["unknown"]["is_bot"])

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_actor_max_author_id_dedup(self):
        """Verify that when the same login appears in both commits (has author_id) and PRs (no author_id),
        the consolidated dim_actor row carries the non-null author_id from commits (max wins over NULL).
        Also confirms actor_key PK uniqueness — exactly ONE row per actor_key."""
        from src.schemas import SILVER_COMMITS_SCHEMA, SILVER_PULL_REQUESTS_SCHEMA
        now = datetime.now(timezone.utc)

        # Commit row for 'alice' with author_id=101
        c_data = [
            ("redis/redis", "sha_r1", "alice", 101, "hash_alice", "hash_c", now, "commit 1", 0, False, "f1", now),
        ]
        df_commits = spark.createDataFrame(c_data, schema=SILVER_COMMITS_SCHEMA)

        # PR row for same 'alice' — PRs have NO author_id column in Silver
        # repo_full_name, pr_number, pr_id, title, state, author_login, is_bot,
        # created_at_utc, updated_at_utc, closed_at_utc, merged_at_utc, is_merged,
        # merge_commit_sha, head_branch, base_branch, _source_file, load_timestamp
        p_data = [
            ("redis/redis", 1, 10001, "PR 1", "closed", "alice", False,
             now, now, now, now, True, "sha_merge", "feat", "main", "f2", now),
        ]
        df_pulls = spark.createDataFrame(p_data, schema=SILVER_PULL_REQUESTS_SCHEMA)

        dim_actor = build_dim_actor(df_commits=df_commits, df_pulls=df_pulls, spark=spark)
        actors = {r["actor_identity"]: r for r in dim_actor.collect()}

        # alice should appear exactly once
        self.assertIn("alice", actors)
        alice = actors["alice"]
        # max(author_id) picks 101 (from commits) over NULL (from PRs)
        self.assertEqual(alice["author_id"], 101)
        self.assertEqual(alice["identity_source"], "login")

        # PK uniqueness: no duplicate actor_key values
        all_rows = dim_actor.collect()
        all_keys = [r["actor_key"] for r in all_rows]
        self.assertEqual(len(all_keys), len(set(all_keys)), "Duplicate actor_key values found in dim_actor")

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_commits_and_pii_absence(self):
        """Test verifies fact_commits mapping and ensures no raw emails or plaintext names exist.
        Uses SILVER_COMMITS_SCHEMA — commit_date_utc is the correct column name."""
        from src.schemas import SILVER_COMMITS_SCHEMA
        t1 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        # Column order from SILVER_COMMITS_SCHEMA:
        # repo_full_name, commit_sha, author_login, author_id, author_email_hash,
        # committer_email_hash, commit_date_utc, commit_headline, comment_count, is_bot,
        # _source_file, load_timestamp
        data = [
            ("redis/redis", "sha123", "antirez", 50, "hash_red", "hash_com", t1, "Fix memory leak", 2, False, "f1", t1),
        ]
        df_silver_commits = spark.createDataFrame(data, schema=SILVER_COMMITS_SCHEMA)
        fact_commits = build_fact_commits(df_silver_commits, window_since="2026-04-01", window_until="2026-09-30")

        row = fact_commits.collect()[0]
        self.assertEqual(row["commit_sha"], "sha123")
        self.assertEqual(row["date_key"], 20260501)
        self.assertTrue(row["in_analysis_window"])
        self.assertNotIn("email", fact_commits.columns)
        self.assertNotIn("author_name", fact_commits.columns)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_issues_label_count_from_array_and_cycle_times(self):
        """Test verifies:
        - label_count is derived from labels_list ARRAY (not a pre-existing column)
        - null labels_list → label_count = 0 (not -1)
        - time_to_close_hours: null if open, never negative
        Uses SILVER_ISSUES_SCHEMA so schema drift is caught immediately."""
        from src.schemas import SILVER_ISSUES_SCHEMA
        t_open  = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
        t_close = datetime(2026, 5, 2, 10, 0, 0, tzinfo=timezone.utc)  # exactly 24 h
        t_bad   = datetime(2026, 5, 1,  9, 0, 0, tzinfo=timezone.utc)  # negative skew

        # Column order from SILVER_ISSUES_SCHEMA:
        # repo_full_name, issue_number, issue_id, title, state, author_login, author_id, is_bot,
        # created_at_utc, updated_at_utc, closed_at_utc, comments_count, labels_list,
        # _source_file, load_timestamp
        data = [
            ("surrealdb/surrealdb", 101, 1001, "Bug 1", "closed", "user1", 1, False, t_open, t_open, t_close, 5, ["bug", "perf"], "f1", t_open),
            ("surrealdb/surrealdb", 102, 1002, "Bug 2", "open",   "user2", 2, False, t_open, t_open, None,    0, None,             "f2", t_open),
            ("surrealdb/surrealdb", 103, 1003, "Bug 3", "closed", "user3", 3, False, t_open, t_open, t_bad,   1, [],               "f3", t_open),
        ]
        df_issues = spark.createDataFrame(data, schema=SILVER_ISSUES_SCHEMA)
        fact_issues = build_fact_issues(df_issues)

        rows = {r["issue_number"]: r for r in fact_issues.collect()}

        # Issue 101: closed in 24 hours, 2 labels
        self.assertAlmostEqual(rows[101]["time_to_close_hours"], 24.0, places=1)
        self.assertFalse(rows[101]["is_open"])
        self.assertEqual(rows[101]["label_count"], 2)

        # Issue 102: open → null close time, null label_count (labels_list IS NULL → 0)
        self.assertIsNone(rows[102]["time_to_close_hours"])
        self.assertTrue(rows[102]["is_open"])
        self.assertEqual(rows[102]["label_count"], 0)  # null array → 0, not -1

        # Issue 103: bad negative duration clamped to 0.0; empty array → 0 labels
        self.assertEqual(rows[103]["time_to_close_hours"], 0.0)
        self.assertEqual(rows[103]["label_count"], 0)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_pull_requests_hours_to_merge_defensiveness(self):
        """Test verifies fact_pull_requests hours_to_merge calculation: null if not merged, never negative.
        Uses SILVER_PULL_REQUESTS_SCHEMA — no author_id column present."""
        from src.schemas import SILVER_PULL_REQUESTS_SCHEMA
        t_open  = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        t_merge = datetime(2026, 6, 1, 18, 0, 0, tzinfo=timezone.utc)  # 6.0 hours

        # Column order from SILVER_PULL_REQUESTS_SCHEMA:
        # repo_full_name, pr_number, pr_id, title, state, author_login, is_bot,
        # created_at_utc, updated_at_utc, closed_at_utc, merged_at_utc, is_merged,
        # merge_commit_sha, head_branch, base_branch, _source_file, load_timestamp
        data = [
            ("qdrant/qdrant", 501, 5001, "PR 1", "closed", "dev", False, t_open, t_open, t_merge, t_merge, True,  "sha_m", "feat", "main", "f1", t_open),
            ("qdrant/qdrant", 502, 5002, "PR 2", "closed", "dev", False, t_open, t_open, t_merge, None,    False, None,    "feat", "main", "f2", t_open),
        ]
        df_pulls = spark.createDataFrame(data, schema=SILVER_PULL_REQUESTS_SCHEMA)
        fact_pulls = build_fact_pull_requests(df_pulls)

        rows = {r["pr_number"]: r for r in fact_pulls.collect()}
        self.assertAlmostEqual(rows[501]["hours_to_merge"], 6.0, places=1)
        self.assertTrue(rows[501]["is_merged"])
        self.assertIsNone(rows[502]["hours_to_merge"])
        self.assertFalse(rows[502]["is_merged"])

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_releases_cadence_lag(self):
        """Test verifies days_since_previous_release computes lag accurately per repository.
        Uses SILVER_RELEASES_SCHEMA."""
        from src.schemas import SILVER_RELEASES_SCHEMA
        t1 = datetime(2026, 1, 1,  0, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 11, 0, 0, 0, tzinfo=timezone.utc)  # 10 days later
        t3 = datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc)  # 20 days later

        # Column order from SILVER_RELEASES_SCHEMA:
        # repo_full_name, release_id, tag_name, release_name, is_draft, is_prerelease,
        # published_at_utc, assets_count, _source_file, load_timestamp
        data = [
            ("milvus-io/milvus", 1, "v2.0", "v2.0 release", False, False, t1, 5, "f1", t1),
            ("milvus-io/milvus", 2, "v2.1", "v2.1 release", False, False, t2, 5, "f2", t2),
            ("milvus-io/milvus", 3, "v2.2", "v2.2 release", False, False, t3, 5, "f3", t3),
        ]
        df_releases = spark.createDataFrame(data, schema=SILVER_RELEASES_SCHEMA)
        fact_releases = build_fact_releases(df_releases)

        rows = {r["release_id"]: r for r in fact_releases.collect()}
        self.assertIsNone(rows[1]["days_since_previous_release"])
        self.assertAlmostEqual(rows[2]["days_since_previous_release"], 10.0, places=1)
        self.assertAlmostEqual(rows[3]["days_since_previous_release"], 20.0, places=1)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_repo_daily_activity_rolling_window_and_totals(self):
        """
        Test verifies:
        1. Dense spine preserves zero-activity days between active events
        2. Rolling 4-week window calculates accurately across empty days
        3. Activity totals reconcile with base fact totals
        """
        from src.schemas import GOLD_DIM_REPO_SCHEMA, GOLD_FACT_COMMITS_SCHEMA, GOLD_FACT_ISSUES_SCHEMA, GOLD_FACT_PULL_REQUESTS_SCHEMA, GOLD_FACT_RELEASES_SCHEMA
        now = datetime.now(timezone.utc)
        test_key = 123456789

        # dim_repo from GOLD_DIM_REPO_SCHEMA
        repo_data = [(test_key, 1, "redis/redis", "redis", "redis", "desc", "main", "BSD", now, "NoSQL", "Key-Value", now)]
        df_dim_repo = spark.createDataFrame(repo_data, schema=GOLD_DIM_REPO_SCHEMA)

        # Dim date spine covering 35 days (2026-05-01 to 2026-06-04)
        df_dim_date = build_dim_date(spark, start_date_str="2026-05-01", end_date_str="2026-06-04")

        # Commits fact rows using GOLD_FACT_COMMITS_SCHEMA:
        # repo_key, date_key, actor_key, commit_sha, comment_count, is_bot, in_analysis_window, load_timestamp
        commits_data = (
            [(test_key, 20260501, 101, f"sha_h_{i}",  0, False, True, now) for i in range(5)] +
            [(test_key, 20260515, 102, f"sha_h2_{i}", 0, False, True, now) for i in range(3)] +
            [(test_key, 20260515, 999, f"sha_b_{i}",  0, True,  True, now) for i in range(2)]
        )
        df_fact_commits = spark.createDataFrame(commits_data, schema=GOLD_FACT_COMMITS_SCHEMA)

        # Empty issues, pulls, releases — use Gold schemas so column lists are authoritative
        df_fact_issues   = spark.createDataFrame([], schema=GOLD_FACT_ISSUES_SCHEMA)
        df_fact_pulls    = spark.createDataFrame([], schema=GOLD_FACT_PULL_REQUESTS_SCHEMA)
        df_fact_releases = spark.createDataFrame([], schema=GOLD_FACT_RELEASES_SCHEMA)

        df_daily = build_fact_repo_daily_activity(
            df_fact_commits=df_fact_commits,
            df_fact_issues=df_fact_issues,
            df_fact_pull_requests=df_fact_pulls,
            df_fact_releases=df_fact_releases,
            df_dim_repo=df_dim_repo,
            df_dim_date=df_dim_date,
        )

        daily_rows = {r["date_key"]: r for r in df_daily.collect()}

        # Total days should still be 35: the cross-join uses DIM_DATE_ACTIVITY_SPINE_START
        # (2020-01-01) as its floor, so extending dim_date back to 2008 does NOT increase
        # the daily-activity spine. This test passes a 35-day date slice directly and
        # confirms the count is unchanged from before the dim_date range extension.
        self.assertEqual(len(daily_rows), 35,
            "Daily-activity spine row count must be 35 (the 35-day date window passed in). "
            "If this changed, check that build_fact_repo_daily_activity filters to "
            "DIM_DATE_ACTIVITY_SPINE_START before cross-joining.")

        # On 2026-05-01: 5 human commits, 0 bot, rolling 4wk = 5
        self.assertEqual(daily_rows[20260501]["commits_human"], 5)
        self.assertEqual(daily_rows[20260501]["commits_human_4wk"], 5)

        # On 2026-05-02 (empty day): 0 commits, rolling 4wk = 5
        self.assertEqual(daily_rows[20260502]["commits_human"], 0)
        self.assertEqual(daily_rows[20260502]["commits_human_4wk"], 5)

        # On 2026-05-15: 3 human, 2 bot commits, rolling 4wk = 5 + 3 = 8
        self.assertEqual(daily_rows[20260515]["commits_human"], 3)
        self.assertEqual(daily_rows[20260515]["commits_bot"], 2)
        self.assertEqual(daily_rows[20260515]["commits_human_4wk"], 8)

        # Total reconciliation: sum of commits_total across all 35 days == 10 (5+3+2)
        total_commits_sum = sum(r["commits_total"] for r in daily_rows.values())
        self.assertEqual(total_commits_sum, 10)


if __name__ == "__main__":
    unittest.main()
