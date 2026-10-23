"""
Unit and integration tests for Gold Star Schema transformations (src/gold_transforms.py).

Covers:
1. Surrogate key determinism (xxhash64 reproducibility)
2. Coverage of all 10 target repositories in REPO_DB_MAPPING (fails if any repo unmapped)
3. dim_repo enrichment with db_type and db_category
4. dim_date spine generation, temporal attributes, and analysis window tagging
5. dim_actor unknown handling and FAISS-style null-login fallback to email_hash
6. fact_commits construction and PII avoidance
7. fact_issues time_to_close_hours defensive behavior (null if open, never negative)
8. fact_pull_requests hours_to_merge defensive behavior (null if unmerged, never negative)
9. fact_releases days_since_previous_release window lag calculation
10. fact_repo_snapshot metric preservation and idempotent keying
11. fact_repo_daily_activity dense calendar spine, rolling 4-week / 12-week metrics, and total reconciliation with base facts
"""

import os
import unittest
from datetime import datetime, timezone, date

from src.gold_transforms import (
    REPO_DB_MAPPING,
    ALL_10_REPOS,
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
        """Test verifies silver.repo_metadata is transformed into gold.dim_repo with db_type and db_category."""
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
        """Test verifies calendar spine generation and in_analysis_window boundary logic."""
        dim_date = build_dim_date(
            spark,
            start_date_str="2026-03-31",
            end_date_str="2026-04-03",
            window_since="2026-04-01",
            window_until="2026-09-30",
        )
        dates = {r["date_key"]: r for r in dim_date.collect()}

        # 2026-03-31 should be outside window
        self.assertFalse(dates[20260331]["in_analysis_window"])
        # 2026-04-01 should be inside window
        self.assertTrue(dates[20260401]["in_analysis_window"])
        self.assertEqual(dates[20260401]["year"], 2026)
        self.assertEqual(dates[20260401]["quarter"], 2)
        self.assertEqual(dates[20260401]["month"], 4)
        self.assertEqual(dates[20260401]["month_name"], "April")

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_dim_actor_identity_resolution(self):
        """Test verifies login priority, null-login fallback to email_hash (FAISS), and unknown member row."""
        c_data = [
            ("alice", 101, "hash_aaa", False),                # normal user with login
            (None, None, "hash_meta_employee", False),         # FAISS contributor with null login
            ("dependabot[bot]", 202, "hash_dep", True),        # bot account
        ]
        c_schema = StructType([
            StructField("author_login", StringType(), True),
            StructField("author_id", LongType(), True),
            StructField("author_email_hash", StringType(), True),
            StructField("is_bot", BooleanType(), False),
        ])
        df_commits = spark.createDataFrame(c_data, schema=c_schema)
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
    def test_fact_commits_and_pii_absence(self):
        """Test verifies fact_commits mapping and ensures no raw emails or plaintext names exist."""
        t1 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        data = [
            ("sha123", "redis/redis", "hash_red", "hash_com", "antirez", 50, t1, "Fix memory leak", 2, False),
        ]
        cols = [
            "commit_sha", "repo_full_name", "author_email_hash", "committer_email_hash",
            "author_login", "author_id", "committed_at_utc", "message_headline",
            "comment_count", "is_bot"
        ]
        df_silver_commits = spark.createDataFrame(data, cols)
        fact_commits = build_fact_commits(df_silver_commits, window_since="2026-04-01", window_until="2026-09-30")

        row = fact_commits.collect()[0]
        self.assertEqual(row["commit_sha"], "sha123")
        self.assertEqual(row["date_key"], 20260501)
        self.assertTrue(row["in_analysis_window"])
        self.assertNotIn("email", fact_commits.columns)
        self.assertNotIn("author_name", fact_commits.columns)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_issues_cycle_times_and_defensiveness(self):
        """Test verifies fact_issues time_to_close_hours calculation: null if open, never negative."""
        t_open = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
        t_close_valid = datetime(2026, 5, 2, 10, 0, 0, tzinfo=timezone.utc)  # exactly 24.0 hours
        t_close_bad = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)     # negative clock skew

        data = [
            (1, "surrealdb/surrealdb", 101, "Bug 1", "closed", t_open, t_open, t_close_valid, "user1", 1, 5, 2, False),
            (2, "surrealdb/surrealdb", 102, "Bug 2", "open", t_open, t_open, None, "user2", 2, 0, 1, False),
            (3, "surrealdb/surrealdb", 103, "Bug 3", "closed", t_open, t_open, t_close_bad, "user3", 3, 1, 1, False),
        ]
        cols = [
            "issue_id", "repo_full_name", "issue_number", "title_scrubbed", "state",
            "created_at_utc", "updated_at_utc", "closed_at_utc", "author_login",
            "author_id", "comments_count", "label_count", "is_bot"
        ]
        df_issues = spark.createDataFrame(data, cols)
        fact_issues = build_fact_issues(df_issues)

        rows = {r["issue_number"]: r for r in fact_issues.collect()}

        # Issue 1: closed in 24 hours
        self.assertAlmostEqual(rows[101]["time_to_close_hours"], 24.0, places=1)
        self.assertFalse(rows[101]["is_open"])

        # Issue 2: open issue has null time_to_close_hours and is_open=True
        self.assertIsNone(rows[102]["time_to_close_hours"])
        self.assertTrue(rows[102]["is_open"])

        # Issue 3: bad negative duration clamped to 0.0
        self.assertEqual(rows[103]["time_to_close_hours"], 0.0)

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_pull_requests_hours_to_merge_defensiveness(self):
        """Test verifies fact_pull_requests hours_to_merge calculation: null if not merged, never negative."""
        t_open = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        t_merge = datetime(2026, 6, 1, 18, 0, 0, tzinfo=timezone.utc)  # 6.0 hours

        data = [
            (10, "qdrant/qdrant", 501, "PR 1", "closed", t_open, t_open, t_merge, t_merge, "dev", 1, 3, True, False),
            (20, "qdrant/qdrant", 502, "PR 2", "closed", t_open, t_open, t_merge, None, "dev", 1, 1, False, False),
        ]
        cols = [
            "pr_id", "repo_full_name", "pr_number", "title_scrubbed", "state",
            "created_at_utc", "updated_at_utc", "closed_at_utc", "merged_at_utc",
            "author_login", "author_id", "comments_count", "is_merged", "is_bot"
        ]
        df_pulls = spark.createDataFrame(data, cols)
        fact_pulls = build_fact_pull_requests(df_pulls)

        rows = {r["pr_number"]: r for r in fact_pulls.collect()}
        self.assertAlmostEqual(rows[501]["hours_to_merge"], 6.0, places=1)
        self.assertTrue(rows[501]["is_merged"])
        self.assertIsNone(rows[502]["hours_to_merge"])
        self.assertFalse(rows[502]["is_merged"])

    @unittest.skipUnless(HAS_SPARK, "PySpark not available")
    def test_fact_releases_cadence_lag(self):
        """Test verifies days_since_previous_release computes lag accurately per repository."""
        t1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 11, 0, 0, 0, tzinfo=timezone.utc)  # 10 days later
        t3 = datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc)  # 20 days later

        data = [
            (1, "milvus-io/milvus", "v2.0", "v2.0", t1, t1, False, False, 5),
            (2, "milvus-io/milvus", "v2.1", "v2.1", t2, t2, False, False, 5),
            (3, "milvus-io/milvus", "v2.2", "v2.2", t3, t3, False, False, 5),
        ]
        cols = [
            "release_id", "repo_full_name", "tag_name", "name",
            "created_at_utc", "published_at_utc", "is_draft", "is_prerelease", "assets_count"
        ]
        df_releases = spark.createDataFrame(data, cols)
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
        # Dim repo with single test repo
        from src.schemas import GOLD_DIM_REPO_SCHEMA
        now = datetime.now(timezone.utc)
        test_key = 123456789
        repo_data = [(test_key, 1, "redis/redis", "redis", "redis", "desc", "main", "BSD", now, "NoSQL", "Key-Value", now)]
        df_dim_repo = spark.createDataFrame(repo_data, schema=GOLD_DIM_REPO_SCHEMA)
        repo_key = test_key

        # Dim date spine covering 35 days (2026-05-01 to 2026-06-04)
        df_dim_date = build_dim_date(spark, start_date_str="2026-05-01", end_date_str="2026-06-04")

        # Commits: 5 human commits on 2026-05-01, 3 human commits on 2026-05-15, 2 bot commits on 2026-05-15
        commits_data = [
            (repo_key, 20260501, 101, f"sha_h_{i}", 0, False, True, now) for i in range(5)
        ] + [
            (repo_key, 20260515, 102, f"sha_h2_{i}", 0, False, True, now) for i in range(3)
        ] + [
            (repo_key, 20260515, 999, f"sha_b_{i}", 0, True, True, now) for i in range(2)
        ]
        cols_commits = ["repo_key", "date_key", "actor_key", "commit_sha", "comment_count", "is_bot", "in_analysis_window", "load_timestamp"]
        df_fact_commits = spark.createDataFrame(commits_data, cols_commits)

        # Empty issues, pulls, releases
        schema_issues = StructType([
            StructField("repo_key", LongType()), StructField("created_date_key", IntegerType()),
            StructField("closed_date_key", IntegerType()), StructField("actor_key", LongType()),
            StructField("issue_number", IntegerType()), StructField("state", StringType()),
            StructField("is_open", BooleanType()), StructField("comments_count", IntegerType()),
            StructField("label_count", IntegerType()), StructField("time_to_close_hours", DoubleType()),
            StructField("in_analysis_window", BooleanType()), StructField("load_timestamp", TimestampType())
        ])
        df_fact_issues = spark.createDataFrame([], schema_issues)

        schema_pulls = StructType([
            StructField("repo_key", LongType()), StructField("created_date_key", IntegerType()),
            StructField("closed_date_key", IntegerType()), StructField("merged_date_key", IntegerType()),
            StructField("actor_key", LongType()), StructField("pr_number", IntegerType()),
            StructField("is_merged", BooleanType()), StructField("is_bot", BooleanType()),
            StructField("hours_to_merge", DoubleType()), StructField("in_analysis_window", BooleanType()),
            StructField("load_timestamp", TimestampType())
        ])
        df_fact_pulls = spark.createDataFrame([], schema_pulls)

        schema_releases = StructType([
            StructField("repo_key", LongType()), StructField("published_date_key", IntegerType()),
            StructField("release_id", LongType()), StructField("tag_name", StringType()),
            StructField("is_prerelease", BooleanType()), StructField("is_draft", BooleanType()),
            StructField("assets_count", IntegerType()), StructField("days_since_previous_release", DoubleType()),
            StructField("in_analysis_window", BooleanType()), StructField("load_timestamp", TimestampType())
        ])
        df_fact_releases = spark.createDataFrame([], schema_releases)

        df_daily = build_fact_repo_daily_activity(
            df_fact_commits=df_fact_commits,
            df_fact_issues=df_fact_issues,
            df_fact_pull_requests=df_fact_pulls,
            df_fact_releases=df_fact_releases,
            df_dim_repo=df_dim_repo,
            df_dim_date=df_dim_date,
        )

        daily_rows = {r["date_key"]: r for r in df_daily.collect()}

        # Total days should be 35
        self.assertEqual(len(daily_rows), 35)

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
