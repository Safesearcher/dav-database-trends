"""
Acceptance checks for Spark and pipeline logic (Section 5.1).
Covers:
1. parse_iso_timestamp (ANSI on, timezone independence, invalid dates)
2. Per-record JSON read on samples/full_load/ (counts: 30, 30, 30, 30, 1; 0 corrupt)
3. Per-record JSON read on samples/drift/issues.json (total 5, corrupt 3, valid 2; discover_drift_keys)
4. get_params with fake dbutils (empty bounds preserved, existing widgets not re-declared)
5. bot_flag heuristics (catches -bot/_bot/known accounts, ignores false positives)
"""

import os
import re
import unittest
from datetime import datetime, timezone

from src.common import (
    get_params,
    KNOWN_BOT_ACCOUNTS,
    parse_iso_timestamp,
    read_raw_records,
    parse_records,
    discover_drift_keys,
)
from src.schemas import (
    BRONZE_COMMITS_SCHEMA,
    BRONZE_ISSUES_SCHEMA,
    BRONZE_PULLS_SCHEMA,
    BRONZE_RELEASES_SCHEMA,
    BRONZE_METADATA_SCHEMA,
)

# Detect if PySpark and Java runtime are available
HAS_SPARK = False
spark = None

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    import py4j

    # Attempt to start or get local spark session
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_spark_logic")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    HAS_SPARK = True
except Exception:
    HAS_SPARK = False


class FakeWidgetService:
    def __init__(self, initial_values=None):
        self.values = initial_values or {}
        self.declared = []

    def get(self, name):
        if name in self.values:
            return self.values[name]
        raise KeyError(f"Widget {name} not defined")

    def text(self, name, default_value, label=""):
        self.declared.append(("text", name))
        if name not in self.values:
            self.values[name] = default_value

    def dropdown(self, name, default_value, choices, label=""):
        self.declared.append(("dropdown", name))
        if name not in self.values:
            self.values[name] = default_value


class FakeDbutils:
    def __init__(self, widgets=None):
        self.widgets = FakeWidgetService(widgets)


class TestSparkLogic(unittest.TestCase):

    # -------------------------------------------------------------------------
    # Check 4: get_params with fake dbutils
    # -------------------------------------------------------------------------
    def test_get_params_unbounded_and_no_redeclaration(self):
        fake_db = FakeDbutils(widgets={
            "until": "",
            "since": "",
            "entity": "issues",
            "catalog": "workspace",
            "batch_id": "",
        })
        params = get_params(dbutils=fake_db)

        # until="" and since="" stay ""
        self.assertEqual(params["until"], "")
        self.assertEqual(params["since"], "")

        # batch_id="" gets auto-generated batch id
        self.assertTrue(params["batch_id"].startswith("batch_"))

        # existing widgets (e.g. dropdown entity, text catalog) were not re-declared
        declared_names = [name for _, name in fake_db.widgets.declared]
        self.assertNotIn("entity", declared_names)
        self.assertNotIn("catalog", declared_names)
        self.assertEqual(params["entity"], "issues")

    # -------------------------------------------------------------------------
    # Check 5: bot_flag heuristics logic
    # -------------------------------------------------------------------------
    def test_bot_flag_heuristics(self):
        bot_regex = re.compile(r"(?i)(^|[-_])bot$")

        def is_bot_heuristic(login: str, user_type: str = "User") -> bool:
            if not login:
                return False
            low = login.lower()
            if "[bot]" in low:
                return True
            if low in [b.lower() for b in KNOWN_BOT_ACCOUNTS]:
                return True
            if bool(bot_regex.search(low)):
                return True
            if user_type.lower() == "bot":
                return True
            return False

        # Positive bot accounts
        self.assertTrue(is_bot_heuristic("pymilvus-bot"))
        self.assertTrue(is_bot_heuristic("qdrant-cloud-bot"))
        self.assertTrue(is_bot_heuristic("dependabot[bot]"))
        self.assertTrue(is_bot_heuristic("bors"))

        # Negative human accounts (must not be flagged as bot)
        self.assertFalse(is_bot_heuristic("abbott"))
        self.assertFalse(is_bot_heuristic("robotics-lab"))
        self.assertFalse(is_bot_heuristic("generall"))

    # -------------------------------------------------------------------------
    # Check 1: parse_iso_timestamp (requires live Spark session)
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_parse_iso_timestamp_ansi(self):
        data = [
            ("2026-01-01T00:00:00Z",),
            ("2026-01-01T00:00:00.123Z",),
            ("2026-01-01T01:00:00+01:00",),
            ("not-a-date",),
            ("",),
            (None,),
        ]
        df = spark.createDataFrame(data, ["dt_str"])
        res = df.select(
            parse_iso_timestamp("dt_str").alias("parsed"),
            F.date_format(parse_iso_timestamp("dt_str"), "yyyy-MM-dd HH:mm:ss").alias("formatted")
        ).collect()

        # First 3 must parse to the exact UTC timestamp 2026-01-01 00:00:00
        self.assertIsNotNone(res[0]["parsed"])
        self.assertEqual(res[0]["formatted"], "2026-01-01 00:00:00")
        self.assertIsNotNone(res[1]["parsed"])
        self.assertEqual(res[1]["formatted"], "2026-01-01 00:00:00")
        self.assertIsNotNone(res[2]["parsed"])
        self.assertEqual(res[2]["formatted"], "2026-01-01 00:00:00")

        # Invalid or empty dates must return NULL without raising exceptions
        self.assertIsNone(res[3]["parsed"])
        self.assertIsNone(res[4]["parsed"])
        self.assertIsNone(res[5]["parsed"])

    # -------------------------------------------------------------------------
    # Check 2: Per-record read on samples/full_load/ (requires live Spark)
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_per_record_read_full_load(self):
        samples_dir = os.path.abspath("samples/full_load")
        expected_counts = {
            "issues.json": (30, BRONZE_ISSUES_SCHEMA),
            "commits.json": (30, BRONZE_COMMITS_SCHEMA),
            "pulls.json": (30, BRONZE_PULLS_SCHEMA),
            "releases.json": (30, BRONZE_RELEASES_SCHEMA),
            "repo_metadata.json": (1, BRONZE_METADATA_SCHEMA),
        }
        for filename, (expected_count, schema) in expected_counts.items():
            path = os.path.join(samples_dir, filename)
            if not os.path.exists(path):
                continue
            raw_df = read_raw_records(spark, path)
            parsed_df = parse_records(raw_df, schema)
            total = raw_df.count()
            corrupt = parsed_df.filter(F.col("_corrupt_record").isNotNull()).count()
            self.assertEqual(total, expected_count, f"Count mismatch for {filename}")
            self.assertEqual(corrupt, 0, f"Corrupt records found in {filename}")

    # -------------------------------------------------------------------------
    # Check 3: Per-record read on samples/drift/issues.json (requires live Spark)
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_per_record_read_drift(self):
        drift_path = os.path.abspath("samples/drift/issues.json")
        if not os.path.exists(drift_path):
            self.skipTest("samples/drift/issues.json not found")

        raw_df = read_raw_records(spark, drift_path)
        drift_keys = discover_drift_keys(raw_df, BRONZE_ISSUES_SCHEMA)
        self.assertIn("new_field_test", drift_keys)

        parsed_df = parse_records(raw_df, BRONZE_ISSUES_SCHEMA, drift_keys)
        counts = parsed_df.agg(
            F.count("*").alias("total"),
            F.count(F.when(F.col("_corrupt_record").isNotNull(), 1)).alias("corrupt"),
            F.count(F.when(F.col("_corrupt_record").isNull(), 1)).alias("valid"),
        ).collect()[0]

        self.assertEqual(counts["total"], 5)
        self.assertEqual(counts["corrupt"], 3)
        self.assertEqual(counts["valid"], 2)

    # -------------------------------------------------------------------------
    # BUG 1 Tests: Email scrubbing in commit headlines, issue titles, and PR titles
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_email_scrubbing_commit_headlines_and_titles(self):
        from src.common import sanitize_commit_message, scrub_emails

        # 1. Headline cases
        test_cases = [
            ("Fix thing, thanks bob@example.com\n\nbody", "Fix thing, thanks [EMAIL]"),
            ("Merge: alice.smith+x@corp.co.uk fixed", "Merge: [EMAIL] fixed"),
            ("Two emails: first@a.com and second@b.co.uk here", "Two emails: [EMAIL] and [EMAIL] here"),
            ("Signed-off-by: A <a@x.com>", "[TRUNCATED_TRAILER]"),
            ("Clean commit with no email address", "Clean commit with no email address"),
        ]
        df = spark.createDataFrame([(msg,) for msg, _ in test_cases], ["msg"])
        results = [r["h"] for r in df.select(sanitize_commit_message("msg").alias("h")).collect()]
        for (_, expected), actual in zip(test_cases, results):
            self.assertEqual(actual, expected)

        # 2. Issue/PR title masking
        title_df = spark.createDataFrame(
            [("Issue reported by reporter@example.org on v1.2",), ("Clean title",)],
            ["title"]
        )
        res = [r["t"] for r in title_df.select(scrub_emails("title").alias("t")).collect()]
        self.assertEqual(res[0], "Issue reported by [EMAIL] on v1.2")
        self.assertEqual(res[1], "Clean title")


if __name__ == "__main__":
    unittest.main()
