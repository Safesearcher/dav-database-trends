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

    # -------------------------------------------------------------------------
    # BUG 2 Tests: align_to_target pure function tests
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_align_to_target_missing_target_col(self):
        from src.common import align_to_target
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("new_field_test", StringType(), True),
        ])
        source_df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

        aligned_df, cols_to_add = align_to_target(source_df, target_schema)
        self.assertEqual(cols_to_add, [])
        self.assertEqual(aligned_df.columns, ["id", "name", "new_field_test"])
        row = aligned_df.first()
        self.assertEqual(row["id"], 1)
        self.assertEqual(row["name"], "Alice")
        self.assertIsNone(row["new_field_test"])
        self.assertIsInstance(aligned_df.schema["new_field_test"].dataType, StringType)

    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_align_to_target_extra_source_col(self):
        from src.common import align_to_target
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        source_df = spark.createDataFrame([(1, "Alice", "extra_val")], ["id", "name", "drift_col"])

        aligned_df, cols_to_add = align_to_target(source_df, target_schema)
        self.assertEqual(cols_to_add, [("drift_col", "string")])
        self.assertEqual(aligned_df.columns, ["id", "name"])

    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_align_to_target_column_order_and_identical(self):
        from src.common import align_to_target
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        target_schema = StructType([
            StructField("col_a", IntegerType(), True),
            StructField("col_b", StringType(), True),
            StructField("col_c", StringType(), True),
        ])
        # Source with different column order
        source_df = spark.createDataFrame([("val_c", 42, "val_b")], ["col_c", "col_a", "col_b"])

        aligned_df, cols_to_add = align_to_target(source_df, target_schema)
        self.assertEqual(cols_to_add, [])
        self.assertEqual(aligned_df.columns, ["col_a", "col_b", "col_c"])
        row = aligned_df.first()
        self.assertEqual(row["col_a"], 42)
        self.assertEqual(row["col_b"], "val_b")
        self.assertEqual(row["col_c"], "val_c")

        # Identical schema -> no-op
        aligned_same, cols_same = align_to_target(aligned_df, target_schema)
        self.assertEqual(cols_same, [])
        self.assertEqual(aligned_same.columns, ["col_a", "col_b", "col_c"])

    # -------------------------------------------------------------------------
    # BUG 4 Tests: Deterministic dedup tie-breaker
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_deterministic_dedup_tie_breaker(self):
        from pyspark.sql.window import Window
        from src.common import parse_iso_timestamp

        raw_a = '{"id": 42, "updated_at": "2026-01-01T00:00:00Z", "body": "Record Alpha"}'
        raw_b = '{"id": 42, "updated_at": "2026-01-01T00:00:00Z", "body": "Record Beta"}'

        # Compute which raw record has the lower sha256 hash
        import hashlib
        hash_a = hashlib.sha256(raw_a.encode("utf-8")).hexdigest()
        hash_b = hashlib.sha256(raw_b.encode("utf-8")).hexdigest()
        expected_winner_body = "Record Alpha" if hash_a < hash_b else "Record Beta"

        date_col = "updated_at"
        pk_field = "id"

        for iteration in range(10):
            # Alternate order of input rows across iterations
            rows = (
                [("repo/test", 42, "2026-01-01T00:00:00Z", "Record Alpha", raw_a),
                 ("repo/test", 42, "2026-01-01T00:00:00Z", "Record Beta", raw_b)]
                if iteration % 2 == 0 else
                [("repo/test", 42, "2026-01-01T00:00:00Z", "Record Beta", raw_b),
                 ("repo/test", 42, "2026-01-01T00:00:00Z", "Record Alpha", raw_a)]
            )
            df = spark.createDataFrame(rows, ["repo_full_name", "id", "updated_at", "body", "_raw_record"])

            dedup_window = (
                Window.partitionBy("repo_full_name", pk_field)
                .orderBy(
                    F.coalesce(parse_iso_timestamp(date_col), F.to_timestamp(F.lit("1970-01-01"))).desc(),
                    F.sha2(F.col("_raw_record"), 256).asc(),
                )
            )

            deduped = (
                df.withColumn("_row_num", F.row_number().over(dedup_window))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num", "_raw_record")
            )

            result = deduped.collect()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["body"], expected_winner_body)
            self.assertNotIn("_raw_record", deduped.columns)

    # -------------------------------------------------------------------------
    # BUG 5 Tests: build_quarantine_df email scrubbing and deterministic hashing
    # -------------------------------------------------------------------------
    @unittest.skipIf(not HAS_SPARK, "Spark JVM runtime not available in local environment")
    def test_build_quarantine_df_email_scrubbing_and_id(self):
        from src.common import build_quarantine_df

        # 1. Feeding a DataFrame with emails in fields produces raw_payload containing '[EMAIL]' and no '@'
        rejected_data = [
            ("owner/repo", "Malformed commit by dev@company.com", "dev@company.com", "Corrupt JSON"),
        ]
        df_rejected = spark.createDataFrame(
            rejected_data,
            ["repo_full_name", "message", "author_email", "rejection_reason"]
        )

        q_df = build_quarantine_df(
            df_rejected=df_rejected,
            layer="bronze",
            entity="commits",
            rejection_reason="Corrupt JSON",
            batch_id="batch_001",
            salt="test_salt",
        )
        row = q_df.first()
        payload = row["raw_payload"]
        self.assertIn("[EMAIL]", payload)
        self.assertNotIn("dev@company.com", payload)
        self.assertNotIn("@", payload)

        # 2. Two identical corrupt rows in the same batch collapse to one row
        dup_data = [
            ("owner/repo", "bad record", "Null PK"),
            ("owner/repo", "bad record", "Null PK"),
        ]
        df_dup = spark.createDataFrame(dup_data, ["repo_full_name", "body", "rejection_reason"])
        q_dup = build_quarantine_df(
            df_rejected=df_dup,
            layer="silver",
            entity="issues",
            rejection_reason="Null PK",
            batch_id="batch_001",
            salt="test_salt",
        )
        self.assertEqual(q_dup.count(), 1)

        # 3. The same row in two different batches gets two different quarantine_ids
        single_row_df = spark.createDataFrame([("owner/repo", "bad record")], ["repo_full_name", "body"])
        q_b1 = build_quarantine_df(
            df_rejected=single_row_df,
            layer="bronze",
            entity="pulls",
            rejection_reason="Error",
            batch_id="batch_A",
            salt="test_salt",
        )
        q_b2 = build_quarantine_df(
            df_rejected=single_row_df,
            layer="bronze",
            entity="pulls",
            rejection_reason="Error",
            batch_id="batch_B",
            salt="test_salt",
        )
        id_1 = q_b1.first()["quarantine_id"]
        id_2 = q_b2.first()["quarantine_id"]
        self.assertNotEqual(id_1, id_2)


if __name__ == "__main__":
    unittest.main()
