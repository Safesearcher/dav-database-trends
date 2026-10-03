"""
Unit tests validating PySpark schema definitions and rules:
1. All schemas explicit (no inferSchema)
2. Every Silver table contains load_timestamp
3. PII masking verified (only email hashes, no plain emails)
4. Bot flag is present on commits, issues, and pull requests
"""

import unittest
from src.schemas import (
    StructType,
    StructField,
    TimestampType,
    BooleanType,
    StringType,
    BRONZE_COMMITS_SCHEMA,
    BRONZE_ISSUES_SCHEMA,
    BRONZE_PULLS_SCHEMA,
    BRONZE_RELEASES_SCHEMA,
    BRONZE_METADATA_SCHEMA,
    SILVER_COMMITS_SCHEMA,
    SILVER_ISSUES_SCHEMA,
    SILVER_PULL_REQUESTS_SCHEMA,
    SILVER_RELEASES_SCHEMA,
    SILVER_REPO_METADATA_SCHEMA,
    SILVER_QUARANTINE_SCHEMA,
    PIPELINE_EXECUTION_LOGS_SCHEMA,
)


class TestMedallionSchemas(unittest.TestCase):

    def test_bronze_schemas_exist_and_are_structtypes(self):
        schemas = [
            BRONZE_COMMITS_SCHEMA,
            BRONZE_ISSUES_SCHEMA,
            BRONZE_PULLS_SCHEMA,
            BRONZE_RELEASES_SCHEMA,
            BRONZE_METADATA_SCHEMA,
        ]
        for s in schemas:
            self.assertIsInstance(s, StructType)
            self.assertGreater(len(s.fields), 0)

    def test_all_silver_schemas_contain_load_timestamp(self):
        silver_schemas = [
            SILVER_COMMITS_SCHEMA,
            SILVER_ISSUES_SCHEMA,
            SILVER_PULL_REQUESTS_SCHEMA,
            SILVER_RELEASES_SCHEMA,
            SILVER_REPO_METADATA_SCHEMA,
            SILVER_QUARANTINE_SCHEMA,
            PIPELINE_EXECUTION_LOGS_SCHEMA,
        ]
        for s in silver_schemas:
            field_names = [f.name for f in s.fields]
            self.assertIn("load_timestamp", field_names, f"Missing load_timestamp in schema: {s}")
            self.assertIsInstance(s["load_timestamp"].dataType, TimestampType)

    def test_silver_commits_pii_masking(self):
        field_names = [f.name for f in SILVER_COMMITS_SCHEMA.fields]
        # Must contain hash columns
        self.assertIn("author_email_hash", field_names)
        self.assertIn("committer_email_hash", field_names)
        # Must NOT contain raw email columns
        self.assertNotIn("author_email", field_names)
        self.assertNotIn("committer_email", field_names)
        self.assertNotIn("email", field_names)

    def test_bot_flag_present(self):
        for schema in [SILVER_COMMITS_SCHEMA, SILVER_ISSUES_SCHEMA, SILVER_PULL_REQUESTS_SCHEMA]:
            self.assertIn("is_bot", [f.name for f in schema.fields])
            self.assertIsInstance(schema["is_bot"].dataType, BooleanType)


if __name__ == "__main__":
    unittest.main()
