"""
Explicit PySpark StructType schemas for the 5 Medallion Bronze entities:
- repo_metadata
- commits
- issues
- pulls
- releases

Also includes schemas for Silver, Quarantine, and Ops Logging layers.
All Bronze schemas include `_corrupt_record` (StringType) for Spark PERMISSIVE mode.
Zero schema inference allowed — strict type safety across Databricks Serverless compute.
"""

try:
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        LongType,
        DoubleType,
        BooleanType,
        TimestampType,
        ArrayType,
    )
except ImportError:
    # Lightweight stub classes for local testing/linting without pyspark installed
    class DataType:
        def __init__(self, *args, **kwargs):
            pass
        def __repr__(self):
            return self.__class__.__name__

    class StringType(DataType):
        pass

    class IntegerType(DataType):
        pass

    class LongType(DataType):
        pass

    class DoubleType(DataType):
        pass

    class BooleanType(DataType):
        pass

    class TimestampType(DataType):
        pass

    class ArrayType(DataType):
        def __init__(self, element_type, containsNull=True):
            self.elementType = element_type
            self.containsNull = containsNull

    class StructField:
        def __init__(self, name, dataType, nullable=True, metadata=None):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable
            self.metadata = metadata or {}

        def __repr__(self):
            return f"StructField('{self.name}', {self.dataType}, {self.nullable})"

    class StructType(DataType):
        def __init__(self, fields=None):
            self.fields = list(fields) if fields else []

        def __getitem__(self, item):
            if isinstance(item, int):
                return self.fields[item]
            for f in self.fields:
                if f.name == item:
                    return f
            raise KeyError(item)

        def __iter__(self):
            return iter(self.fields)

        def __len__(self):
            return len(self.fields)


# =============================================================================
# Primary Keys Definition
# =============================================================================
PRIMARY_KEYS = {
    "commits": "sha",
    "issues": "id",
    "pulls": "id",
    "releases": "id",
    "repo_metadata": "id",
}


# =============================================================================
# Reusable Nested Schemas
# =============================================================================

USER_STRUCT = StructType([
    StructField("login", StringType(), True),
    StructField("id", LongType(), True),
    StructField("node_id", StringType(), True),
    StructField("avatar_url", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("type", StringType(), True),
    StructField("site_admin", BooleanType(), True),
])

LABEL_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("node_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("color", StringType(), True),
    StructField("default", BooleanType(), True),
    StructField("description", StringType(), True),
])

BRANCH_REF_STRUCT = StructType([
    StructField("label", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("sha", StringType(), True),
    StructField("user", USER_STRUCT, True),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
    ]), True),
])

ASSET_STRUCT = StructType([
    StructField("url", StringType(), True),
    StructField("id", LongType(), True),
    StructField("node_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("label", StringType(), True),
    StructField("uploader", USER_STRUCT, True),
    StructField("content_type", StringType(), True),
    StructField("state", StringType(), True),
    StructField("size", LongType(), True),
    StructField("download_count", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("browser_download_url", StringType(), True),
])


# =============================================================================
# Bronze Raw Schemas (Reflecting GitHub REST API Responses)
# =============================================================================

BRONZE_COMMITS_SCHEMA = StructType([
    StructField("sha", StringType(), False),
    StructField("node_id", StringType(), True),
    StructField("commit", StructType([
        StructField("author", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True),
        ]), True),
        StructField("committer", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True),
        ]), True),
        StructField("message", StringType(), True),
        StructField("tree", StructType([
            StructField("sha", StringType(), True),
            StructField("url", StringType(), True),
        ]), True),
        StructField("url", StringType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("verification", StructType([
            StructField("verified", BooleanType(), True),
            StructField("reason", StringType(), True),
            StructField("signature", StringType(), True),
            StructField("payload", StringType(), True),
        ]), True),
    ]), True),
    StructField("url", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("comments_url", StringType(), True),
    StructField("author", USER_STRUCT, True),
    StructField("committer", USER_STRUCT, True),
    StructField("parents", ArrayType(StructType([
        StructField("sha", StringType(), True),
        StructField("url", StringType(), True),
        StructField("html_url", StringType(), True),
    ])), True),
    StructField("_corrupt_record", StringType(), True),
])
"""Schema for bronze commits (PK: sha). Captures git commit tree, author/committer emails for PII hashing, and verification."""

BRONZE_ISSUES_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("node_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("repository_url", StringType(), True),
    StructField("labels_url", StringType(), True),
    StructField("comments_url", StringType(), True),
    StructField("events_url", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("number", IntegerType(), False),
    StructField("state", StringType(), True),
    StructField("state_reason", StringType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("user", USER_STRUCT, True),
    StructField("labels", ArrayType(LABEL_STRUCT), True),
    StructField("locked", BooleanType(), True),
    StructField("assignee", USER_STRUCT, True),
    StructField("assignees", ArrayType(USER_STRUCT), True),
    StructField("milestone", StructType([
        StructField("id", LongType(), True),
        StructField("number", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("state", StringType(), True),
    ]), True),
    StructField("comments", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("author_association", StringType(), True),
    StructField("active_lock_reason", StringType(), True),
    StructField("draft", BooleanType(), True),
    StructField("pull_request", StructType([
        StructField("url", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("diff_url", StringType(), True),
        StructField("patch_url", StringType(), True),
        StructField("merged_at", StringType(), True),
    ]), True),
    StructField("timeline_url", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])
"""Schema for bronze issues (PK: id). Includes pull_request struct used in Silver to filter out embedded PRs."""

BRONZE_PULLS_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("node_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("diff_url", StringType(), True),
    StructField("patch_url", StringType(), True),
    StructField("issue_url", StringType(), True),
    StructField("number", IntegerType(), False),
    StructField("state", StringType(), True),
    StructField("locked", BooleanType(), True),
    StructField("title", StringType(), True),
    StructField("user", USER_STRUCT, True),
    StructField("body", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("merged_at", StringType(), True),
    StructField("merge_commit_sha", StringType(), True),
    StructField("assignee", USER_STRUCT, True),
    StructField("assignees", ArrayType(USER_STRUCT), True),
    StructField("requested_reviewers", ArrayType(USER_STRUCT), True),
    StructField("requested_teams", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
    ])), True),
    StructField("labels", ArrayType(LABEL_STRUCT), True),
    StructField("milestone", StructType([
        StructField("id", LongType(), True),
        StructField("number", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("state", StringType(), True),
    ]), True),
    StructField("draft", BooleanType(), True),
    StructField("commits_url", StringType(), True),
    StructField("review_comments_url", StringType(), True),
    StructField("review_comment_url", StringType(), True),
    StructField("comments_url", StringType(), True),
    StructField("statuses_url", StringType(), True),
    StructField("head", BRANCH_REF_STRUCT, True),
    StructField("base", BRANCH_REF_STRUCT, True),
    StructField("author_association", StringType(), True),
    StructField("auto_merge", StringType(), True),
    StructField("active_lock_reason", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])
"""Schema for bronze pull requests (PK: id). Captures head/base branches, lifecycle timestamps, and merge commit SHAs."""

BRONZE_RELEASES_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("node_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("assets_url", StringType(), True),
    StructField("upload_url", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("tag_name", StringType(), True),
    StructField("target_commitish", StringType(), True),
    StructField("name", StringType(), True),
    StructField("draft", BooleanType(), True),
    StructField("immutable", BooleanType(), True),
    StructField("prerelease", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("author", USER_STRUCT, True),
    StructField("assets", ArrayType(ASSET_STRUCT), True),
    StructField("tarball_url", StringType(), True),
    StructField("zipball_url", StringType(), True),
    StructField("body", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])
"""Schema for bronze releases (PK: id). Captures tag names, pre-release indicators, and download asset metadata."""

BRONZE_METADATA_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("node_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("full_name", StringType(), False),
    StructField("private", BooleanType(), True),
    StructField("owner", USER_STRUCT, True),
    StructField("html_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("fork", BooleanType(), True),
    StructField("url", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("pushed_at", StringType(), True),
    StructField("size", LongType(), True),
    StructField("stargazers_count", IntegerType(), True),
    StructField("watchers_count", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("forks_count", IntegerType(), True),
    StructField("open_issues_count", IntegerType(), True),
    StructField("license", StructType([
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        StructField("spdx_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("node_id", StringType(), True),
    ]), True),
    StructField("default_branch", StringType(), True),
    StructField("topics", ArrayType(StringType()), True),
    StructField("subscribers_count", IntegerType(), True),
    StructField("network_count", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True),
])
"""Schema for bronze repository metadata (PK: id). Captures community stars, forks, default branch, and licensing."""

BRONZE_SCHEMAS = {
    "repo_metadata": BRONZE_METADATA_SCHEMA,
    "commits": BRONZE_COMMITS_SCHEMA,
    "issues": BRONZE_ISSUES_SCHEMA,
    "pulls": BRONZE_PULLS_SCHEMA,
    "releases": BRONZE_RELEASES_SCHEMA,
}


# =============================================================================
# Silver Cleaned / Typed Schemas
# =============================================================================

SILVER_COMMITS_SCHEMA = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("commit_sha", StringType(), False),
    StructField("author_login", StringType(), True),
    StructField("author_id", LongType(), True),
    StructField("author_email_hash", StringType(), True),
    StructField("committer_email_hash", StringType(), True),
    StructField("commit_date_utc", TimestampType(), True),
    StructField("commit_headline", StringType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("is_bot", BooleanType(), False),
    StructField("_source_file", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Silver commits schema: deduplicated on (repo_full_name, commit_sha) with salted SHA-256 PII masking and is_bot tagging."""

SILVER_ISSUES_SCHEMA = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("issue_number", IntegerType(), False),
    StructField("issue_id", LongType(), False),
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("author_login", StringType(), True),
    StructField("author_id", LongType(), True),
    StructField("is_bot", BooleanType(), False),
    StructField("created_at_utc", TimestampType(), True),
    StructField("updated_at_utc", TimestampType(), True),
    StructField("closed_at_utc", TimestampType(), True),
    StructField("comments_count", IntegerType(), True),
    StructField("labels_list", ArrayType(StringType()), True),
    StructField("_source_file", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Silver issues schema: true issues only (pull_request IS NULL), keyed on (repo_full_name, issue_number)."""

SILVER_PULL_REQUESTS_SCHEMA = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("pr_number", IntegerType(), False),
    StructField("pr_id", LongType(), False),
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("author_login", StringType(), True),
    StructField("is_bot", BooleanType(), False),
    StructField("created_at_utc", TimestampType(), True),
    StructField("updated_at_utc", TimestampType(), True),
    StructField("closed_at_utc", TimestampType(), True),
    StructField("merged_at_utc", TimestampType(), True),
    StructField("is_merged", BooleanType(), False),
    StructField("merge_commit_sha", StringType(), True),
    StructField("head_branch", StringType(), True),
    StructField("base_branch", StringType(), True),
    StructField("_source_file", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Silver pull requests schema: keyed on (repo_full_name, pr_number) with merge timestamps, branch refs, and is_bot."""

SILVER_RELEASES_SCHEMA = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("release_id", LongType(), False),
    StructField("tag_name", StringType(), True),
    StructField("release_name", StringType(), True),
    StructField("is_draft", BooleanType(), False),
    StructField("is_prerelease", BooleanType(), False),
    StructField("published_at_utc", TimestampType(), True),
    StructField("assets_count", IntegerType(), True),
    StructField("_source_file", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Silver releases schema: keyed on (repo_full_name, release_id) with asset counts and pre-release flags."""

SILVER_REPO_METADATA_SCHEMA = StructType([
    StructField("repo_id", LongType(), False),
    StructField("repo_full_name", StringType(), False),
    StructField("repo_name", StringType(), True),
    StructField("owner_login", StringType(), True),
    StructField("description", StringType(), True),
    StructField("default_branch", StringType(), True),
    StructField("license_spdx_id", StringType(), True),
    StructField("stargazers_count", IntegerType(), True),
    StructField("forks_count", IntegerType(), True),
    StructField("open_issues_count", IntegerType(), True),
    StructField("created_at_utc", TimestampType(), True),
    StructField("updated_at_utc", TimestampType(), True),
    StructField("pushed_at_utc", TimestampType(), True),
    StructField("topics", ArrayType(StringType()), True),
    StructField("_source_file", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Silver repo metadata schema: keyed on (repo_id, repo_full_name) capturing community stats and license governance."""

SILVER_SCHEMAS = {
    "repo_metadata": SILVER_REPO_METADATA_SCHEMA,
    "commits": SILVER_COMMITS_SCHEMA,
    "issues": SILVER_ISSUES_SCHEMA,
    "pull_requests": SILVER_PULL_REQUESTS_SCHEMA,
    "releases": SILVER_RELEASES_SCHEMA,
}


# =============================================================================
# Quarantine & Operations Log Schemas
# =============================================================================

SILVER_QUARANTINE_SCHEMA = StructType([
    StructField("quarantine_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("repo_full_name", StringType(), True),
    StructField("rejection_reason", StringType(), False),
    StructField("raw_payload", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Quarantine schema: captures malformed, unparseable, or schema-violating rows without failing pipeline batches."""

PIPELINE_EXECUTION_LOGS_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("parameter", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])
"""Operational audit log schema: tracks batch runs, execution durations, row metrics, and error traces in ops table."""
