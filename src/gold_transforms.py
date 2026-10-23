"""
Pure, testable PySpark transformation functions and repository classification mappings
for the Gold Star Schema layer (Medallion Architecture).
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone, date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
)

# =============================================================================
# 1. Repository Ecosystem Classification Mapping
# =============================================================================

REPO_DB_MAPPING: Dict[str, Dict[str, str]] = {
    "postgres/postgres": {
        "db_type": "SQL",
        "db_category": "Relational",
    },
    "cockroachdb/cockroach": {
        "db_type": "SQL",
        "db_category": "NewSQL/Distributed",
    },
    "mongodb/mongo": {
        "db_type": "NoSQL",
        "db_category": "Document",
    },
    "surrealdb/surrealdb": {
        "db_type": "NoSQL",
        "db_category": "Multi-model",
    },
    "redis/redis": {
        "db_type": "NoSQL",
        "db_category": "Key-Value",
    },
    "apache/cassandra": {
        "db_type": "NoSQL",
        "db_category": "Wide-Column",
    },
    "facebookresearch/faiss": {
        "db_type": "Vector",
        "db_category": "Vector Index/Search",
    },
    "qdrant/qdrant": {
        "db_type": "Vector",
        "db_category": "Vector Database",
    },
    "milvus-io/milvus": {
        "db_type": "Vector",
        "db_category": "Vector Database",
    },
    "chroma-core/chroma": {
        "db_type": "Vector",
        "db_category": "Vector Database",
    },
}

ALL_10_REPOS = sorted(list(REPO_DB_MAPPING.keys()))

# Default balanced analysis window established in audit/bronze_quality_report.md
DEFAULT_ANALYSIS_WINDOW_START = "2026-04-01"
DEFAULT_ANALYSIS_WINDOW_END = "2026-09-30"


# =============================================================================
# 2. Key Generation Helpers
# =============================================================================

def compute_repo_key(col_or_name):
    """Deterministic surrogate key for dim_repo using xxhash64 on repo_full_name."""
    c = F.col(col_or_name) if isinstance(col_or_name, str) else col_or_name
    return F.xxhash64(F.lower(F.trim(c)))


def compute_date_key(col_or_name):
    """Integer yyyymmdd surrogate key for dim_date."""
    c = F.col(col_or_name) if isinstance(col_or_name, str) else col_or_name
    return F.when(c.isNotNull(), F.date_format(c, "yyyyMMdd").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))


def compute_actor_identity(login_col, email_hash_col):
    """
    Computes natural identity:
    coalesce(author_login, concat('hash:', author_email_hash), 'unknown')
    """
    l_col = F.col(login_col) if isinstance(login_col, str) else login_col
    h_col = F.col(email_hash_col) if isinstance(email_hash_col, str) else email_hash_col

    clean_login = F.when(l_col.isNotNull() & (F.length(F.trim(l_col)) > 0), F.trim(l_col)).otherwise(None)
    clean_hash = F.when(h_col.isNotNull() & (F.length(F.trim(h_col)) > 0), F.concat(F.lit("hash:"), F.trim(h_col))).otherwise(None)

    return F.coalesce(clean_login, clean_hash, F.lit("unknown"))


def compute_actor_key(identity_col):
    """Deterministic surrogate key for dim_actor using xxhash64 on actor_identity."""
    c = F.col(identity_col) if isinstance(identity_col, str) else identity_col
    return F.xxhash64(c)


# =============================================================================
# 3. Dimension Transformations
# =============================================================================

def build_dim_repo(df_silver_repo_metadata: DataFrame) -> DataFrame:
    """
    Transforms silver.repo_metadata into gold.dim_repo (SCD Type 1).
    Enriches with db_type and db_category from REPO_DB_MAPPING.
    """
    # Build classification mapping expressions
    type_expr = F.lit("Unknown")
    cat_expr = F.lit("Unknown")

    for repo, meta in REPO_DB_MAPPING.items():
        type_expr = F.when(F.col("repo_full_name") == repo, F.lit(meta["db_type"])).otherwise(type_expr)
        cat_expr = F.when(F.col("repo_full_name") == repo, F.lit(meta["db_category"])).otherwise(cat_expr)

    return (
        df_silver_repo_metadata
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("db_type", type_expr)
        .withColumn("db_category", cat_expr)
        .select(
            "repo_key",
            "repo_id",
            "repo_full_name",
            "repo_name",
            "owner_login",
            "description",
            "default_branch",
            "license_spdx_id",
            "created_at_utc",
            "db_type",
            "db_category",
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key"])
    )


def build_dim_date(
    spark: SparkSession,
    start_date_str: str = "2020-01-01",
    end_date_str: str = "2027-01-01",
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Generates a continuous calendar day spine dimension for dim_date.
    Includes calendar attributes, weekend flags, and in_analysis_window flags.
    """
    # Generate days sequence using Spark SQL sequence function
    df_seq = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date_str}'), to_date('{end_date_str}'), interval 1 day)) AS full_date
    """)

    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    return (
        df_seq
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date").cast(IntegerType()))
        .withColumn("quarter", F.quarter("full_date").cast(IntegerType()))
        .withColumn("month", F.month("full_date").cast(IntegerType()))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date").cast(IntegerType()))
        .withColumn("day_of_month", F.dayofmonth("full_date").cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek("full_date").cast(IntegerType()))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        .withColumn(
            "in_analysis_window",
            (F.col("full_date") >= F.to_date(F.lit(w_since))) & (F.col("full_date") <= F.to_date(F.lit(w_until)))
        )
        .withColumn("load_timestamp", F.current_timestamp())
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "is_weekend",
            "in_analysis_window",
            "load_timestamp",
        )
    )


def build_dim_actor(
    df_commits: Optional[DataFrame] = None,
    df_issues: Optional[DataFrame] = None,
    df_pulls: Optional[DataFrame] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Unions actors from silver.commits, silver.issues, and silver.pull_requests.
    Resolves natural identity:
      - login when present
      - 'hash:' + email_hash when login is null (FAISS-style contributor identity)
      - 'unknown' member row
    Surrogate key: xxhash64(actor_identity).
    Ensures zero raw PII reaches dim_actor.
    """
    dfs_to_union = []

    if df_commits is not None and "author_login" in df_commits.columns:
        c_actors = (
            df_commits
            .select(
                F.col("author_login"),
                F.col("author_id").cast(LongType()),
                F.col("author_email_hash"),
                F.col("is_bot"),
            )
            .filter(F.col("author_login").isNotNull() | F.col("author_email_hash").isNotNull())
        )
        dfs_to_union.append(c_actors)

    if df_issues is not None and "author_login" in df_issues.columns:
        i_actors = (
            df_issues
            .select(
                F.col("author_login"),
                F.col("author_id").cast(LongType()),
                F.lit(None).cast(StringType()).alias("author_email_hash"),
                F.col("is_bot"),
            )
            .filter(F.col("author_login").isNotNull())
        )
        dfs_to_union.append(i_actors)

    if df_pulls is not None and "author_login" in df_pulls.columns:
        p_actors = (
            df_pulls
            .select(
                F.col("author_login"),
                F.col("author_id").cast(LongType()),
                F.lit(None).cast(StringType()).alias("author_email_hash"),
                F.col("is_bot"),
            )
            .filter(F.col("author_login").isNotNull())
        )
        dfs_to_union.append(p_actors)

    if dfs_to_union:
        base_union = dfs_to_union[0]
        for df in dfs_to_union[1:]:
            base_union = base_union.unionByName(df)
    else:
        raise ValueError("At least one DataFrame must be provided to build_dim_actor.")

    # Compute identity and identity_source
    annotated = (
        base_union
        .withColumn("actor_identity", compute_actor_identity("author_login", "author_email_hash"))
        .withColumn(
            "identity_source",
            F.when(F.col("author_login").isNotNull() & (F.length(F.trim(F.col("author_login"))) > 0), F.lit("login"))
            .when(F.col("author_email_hash").isNotNull() & (F.length(F.trim(F.col("author_email_hash"))) > 0), F.lit("email_hash"))
            .otherwise(F.lit("unknown"))
        )
    )

    # Group by actor_identity to consolidate multiple appearances and get canonical attributes
    consolidated = (
        annotated
        .groupBy("actor_identity")
        .agg(
            F.first("author_login", ignorenulls=True).alias("author_login"),
            F.first("author_id", ignorenulls=True).alias("author_id"),
            F.first("author_email_hash", ignorenulls=True).alias("author_email_hash"),
            F.first("identity_source", ignorenulls=True).alias("identity_source"),
            F.max("is_bot").alias("is_bot"),
        )
        .withColumn("actor_key", compute_actor_key("actor_identity"))
    )

    # Create explicit Unknown member row
    active_spark = spark or consolidated.sparkSession
    unknown_schema = StructType([
        StructField("actor_identity", StringType(), False),
        StructField("author_login", StringType(), True),
        StructField("author_id", LongType(), True),
        StructField("author_email_hash", StringType(), True),
        StructField("identity_source", StringType(), False),
        StructField("is_bot", BooleanType(), False),
    ])
    df_unknown = (
        active_spark.createDataFrame(
            [(
                "unknown",
                None,
                None,
                None,
                "unknown",
                False,
            )],
            schema=unknown_schema
        )
        .withColumn("actor_key", compute_actor_key(F.col("actor_identity")))
        .withColumn("load_timestamp", F.current_timestamp())
    )

    result = (
        consolidated
        .withColumn("load_timestamp", F.current_timestamp())
        .select(
            "actor_key",
            "actor_identity",
            "author_login",
            "author_id",
            "author_email_hash",
            "identity_source",
            "is_bot",
            "load_timestamp",
        )
        .unionByName(df_unknown)
        .dropDuplicates(["actor_key"])
    )

    return result


# =============================================================================
# 4. Base Fact Transformations
# =============================================================================

def build_fact_commits(
    df_silver_commits: DataFrame,
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Transforms silver.commits into gold.fact_commits.
    Grain: one row per commit.
    Resolves actor identity with fallback to author_email_hash when author_login is null.
    """
    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    actor_id_expr = compute_actor_identity("author_login", "author_email_hash")

    return (
        df_silver_commits
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("date_key", compute_date_key("committed_at_utc"))
        .withColumn("actor_key", compute_actor_key(actor_id_expr))
        .withColumn(
            "in_analysis_window",
            (F.to_date("committed_at_utc") >= F.to_date(F.lit(w_since))) &
            (F.to_date("committed_at_utc") <= F.to_date(F.lit(w_until)))
        )
        .select(
            "repo_key",
            "date_key",
            "actor_key",
            "commit_sha",
            F.coalesce(F.col("comment_count"), F.lit(0)).cast(IntegerType()).alias("comment_count"),
            F.coalesce(F.col("is_bot"), F.lit(False)).alias("is_bot"),
            F.coalesce(F.col("in_analysis_window"), F.lit(False)).alias("in_analysis_window"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key", "commit_sha"])
    )


def build_fact_issues(
    df_silver_issues: DataFrame,
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Transforms silver.issues into gold.fact_issues.
    Grain: one row per true issue.
    Calculates time_to_close_hours defensively (null if open, never negative).
    """
    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    actor_id_expr = compute_actor_identity("author_login", F.lit(None))

    # Duration in hours between created and closed
    raw_close_hours = (F.unix_timestamp("closed_at_utc") - F.unix_timestamp("created_at_utc")) / 3600.0
    safe_close_hours = F.when(
        F.col("closed_at_utc").isNotNull(),
        F.greatest(F.lit(0.0), raw_close_hours.cast(DoubleType()))
    ).otherwise(F.lit(None).cast(DoubleType()))

    is_open_expr = F.lower(F.col("state")) == "open"

    return (
        df_silver_issues
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("created_date_key", compute_date_key("created_at_utc"))
        .withColumn("closed_date_key", compute_date_key("closed_at_utc"))
        .withColumn("actor_key", compute_actor_key(actor_id_expr))
        .withColumn("is_open", is_open_expr)
        .withColumn("time_to_close_hours", safe_close_hours)
        .withColumn(
            "in_analysis_window",
            (F.to_date("created_at_utc") >= F.to_date(F.lit(w_since))) &
            (F.to_date("created_at_utc") <= F.to_date(F.lit(w_until)))
        )
        .select(
            "repo_key",
            "created_date_key",
            "closed_date_key",
            "actor_key",
            F.col("issue_number").cast(IntegerType()),
            F.col("state"),
            "is_open",
            F.coalesce(F.col("comments_count"), F.lit(0)).cast(IntegerType()).alias("comments_count"),
            F.coalesce(F.col("label_count"), F.lit(0)).cast(IntegerType()).alias("label_count"),
            "time_to_close_hours",
            F.coalesce(F.col("in_analysis_window"), F.lit(False)).alias("in_analysis_window"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key", "issue_number"])
    )


def build_fact_pull_requests(
    df_silver_pulls: DataFrame,
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Transforms silver.pull_requests into gold.fact_pull_requests.
    Grain: one row per PR.
    Calculates hours_to_merge defensively (null if not merged, never negative).
    """
    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    actor_id_expr = compute_actor_identity("author_login", F.lit(None))

    raw_merge_hours = (F.unix_timestamp("merged_at_utc") - F.unix_timestamp("created_at_utc")) / 3600.0
    safe_merge_hours = F.when(
        F.col("is_merged") & F.col("merged_at_utc").isNotNull(),
        F.greatest(F.lit(0.0), raw_merge_hours.cast(DoubleType()))
    ).otherwise(F.lit(None).cast(DoubleType()))

    return (
        df_silver_pulls
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("created_date_key", compute_date_key("created_at_utc"))
        .withColumn("closed_date_key", compute_date_key("closed_at_utc"))
        .withColumn("merged_date_key", compute_date_key("merged_at_utc"))
        .withColumn("actor_key", compute_actor_key(actor_id_expr))
        .withColumn("hours_to_merge", safe_merge_hours)
        .withColumn(
            "in_analysis_window",
            (F.to_date("created_at_utc") >= F.to_date(F.lit(w_since))) &
            (F.to_date("created_at_utc") <= F.to_date(F.lit(w_until)))
        )
        .select(
            "repo_key",
            "created_date_key",
            "closed_date_key",
            "merged_date_key",
            "actor_key",
            F.col("pr_number").cast(IntegerType()),
            F.coalesce(F.col("is_merged"), F.lit(False)).alias("is_merged"),
            F.coalesce(F.col("is_bot"), F.lit(False)).alias("is_bot"),
            "hours_to_merge",
            F.coalesce(F.col("in_analysis_window"), F.lit(False)).alias("in_analysis_window"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key", "pr_number"])
    )


def build_fact_releases(
    df_silver_releases: DataFrame,
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Transforms silver.releases into gold.fact_releases.
    Grain: one row per release.
    Calculates days_since_previous_release via lag window function per repo.
    """
    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    # Partition by repo_full_name, order chronologically by published_at_utc
    w_repo = Window.partitionBy("repo_full_name").orderBy("published_at_utc", "release_id")
    prev_pub_time = F.lag("published_at_utc", 1).over(w_repo)
    raw_days_diff = (F.unix_timestamp("published_at_utc") - F.unix_timestamp(prev_pub_time)) / 86400.0

    safe_days_diff = F.when(
        prev_pub_time.isNotNull(),
        F.greatest(F.lit(0.0), raw_days_diff.cast(DoubleType()))
    ).otherwise(F.lit(None).cast(DoubleType()))

    return (
        df_silver_releases
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("published_date_key", compute_date_key("published_at_utc"))
        .withColumn("days_since_previous_release", safe_days_diff)
        .withColumn(
            "in_analysis_window",
            (F.to_date("published_at_utc") >= F.to_date(F.lit(w_since))) &
            (F.to_date("published_at_utc") <= F.to_date(F.lit(w_until)))
        )
        .select(
            "repo_key",
            "published_date_key",
            F.col("release_id").cast(LongType()),
            F.col("tag_name"),
            F.coalesce(F.col("is_prerelease"), F.lit(False)).alias("is_prerelease"),
            F.coalesce(F.col("is_draft"), F.lit(False)).alias("is_draft"),
            F.coalesce(F.col("assets_count"), F.lit(0)).cast(IntegerType()).alias("assets_count"),
            "days_since_previous_release",
            F.coalesce(F.col("in_analysis_window"), F.lit(False)).alias("in_analysis_window"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key", "release_id"])
    )


def build_fact_repo_snapshot(
    df_silver_repo_metadata: DataFrame,
    as_of_date_str: Optional[str] = None,
    window_since: str = DEFAULT_ANALYSIS_WINDOW_START,
    window_until: str = DEFAULT_ANALYSIS_WINDOW_END,
) -> DataFrame:
    """
    Transforms silver.repo_metadata into gold.fact_repo_snapshot.
    Grain: (repo_key, snapshot_date_key).
    Captures periodic community metrics (stars, forks, open issues).
    """
    w_since = window_since[:10] if window_since else DEFAULT_ANALYSIS_WINDOW_START
    w_until = window_until[:10] if window_until else DEFAULT_ANALYSIS_WINDOW_END

    if as_of_date_str:
        date_expr = F.to_date(F.lit(as_of_date_str))
    else:
        date_expr = F.current_date()

    return (
        df_silver_repo_metadata
        .withColumn("repo_key", compute_repo_key("repo_full_name"))
        .withColumn("snapshot_date", date_expr)
        .withColumn("snapshot_date_key", F.date_format("snapshot_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn(
            "in_analysis_window",
            (F.col("snapshot_date") >= F.to_date(F.lit(w_since))) &
            (F.col("snapshot_date") <= F.to_date(F.lit(w_until)))
        )
        .select(
            "repo_key",
            "snapshot_date_key",
            F.coalesce(F.col("stargazers_count"), F.lit(0)).cast(IntegerType()).alias("stargazers_count"),
            F.coalesce(F.col("forks_count"), F.lit(0)).cast(IntegerType()).alias("forks_count"),
            F.coalesce(F.col("open_issues_count"), F.lit(0)).cast(IntegerType()).alias("open_issues_count"),
            F.coalesce(F.col("in_analysis_window"), F.lit(False)).alias("in_analysis_window"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["repo_key", "snapshot_date_key"])
    )


# =============================================================================
# 5. Aggregate Fact Transformation (Dense Day Spine + Rolling Windows)
# =============================================================================

def build_fact_repo_daily_activity(
    df_fact_commits: DataFrame,
    df_fact_issues: DataFrame,
    df_fact_pull_requests: DataFrame,
    df_fact_releases: DataFrame,
    df_dim_repo: DataFrame,
    df_dim_date: DataFrame,
) -> DataFrame:
    """
    Builds the high-value AGGREGATE fact gold.fact_repo_daily_activity.
    Grain: repo_key x date_key (one row per repo per calendar day).

    Methodology:
    1. Cross-joins dim_repo with dim_date to generate an exhaustive, dense day spine per repo.
    2. Aggregates commits, issues, PRs, and releases to (repo_key, date_key).
    3. Left-joins spine with all daily activity aggregates, zero-filling missing measures.
    4. Applies rolling 4-week (28 rows) and 12-week (84 rows) window functions on human commits.
       The dense spine guarantees that days without activity count as zero and do not distort
       the rolling sums.
    """
    # Step 1: Build dense spine
    spine = (
        df_dim_repo.select("repo_key")
        .crossJoin(df_dim_date.select("date_key", "full_date", "in_analysis_window"))
    )

    # Step 2: Commits aggregate
    agg_commits = (
        df_fact_commits
        .groupBy("repo_key", "date_key")
        .agg(
            F.count(F.lit(1)).cast(LongType()).alias("commits_total"),
            F.sum(F.when(~F.col("is_bot"), F.lit(1)).otherwise(F.lit(0))).cast(LongType()).alias("commits_human"),
            F.sum(F.when(F.col("is_bot"), F.lit(1)).otherwise(F.lit(0))).cast(LongType()).alias("commits_bot"),
            F.countDistinct(F.when(~F.col("is_bot"), F.col("actor_key"))).cast(LongType()).alias("distinct_human_authors"),
        )
    )

    # Step 3: Issues aggregates
    agg_issues_opened = (
        df_fact_issues
        .groupBy("repo_key", F.col("created_date_key").alias("date_key"))
        .agg(F.count(F.lit(1)).cast(LongType()).alias("issues_opened"))
    )

    agg_issues_closed = (
        df_fact_issues
        .filter(F.col("closed_date_key").isNotNull())
        .groupBy("repo_key", F.col("closed_date_key").alias("date_key"))
        .agg(F.count(F.lit(1)).cast(LongType()).alias("issues_closed"))
    )

    # Step 4: Pull request aggregates
    agg_prs_opened = (
        df_fact_pull_requests
        .groupBy("repo_key", F.col("created_date_key").alias("date_key"))
        .agg(F.count(F.lit(1)).cast(LongType()).alias("prs_opened"))
    )

    agg_prs_merged = (
        df_fact_pull_requests
        .filter(F.col("merged_date_key").isNotNull())
        .groupBy("repo_key", F.col("merged_date_key").alias("date_key"))
        .agg(F.count(F.lit(1)).cast(LongType()).alias("prs_merged"))
    )

    # Step 5: Releases aggregate
    agg_releases = (
        df_fact_releases
        .groupBy("repo_key", F.col("published_date_key").alias("date_key"))
        .agg(F.count(F.lit(1)).cast(LongType()).alias("releases_published"))
    )

    # Step 6: Join spine with aggregates
    joined = (
        spine
        .join(agg_commits, on=["repo_key", "date_key"], how="left")
        .join(agg_issues_opened, on=["repo_key", "date_key"], how="left")
        .join(agg_issues_closed, on=["repo_key", "date_key"], how="left")
        .join(agg_prs_opened, on=["repo_key", "date_key"], how="left")
        .join(agg_prs_merged, on=["repo_key", "date_key"], how="left")
        .join(agg_releases, on=["repo_key", "date_key"], how="left")
        .select(
            F.col("repo_key"),
            F.col("date_key"),
            F.col("full_date"),
            F.col("in_analysis_window"),
            F.coalesce(F.col("commits_total"), F.lit(0)).cast(LongType()).alias("commits_total"),
            F.coalesce(F.col("commits_human"), F.lit(0)).cast(LongType()).alias("commits_human"),
            F.coalesce(F.col("commits_bot"), F.lit(0)).cast(LongType()).alias("commits_bot"),
            F.coalesce(F.col("distinct_human_authors"), F.lit(0)).cast(LongType()).alias("distinct_human_authors"),
            F.coalesce(F.col("issues_opened"), F.lit(0)).cast(LongType()).alias("issues_opened"),
            F.coalesce(F.col("issues_closed"), F.lit(0)).cast(LongType()).alias("issues_closed"),
            F.coalesce(F.col("prs_opened"), F.lit(0)).cast(LongType()).alias("prs_opened"),
            F.coalesce(F.col("prs_merged"), F.lit(0)).cast(LongType()).alias("prs_merged"),
            F.coalesce(F.col("releases_published"), F.lit(0)).cast(LongType()).alias("releases_published"),
        )
    )

    # Step 7: Rolling window calculations over chronological full_date per repo
    # 4 weeks = 28 days (current day + 27 prior days)
    # 12 weeks = 84 days (current day + 83 prior days)
    w_4wk = Window.partitionBy("repo_key").orderBy("full_date").rowsBetween(-27, 0)
    w_12wk = Window.partitionBy("repo_key").orderBy("full_date").rowsBetween(-83, 0)

    result = (
        joined
        .withColumn("commits_human_4wk", F.sum("commits_human").over(w_4wk).cast(LongType()))
        .withColumn("commits_human_12wk", F.sum("commits_human").over(w_12wk).cast(LongType()))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(
            "repo_key",
            "date_key",
            "commits_total",
            "commits_human",
            "commits_bot",
            "distinct_human_authors",
            "issues_opened",
            "issues_closed",
            "prs_opened",
            "prs_merged",
            "releases_published",
            "commits_human_4wk",
            "commits_human_12wk",
            "in_analysis_window",
            "load_timestamp",
        )
    )

    return result
