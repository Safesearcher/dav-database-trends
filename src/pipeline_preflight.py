"""Pre-flight checks for the raw source layout used by the pipeline orchestrator."""

from collections.abc import Callable, Iterable


def normalize_source_path(base_path: str) -> str:
    """Return a normalized source root or reject the known malformed layout."""
    normalized = base_path.strip().rstrip("/")
    if not normalized:
        raise ValueError(
            "Source path is empty. Set base_path to "
            "/Volumes/workspace/bronze_data/raw or "
            "/Volumes/workspace/bronze_data/raw_api."
        )

    if "/raw/raw_api" in normalized:
        raise ValueError(
            f"Malformed source path {base_path!r}: raw_api must be a sibling of raw, "
            "not nested below it. Set base_path to "
            "/Volumes/workspace/bronze_data/raw or "
            "/Volumes/workspace/bronze_data/raw_api."
        )
    return normalized


def expected_source_files(
    base_path: str,
    repos: Iterable[str],
    entities: Iterable[str],
) -> list[str]:
    """Build the raw JSON paths required for a pipeline source batch."""
    root = normalize_source_path(base_path)
    return [
        f"{root}/{repo.replace('/', '_')}/{entity}.json"
        for repo in repos
        for entity in entities
    ]


def find_missing_source_files(
    base_path: str,
    repos: Iterable[str],
    entities: Iterable[str],
    file_exists: Callable[[str], bool],
) -> list[str]:
    """Return required source files that are absent or empty."""
    return [
        path
        for path in expected_source_files(base_path, repos, entities)
        if not file_exists(path)
    ]


def ensure_source_layout(
    base_path: str,
    repos: Iterable[str],
    entities: Iterable[str],
    file_exists: Callable[[str], bool],
    ingest_incremental: Callable[[], str],
) -> str:
    """Validate a batch, refreshing it once when stars_history is incomplete."""
    root = normalize_source_path(base_path)
    missing = find_missing_source_files(root, repos, entities, file_exists)
    if missing and any(path.endswith("/stars_history.json") for path in missing):
        root = normalize_source_path(ingest_incremental())
        missing = find_missing_source_files(root, repos, entities, file_exists)

    if missing:
        preview = "\n".join(f"  - {path}" for path in missing[:10])
        more = f"\n  ... and {len(missing) - 10} more" if len(missing) > 10 else ""
        raise FileNotFoundError(
            "A-2 pre-flight failed: required staged source files are missing or empty:\n"
            f"{preview}{more}\n"
            "Stage all repository/entity JSON files, or verify that "
            "00_ingest_github_api can complete in incremental mode."
        )
    return root
