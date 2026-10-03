"""
scripts/make_drift_sample.py — Schema-Drift Test Sample Generator
===================================================================
Copies samples/issues.json AND samples/commits.json into samples/drift/
and deliberately injects the following anomalies to test Bronze schema-drift
detection and Silver quarantine logic in the pipeline:

Anomaly A — New unexpected top-level field:
    Every record gets  "new_field_test": "drift_injected"
    Bronze absorbs it via schema evolution (evolve_table_schema), adding a new
    STRING column to the Bronze table without dropping data.

Anomaly B — Type corruption (issues only):
    3 records (indices 1, 2, 3) have their "comments" field changed from an integer to the
    string "many". In Bronze per-record parsing, these 3 records fail schema parsing
    and are quarantined in ops.silver_quarantine with layer='bronze'.

Anomaly C — Unparseable date (issues only):
    1 true issue record (index 4) has its "created_at" set to "not-a-date".
    Bronze accepts it, but Silver fails to parse it as TimestampType and quarantines
    that row in ops.silver_quarantine with layer='silver'.

Usage:
    python scripts/make_drift_sample.py
    python scripts/make_drift_sample.py --source-dir samples --out-dir samples/drift
    python scripts/make_drift_sample.py --dry-run   (prints what would be written)

Output layout:
    samples/drift/
        issues.json   (5 records: anomaly A on all, B on 3, C on 1)
        commits.json  (5 records: anomaly A on all)
"""

import argparse
import copy
import json
import os
import sys

# Ensure UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

NEW_FIELD = "new_field_test"
NEW_FIELD_VALUE = "drift_injected"

# Indices (0-based) that get the type-corruption anomaly (B)
TYPE_CORRUPT_INDICES = [1, 2, 3]

# Index (0-based) that gets the bad-date anomaly (C) - index 4 is a true issue
BAD_DATE_INDEX = 4
BAD_DATE_VALUE = "not-a-date"


# ---------------------------------------------------------------------------
# Mutation helpers
# ---------------------------------------------------------------------------
def inject_issues(records: list) -> list:
    """
    Apply all three drift anomalies to issues records.
      A: new_field_test on every record
      B: comments = "many"  on records at TYPE_CORRUPT_INDICES
      C: created_at = "not-a-date" on record at BAD_DATE_INDEX
    Returns a new list (original is not mutated).
    """
    out = []
    for idx, rec in enumerate(records):
        r = copy.deepcopy(rec)

        # A — unexpected field on every record
        r[NEW_FIELD] = NEW_FIELD_VALUE

        # B — type corruption: integer field → string
        if idx in TYPE_CORRUPT_INDICES:
            r["comments"] = "many"

        # C — unparseable date
        if idx == BAD_DATE_INDEX:
            r["created_at"] = BAD_DATE_VALUE

        out.append(r)
    return out


def inject_commits(records: list) -> list:
    """
    Apply anomaly A (new_field_test) to every commit record.
    Commits don't have a 'comments' count field at the top level,
    and Silver quarantines on unparseable dates only — so only A is injected.
    Returns a new list.
    """
    out = []
    for rec in records:
        r = copy.deepcopy(rec)
        r[NEW_FIELD] = NEW_FIELD_VALUE
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------
def print_summary(entity: str, original: list, drifted: list) -> None:
    print(f"\n  Entity: {entity}")
    print(f"    Source records  : {len(original)}")
    print(f"    Drifted records : {len(drifted)}")
    if entity == "issues":
        a_count = sum(1 for r in drifted if NEW_FIELD in r)
        b_count = sum(1 for r in drifted if r.get("comments") == "many")
        c_count = sum(1 for r in drifted if r.get("created_at") == BAD_DATE_VALUE)
        print(f"    Anomaly A ({NEW_FIELD!r})     : {a_count} records")
        print(f"    Anomaly B (comments='many')         : {b_count} records  [expect Bronze quarantine]")
        print(f"    Anomaly C (created_at='not-a-date') : {c_count} record   [expect Silver quarantine]")
    elif entity == "commits":
        a_count = sum(1 for r in drifted if NEW_FIELD in r)
        print(f"    Anomaly A ({NEW_FIELD!r})     : {a_count} records")


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
def run(source_dir: str, out_dir: str, dry_run: bool) -> None:
    issues_src = os.path.join(source_dir, "issues.json")
    commits_src = os.path.join(source_dir, "commits.json")

    for path in (issues_src, commits_src):
        if not os.path.exists(path):
            print(f"Error: source file not found: {path}", file=sys.stderr)
            sys.exit(1)

    issues_raw: list = json.load(open(issues_src, encoding="utf-8"))
    commits_raw: list = json.load(open(commits_src, encoding="utf-8"))

    if not isinstance(issues_raw, list):
        issues_raw = [issues_raw]
    if not isinstance(commits_raw, list):
        commits_raw = [commits_raw]

    issues_drifted = inject_issues(issues_raw)
    commits_drifted = inject_commits(commits_raw)

    print("=" * 60)
    print("DRIFT SAMPLE GENERATOR")
    print(f"  Source dir : {source_dir}")
    print(f"  Output dir : {out_dir}")
    print(f"  Dry run    : {dry_run}")
    print_summary("issues", issues_raw, issues_drifted)
    print_summary("commits", commits_raw, commits_drifted)

    if dry_run:
        print("\n[DRY-RUN] No files written.")
        return

    os.makedirs(out_dir, exist_ok=True)

    issues_out = os.path.join(out_dir, "issues.json")
    commits_out = os.path.join(out_dir, "commits.json")

    with open(issues_out, "w", encoding="utf-8") as f:
        json.dump(issues_drifted, f, indent=2)
    print(f"\n  Written: {issues_out}")

    with open(commits_out, "w", encoding="utf-8") as f:
        json.dump(commits_drifted, f, indent=2)
    print(f"  Written: {commits_out}")

    # ----------------------------------------------------------------
    # Quick validation — read back and assert anomalies are present
    # ----------------------------------------------------------------
    v_issues = json.load(open(issues_out, encoding="utf-8"))
    v_commits = json.load(open(commits_out, encoding="utf-8"))

    assert all(r.get(NEW_FIELD) == NEW_FIELD_VALUE for r in v_issues), "Anomaly A missing in issues"
    assert all(r.get(NEW_FIELD) == NEW_FIELD_VALUE for r in v_commits), "Anomaly A missing in commits"
    type_bad = [r for r in v_issues if r.get("comments") == "many"]
    assert len(type_bad) == len(TYPE_CORRUPT_INDICES), \
        f"Expected {len(TYPE_CORRUPT_INDICES)} type-corrupt records, got {len(type_bad)}"
    date_bad = [r for r in v_issues if r.get("created_at") == BAD_DATE_VALUE]
    assert len(date_bad) == 1, f"Expected 1 bad-date record, got {len(date_bad)}"

    print("\n  Validation: ALL assertions passed.")
    print("\nDone. Load these files into the Bronze notebook with run_mode=full")
    print("to verify drift detection, quarantine, and mergeSchema evolution.\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate schema-drift test samples for Bronze/Silver pipeline testing.",
        epilog="Example: python scripts/make_drift_sample.py --out-dir samples/drift",
    )
    p.add_argument(
        "--source-dir",
        default="samples",
        metavar="PATH",
        help="Directory containing the source issues.json and commits.json (default: samples).",
    )
    p.add_argument(
        "--out-dir",
        default="samples/drift",
        metavar="PATH",
        help="Output directory (default: samples/drift).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without creating files.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(source_dir=args.source_dir, out_dir=args.out_dir, dry_run=args.dry_run)
