"""
Plain-Python Schema Verification Script.
Verifies that:
1. Every field name defined in each entity schema exists in at least one real JSON record across bronze/
2. Reports any fields in the real JSON that are omitted from the schema.
Zero external dependencies (uses standard library and src.schemas stubs).
"""

import json
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.schemas import BRONZE_SCHEMAS, PRIMARY_KEYS, StructType


def get_field_names(schema):
    """Recursively or top-level extract field names from StructType schema."""
    return [field.name for field in schema.fields]


def collect_json_keys(entity_name, bronze_dir):
    """
    Collects all unique top-level keys across all 10 repositories for a given entity.
    Also collects sample records to verify nested field presence.
    """
    all_keys = set()
    records_sampled = 0
    nested_keys = {
        "commit_subkeys": set(),
        "author_subkeys": set(),
        "user_subkeys": set(),
        "pull_request_subkeys": set(),
        "head_subkeys": set(),
        "base_subkeys": set(),
        "asset_subkeys": set(),
    }

    repos = sorted([
        d for d in os.listdir(bronze_dir)
        if os.path.isdir(os.path.join(bronze_dir, d))
    ])

    for repo in repos:
        file_path = os.path.join(bronze_dir, repo, f"{entity_name}.json")
        if not os.path.exists(file_path):
            continue

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [Warning] Could not load {file_path}: {e}")
            continue

        records = [data] if isinstance(data, dict) else data
        for r in records[:50]:  # sample up to 50 records per repo
            if not isinstance(r, dict):
                continue
            records_sampled += 1
            all_keys.update(r.keys())

            # Check nested keys
            if "commit" in r and isinstance(r["commit"], dict):
                nested_keys["commit_subkeys"].update(r["commit"].keys())
            if "author" in r and isinstance(r["author"], dict):
                nested_keys["author_subkeys"].update(r["author"].keys())
            if "user" in r and isinstance(r["user"], dict):
                nested_keys["user_subkeys"].update(r["user"].keys())
            if "pull_request" in r and isinstance(r["pull_request"], dict):
                nested_keys["pull_request_subkeys"].update(r["pull_request"].keys())
            if "head" in r and isinstance(r["head"], dict):
                nested_keys["head_subkeys"].update(r["head"].keys())
            if "base" in r and isinstance(r["base"], dict):
                nested_keys["base_subkeys"].update(r["base"].keys())
            if "assets" in r and isinstance(r["assets"], list) and r["assets"]:
                for a in r["assets"]:
                    if isinstance(a, dict):
                        nested_keys["asset_subkeys"].update(a.keys())

    return all_keys, nested_keys, records_sampled


def main():
    bronze_dir = os.path.join(PROJECT_ROOT, "bronze")
    if not os.path.exists(bronze_dir):
        print(f"Error: bronze/ directory not found at {bronze_dir}")
        sys.exit(1)

    print("=" * 80)
    print("BRONZE SCHEMAS VERIFICATION REPORT ACROSS ALL 10 REPOSITORIES")
    print("=" * 80)

    overall_passed = True

    for entity, schema in BRONZE_SCHEMAS.items():
        print(f"\nEntity: '{entity}' (Primary Key: '{PRIMARY_KEYS.get(entity)}')")
        print("-" * 80)

        schema_fields = get_field_names(schema)
        json_keys, nested_keys, sampled_count = collect_json_keys(entity, bronze_dir)

        print(f"  Total records sampled across repos: {sampled_count}")
        print(f"  Total schema fields:                {len(schema_fields)}")
        print(f"  Total unique JSON keys found:       {len(json_keys)}")

        # 1. Verify schema fields in JSON (excluding Spark internal _corrupt_record)
        missing_from_json = []
        verified_fields = []

        for field in schema_fields:
            if field == "_corrupt_record":
                continue
            if field in json_keys:
                verified_fields.append(field)
            else:
                missing_from_json.append(field)

        print(f"  Verified schema fields in real JSON: {len(verified_fields)} / {len(schema_fields) - 1}")

        if missing_from_json:
            print(f"  ❌ ERROR: Schema fields NOT found in any real JSON record: {missing_from_json}")
            overall_passed = False
        else:
            print("  ✓ All schema fields confirmed present in real Bronze data.")

        # 2. Report fields in JSON that are not in schema
        omitted_from_schema = sorted(list(json_keys - set(schema_fields)))
        print(f"  ℹ JSON fields omitted from schema ({len(omitted_from_schema)} total):")
        if omitted_from_schema:
            print(f"    {omitted_from_schema}")
        else:
            print("    [None - 100% field coverage!]")

        # 3. Report nested struct verification
        if entity == "commits":
            print(f"    Nested 'commit' subkeys verified: {sorted(list(nested_keys['commit_subkeys']))}")
        elif entity == "issues":
            print(f"    Nested 'pull_request' subkeys verified: {sorted(list(nested_keys['pull_request_subkeys']))}")
        elif entity == "pulls":
            print(f"    Nested 'head' subkeys verified: {sorted(list(nested_keys['head_subkeys']))}")
            print(f"    Nested 'base' subkeys verified: {sorted(list(nested_keys['base_subkeys']))}")
        elif entity == "releases":
            print(f"    Nested 'assets' subkeys verified: {sorted(list(nested_keys['asset_subkeys']))}")

    print("\n" + "=" * 80)
    if overall_passed:
        print("VERDICT: ALL SCHEMAS VERIFIED SUCCESSFULLY AGAINST REAL BRONZE DATA! (PASS)")
    else:
        print("VERDICT: SCHEMA VERIFICATION FAILED (CHECK ERRORS ABOVE)")
    print("=" * 80)

    sys.exit(0 if overall_passed else 1)


if __name__ == "__main__":
    main()
