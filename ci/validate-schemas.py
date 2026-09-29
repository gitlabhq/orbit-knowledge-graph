#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = {
    "ontology": [("ontology", "config/ontology/**/*.yaml")],
    "named-queries": [("named_query", "config/named_queries/**/*.yaml")],
    "versions": [("versions", "config/versions.yaml")],
    "migration-ledger": [("schema-migrations", "config/schema-migrations.yaml")],
    "indexer-scenarios": [
        ("indexer_scenario", "crates/integration-tests/tests/indexer/scenarios/**/*.yaml")
    ],
    "setup": [
        ("setup_agent", "config/setup/agents/**/*.yaml"),
        ("setup", "config/setup/setup.yaml"),
    ],
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate repository YAML schemas.")
    parser.add_argument("selection", choices=[*SCHEMAS, "all"], nargs="?", default="all")
    args = parser.parse_args()

    failed = False
    for selection in SCHEMAS if args.selection == "all" else [args.selection]:
        for schema, pattern in SCHEMAS[selection]:
            schema_path = Path(f"config/schemas/{schema}.schema.json")
            if selection == "ontology" and (REPO_ROOT / schema_path).stat().st_size > 65536:
                print(f"{schema_path} must stay at or below 64 KB", file=sys.stderr)
                failed = True
            files = sorted(
                str(path.relative_to(REPO_ROOT))
                for path in REPO_ROOT.glob(pattern)
                if selection != "ontology" or path.name != "reference.yaml"
            )
            if not files:
                print(f"No files matched {pattern} for {schema_path}", file=sys.stderr)
                failed = True
                continue
            result = subprocess.run(
                [sys.executable, "-m", "check_jsonschema", "--verbose", "--schemafile", str(schema_path), *files],
                cwd=REPO_ROOT,
            )
            if result.returncode:
                failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
