import argparse
from pathlib import Path
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Check vendored dependencies, reporting every failure.")
    parser.add_argument("name", nargs="?", default="all")
    args = parser.parse_args()
    if args.name == "all":
        try:
            entries = yaml.safe_load((ROOT / "config/versions.yaml").read_bytes())["vendored"]
            names = [name for name, entry in entries.items() if entry.get("check_script") is not None]
        except (OSError, KeyError, TypeError, AttributeError, yaml.YAMLError) as error:
            print(f"ERROR: config/versions.yaml: {error}", file=sys.stderr)
            return 1
    else:
        names = [args.name]

    status = 0
    for name in names:
        print(f"Checking vendored dependency {name}", flush=True)
        try:
            result = subprocess.run(["bash", "scripts/vendored/run.sh", "check", name], cwd=ROOT)
            if result.returncode:
                print(f"ERROR: {name}: check failed (exit {result.returncode}).", file=sys.stderr)
                status = 1
        except OSError as error:
            print(f"ERROR: {name}: {error}", file=sys.stderr)
            status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
