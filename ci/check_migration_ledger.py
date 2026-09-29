import argparse
from pathlib import Path
import subprocess
import sys

from skip_check import skip_requested

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Check the migration ledger against a base ref.")
    parser.add_argument("--base", default="origin/main")
    args = parser.parse_args()
    if skip_requested("migration-ledger-check", args.base):
        print("✅ [skip migration-ledger-check] — skipping.")
        return 0
    return subprocess.run(
        ["cargo", "xtask", "migration-ledger", "check", "--base", args.base], cwd=ROOT,
    ).returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except OSError as error:
        sys.exit(str(error))
