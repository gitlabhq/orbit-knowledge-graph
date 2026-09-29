import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def skip_requested(name, base_ref=None):
    tag = f"[skip {name}]"
    environment_name = "SKIP_" + name.upper().replace("-", "_")
    if os.environ.get(environment_name) == "1" or any(
        tag in os.environ.get(field, "")
        for field in ("CI_MERGE_REQUEST_DESCRIPTION", "CI_MERGE_REQUEST_TITLE")
    ):
        return True
    if not base_ref:
        return False
    if diff_base := os.environ.get("CI_MERGE_REQUEST_DIFF_BASE_SHA"):
        subprocess.run(
            ["git", "fetch", "origin", diff_base, "--depth=1"],
            cwd=ROOT, stderr=subprocess.DEVNULL,
        )
    messages = subprocess.run(
        ["git", "log", f"{base_ref}...HEAD", "--format=%B"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    ).stdout
    return tag.encode() in messages


def main():
    parser = argparse.ArgumentParser(description="Exit 0 when a named check has a skip request, otherwise 1.")
    parser.add_argument("name")
    args = parser.parse_args()
    return int(not skip_requested(args.name, os.environ.get("BASE_REF")))


if __name__ == "__main__":
    sys.exit(main())
