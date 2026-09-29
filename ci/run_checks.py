import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Run every check in a CI job and report failures.")
    parser.add_argument("group", choices=("checks", "advisory", "hygiene", "generated", "tests"))
    group = parser.parse_args().group
    python = [sys.executable]
    merge_request = os.environ.get("CI_PIPELINE_SOURCE") == "merge_request_event"
    base = os.environ.get("CI_MERGE_REQUEST_DIFF_BASE_SHA")
    tests = [
        python + ["-m", "unittest", "discover", "-s", directory, "-p", "*_test.py"]
        for directory in ("ci/linting", "ci/tests")
    ] + [python + ["ci/version_checks_test.py"]]

    if group == "checks":
        commands = [
            python + ["ci/check-repository.py"],
            python + ["ci/validate-schemas.py", "all"],
            *tests,
            python + ["ci/linting/prose_lint.py"] + (["--diff-base", base] if base else ["--all"]),
        ]
        if merge_request:
            commands.append(python + ["ci/check-version-bumps.py"])
    elif group == "advisory":
        commands = [python + ["ci/linting/check_narration.py"] + (["--diff-base", base] if base else [])]
        if merge_request:
            commands.append(python + ["ci/linting/check_mr_description.py"])
    elif group == "hygiene":
        commands = [["mise", "lint:deps"]]
        if merge_request:
            commands += [["mise", "lint:fmt"], ["mise", "lint:newlines"]]
    elif group == "generated":
        commands = [python + ["ci/check_generated.py"]]
        if merge_request:
            target = os.environ["CI_MERGE_REQUEST_TARGET_BRANCH_NAME"]
            commands.append(["git", "fetch", "origin", target, "--depth=1"])
            commands.append(python + ["ci/check_migration_ledger.py", "--base", f"origin/{target}"])
    else:
        commands = tests

    failed = []
    for command in commands:
        print(f"Running {' '.join(command)}", flush=True)
        try:
            if subprocess.run(command, cwd=ROOT).returncode:
                failed.append(" ".join(command))
        except OSError as error:
            print(error, file=sys.stderr)
            failed.append(" ".join(command))
    if failed:
        print("Failed checks:\n" + "\n".join(failed), file=sys.stderr)
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
