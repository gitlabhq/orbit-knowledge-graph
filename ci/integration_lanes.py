import argparse
from pathlib import Path
import re
import subprocess
import sys

import yaml


LANES = ("integration-test", "integration-test-data-correctness", "corpus-smoke-test")


class GitLabLoader(yaml.SafeLoader):
    pass


GitLabLoader.add_constructor("!reference", GitLabLoader.construct_sequence)


def list_tests(filter_expression=None):
    command = [
        "cargo", "nextest", "list", "--all-features", "--test", "containers",
        "-p", "integration-tests", "--message-format", "oneline",
    ]
    if filter_expression is not None:
        command.extend(["-E", filter_expression])
    result = subprocess.run(command, capture_output=True)
    if result.returncode:
        raise ValueError(f"cargo nextest list failed:\n{result.stderr.decode(errors='replace').strip()}")
    tests = set()
    for line in result.stdout.decode("utf-8").splitlines():
        if not line.strip():
            continue
        parts = re.split(r"\s", line, maxsplit=1)
        if len(parts) != 2 or not parts[1].strip():
            raise ValueError(f"unexpected cargo nextest list output: {line}")
        tests.add(parts[1].strip())
    return tests


def main():
    parser = argparse.ArgumentParser(description="Verify the integration test lane partition.")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if not args.check:
        raise ValueError("pass --check to verify the integration test lane partition")
    config = yaml.load(Path(".gitlab-ci.yml").read_text(encoding="utf-8"), Loader=GitLabLoader)
    all_tests = list_tests()
    if not all_tests:
        raise ValueError("cargo nextest listed no container tests")

    assignments = {}
    for lane in LANES:
        if lane not in config:
            raise ValueError(f"missing integration lane job {lane} in .gitlab-ci.yml")
        try:
            expression = config[lane]["variables"]["NEXTEST_FILTER"]
        except (KeyError, TypeError) as error:
            raise ValueError(f"parsing integration lane job {lane}") from error
        if not isinstance(expression, str):
            raise ValueError(f"parsing integration lane job {lane}: NEXTEST_FILTER must be a string")
        if not expression.strip():
            raise ValueError(f"{lane}.variables.NEXTEST_FILTER must not be empty")
        tests = list_tests(expression)
        print(f"{lane}: {len(tests)} tests")
        for test in tests:
            assignments.setdefault(test, []).append(lane)

    missing = sorted(all_tests - assignments.keys())
    duplicates = sorted(test for test in all_tests if len(assignments.get(test, [])) > 1)
    if missing:
        print("Container tests missing from every integration lane:", file=sys.stderr)
        for test in missing:
            print(f"  {test}", file=sys.stderr)
    if duplicates:
        print("Container tests assigned to more than one integration lane:", file=sys.stderr)
        for test in duplicates:
            print(f"  {test}: {', '.join(sorted(assignments[test]))}", file=sys.stderr)
    if missing or duplicates:
        raise ValueError("integration lane filters do not cover every container test exactly once")
    print(f"All {len(all_tests)} container tests are assigned to exactly one integration lane.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, yaml.YAMLError) as error:
        sys.exit(str(error))
