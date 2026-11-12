#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11"
# ///
"""Reject Rust tests without a `// unit-test: <reason>` opt-in.

Usage: check.py             check every file that config.toml includes
       check.py FILE...     check these files, read from the git index
"""

import re
import subprocess
import sys
import tomllib
from pathlib import Path

CONFIG = tomllib.loads(Path(__file__).with_name("config.toml").read_text())
PATTERNS = {name: re.compile(pattern) for name, pattern in CONFIG["patterns"].items()}
MIN_REASON = CONFIG["min_reason"]

GUIDANCE = f"""\
Behavior tests belong in YAML: plan-shape fixtures for compiler output,
data-correctness scenarios for query results, and indexer scenarios for indexing.

A Rust unit test is opt-in. Put this on the line above `fn`, with a reason:

    // unit-test: <why only a Rust test can check this>

The reason must be at least {MIN_REASON} characters and say why YAML cannot check
the behavior. Do not restate the test name.

Good reasons name mission-critical logic that YAML cannot reach, for example:
  - cryptography, such as token signing or constant-time comparison
  - data structure correctness, such as an index or cache invariant
  - security policy edge cases, such as role floors or path scoping
  - parsers and encoders with exact byte-level output
  - concurrency primitives, such as locks, leases, or ordering"""


def git(*args: str) -> str:
    return subprocess.run(
        ["git", "-c", "core.quotePath=false", *args], capture_output=True, text=True, check=True
    ).stdout


def checked_files(files: list[str]) -> list[str]:
    pathspecs = [f":(glob){glob}" for glob in CONFIG["include"]]
    pathspecs += [f":(glob,exclude){glob}" for glob in CONFIG["ignore"]]
    tracked = git("ls-files", "-z", "--", *pathspecs).split("\0")
    selected = set(files) if files else None
    return [path for path in tracked if path and (selected is None or path in selected)]


def tests(lines: list[str]) -> list[tuple[str, int, int]]:
    found = []
    for index, text in enumerate(lines):
        if not PATTERNS["test_attribute"].match(text):
            continue
        top = index
        while top and PATTERNS["attribute_or_comment"].match(lines[top - 1]):
            top -= 1
        end = index
        while end < len(lines) and not PATTERNS["function"].match(lines[end]):
            end += 1
        if end < len(lines):
            found.append((PATTERNS["function"].match(lines[end]).group(1), top + 1, end + 1))
    return found


def subtests(lines: list[str]) -> list[tuple[str, int, int]]:
    found = []
    for index, text in enumerate(lines):
        if not PATTERNS["subtest_macro"].search(text):
            continue
        depth = text.count("(") - text.count(")")
        line = index + 1
        while depth > 0 and line < len(lines):
            depth += lines[line].count("(") - lines[line].count(")")
            name = PATTERNS["subtest_name"].match(lines[line])
            if name and name.group(1) != "ctx":
                top = line
                while top and lines[top - 1].strip().startswith("//"):
                    top -= 1
                found.append((name.group(1), top + 1, line + 1))
            line += 1
    return found


def reason(lines: list[str], top: int, end: int) -> str | None:
    for text in lines[top - 1 : end]:
        if match := PATTERNS["opt_in"].search(text):
            return match.group(1).strip()
    return None


def check(path: str, lines: list[str]) -> tuple[list[str], list[str]]:
    rejected, short = [], []
    for name, top, end in tests(lines) + subtests(lines):
        given = reason(lines, top, end)
        if given is None:
            rejected.append(f"{path}/{name}/{end}")
        elif len(given) < MIN_REASON:
            short.append(f"{path}/{name}/{end}")
    return rejected, short


def main(files: list[str]) -> int:
    rejected, short = [], []
    for path in checked_files(files):
        source = git("show", f":{path}") if files else Path(path).read_text()
        found = check(path, source.splitlines())
        rejected += found[0]
        short += found[1]
    if not rejected and not short:
        return 0
    if rejected:
        print("tests rejected:", *rejected, sep="\n")
    if short:
        print(f"\nopt-ins with a reason under {MIN_REASON} characters:", *short, sep="\n")
    count = len(rejected) + len(short)
    print(f"\n{count} {'test needs' if count == 1 else 'tests need'} a YAML fixture or an opt-in.")
    print(f"\n{GUIDANCE}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
