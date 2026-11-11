"""Reject Rust test functions that a diff adds without an opt-in reason.

Usage: check.py --staged | check.py --range BASE HEAD
"""

import json
import re
import subprocess
import sys
from pathlib import Path

CONFIG = Path(__file__).with_name("sgconfig.yml")
OPT_IN = re.compile(r"ast-grep-ignore:\s*unit-test\b(?P<rest>.*)")
REASON = re.compile(r"^\s*--\s*(?P<reason>\S.{9,})")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
FUNCTION = re.compile(r"\bfn\s+(\w+)")

GUIDANCE = """\
Behavior tests belong in YAML: plan-shape fixtures for compiler output,
data-correctness scenarios for query results, and indexer scenarios for indexing.

A Rust unit test is opt-in. Put this on the line above `fn`, with a reason:

    // ast-grep-ignore: unit-test -- <why only a Rust test can check this>

Good reasons name mission-critical logic that YAML cannot reach, for example:
  - cryptography, such as token signing or constant-time comparison
  - data structure correctness, such as an index or cache invariant
  - security policy edge cases, such as role floors or path scoping
  - parsers and encoders with exact byte-level output
  - concurrency primitives, such as locks, leases, or ordering"""


def added_lines(diff: str) -> dict[str, dict[int, str]]:
    added: dict[str, dict[int, str]] = {}
    path = None
    line = 0
    for text in diff.splitlines():
        if text.startswith("+++ "):
            target = text[4:]
            path = target[2:] if target.startswith("b/") else None
            if path is not None:
                added.setdefault(path, {})
        elif match := HUNK.match(text):
            line = int(match.group(1))
        elif path is not None and text.startswith("+"):
            added[path][line] = text[1:]
            line += 1
        elif path is not None and not text.startswith("-"):
            line += 1
    return added


def rejected_tests(hits: list[dict], added: dict[str, dict[int, str]]) -> list[str]:
    rejected = []
    for hit in hits:
        start = hit["range"]["start"]["line"] + 1
        if start not in added.get(hit["file"], {}):
            continue
        name = FUNCTION.search(hit["text"])
        rejected.append(f"{hit['file']}/{name.group(1) if name else '?'}/{start}")
    return sorted(rejected)


def opt_ins_without_reason(added: dict[str, dict[int, str]]) -> list[str]:
    missing = []
    for path, lines in added.items():
        for number, text in lines.items():
            opt_in = OPT_IN.search(text)
            if opt_in and not REASON.match(opt_in.group("rest")):
                missing.append(f"{path}/{number}")
    return sorted(missing)


def scan(files: list[str]) -> list[dict]:
    if not files:
        return []
    result = subprocess.run(
        ["ast-grep", "scan", "-c", str(CONFIG), "--off=unused-suppression", "--json=compact", *files],
        capture_output=True,
        text=True,
        check=False,
    )
    return json.loads(result.stdout or "[]")


def diff(args: list[str]) -> str:
    if args == ["--staged"]:
        command = ["git", "diff", "--cached"]
    elif len(args) == 3 and args[0] == "--range":
        command = ["git", "diff", args[1], args[2]]
    else:
        sys.exit(__doc__)
    return subprocess.run(
        [*command, "-U0", "--no-color", "--diff-filter=AM", "--", "crates/**/*.rs"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def main(args: list[str]) -> int:
    added = added_lines(diff(args))
    files = [path for path in added if Path(path).exists()]
    rejected = rejected_tests(scan(files), added)
    missing = opt_ins_without_reason(added)
    if not rejected and not missing:
        return 0
    if rejected:
        print("tests rejected:", *rejected, sep="\n")
    if missing:
        print("\nopt-ins without a reason:", *missing, sep="\n")
    print(f"\n{GUIDANCE}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
