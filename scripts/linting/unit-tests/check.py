"""Reject Rust tests that a diff adds without a `// unit-test: <reason>` opt-in.

Usage: check.py --staged | check.py --range BASE HEAD
"""

import re
import subprocess
import sys

TEST_ATTRIBUTE = re.compile(r"^\s*#\[(?:tokio::test|test|rstest)\b")
ATTRIBUTE_OR_COMMENT = re.compile(r"^\s*(?:#\[|//)")
FUNCTION = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")
SUBTESTS = re.compile(r"\brun_subtests(?:_shared)?!\s*\(")
SUBTEST = re.compile(r"^\s*(?:\w+::)*(\w+)\s*,?\s*$")
OPT_IN = re.compile(r"//\s*unit-test:(.*)")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
MIN_REASON = 10

GUIDANCE = """\
Behavior tests belong in YAML: plan-shape fixtures for compiler output,
data-correctness scenarios for query results, and indexer scenarios for indexing.

A Rust unit test is opt-in. Put this on the line above `fn`, with a reason:

    // unit-test: <why only a Rust test can check this>

The reason must say why YAML cannot check the behavior. Do not restate the
test name.

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


def added_lines(diff: str) -> dict[str, set[int]]:
    added: dict[str, set[int]] = {}
    path = None
    line = 0
    for text in diff.splitlines():
        if text.startswith("+++ "):
            path = text[6:] if text.startswith("+++ b/") else None
            if path:
                added.setdefault(path, set())
        elif match := HUNK.match(text):
            line = int(match.group(1))
        elif path and text.startswith("+"):
            added[path].add(line)
            line += 1
    return added


def tests(lines: list[str]) -> list[tuple[str, int, set[int]]]:
    found = []
    for index, text in enumerate(lines):
        if not TEST_ATTRIBUTE.match(text):
            continue
        top = index
        while top and ATTRIBUTE_OR_COMMENT.match(lines[top - 1]):
            top -= 1
        end = index
        while end < len(lines) and not FUNCTION.match(lines[end]):
            end += 1
        if end < len(lines):
            name = FUNCTION.match(lines[end]).group(1)
            found.append((name, top + 1, {index + 1, end + 1}))
    return found


def subtests(lines: list[str]) -> list[tuple[str, int, set[int]]]:
    found = []
    for index, text in enumerate(lines):
        if not SUBTESTS.search(text):
            continue
        depth = text.count("(") - text.count(")")
        line = index + 1
        while depth > 0 and line < len(lines):
            depth += lines[line].count("(") - lines[line].count(")")
            name = SUBTEST.match(lines[line])
            if name and name.group(1) != "ctx":
                top = line
                while top and lines[top - 1].strip().startswith("//"):
                    top -= 1
                found.append((name.group(1), top + 1, {line + 1}))
            line += 1
    return found


def reason(lines: list[str], top: int, end: int) -> str | None:
    for text in lines[top - 1 : end]:
        if match := OPT_IN.search(text):
            return match.group(1).strip()
    return None


def check(path: str, lines: list[str], added: set[int]) -> tuple[list[str], list[str]]:
    rejected, reasonless = [], []
    for name, top, triggers in tests(lines) + subtests(lines):
        if not added & triggers:
            continue
        end = max(triggers)
        given = reason(lines, top, end)
        if given is None:
            rejected.append(f"{path}/{name}/{end}")
        elif len(given) < MIN_REASON:
            reasonless.append(f"{path}/{name}/{end}")
    return rejected, reasonless


def main(args: list[str]) -> int:
    if args == ["--staged"]:
        diff_args, revision = ["--cached"], ""
    elif len(args) == 3 and args[0] == "--range":
        diff_args, revision = [args[1], args[2]], args[2]
    else:
        sys.exit(__doc__)
    diff = git(
        "diff", *diff_args, "-U0", "-M", "--no-color", "--no-ext-diff",
        "--src-prefix=a/", "--dst-prefix=b/", "--diff-filter=AMR", "--", "crates/**/*.rs",
    )
    rejected, reasonless = [], []
    for path, lines in added_lines(diff).items():
        source = git("show", f"{revision}:{path}").splitlines()
        found = check(path, source, lines)
        rejected += found[0]
        reasonless += found[1]
    if not rejected and not reasonless:
        return 0
    if rejected:
        print("tests rejected:", *sorted(rejected), sep="\n")
    if reasonless:
        print(f"\nopt-ins with a reason under {MIN_REASON} characters:", *sorted(reasonless), sep="\n")
    print(f"\n{GUIDANCE}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
