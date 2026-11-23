#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml>=6.0"]
# ///
from __future__ import annotations

import argparse
import collections
import dataclasses
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "sgconfig.yml"
LEDGER = HERE / "ledger"
LEDGER_KEYS = {"category", "reason", "preview", "hash", "lines", "rules"}
CATEGORIES = {
    "safety", "security", "compliance", "upstream", "contract", "invariant",
    "magic-value", "known-gap", "tool-read", "generated", "vendored", "test-data",
}
PREVIEW_CHARS = 72
SUPPRESSION = re.compile(r"ast-grep-ignore(?P<ids>:[^\n]*)?")
NO_IGNORE = [arg for kind in ("hidden", "dot", "exclude", "global", "parent", "vcs") for arg in ("--no-ignore", kind)]
SHEBANG = re.compile(r"#!\S+( \S+)*")
DIRECTIVES = [
    re.compile(pattern)
    for pattern in (
        r"#\s*shellcheck( (disable|enable|source|shell)=[\w,./-]+)+",
        r"#\s*noqa(: [A-Z]+[0-9]+(, ?[A-Z]+[0-9]+)*)?",
        r"#\s*type: ignore(\[[\w-]+(, ?[\w-]+)*\])?",
        r"#\s*pragma: no cover",
        r"#\s*yamllint (disable|enable)(-line)?( rule:[\w-]+)*",
        r"#\s*frozen_string_literal: (true|false)",
        r"#\s*rubocop:(disable|enable|todo)( [\w/]+(, ?[\w/]+)*)?",
        r"#\s*yaml-language-server: \$schema=\S+",
        r"(//|/\*)\s*eslint-(disable|enable)(-next-line|-line)?( [\w@/-]+(, ?[\w@/-]+)*)?\s*(\*/)?",
        r"(//|/\*)\s*prettier-ignore\s*(\*/)?",
        r"//\s*@ts-(expect-error|ignore|nocheck|check)",
        r"///\s*<reference (types|path|lib)=\"[^\"]+\"\s*/>",
        r"/\*\s*istanbul ignore (next|if|else|file)\s*\*/",
        r"(#|//)\s*ast-grep-ignore: [a-z0-9-]+(, ?[a-z0-9-]+)*( -- \S.*)?",
        r"#\s*gitleaks:allow",
        r"/\*\*\s*@jsxImportSource \S+\s*\*/",
    )
]

GUIDE = """\
Code comments are not allowed in this repository.

A comment is not checked by any tool. It goes stale, and the next agent copies
it as a pattern. A comment that justifies a workaround keeps the workaround in
the code. Remove each comment above and fix the root cause in the code:

  The comment says what the code does.
    Rename the item, or extract a function whose name says it.
  The comment justifies a workaround (a sleep, a retry, a clone, a magic value).
    Fix the design that needs the workaround. If you cannot fix it in this MR,
    open an issue and link it in the MR description.
  The comment states an invariant, a limit, or a unit.
    Put it in a type, a named const, an assert, or a test.
  The comment is a TODO or a FIXME.
    Open an issue.
  The comment explains a decision.
    Put it in the MR description or the commit message.

Exception process: if no step applies and a reviewer agrees, add an entry to the
area file under scripts/linting/comments/ledger/. Code owners approve each change.
Print a ready entry for the comment block at a line, then fill in the reason:

  mise run lint:comments -- --entry crates/example/src/lib.rs:42

  crates/example/src/lib.rs:
  - category: invariant
    reason: why no change to the code can replace this comment
    preview: "// the first line of the comment"
    hash: 3f2a9c1e5b7d4a60
    lines: 2

The hash covers every line of the block, so an edit to a kept comment fails
until you print the entry again and update it in the same MR. An entry without
a hash exempts every comment in the files that its path glob matches. "rules"
limits an entry to some comments-* rules. An entry that exempts no comment fails.
Categories: {categories}.

Inline suppression (ast-grep-ignore) of the comment rules is not allowed."""
GUIDE = GUIDE.replace("{categories}", ", ".join(sorted(CATEGORIES)))

HINTS = [
    (re.compile(r"\b(todo|fixme|xxx)\b", re.I), "open an issue for this work"),
    (
        re.compile(r"\b(workaround|hack|for now|temporar|sleep|retry|timeout|wait|flaky)", re.I),
        "fix the design that needs this workaround",
    ),
    (
        re.compile(r"\b(must|never|always|invariant|at most|at least|limit|bytes|seconds|ms)\b", re.I),
        "put this rule in a type, a named const, an assert, or a test",
    ),
    (re.compile(r"\b(because|why|so that|in order to|reason)\b", re.I), "put this reason in the MR description"),
    (re.compile(r"^(///|//!|/\*\*|\"\"\"|''')"), "make the name and the types say this"),
]
DEFAULT_HINT = "rename or extract so the code says this"

Finding = collections.namedtuple("Finding", "path line text rule")


@dataclasses.dataclass
class Entry:
    path: str
    pattern: re.Pattern
    rules: frozenset[str] | None
    category: str
    reason: str
    preview: str
    hash: str | None
    lines: int
    source: str
    used: int = 0

    def allows(self, finding: Finding) -> bool:
        return self.rules is None or finding.rule in self.rules

    def run_at(self, findings: list[Finding], start: int) -> bool:
        run = findings[start : start + self.lines]
        return (
            len(run) == self.lines
            and all(follows(prev, nxt) for prev, nxt in zip(run, run[1:]))
            and all(self.allows(finding) for finding in run)
            and block_hash(run) == self.hash
        )


def key(text: str) -> str:
    return hashlib.sha256(" ".join(text.split()).encode()).hexdigest()[:16]


def follows(prev: Finding, nxt: Finding) -> bool:
    return nxt.path == prev.path and nxt.line == prev.line + prev.text.count("\n") + 1


def block_hash(run: list[Finding]) -> str:
    return key("\n".join(finding.text for finding in run))


def preview(text: str) -> str:
    first = text.strip().splitlines()[0].strip()
    return first if len(first) <= PREVIEW_CHARS else first[: PREVIEW_CHARS - 1] + "…"


def area_file(path: str) -> str:
    parts = path.split("/")
    if parts[0] == "crates" and len(parts) > 2:
        return f"crates/{parts[1]}.yaml"
    return f"{parts[0]}.yaml" if len(parts) > 1 else "root.yaml"


def is_directive(text: str, line: int) -> bool:
    return (line == 1 and SHEBANG.fullmatch(text) is not None) or any(d.fullmatch(text) for d in DIRECTIVES)


def hint(text: str) -> str:
    return next((advice for pattern, advice in HINTS if pattern.search(text)), DEFAULT_HINT)


def glob_to_regex(glob: str) -> re.Pattern:
    parts = re.split(r"(\*\*/|\*\*|\*|\?)", glob)
    tokens = {"**/": "(?:.*/)?", "**": ".*", "*": "[^/]*", "?": "[^/]"}
    return re.compile("".join(tokens.get(part, re.escape(part)) for part in parts))


def load_ledger() -> list[Entry]:
    entries = []
    for file in sorted(LEDGER.rglob("*.yaml")):
        source = str(file.relative_to(HERE))
        for path, items in (yaml.safe_load(file.read_text()) or {}).items():
            for raw in items:
                unknown = set(raw) - LEDGER_KEYS
                if unknown or raw.get("category") not in CATEGORIES or not str(raw.get("reason", "")).strip():
                    sys.exit(f"comment check: {source}: entry for {path} needs a category from {sorted(CATEGORIES)}, a reason, and only {sorted(LEDGER_KEYS)}")
                if ("hash" in raw) != ("lines" in raw):
                    sys.exit(f"comment check: {source}: entry for {path} needs both hash and lines, or neither")
                rules = frozenset(raw["rules"]) if "rules" in raw else None
                entries.append(
                    Entry(path, glob_to_regex(path), rules, raw["category"], raw["reason"], raw.get("preview", ""), raw.get("hash"), int(raw.get("lines", 0)), source)
                )
    return entries


def scan() -> list[Finding]:
    proc = subprocess.run(
        ["ast-grep", "scan", "-c", str(CONFIG), "--warning", "--json=stream", *NO_IGNORE, *tracked()],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"comment check: ast-grep failed:\n{proc.stderr}")
    findings = []
    for line in proc.stdout.splitlines():
        match = json.loads(line)
        start = match["range"]["start"]["line"] + 1
        text = match["text"].strip()
        if match["ruleId"].startswith("comments-") and not is_directive(text, start):
            findings.append(Finding(match["file"], start, text, match["ruleId"]))
    return sorted(findings)


def apply_ledger(findings: list[Finding], ledger: list[Entry]) -> list[Finding]:
    exact: dict[str, list[Entry]] = collections.defaultdict(list)
    globbed = []
    for entry in ledger:
        (globbed if any(char in entry.path for char in "*?") else exact[entry.path]).append(entry)
    by_file: dict[str, list[Finding]] = collections.defaultdict(list)
    for finding in findings:
        by_file[finding.path].append(finding)
    remaining = []
    for path, in_file in by_file.items():
        exempt = [False] * len(in_file)
        entries = [*exact.get(path, ()), *(entry for entry in globbed if entry.pattern.fullmatch(path))]
        for entry in entries:
            for index, finding in enumerate(in_file):
                if exempt[index]:
                    continue
                if entry.hash is None:
                    if entry.allows(finding):
                        exempt[index] = True
                        entry.used += 1
                    continue
                if any(exempt[index : index + entry.lines]) or not entry.run_at(in_file, index):
                    continue
                exempt[index : index + entry.lines] = [True] * entry.lines
                entry.used += 1
                if path in exact and entry in exact[path]:
                    break
        remaining.extend(finding for finding, done in zip(in_file, exempt) if not done)
    return sorted(remaining)


def block_at(findings: list[Finding], path: str, line: int) -> list[Finding]:
    in_file = [finding for finding in findings if finding.path == path]
    index = next((i for i, finding in enumerate(in_file) if finding.line <= line <= finding.line + finding.text.count("\n")), None)
    if index is None:
        sys.exit(f"comment check: no comment at {path}:{line}")
    start = end = index
    while start > 0 and follows(in_file[start - 1], in_file[start]):
        start -= 1
    while end + 1 < len(in_file) and follows(in_file[end], in_file[end + 1]):
        end += 1
    return in_file[start : end + 1]


def entry_for(location: str) -> int:
    path, _, line = location.rpartition(":")
    run = block_at(scan(), path, int(line))
    item = {"category": "invariant", "reason": "why no change to the code can replace this comment", "preview": preview(run[0].text), "hash": block_hash(run), "lines": len(run)}
    print(f"# scripts/linting/comments/ledger/{area_file(path)}")
    print(yaml.safe_dump({path: [item]}, sort_keys=False, allow_unicode=True, width=1000), end="")
    return 0


def tracked() -> list[str]:
    paths = subprocess.run(["git", "ls-files", "-z"], capture_output=True, text=True, check=True).stdout.split("\0")
    return [path for path in paths if path and Path(path).is_file()]


def suppressions() -> list[str]:
    found = []
    for path in tracked():
        if Path(path).suffix in {".md", ".mdx", ".toml", ".txt"} or LEDGER in Path(path).resolve().parents or Path(path).resolve() == Path(__file__).resolve():
            continue
        text = Path(path).read_text(errors="replace")
        for match in SUPPRESSION.finditer(text):
            ids = match["ids"]
            if ids is None or "comments-" in ids.split(" -- ", 1)[0]:
                found.append(f"{path}:{text.count(chr(10), 0, match.start()) + 1}")
    return found


def report(fresh: list[Finding], hidden: list[str]) -> None:
    for finding in fresh:
        first = finding.text.splitlines()[0][:100]
        print(f"{finding.path}:{finding.line}: {first}\n  -> {hint(finding.text)}")
    for location in hidden:
        print(f"{location}: ast-grep-ignore must name a rule, and must not name a comments-* rule")
    print(f"\n{GUIDE}")


def check() -> int:
    ledger = load_ledger()
    findings = apply_ledger(scan(), ledger)
    hidden = suppressions()
    status = 0
    if findings or hidden:
        print(f"comment check: {len(findings)} comment(s), {len(hidden)} suppression(s)\n")
        report(findings, hidden)
        status = 1
    for entry in ledger:
        if entry.used == 0:
            print(f"comment check: {entry.source}: {entry.path} {entry.preview!r} exempts no comment. Update it with --entry, or remove it.")
            status = 1
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description="Block code comments that the comment ledger does not allow.")
    parser.add_argument("--entry", metavar="PATH:LINE", help="print a ledger entry for the comment block at PATH:LINE")
    args = parser.parse_args()
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True)
    os.chdir(top.stdout.strip())
    return entry_for(args.entry) if args.entry else check()


if __name__ == "__main__":
    sys.exit(main())
