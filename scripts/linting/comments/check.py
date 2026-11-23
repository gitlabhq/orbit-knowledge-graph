#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11"
# dependencies = []
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
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "sgconfig.yml"
LEDGER = HERE / "ledger.toml"
LEDGER_KEYS = {"glob", "rules", "text", "reason"}
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
        r"(#|//)\s*ast-grep-ignore: [a-z0-9-]+(, ?[a-z0-9-]+)*",
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

Exception process: if no step applies and a reviewer agrees, add an entry to
scripts/linting/comments/ledger.toml. Code owners approve each change to it.
An entry without "text" exempts every comment in the files that "glob" matches.
An entry with "text" exempts only that exact comment; a multi-line "text"
exempts each of its lines. "rules" limits the entry
to some comments-* rules. Each entry needs a reason, and an entry that exempts
no comment fails the check.

  [[entry]]
  glob = "crates/example/src/lib.rs"
  text = "// the exact comment text"
  reason = "why no change to the code can replace this comment"

Inline suppression (ast-grep-ignore) of the comment rules is not allowed."""

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
    glob: str
    pattern: re.Pattern
    rules: frozenset[str] | None
    text: str | None
    reason: str
    keys: frozenset[str] = frozenset()
    used: int = 0

    def covers(self, finding: Finding) -> bool:
        return (
            self.pattern.fullmatch(finding.path) is not None
            and (self.rules is None or finding.rule in self.rules)
            and (self.text is None or key(finding.text) in self.keys)
        )


def key(text: str) -> str:
    return hashlib.sha256(" ".join(text.split()).encode()).hexdigest()[:16]


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
    for raw in tomllib.loads(LEDGER.read_text()).get("entry", []):
        unknown = set(raw) - LEDGER_KEYS
        if unknown or "glob" not in raw or not raw.get("reason", "").strip():
            sys.exit(f"comment check: ledger entry {raw} needs glob and reason, and only {sorted(LEDGER_KEYS)}")
        rules = frozenset(raw["rules"]) if "rules" in raw else None
        text = raw.get("text")
        lines = [line for line in (text or "").splitlines() if line.strip()]
        keys = frozenset([key(text), *map(key, lines)]) if text else frozenset()
        entries.append(Entry(raw["glob"], glob_to_regex(raw["glob"]), rules, text, raw["reason"], keys))
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
    by_path: dict[str, list[Entry]] = collections.defaultdict(list)
    globbed = []
    for entry in ledger:
        if any(char in entry.glob for char in "*?"):
            globbed.append(entry)
        else:
            by_path[entry.glob].append(entry)
    remaining = []
    for finding in findings:
        candidates = [*by_path.get(finding.path, ()), *globbed]
        entry = next((entry for entry in candidates if entry.covers(finding)), None)
        if entry is None:
            remaining.append(finding)
        else:
            entry.used += 1
    return remaining


def tracked() -> list[str]:
    paths = subprocess.run(["git", "ls-files", "-z"], capture_output=True, text=True, check=True).stdout.split("\0")
    return [path for path in paths if path and Path(path).is_file()]


def suppressions() -> list[str]:
    found = []
    for path in tracked():
        if Path(path).suffix in {".md", ".mdx"} or Path(path).resolve() == Path(__file__).resolve():
            continue
        text = Path(path).read_text(errors="replace")
        for match in SUPPRESSION.finditer(text):
            ids = match["ids"]
            if ids is None or "comments-" in ids:
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
            print(f"comment check: ledger entry {entry.glob!r} exempts no comment. Remove it from ledger.toml.")
            status = 1
    return status


def main() -> int:
    argparse.ArgumentParser(description="Block code comments that ledger.toml does not allow.").parse_args()
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True)
    os.chdir(top.stdout.strip())
    return check()


if __name__ == "__main__":
    sys.exit(main())
