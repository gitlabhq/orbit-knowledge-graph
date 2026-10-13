#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml>=6.0"]
# ///
"""
Layout lint for the YAML test suites.

Prettier sets indentation, quotes, and spacing inside a line. This script adds
one rule that Prettier does not have: a blank line separates two sibling
entries when one of them spans more than one line. Siblings are the top-level
keys and the items of each top-level list, for example the cases of a suite.

Usage:
  yaml_lint.py [--fix] [FILE...]   # no FILE checks every suite file

Exit codes: 0 clean, 1 findings.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SUITES = tuple(
    ROOT / suite
    for suite in (
        "crates/integration-tests/tests",
        "crates/integration-tests-codegraph/fixtures",
        "crates/integration-tests-codegraph/fixtures_incremental",
        "fixtures/queries",
    )
)


def suite_files(paths: list[str]) -> list[Path]:
    if not paths:
        return sorted(p for suite in SUITES for p in suite.rglob("*.y*ml"))
    files = [Path(p).resolve() for p in paths]
    return [f for f in files if f.suffix in (".yaml", ".yml") and any(f.is_relative_to(s) for s in SUITES)]


def run_prettier(files: list[Path], fix: bool) -> bool:
    version = json.loads((ROOT / "package.json").read_text())["devDependencies"]["prettier"]
    mode = "--write" if fix else "--check"
    command = ["npx", "--yes", f"prettier@{version}", mode, "--log-level", "warn", *map(str, files)]
    return subprocess.run(command, cwd=ROOT).returncode == 0


def sibling_groups(root: yaml.Node, line_count: int) -> list[list[int]]:
    if not isinstance(root, yaml.MappingNode):
        return []
    keys = [key.start_mark.line for key, _ in root.value]
    groups = [keys + [line_count]]
    for (_, value), end in zip(root.value, keys[1:] + [line_count]):
        if isinstance(value, yaml.SequenceNode) and not value.flow_style:
            groups.append([item.start_mark.line for item in value.value] + [end])
    return groups


def indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def comment_start(lines: list[str], line: int) -> int:
    depth = indent(lines[line]) if line < len(lines) else 0
    while line > 0 and lines[line - 1].lstrip().startswith("#") and indent(lines[line - 1]) <= depth:
        line -= 1
    return line


def spans_lines(lines: list[str], start: int, end: int) -> bool:
    body = [line.strip() for line in lines[start:end] if line.strip()]
    while body and body[-1].startswith("#"):
        body.pop()
    return len(body) > 1


def missing_gaps(lines: list[str], root: yaml.Node) -> list[int]:
    gaps = []
    for bounds in sibling_groups(root, len(lines)):
        entries = list(zip(bounds, bounds[1:]))
        for previous, (start, end) in zip(entries, entries[1:]):
            anchor = comment_start(lines, start)
            if lines[anchor - 1].strip() and (spans_lines(lines, *previous) or spans_lines(lines, start, end)):
                gaps.append(anchor)
    return sorted(set(gaps))


def check_spacing(path: Path, fix: bool) -> list[str]:
    text = path.read_text()
    lines = text.split("\n")
    gaps = missing_gaps(lines, yaml.compose(text))
    findings = [f"{path.relative_to(ROOT)}:{anchor + 1}: add a blank line above this entry" for anchor in gaps]
    if not fix or not gaps:
        return findings
    for anchor in reversed(gaps):
        lines.insert(anchor, "")
    fixed = "\n".join(lines)
    if yaml.safe_load(fixed) != yaml.safe_load(text):
        return findings
    path.write_text(fixed)
    return []


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fix", action="store_true")
    parser.add_argument("files", nargs="*")
    args = parser.parse_args(argv)
    files = suite_files(args.files)
    if not files:
        return 0
    if not run_prettier(files, args.fix):
        return 1
    findings = [finding for path in files for finding in check_spacing(path, args.fix)]
    if not findings:
        return 0
    hint = "A fix here would change a value. Add these blank lines by hand." if args.fix else "Run `mise run lint:yaml:fix` to fix the layout."
    print(*findings, hint, sep="\n", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
