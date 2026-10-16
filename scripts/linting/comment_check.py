#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "tree-sitter==0.25.2",
#   "tree-sitter-rust==0.24.2",
#   "tree-sitter-yaml==0.7.2",
#   "tree-sitter-toml==0.7.0",
#   "tree-sitter-bash==0.25.1",
#   "tree-sitter-python==0.25.0",
#   "tree-sitter-javascript==0.25.0",
#   "tree-sitter-typescript==0.23.2",
#   "tree-sitter-ruby==0.23.1",
#   "tree-sitter-css==0.25.0",
# ]
# ///
"""Flag comments added or changed in a diff.

Modes:
  comment_check.py --staged           index vs HEAD (pre-commit)
  comment_check.py --range BASE HEAD  two revisions (CI)

Exit: 0 clean, acknowledged, or skipped; 3 when comments are flagged; 2 on an
internal error in --range mode, including a file skipped on error, so CI does
not report a check that never ran as clean. In --staged mode an internal error exits 0. Failures before this
script starts (uv or mise) are handled by comment_check_hook.sh.

Environment:
  COMMENTS_CHECK=0      skip the check entirely
  COMMENTS_OK=<digest>  acknowledge the flagged comments (the digest is printed
                        with the failure and changes when the comments change)
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import importlib
import os
import re
import subprocess
import sys
from collections import Counter

GRAMMARS = {
    ".rs": ("tree_sitter_rust", "language"),
    ".yaml": ("tree_sitter_yaml", "language"),
    ".yml": ("tree_sitter_yaml", "language"),
    ".toml": ("tree_sitter_toml", "language"),
    ".sh": ("tree_sitter_bash", "language"),
    ".py": ("tree_sitter_python", "language"),
    ".js": ("tree_sitter_javascript", "language"),
    ".mjs": ("tree_sitter_javascript", "language"),
    ".ts": ("tree_sitter_typescript", "language_typescript"),
    ".rb": ("tree_sitter_ruby", "language"),
    ".css": ("tree_sitter_css", "language"),
}
SKIP_GLOBS = [
    "fixtures/*", "*/fixtures*/*", "docs-locale/*", "scripts/vendored/*",
    "*/node_modules/*", "*/testdata/*", "*/snapshots/*",
    "crates/code-graph-incremental/langs/*",
]
DIRECTIVE = re.compile(
    r"^(#!|#\s*(///|yamllint|noqa|type:|pragma|shellcheck|vale|frozen_string_literal|"
    r"rubocop|yaml-language-server|-\*-|:schema|@ts-|eslint|prettier|markdownlint)|"
    r"//\s*(@ts-|eslint|prettier|rustfmt|clippy|nolint)|/\*\s*(eslint|prettier|istanbul))",
    re.I,
)
RUSTDOC = re.compile(r"^(///(?!/)|//!|/\*\*(?![*/])|/\*!)")
GENERATED_MARKER = b"@" + b"generated"  # split so this file does not skip itself
GENERATED_SCAN_BYTES = 2048
LINE_MARKER = re.compile(r"//[/!]?|#")

EXIT_ERROR = 2
EXIT_FLAGGED = 3

RULE = "Comments explain why. Do not narrate what the code shows."
RULE_LINK = "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/CONTRIBUTING.md#engineering-conventions"

_parsers: dict = {}


def run(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def git_show(rev: str | None, path: str) -> bytes | None:
    spec = f":{path}" if rev is None else f"{rev}:{path}"
    proc = subprocess.run(["git", "show", spec], capture_output=True)
    return proc.stdout if proc.returncode == 0 else None


def parser_for(ext: str):
    if ext not in _parsers:
        from tree_sitter import Language, Parser

        module, function = GRAMMARS[ext]
        grammar = getattr(importlib.import_module(module), function)()
        _parsers[ext] = Parser(Language(grammar))
    return _parsers[ext]


def comments(ext: str, source: bytes) -> list[tuple[int, int, str]]:
    """Return (first line, last line, text), 1-based, for every comment node."""
    found = []
    stack = [parser_for(ext).parse(source).root_node]
    while stack:
        node = stack.pop()
        if "comment" not in node.type:
            stack.extend(node.children)
            continue
        # Rust line comments end at column 0 of the next row.
        spans_to_next_row = node.end_point[1] == 0 and node.end_point[0] > node.start_point[0]
        last_row = node.end_point[0] - (1 if spans_to_next_row else 0)
        text = source[node.start_byte:node.end_byte].decode("utf8", "replace").rstrip("\n")
        found.append((node.start_point[0] + 1, last_row + 1, text))
    return sorted(found)


def normalize(text: str) -> str:
    text = re.sub(r"^\s*(//[/!]?|#+|/\*+|\*/|\*)\s?", "", text, flags=re.M)
    return re.sub(r"\s+", " ", text.replace("*/", "")).strip().lower()


def diff_args(base: str | None, head: str | None) -> list[str]:
    return ["--cached"] if head is None else [base, head]


def changed_paths(base: str | None, head: str | None) -> list[tuple[str | None, str]]:
    """Return (old path, new path) per added, modified, copied, or renamed file."""
    out = run("git", "diff", "-M", "--name-status", "-z", "--diff-filter=ACMR", *diff_args(base, head))
    tokens = out.split("\0")
    pairs, i = [], 0
    while i < len(tokens) and tokens[i]:
        status = tokens[i]
        if status[0] in "RC":
            pairs.append((tokens[i + 1], tokens[i + 2]))
            i += 3
        else:
            pairs.append((tokens[i + 1] if status[0] == "M" else None, tokens[i + 1]))
            i += 2
    return pairs


def added_lines(base: str | None, head: str | None, old: str | None, new: str) -> set[int]:
    paths = [new] if old in (None, new) else [old, new]
    out = run("git", "diff", "-U0", "-M", "--no-color", *diff_args(base, head), "--", *paths)
    lines: set[int] = set()
    for match in re.finditer(r"^@@ -\S+ \+(\d+)(?:,(\d+))? @@", out, re.M):
        start = int(match[1])
        count = int(match[2]) if match[2] is not None else 1
        lines.update(range(start, start + count))
    return lines


def inline_metadata_lines(source: bytes) -> set[int]:
    """Lines of PEP 723 blocks, from `# /// <type>` through the closing `# ///`."""
    lines: set[int] = set()
    start = None
    for number, line in enumerate(source.decode("utf8", "replace").splitlines(), 1):
        if start is None:
            if re.fullmatch(r"# /// [A-Za-z0-9-]+", line):
                start = number
        elif line == "# ///":
            lines.update(range(start, number + 1))
            start = None
        elif not re.fullmatch(r"#( .*)?", line):
            start = None
    return lines


def is_ignored_comment(text: str) -> bool:
    text = text.strip()
    return bool(DIRECTIVE.match(text) or RUSTDOC.match(text))


def merge_adjacent(findings: list[tuple[str, int, str]]) -> list[tuple[str, int, str]]:
    """Consecutive line comments in one file form one logical comment."""
    merged: list[tuple[str, int, str]] = []
    for path, line, text in findings:
        marker = LINE_MARKER.match(text)
        if merged:
            prev_path, prev_line, prev_text = merged[-1]
            prev_marker = LINE_MARKER.match(prev_text)
            continues = line == prev_line + prev_text.count("\n") + 1
            if prev_path == path and continues and marker and prev_marker and marker[0] == prev_marker[0]:
                merged[-1] = (path, prev_line, prev_text + "\n" + text)
                continue
        merged.append((path, line, text))
    return merged


def scan_file(base, head, old_path, new_path) -> list[tuple[str, int, str]]:
    ext = os.path.splitext(new_path)[1]
    if ext not in GRAMMARS or any(fnmatch.fnmatch(new_path, glob) for glob in SKIP_GLOBS):
        return []
    new_source = git_show(head, new_path)
    if new_source is None or GENERATED_MARKER in new_source[:GENERATED_SCAN_BYTES]:
        return []
    old_source = git_show(base if head else "HEAD", old_path) if old_path else None
    old_texts = Counter(normalize(text) for _, _, text in comments(ext, old_source)) if old_source else Counter()
    touched = added_lines(base, head, old_path, new_path) - inline_metadata_lines(new_source)
    findings = []
    for first, last, text in comments(ext, new_source):
        if not touched.intersection(range(first, last + 1)) or is_ignored_comment(text):
            continue
        key = normalize(text)
        if old_texts[key] > 0:
            old_texts[key] -= 1
            continue
        findings.append((new_path, first, text.strip()))
    return findings


def scan(base: str | None, head: str | None, skipped: list[str] | None = None) -> list[tuple[str, int, str]]:
    findings = []
    for old_path, new_path in changed_paths(base, head):
        try:
            findings.extend(scan_file(base, head, old_path, new_path))
        except Exception as error:  # a broken grammar or odd file must not block a commit
            print(f"comment check: skipped {new_path}: {error}", file=sys.stderr)
            if skipped is not None:
                skipped.append(new_path)
    return merge_adjacent(findings)


def digest(findings: list[tuple[str, int, str]]) -> str:
    payload = "\n".join(f"{path}\0{normalize(text)}" for path, _, text in findings)
    return hashlib.sha256(payload.encode()).hexdigest()[:8]


def first_content_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip().strip("/#*!"):
            return line.strip()
    return text.strip()


def report(findings: list[tuple[str, int, str]], code: str) -> None:
    print(f"comment check: {len(findings)} added/changed comment(s):\n")
    for path, line, text in findings:
        print(f"  {path}:{line}  {first_content_line(text)[:100]}")
    print(
        f'\nCONTRIBUTING.md, "Engineering conventions": "{RULE}"\n{RULE_LINK}\n'
        "For each comment above: delete it if the code or a better name says the same\n"
        "thing; keep it only for a constraint, invariant, gotcha, or workaround.\n"
        f"If every comment above is essential, re-run with COMMENTS_OK={code}\n"
        "If you delete any, commit again to get a new digest.\n"
        "To skip this check: COMMENTS_CHECK=0, or LEFTHOOK_EXCLUDE=comments for the hook."
    )


def merge_in_progress() -> bool:
    path = run("git", "rev-parse", "--git-path", "MERGE_HEAD").strip()
    return os.path.exists(path)


def main(argv: list[str] | None = None) -> int:
    if os.environ.get("COMMENTS_CHECK") == "0":
        return 0
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--staged", action="store_true")
    group.add_argument("--range", nargs=2, metavar=("BASE", "HEAD"))
    args = parser.parse_args(argv)
    skipped: list[str] = []
    try:
        if args.staged and merge_in_progress():
            return 0
        base, head = (None, None) if args.staged else args.range
        findings = scan(base, head, skipped)
    except Exception as error:
        print(f"comment check: skipped, internal error: {error}", file=sys.stderr)
        return 0 if args.staged else EXIT_ERROR
    if not findings:
        return EXIT_ERROR if skipped and args.range else 0
    code = digest(findings)
    if os.environ.get("COMMENTS_OK") == code:
        print(f"comment check: acknowledged {len(findings)} comment(s) ({code})")
        return 0
    report(findings, code)
    return EXIT_FLAGGED


if __name__ == "__main__":
    sys.exit(main())
