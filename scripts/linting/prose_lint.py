#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml>=6.0"]
# ///
"""
Prose lint for the text this repo ships to LLMs and reviewers.

Scores prompt yaml, the setup block, skills, agent guides, the glossary, and
the MR and issue templates. Every finding names a file and line.

Usage:
  prose_lint.py FILE...             # only files inside the lint scope are scored
  prose_lint.py --all               # the whole tree
  prose_lint.py --diff-base <sha>   # files changed between <sha> and the MR head

Exit codes: 0 clean, 1 findings, 2 the lint could not run.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path

import yaml

SCOPE = (
    "AGENTS.md",
    "CONTEXT.md",
    "crates/*/AGENTS.md",
    "docs/dev/agents-*.md",
    ".gitlab/merge_request_templates/*.md",
    ".gitlab/issue_templates/*.md",
    "skills/**/*.md",
    "config/prompts/**/*.yml",
    "config/setup/setup.yaml",
)
GENERATED = {"skills/orbit/references/query_language.md"}

MAX_SENTENCE_WORDS = 25
MAX_AVERAGE_WORDS = 20.0
MIN_SENTENCES_FOR_AVERAGE = 3

SKIP_KEYS = {"name", "version", "variables", "license", "metadata", "allowed-tools", "compatibility"}

TELL_WORDS = re.compile(
    r"\b(?:"
    r"delv\w*|underscor\w*|showcas\w*|tapestr\w*|testament|pivotal|crucial\w*|meticulous\w*"
    r"|intrica\w*|realm\w*|multifaceted|myriad|plethora|elucidat\w*|embark\w*|garner\w*"
    r"|bolster\w*|synerg\w*|holistic|leverag\w*|seamless\w*|robust\w*|comprehensive\w*"
    r"|foster\w*|elevat\w*|utiliz\w*|facilitat\w*|streamlin\w*|landscape|vibrant|ecosystem"
    r"|empower\w*|unleash\w*|cutting-edge|game-chang\w*|groundbreaking|transformative"
    r"|paradigm|ever-evolving|nuanced|commendable|noteworthy|invaluable|versatile"
    r"|unparalleled|unprecedented|revolutioni\w*|innovative|actionable|supercharg\w*"
    r"|state-of-the-art|best-in-class|additionally|moreover|furthermore"
    r")\b",
    re.I,
)
TELL_PHRASES = re.compile(
    r"\b(?:"
    r"it(?:'| i)s (?:worth noting|important to note)|plays? an? (?:vital|crucial|pivotal|key|critical) role"
    r"|(?:stands|serves) as an?|in today'?s|let'?s (?:dive|unpack|explore|break (?:this|it) down)"
    r"|in (?:conclusion|summary)(?=[,.:!]|$)|not (?:just|only|merely)\b[^.?!\n]{1,80}\bbut(?: also)?"
    r"|i hope this helps|you'?re absolutely right|great question|certainly!|of course!|i'?d be happy to"
    r"|as of my (?:last|latest) (?:knowledge|training)|at its core|here'?s the (?:thing|kicker)"
    r"|when it comes to|at the end of the day|low-hanging fruit|move the needle|load-bearing"
    r")\b"
    r"|,\s+(?:highlighting|underscoring|showcasing|ensuring|fostering|reflecting|emphasizing|demonstrating)\b",
    re.I,
)
SHOUTING = re.compile(r"\b(?:IMPORTANT|CRITICAL|WARNING|NOTE|MUST|NEVER|ALWAYS|DO NOT)\b")
FILLER = re.compile(
    r"\b(?:in order to|make sure (?:to|that)|be sure to|note that|keep in mind|feel free to|please"
    r"|needless to say|basically|you are an? (?:helpful|expert|senior)|as an ai)\b",
    re.I,
)
DASHES = re.compile("[—–]")

WORD = re.compile(r"[A-Za-z0-9][A-Za-z0-9'_-]*")
SENTENCE = re.compile(r"\S.*?(?:[.!?]+[\"')\]*_]*(?=\s|$)|$)", re.S)
PARAGRAPH = re.compile(r"[^\n]+(?:\n[^\n]+)*")
INLINE_CODE = re.compile(r"`[^`\n]*`")
PLACEHOLDER = re.compile(r"\{\{.*?\}\}")
LINK_TARGET = re.compile(r"\]\([^)]*\)")
ABBREVIATION = re.compile(r"\b(e\.g|i\.e|vs|etc)\.", re.I)
LIST_MARKER = re.compile(r"^\s*(?:[-*+]|\d+\.)\s+")
SKIPPED_LINE = re.compile(r"^\s*(?:#|\||---|<!--\s*$|-->\s*$)")
ALIGNED_COLUMNS = re.compile(r"(?<=\S) {3,}(?=\S)")
FRONTMATTER_KEY = re.compile(r"^[\w-]+:")
COMMENT_MARKER = re.compile(r"<!--|-->")


class LintError(Exception):
    pass


@dataclass
class LineMap:
    first: int
    starts: list[int]

    def line_of(self, offset: int) -> int:
        return self.first + bisect_right(self.starts, offset) - 1


@dataclass
class Sentence:
    text: str
    words: list[str]
    offset: int
    lines: LineMap

    @property
    def line(self) -> int:
        return self.lines.line_of(self.offset)

    def line_at(self, position: int) -> int:
        return self.lines.line_of(self.offset + position)


@dataclass
class Unit:
    path: str
    line: int
    sentences: list[Sentence]

    @property
    def words(self) -> int:
        return sum(len(s.words) for s in self.sentences)


@dataclass
class Finding:
    path: str
    line: int
    rule: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.rule}: {self.message}"


def clean(line: str) -> str:
    line = COMMENT_MARKER.sub("", line).replace("...", "\u2026")
    line = PLACEHOLDER.sub("", line)
    line = LINK_TARGET.sub("]", line)
    line = INLINE_CODE.sub("code", line)
    return ABBREVIATION.sub(lambda m: m.group(1).replace(".", ""), line)


def sentences(lines: list[str], first_line: int) -> list[Sentence]:
    starts, pos = [], 0
    for line in lines:
        starts.append(pos)
        pos += len(line) + 1
    line_map = LineMap(first_line, starts)
    text = "\n".join(lines)
    out = []
    for paragraph in PARAGRAPH.finditer(text):
        flat = paragraph.group().replace("\n", " ")
        for match in SENTENCE.finditer(flat):
            words = WORD.findall(match.group())
            if words:
                out.append(Sentence(match.group().strip(), words, paragraph.start() + match.start(), line_map))
    return out


def prose_lines(lines: list[str]) -> list[str]:
    cleaned, in_fence = [], False
    for line in lines:
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            cleaned.append("")
        elif in_fence or SKIPPED_LINE.match(line):
            cleaned.append("")
        else:
            text = clean(line).strip()
            if ALIGNED_COLUMNS.search(text):
                text = "\n" + ALIGNED_COLUMNS.sub(". ", text) + "\n"
            elif LIST_MARKER.match(line):
                text = "\n" + LIST_MARKER.sub("", text)
            cleaned.append(text)
    return cleaned


def markdown_units(path: str, text: str) -> list[Unit]:
    lines = text.splitlines()
    body_start = 0
    units = []
    if len(lines) > 2 and lines[0] == "---" and FRONTMATTER_KEY.match(lines[1]) and "---" in lines[1:]:
        body_start = lines.index("---", 1) + 1
        units += yaml_units(path, "\n".join(lines[1 : body_start - 1]), line_offset=1)
    units.append(Unit(path, body_start + 1, sentences(prose_lines(lines[body_start:]), body_start + 1)))
    return units


def yaml_units(path: str, text: str, line_offset: int = 0) -> list[Unit]:
    lines = text.splitlines()
    units = []

    def block_lines(start: int, indent: int) -> list[str]:
        raw = []
        for line in lines[start:]:
            if line.strip() and len(line) - len(line.lstrip()) <= indent:
                break
            raw.append(line.strip())
        return prose_lines(raw)

    def scalar_lines(node: yaml.ScalarNode) -> list[str]:
        last = node.end_mark.line if node.end_mark.column else node.end_mark.line - 1
        span = lines[node.start_mark.line : last + 1]
        span[0] = span[0][node.start_mark.column :]
        return [clean(line.strip().strip("\"'")) for line in span]

    def walk(node: yaml.Node, key: str | None, indent: int) -> None:
        if isinstance(node, yaml.MappingNode):
            for key_node, value in node.value:
                walk(value, key_node.value, key_node.start_mark.column)
        elif isinstance(node, yaml.SequenceNode):
            for item in node.value:
                walk(item, key, indent)
        elif isinstance(node, yaml.ScalarNode) and key not in SKIP_KEYS and node.tag.endswith(":str"):
            first = node.start_mark.line + 1
            if node.style in ("|", ">"):
                raw = block_lines(first, indent)
                first += 1
            else:
                raw = scalar_lines(node)
            units.append(Unit(path, first + line_offset, sentences(raw, first + line_offset)))

    try:
        root = yaml.compose(text)
    except yaml.YAMLError as exc:
        raise LintError(f"{path}: {exc}") from exc
    if root is not None:
        walk(root, None, 0)
    return [u for u in units if u.words]


def units_for(path: str) -> list[Unit]:
    text = Path(path).read_text(encoding="utf-8")
    return markdown_units(path, text) if path.endswith(".md") else yaml_units(path, text)


def check(unit: Unit) -> list[Finding]:
    found = []

    def add(line: int, rule: str, message: str) -> None:
        found.append(Finding(unit.path, line, rule, message))

    for s in unit.sentences:
        if len(s.words) > MAX_SENTENCE_WORDS:
            add(s.line, "sentence", f"{len(s.words)} words (max {MAX_SENTENCE_WORDS})")
        for m in DASHES.finditer(s.text):
            add(s.line_at(m.start()), "dash", "em or en dash; use a comma, colon, or a new sentence")
        for m in TELL_WORDS.finditer(s.text):
            add(s.line_at(m.start()), "tell", f"'{m.group()}' marks machine-written prose; use a plain word")
        for m in TELL_PHRASES.finditer(s.text):
            add(s.line_at(m.start()), "tell", f"'{m.group().strip(', ')}' is a machine-writing phrase; cut or restate it")
        for m in SHOUTING.finditer(s.text):
            add(s.line_at(m.start()), "prompt", f"'{m.group()}' shouts; state the rule and its reason in plain case")
        for m in FILLER.finditer(s.text):
            add(s.line_at(m.start()), "prompt", f"'{m.group()}' adds no instruction; cut it")
    if len(unit.sentences) >= MIN_SENTENCES_FOR_AVERAGE:
        average = unit.words / len(unit.sentences)
        if average > MAX_AVERAGE_WORDS:
            longest = max(unit.sentences, key=lambda s: len(s.words))
            add(longest.line, "average", f"{average:.1f} words per sentence (max {MAX_AVERAGE_WORDS:g})")
    return found


def scoped_files() -> list[str]:
    files = {str(p) for pattern in SCOPE for p in Path().glob(pattern) if p.is_file()}
    return sorted(files - GENERATED)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def changed_files(base: str) -> list[str]:
    head = os.environ.get("CI_MERGE_REQUEST_SOURCE_BRANCH_SHA", "HEAD")
    if subprocess.run(["git", "cat-file", "-e", f"{base}^{{commit}}"], capture_output=True).returncode:
        subprocess.run(["git", "fetch", "origin", base, "--depth=1"], capture_output=True)
    try:
        changed = set(git("diff", "--name-only", "--diff-filter=d", f"{base}...{head}").split())
    except subprocess.CalledProcessError as exc:
        raise LintError(f"diff base {base} is unreachable; the lint did not run: {exc.stderr.strip()}") from exc
    return [f for f in scoped_files() if f in changed]


def main(argv: list[str]) -> int:
    if argv == ["--all"]:
        files = scoped_files()
    elif argv[:1] == ["--diff-base"] and len(argv) == 2:
        files = changed_files(argv[1])
    elif argv and not argv[0].startswith("--"):
        missing = [f for f in argv if not Path(f).exists()]
        if missing:
            raise LintError(f"no such file: {', '.join(missing)}")
        in_scope = set(scoped_files())
        files = [f for f in argv if f in in_scope]
    else:
        print(__doc__, file=sys.stderr)
        return 2
    if not files:
        print("prose lint: no files in scope.")
        return 0
    findings, errors = [], []
    for path in files:
        try:
            findings += [f for unit in units_for(path) for f in check(unit)]
        except (LintError, OSError) as exc:
            errors.append(str(exc))
    for finding in sorted(findings, key=lambda f: (f.path, f.line, f.rule)):
        print(finding)
    for error in errors:
        print(f"prose lint: error: {error}", file=sys.stderr)
    if errors:
        return 2
    if findings:
        print(f"\nprose lint: {len(findings)} finding(s) in {len({f.path for f in findings})} of {len(files)} file(s).")
        print("Rewrite the sentence; do not widen the gate. Rules: scripts/linting/README.md")
        return 1
    print(f"prose lint: {len(files)} file(s) pass.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except LintError as exc:
        print(f"prose lint: error: {exc}", file=sys.stderr)
        sys.exit(2)
