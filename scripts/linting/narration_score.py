#!/usr/bin/env python3
"""
Narration-comment scorer for Rust.

Two detectors, both deliberately high-precision (H2):

  1. block_label  - a comment that is a short imperative/section label with no
                    "why" content (e.g. `// Setup`, `// Clear env vars`,
                    `// Arrange`). Matched against a small denylist of opener
                    verbs + a length/shape gate.
  2. token_overlap - a `// what` comment whose alphabetic tokens are (almost)
                    entirely a subset of the tokens on the *next* code line.
                    This catches `// clear env vars` above `env::remove_var(...)`.

A comment flagged by EITHER detector is counted as narration.

Doc comments (///, //!) are never flagged. Comments that contain a "why"
signal (because, so, since, otherwise, must, note, safety, gotcha, http(s)
URLs, issue refs like #123) are exempt from the block_label detector — those
are the legitimate shapes the AGENTS.md rule wants to keep.

Usage:
  narration_score.py FILE.rs          # human report
  narration_score.py --tsv FILE.rs    # tab-separated: file<TAB>count
  cat code | narration_score.py -      # read from stdin
"""
import sys
import re

# Imperative / section-label openers that begin a narration comment.
BLOCK_LABEL_OPENERS = {
    "setup", "set", "cleanup", "clean", "teardown", "arrange", "act",
    "assert", "given", "when", "then", "test", "tests", "testing",
    "create", "creates", "build", "builds", "make", "makes", "insert",
    "inserts", "add", "adds", "verify", "verifies", "check", "checks",
    "clear", "clears", "call", "calls", "run", "runs", "get", "gets",
    "fetch", "fetches", "remove", "removes", "delete", "deletes",
    "initialize", "init", "prepare", "register", "registers", "define",
    "defines", "now", "first", "next", "finally", "start", "starts",
    "return", "returns", "loop", "iterate", "parse", "parses", "load",
    "loads", "save", "saves", "spawn", "spawns", "mock", "stub", "wait",
    "drop", "drops", "configure", "enable", "disable", "apply", "applies",
    "compute", "computes", "collect", "collects", "convert", "converts",
    "extract", "extracts", "validate", "validates", "process", "handle",
    "handles", "open", "opens", "close", "closes", "push", "pop", "send",
    "sends", "receive", "store", "stores", "update", "updates", "find",
    "finds", "filter", "filters", "map", "skip", "skips", "advance",
}

# Tokens that signal a *why* comment (exempt from block_label flagging).
WHY_SIGNALS = {
    "because", "so", "since", "otherwise", "must", "note", "safety",
    "gotcha", "invariant", "intentionally", "deliberately", "avoid",
    "avoids", "prevents", "prevent", "ensures", "ensure", "would",
    "cannot", "can't", "won't", "not", "never", "always", "only",
    "instead", "rather", "due", "workaround", "hack", "fixme", "todo",
    "bug", "edge", "race", "deadlock", "panic", "unsafe", "assumes",
    "assume", "expects", "requires", "needs", "guarantees", "guarantee",
}

WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
URL_RE = re.compile(r"https?://")
ISSUE_RE = re.compile(r"#\d+|![0-9]+|[A-Z]+-\d+")

# words too generic to count toward token-overlap signal
STOP = {"the", "a", "an", "to", "of", "in", "on", "for", "and", "or",
        "is", "are", "be", "with", "this", "that", "it", "as", "at",
        "by", "from", "into", "we", "i", "let", "self", "mut", "fn",
        "if", "else", "return"}


def tokens(s):
    return [t.lower() for t in WORD_RE.findall(s)]


PARENS_RE = re.compile(r"\([^)]+\)")


def has_why(text):
    toks = set(tokens(text))
    if toks & WHY_SIGNALS:
        return True
    if URL_RE.search(text) or ISSUE_RE.search(text):
        return True
    # An inline parenthetical that adds words not present in the head is
    # usually a rationale/definition ("(values outside the SCC)",
    # "(after data load for efficiency)") — treat as why content.
    m = PARENS_RE.search(text)
    if m:
        inner = [t for t in tokens(m.group(0)) if t not in STOP]
        head = set(tokens(text[: m.start()]))
        novel = [t for t in inner if t not in head]
        if len(novel) >= 2:
            return True
    return False


SEP_CHARS = set("─-=*#~ ")


def comment_text(line):
    """Return the text after `//` if this stripped line is a line comment
    that is NOT a doc comment (/// or //!). Else None.

    Section dividers (`// ── Tests ──`, `// --- Setup ---`) are a deliberate
    file-organization convention, not next-line narration, so we strip the
    decoration and only keep the inner label for normal classification; a
    divider whose inner label still trips a detector is treated as prose
    (returns None) to keep precision high."""
    s = line.strip()
    if not s.startswith("//"):
        return None
    if s.startswith("///") or s.startswith("//!"):
        return None
    body = s[2:].strip()
    # Drop pure decoration runs entirely.
    if body and all(c in SEP_CHARS for c in body):
        return None
    # A decorated section divider (leading or trailing run of ─/-/=/* chars)
    # is structural, not narration — exempt it.
    if body and (body[0] in "─-=*#~" or body[-1] in "─-=*#~"):
        stripped = body.strip("─-=*#~ ")
        if stripped != body:        # had decoration on at least one side
            return None
    return body


def next_code_line(lines, idx):
    """First non-blank, non-comment line after idx."""
    for j in range(idx + 1, len(lines)):
        s = lines[j].strip()
        if not s:
            continue
        if s.startswith("//"):
            continue
        return s
    return None


def in_multiline_block(lines, idx):
    """True if this comment line is part of a >=2-line run of // comments.
    Multi-line comment blocks are almost always prose/why, not one-line
    narration labels, so we exempt them to keep precision high."""
    prev = lines[idx - 1].strip() if idx > 0 else ""
    nxt = lines[idx + 1].strip() if idx + 1 < len(lines) else ""

    def is_cmt(s):
        return s.startswith("//") and not s.startswith("///") \
            and not s.startswith("//!")
    return is_cmt(prev) or is_cmt(nxt)


def is_block_label(text):
    """Short imperative section label, no why-content."""
    if has_why(text):
        return False
    toks = tokens(text)
    if not toks:
        return False
    # Pure section labels are short. >7 words is almost always real prose.
    if len(toks) > 7:
        return False
    # Must start with a known imperative/section opener.
    if toks[0] not in BLOCK_LABEL_OPENERS:
        return False
    # A trailing period + long-ish clause usually means a sentence (why).
    # Block labels are fragments; reject if it ends with punctuation AND is wordy.
    return True


def is_token_overlap(text, code):
    """Comment tokens are (almost) a subset of next code line tokens."""
    if code is None:
        return False
    if has_why(text):
        return False
    ctoks = [t for t in tokens(text) if t not in STOP]
    if len(ctoks) < 2:        # need signal; 1-word handled by block_label
        return False
    code_toks = set(tokens(code))
    # fuzzy: also match singular/plural and verb stems by prefix
    def covered(t):
        if t in code_toks:
            return True
        # stem-ish: env -> remove_var? check substring against any code token
        for c in code_toks:
            if len(t) >= 4 and (t in c or c in t):
                return True
        return False
    matched = sum(1 for t in ctoks if covered(t))
    return matched / len(ctoks) >= 0.6


def score(text_lines):
    lines = text_lines
    flags = []  # (lineno, kind, text)
    for i, line in enumerate(lines):
        ct = comment_text(line)
        if ct is None:
            continue
        if ct == "":
            continue
        if in_multiline_block(lines, i):
            continue
        kind = None
        if is_block_label(ct):
            kind = "block_label"
        else:
            code = next_code_line(lines, i)
            if is_token_overlap(ct, code):
                kind = "token_overlap"
        if kind:
            flags.append((i + 1, kind, ct))
    return flags


def main():
    args = sys.argv[1:]
    tsv = False
    if args and args[0] == "--tsv":
        tsv = True
        args = args[1:]
    if not args:
        print("usage: narration_score.py [--tsv] FILE|-", file=sys.stderr)
        sys.exit(2)
    path = args[0]
    if path == "-":
        content = sys.stdin.read()
        name = "<stdin>"
    else:
        with open(path, encoding="utf-8", errors="replace") as f:
            content = f.read()
        name = path
    flags = score(content.splitlines())
    if tsv:
        print(f"{name}\t{len(flags)}")
    else:
        for ln, kind, txt in flags:
            print(f"{name}:{ln}\t{kind}\t// {txt}")
        print(f"# {name}: {len(flags)} narration comment(s)", file=sys.stderr)
    if flags:
        sys.exit(1)


if __name__ == "__main__":
    main()
