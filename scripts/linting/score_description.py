#!/usr/bin/env python3
"""
MR-description headline-section scorer (the gate spec, reconstructed from
issue #2933 / prior art MR !1831).

Rule: the headline section ("What does this MR do and why?") must be a terse,
reviewer-facing summary. Limits:
  * word_cap        : <= 100 words
  * code_span_cap   : <= 3 inline `code` spans
  * bare_idents     : <= BARE_IDENT_CAP "bare identifier" mentions (snake_case
                      / CamelCase / path::like tokens dumped into prose), a
                      proxy for "implementation mechanics in the headline"

PASS only if all three are within limits. The Agent context <details> block and
every section after the headline are EXEMPT (long-form goes there by design).

The `bare_idents` regex is the known over-flagger: the v1 form mis-counts
"URL", "API", "CI", `Closes #12`, MR refs `!123`, and ordinary capitalized
words ("ClickHouse", "GitLab"). v2 (this file) tightens it — see is_bare_ident.

Usage:
  score_description.py FILE.md            # human verdict
  score_description.py --tsv FILE.md      # file<TAB>verdict<TAB>words<TAB>spans<TAB>bare
"""
import sys
import re

# Research-tuned thresholds (task #2933): the span cap (<=3) is the robust
# signal — bloated headlines dump 7-16 inline-code spans, good ones have 0-3.
# The word cap is relaxed to <=100 (from <=80) to clear the measured 81-100
# false-positive band of good-but-slightly-long headlines.
WORD_CAP = 100
CODE_SPAN_CAP = 3
BARE_IDENT_CAP = 3

# Common all-caps acronyms / proper nouns that are NOT implementation-mechanic
# bare identifiers — these were the v1 false-positive sources.
ACRONYM_ALLOW = {
    "URL", "URI", "API", "CI", "CD", "MR", "MRs", "SQL", "DDL", "DSL", "HTTP",
    "HTTPS", "GRPC", "JSON", "YAML", "TOML", "CDC", "NATS", "K8S", "JWT",
    "SLO", "SLA", "ADR", "ID", "IDs", "UUID", "TTL", "GC", "OK", "TODO",
    "ETL", "RAW", "GOON", "SOX", "EE", "CE", "AST", "CPU", "RAM", "IO",
    "OS", "PR", "ClickHouse", "DuckDB", "GitLab", "PostgreSQL", "Rust",
    "Docker", "Kubernetes", "Rails", "Siphon", "Snowplow", "Iglu", "Arrow",
    "GitLab-org", "Orbit", "GKG",
}

# Tokens that look like code identifiers when they leak into prose.
SNAKE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")          # foo_bar_baz
PATHY = re.compile(r"\b\w+(?:::\w+)+\b")                           # a::b::c
CAMEL = re.compile(r"\b[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+\b")      # FooBar
DOTPATH = re.compile(r"\b\w+(?:\.\w+){2,}\b")                      # a.b.c.d (>=3 segs)

INLINE_CODE = re.compile(r"`[^`]+`")
ISSUE_REF = re.compile(r"(Closes|Close|Fixes|Fix|Relates to|Related to|See)\s+[!#][0-9]+",
                       re.IGNORECASE)
BARE_REF = re.compile(r"[!#][0-9]+")


def extract_headline(md):
    """Return the body of the 'What does this MR do and why?' section, or, if
    the template heading is absent, everything before the first `### ` heading /
    the Agent context <details> block (whichever comes first)."""
    # Strip HTML comments (template guidance) — they are not author content.
    md = re.sub(r"<!--.*?-->", "", md, flags=re.DOTALL)
    # Cut at the Agent context details block — everything after is exempt.
    cut = re.search(r"<details", md)
    if cut:
        md = md[: cut.start()]

    m = re.search(r"#+\s*What does this MR do.*?\n", md, re.IGNORECASE)
    if m:
        rest = md[m.end():]
        nxt = re.search(r"\n#+\s", rest)
        body = rest[: nxt.start()] if nxt else rest
        return body
    # No template heading: take text up to the first sub-heading.
    nxt = re.search(r"\n#+\s", md)
    return md[: nxt.start()] if nxt else md


def strip_code_and_quotes(text):
    text = INLINE_CODE.sub(" ", text)
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    return text


def count_words(text):
    t = strip_code_and_quotes(text)
    # Drop markdown link URLs and bare URLs from the word count head sense?
    # Keep link *text*, drop the (url).
    t = re.sub(r"\(https?://[^)]+\)", " ", t)
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"/(label|assign|request_review)\b.*", " ", t)  # quick actions
    words = re.findall(r"[A-Za-z0-9_]+(?:[-'][A-Za-z0-9_]+)*", t)
    return len(words)


def count_code_spans(text):
    return len(INLINE_CODE.findall(text))


def is_bare_ident(tok):
    if tok in ACRONYM_ALLOW:
        return False
    return True


def count_bare_idents(text, version=2):
    """v1: raw regex over snake/camel/path tokens (over-flags acronyms/refs).
    v2: same, minus ACRONYM_ALLOW and minus issue/MR refs."""
    # Bare idents only make sense in PROSE, not inside inline code, so strip code.
    t = strip_code_and_quotes(text)
    cands = []
    for rx in (SNAKE, PATHY, CAMEL, DOTPATH):
        cands += rx.findall(t)
    if version == 1:
        # v1 also (wrongly) counted issue refs and acronyms via a looser token
        # rule; emulate by counting refs too.
        cands += BARE_REF.findall(t)
        return len(cands)
    # v2: filter
    # Remove issue/MR refs entirely (they are required by the template).
    t2 = ISSUE_REF.sub(" ", t)
    t2 = BARE_REF.sub(" ", t2)
    cands = []
    for rx in (SNAKE, PATHY, CAMEL, DOTPATH):
        cands += rx.findall(t2)
    cands = [c for c in cands if is_bare_ident(c)]
    return len(cands)


def score(md, bare_version=2):
    head = extract_headline(md)
    words = count_words(head)
    spans = count_code_spans(head)
    bare = count_bare_idents(head, version=bare_version)
    fails = []
    if words > WORD_CAP:
        fails.append(f"words {words}>{WORD_CAP}")
    if spans > CODE_SPAN_CAP:
        fails.append(f"spans {spans}>{CODE_SPAN_CAP}")
    if bare > BARE_IDENT_CAP:
        fails.append(f"bare_idents {bare}>{BARE_IDENT_CAP}")
    verdict = "PASS" if not fails else "FAIL"
    return verdict, words, spans, bare, fails


def main():
    args = sys.argv[1:]
    tsv = False
    bver = 2
    while args and args[0].startswith("--"):
        if args[0] == "--tsv":
            tsv = True
        elif args[0] == "--bare-v1":
            bver = 1
        args = args[1:]
    if not args:
        print("usage: score_description.py [--tsv] [--bare-v1] FILE", file=sys.stderr)
        sys.exit(2)
    path = args[0]
    with open(path, encoding="utf-8", errors="replace") as f:
        md = f.read()
    if not md.strip():
        # Empty description: treat as PASS (no headline to bloat) but note it.
        if tsv:
            print(f"{path}\tEMPTY\t0\t0\t0")
        else:
            print(f"{path}: EMPTY description")
        return
    verdict, words, spans, bare, fails = score(md, bare_version=bver)
    if tsv:
        print(f"{path}\t{verdict}\t{words}\t{spans}\t{bare}")
    else:
        print(f"{path}: {verdict}  words={words} spans={spans} bare_idents={bare}")
        if fails:
            print("  fails:", "; ".join(fails))
    if verdict == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    main()
