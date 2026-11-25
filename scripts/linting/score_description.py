#!/usr/bin/env python3
import sys
import re

WORD_CAP = 100
CODE_SPAN_CAP = 3
BARE_IDENT_CAP = 3

ACRONYM_ALLOW = {
    "URL", "URI", "API", "CI", "CD", "MR", "MRs", "SQL", "DDL", "DSL", "HTTP",
    "HTTPS", "GRPC", "JSON", "YAML", "TOML", "CDC", "NATS", "K8S", "JWT",
    "SLO", "SLA", "ADR", "ID", "IDs", "UUID", "TTL", "GC", "OK", "TODO",
    "ETL", "RAW", "GOON", "SOX", "EE", "CE", "AST", "CPU", "RAM", "IO",
    "OS", "PR", "ClickHouse", "DuckDB", "GitLab", "PostgreSQL", "Rust",
    "Docker", "Kubernetes", "Rails", "Siphon", "Snowplow", "Iglu", "Arrow",
    "GitLab-org", "Orbit", "GKG",
}

SNAKE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")
PATHY = re.compile(r"\b\w+(?:::\w+)+\b")
CAMEL = re.compile(r"\b[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+\b")
DOTPATH = re.compile(r"\b\w+(?:\.\w+){2,}\b")

INLINE_CODE = re.compile(r"`[^`]+`")
ISSUE_REF = re.compile(r"(Closes|Close|Fixes|Fix|Relates to|Related to|See)\s+[!#][0-9]+",
                       re.IGNORECASE)
BARE_REF = re.compile(r"[!#][0-9]+")


def extract_headline(md):
    md = re.sub(r"<!--.*?-->", "", md, flags=re.DOTALL)
    cut = re.search(r"<details", md)
    if cut:
        md = md[: cut.start()]

    m = re.search(r"#+\s*What does this MR do.*?\n", md, re.IGNORECASE)
    if m:
        rest = md[m.end():]
        nxt = re.search(r"\n#+\s", rest)
        body = rest[: nxt.start()] if nxt else rest
        return body
    nxt = re.search(r"\n#+\s", md)
    return md[: nxt.start()] if nxt else md


def strip_code_and_quotes(text):
    text = INLINE_CODE.sub(" ", text)
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    return text


def count_words(text):
    t = strip_code_and_quotes(text)
    t = re.sub(r"\(https?://[^)]+\)", " ", t)
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"/(label|assign|request_review)\b.*", " ", t)
    words = re.findall(r"[A-Za-z0-9_]+(?:[-'][A-Za-z0-9_]+)*", t)
    return len(words)


def count_code_spans(text):
    return len(INLINE_CODE.findall(text))


def is_bare_ident(tok):
    if tok in ACRONYM_ALLOW:
        return False
    return True


def count_bare_idents(text, version=2):
    t = strip_code_and_quotes(text)
    cands = []
    for rx in (SNAKE, PATHY, CAMEL, DOTPATH):
        cands += rx.findall(t)
    if version == 1:
        cands += BARE_REF.findall(t)
        return len(cands)
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
