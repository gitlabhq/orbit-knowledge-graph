#!/usr/bin/env python3
"""
Prompt prose scorer.

Scores the agent-facing text this repo ships: the summary, short, and
description fields of config/prompts/**/*.yml, the instructions, nudges, and
template vars of config/setup/setup.yaml, and the body plus frontmatter
description of skills/**/*.md. Fences, tables, headings, link targets, and
template placeholders are dropped before scoring. Every unit is scored on the
longest sentence, the average sentence length, the Flesch-Kincaid grade, em or
en dashes, and a short denylist of words that mark machine-written prose.

Usage:
  prompt_score.py FILE...          # human report, exit 1 on findings
  prompt_score.py --tsv FILE...    # file<TAB>unit<TAB>words<TAB>avg<TAB>grade<TAB>verdict
  prompt_score.py --self-test
"""
import re
import sys

MAX_SENTENCE_WORDS = 25
MAX_AVG_WORDS = 20.0
MAX_GRADE = 10.0
TELL_WORDS = {
    "delve", "leverage", "robust", "seamless", "seamlessly", "showcase",
    "crucial", "pivotal", "tapestry", "testament", "underscore", "underscores",
    "streamline", "comprehensive", "vibrant", "landscape", "foster", "elevate",
}
DASHES = "—–"
SKIP_KEYS = {"name", "version", "variables", "license", "metadata"}
WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9'_-]*")
PLACEHOLDER_RE = re.compile(r"\{\{.*?\}\}")
INLINE_CODE_RE = re.compile(r"`[^`\n]*`")
LINK_TARGET_RE = re.compile(r"\]\([^)]*\)")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def yaml_units(text):
    units = {}
    key = None
    style = None
    buf = []
    nested = False

    def flush():
        if key and key not in SKIP_KEYS and buf:
            joiner = "\n" if style and style.startswith("|") else " "
            value = joiner.join(line.strip() for line in buf if line.strip()) if joiner == " " else "\n".join(buf)
            units[key] = value.strip().strip('"').strip("'")

    for line in text.splitlines():
        top = re.match(r"^([A-Za-z_]+):\s*(.*)$", line)
        sub = re.match(r"^\s+([A-Za-z_]+):\s*(.+)$", line)
        if top:
            flush()
            key, value = top.group(1), top.group(2).strip()
            style = value if value in ("|", "|-", ">", ">-") else None
            buf = [] if style or not value else [value]
            nested = not value and not style
        elif key and nested and sub:
            name, value = sub.group(1), sub.group(2).strip()
            units[f"{key}.{name}"] = value.strip('"').strip("'")
        elif key and style and (line.startswith("  ") or not line.strip()):
            buf.append(line[2:] if line.startswith("  ") else "")
    flush()
    return units


def markdown_units(text):
    units = {}
    body = text
    if body.startswith("---\n"):
        end = body.find("\n---\n", 4)
        if end != -1:
            front = body[4:end]
            body = body[end + 5:]
            desc = yaml_units(front).get("description")
            if desc:
                units["frontmatter.description"] = desc
    body = re.sub(r"^```.*?^```\s*$", "", body, flags=re.S | re.M)
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S)
    kept = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("|", "#", "---")):
            continue
        kept.append(re.sub(r"^\s*(?:[-*]|\d+\.)\s+", "", line))
    units["body"] = "\n".join(kept)
    return units


def clean(text):
    text = PLACEHOLDER_RE.sub("", text)
    text = LINK_TARGET_RE.sub("]", text)
    text = INLINE_CODE_RE.sub("code", text)
    return text


def sentences(text):
    out = []
    for line in clean(text).splitlines():
        for sentence in SENTENCE_SPLIT_RE.split(line.strip()):
            words = WORD_RE.findall(sentence)
            if words:
                out.append(words)
    return out


def syllables(word):
    word = word.lower().strip("'-_")
    groups = re.findall(r"[aeiouy]+", word)
    count = len(groups)
    if word.endswith("e") and not word.endswith(("le", "ee")) and count > 1:
        count -= 1
    return max(count, 1)


def score_unit(text):
    sents = sentences(text)
    words = [w for s in sents for w in s]
    if not sents:
        return 0, 0.0, 0.0, []
    total = len(words)
    avg = total / len(sents)
    grade = 0.39 * avg + 11.8 * (sum(syllables(w) for w in words) / total) - 15.59
    longest = max(len(s) for s in sents)
    fails = []
    if longest > MAX_SENTENCE_WORDS:
        fails.append(f"sentence {longest}w>{MAX_SENTENCE_WORDS}")
    if len(sents) > 1 and avg > MAX_AVG_WORDS:
        fails.append(f"avg {avg:.1f}>{MAX_AVG_WORDS:g}")
    if total >= 20 and grade > MAX_GRADE:
        fails.append(f"grade {grade:.1f}>{MAX_GRADE:g}")
    dashes = sum(text.count(d) for d in DASHES)
    if dashes:
        fails.append(f"dashes {dashes}")
    tells = sorted({w.lower() for w in words if w.lower() in TELL_WORDS})
    if tells:
        fails.append("tells " + ",".join(tells))
    return total, avg, grade, fails


def score_file(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read()
    units = markdown_units(text) if path.endswith(".md") else yaml_units(text)
    return [(unit, *score_unit(value)) for unit, value in units.items() if value.strip()]


def self_test():
    bad = "We delve into the graph — and this sentence keeps going and going without any stop so that it runs far past the twenty five word limit that the gate enforces here."
    good = "Query the graph before shell grep. Use one to three identifier keywords. Cite file and line."
    assert score_unit(bad)[3], "bad sample must fail"
    assert not score_unit(good)[3], f"good sample must pass: {score_unit(good)[3]}"
    assert yaml_units("name: x\nversion: 1.0.0\nshort: Short line\ndescription: >-\n  Folded\n  text.\n") == {"short": "Short line", "description": "Folded text."}
    assert markdown_units("---\nname: s\ndescription: Front desc.\n---\n# H\n\n- item one.\n\n```\ncode\n```\n| t |\n")["frontmatter.description"] == "Front desc."
    print("self-test ok")


def main():
    args = sys.argv[1:]
    if args == ["--self-test"]:
        self_test()
        return
    tsv = False
    if args and args[0] == "--tsv":
        tsv = True
        args = args[1:]
    if not args:
        print("usage: prompt_score.py [--tsv] FILE... | --self-test", file=sys.stderr)
        sys.exit(2)
    failed = 0
    for path in args:
        for unit, words, avg, grade, fails in score_file(path):
            verdict = "FAIL " + "; ".join(fails) if fails else "PASS"
            if tsv:
                print(f"{path}\t{unit}\t{words}\t{avg:.1f}\t{grade:.1f}\t{verdict}")
            elif fails:
                print(f"{path}\t{unit}\twords={words} avg={avg:.1f} grade={grade:.1f}\t{verdict}")
            failed += bool(fails)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
