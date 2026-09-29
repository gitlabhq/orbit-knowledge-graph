import pytest

from prose_lint import check, markdown_units, yaml_units

PROMPT = """\
name: grep
version: 1.0.0
short: Search local definition names
description: >-
  Search local definition names.


  Returns ranked `Definition:<id>` references and — crucially — the source for
  the top three, which underscores why you should leverage it.
nudges:
  read: |
    Reuse the source you already have. Missing code? Run {{orbit}} context.
    Additionally, IMPORTANT: make sure to read everything.

    ```json
    {"leverage": "synergy"}
    ```
    | robust | table |
"""

SKILL = """\
---
name: orbit
description: Query the graph before shell grep. Use it for symbols and callers.
---
# Orbit

Use `orbit grep` first. Then read the returned
source, and edit when it is enough.

- Cite file and line.
- Never truncate Orbit output,
  even when it is long.

```shell
orbit grep "delve"
```

| tool | robust |
|---|---|

> Quoted people may delve as they please.

<!--
Keep this short. It's worth noting that seamless prose is not just nice, but essential.
-->
Areas:
  orbit::query      Query engine, DSL, compiler, pagination, ergonomics
  orbit::dx         CI, tooling, and a seamless contributor flow
"""

RULE_FIRST = """\
---

Leverage the graph. Then stop.
---
name: not frontmatter
"""

QUOTED = """\
name: x
summary: "A quoted scalar that wraps
  onto a second line with robust prose."
"""


def rules(findings):
    return sorted((f.line, f.rule) for f in findings)


def test_yaml_prose_preserves_lines_and_skips_metadata_fences_and_tables():
    units = yaml_units("p.yml", PROMPT)
    assert [u.line for u in units] == [3, 5, 12]
    assert units[1].sentences[1].line == 8
    assert units[2].sentences[-1].line == 13
    assert not {w for s in units[2].sentences for w in s.words} & {"leverage", "synergy", "robust"}
    assert not {s.text for u in units for s in u.sentences} & {"grep", "1.0.0"}
    findings = [f for u in units for f in check(u)]
    assert rules(findings) == [(8, "dash"), (8, "dash"), (8, "tell"), (9, "tell"), (9, "tell"),
                               (13, "prompt"), (13, "prompt"), (13, "tell")]
    assert sorted(f.message.split("'")[1] for f in findings if f.rule == "tell") == [
        "Additionally", "crucially", "leverage", "underscores",
    ]
    assert rules(check(yaml_units("q.yml", QUOTED)[0])) == [(3, "tell")]


def test_markdown_prose_preserves_frontmatter_wrapping_lists_and_comment_findings():
    front, body = markdown_units("s.md", SKILL)
    assert (front.line, len(front.sentences)) == (3, 2)
    texts = {s.text for s in body.sentences}
    assert {"Then read the returned source, and edit when it is enough.", "Cite file and line.",
            "Never truncate Orbit output, even when it is long.",
            "Query engine, DSL, compiler, pagination, ergonomics"} <= texts
    assert "Orbit" not in texts
    assert not {w for s in body.sentences for w in s.words} & {"delve", "robust"}
    assert [f.line for f in check(body) if "seamless" in f.message] == [24, 28]
    assert rules(check(body))[:3] == [(24, "tell")] * 3


@pytest.mark.parametrize("text,count", [
    ("See https://docs.gitlab.com/ee/api.html and v0.113.1 or Definition:a.b today.", 1),
    ('See e.g. the docs, i.e. this file... Then ask "why not?" (See below.) Stop.', 3),
    ("Columns get renamed, dropped, etc. New rows arrive. Compare a vs. b, etc. and stop.", 3),
    ("**Do not mirror it.** Then check the ontology. _Really._ Stop. “Quoted.” Done.", 6),
])
def test_sentence_boundaries(text, count):
    assert len(markdown_units("s.md", text)[0].sentences) == count


@pytest.mark.parametrize("text,expected", [
    (RULE_FIRST, [(3, "tell")]),
    ("\n\n".join([" ".join(["word"] * 26) + "."] * 3), [(1, "average"), (1, "sentence"), (3, "sentence"), (5, "sentence")]),
    ("Short one.\n\n" + "\n\n".join([" ".join(["word"] * 35) + "."] * 2), [(3, "average"), (3, "sentence"), (5, "sentence")]),
    ("CPU utilization rose with elevated privileges; IDs allow an underscore in the realm of names.", []),
    ("This is not just a cache, but a graph, ensuring speed.", [(1, "tell"), (1, "tell")]),
])
def test_prose_rule_findings(text, expected):
    assert rules(check(markdown_units("s.md", text)[0])) == expected


def test_list_items_inline_code_and_summary_boundaries():
    unit, = markdown_units("s.md", "Intro line\n- item one leverages things\n- item two underscores things")
    assert [(s.line, s.text) for s in unit.sentences][1:] == [(2, "item one leverages things"), (3, "item two underscores things")]
    assert rules(check(unit)) == [(2, "tell"), (3, "tell")]
    unit, = markdown_units("s.md", "Run `" + " ".join(["flag"] * 40) + "` now.")
    assert unit.sentences[0].words == ["Run", "code", "now"]
    unit, = markdown_units("s.md", "Use a colon in summary lines. In summary, stop.")
    assert [f.message.split("'")[1] for f in check(unit)] == ["In summary"]


def test_prose_cli_missing_and_out_of_scope_files(repo):
    repo.copy("ci/linting/prose_lint.py")
    assert "no such file" in repo.invoke("linting/prose_lint.py", "does-not-exist.md", status=2)
    assert "no files in scope" in repo.invoke("linting/prose_lint.py", "ci/linting/prose_lint.py")


def test_prose_diff_base_does_not_require_connected_history(repo, monkeypatch):
    repo.copy("ci/linting/prose_lint.py")
    repo.git("init")
    repo.write("AGENTS.md", "Base.\n")
    base = repo.commit()
    repo.git("checkout", "--orphan", "head")
    repo.write("AGENTS.md", "Leverage the graph.\n")
    monkeypatch.setenv("CI_MERGE_REQUEST_SOURCE_BRANCH_SHA", repo.commit())
    assert "AGENTS.md:1: tell:" in repo.invoke("linting/prose_lint.py", "--diff-base", base, status=1)
