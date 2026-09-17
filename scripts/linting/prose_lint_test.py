#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml>=6.0"]
# ///
import unittest

from prose_lint import LintError, check, main, markdown_units, yaml_units

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


class YamlUnits(unittest.TestCase):
    def test_scores_every_prose_scalar_with_its_line(self):
        units = yaml_units("p.yml", PROMPT)
        self.assertEqual([u.line for u in units], [3, 5, 12])
        self.assertEqual(units[1].sentences[1].line, 8)
        self.assertEqual(units[2].sentences[-1].line, 13)

    def test_quoted_scalar_keeps_its_lines(self):
        (unit,) = yaml_units("q.yml", QUOTED)
        self.assertEqual([(f.line, f.rule) for f in check(unit)], [(3, "tell")])

    def test_block_scalars_skip_fences_and_tables(self):
        words = {w for s in yaml_units("p.yml", PROMPT)[2].sentences for w in s.words}
        self.assertFalse(words & {"leverage", "synergy", "robust"})

    def test_skips_name_and_version(self):
        texts = [s.text for u in yaml_units("p.yml", PROMPT) for s in u.sentences]
        self.assertNotIn("grep", texts)
        self.assertNotIn("1.0.0", texts)

    def test_findings(self):
        findings = [f for u in yaml_units("p.yml", PROMPT) for f in check(u)]
        self.assertEqual(
            rules(findings),
            [(8, "dash"), (8, "dash"), (8, "tell"), (9, "tell"), (9, "tell"), (13, "prompt"), (13, "prompt"), (13, "tell")],
        )
        self.assertEqual(
            sorted(f.message.split("'")[1] for f in findings if f.rule == "tell"),
            ["Additionally", "crucially", "leverage", "underscores"],
        )


class MarkdownUnits(unittest.TestCase):
    def setUp(self):
        self.front, self.body = markdown_units("s.md", SKILL)

    def test_frontmatter_description_is_a_unit(self):
        self.assertEqual(self.front.line, 3)
        self.assertEqual(len(self.front.sentences), 2)

    def test_body_joins_wrapped_lines_and_splits_list_items(self):
        texts = [s.text for s in self.body.sentences]
        self.assertIn("Then read the returned source, and edit when it is enough.", texts)
        self.assertIn("Cite file and line.", texts)
        self.assertIn("Never truncate Orbit output, even when it is long.", texts)

    def test_skips_fences_tables_headings_and_blockquotes(self):
        words = {w for s in self.body.sentences for w in s.words}
        self.assertNotIn("delve", words)
        self.assertNotIn("robust", words)
        self.assertNotIn("delve", words)
        self.assertNotIn("Orbit", {s.text for s in self.body.sentences})

    def test_aligned_columns_split_into_short_sentences(self):
        texts = [s.text for s in self.body.sentences]
        self.assertIn("Query engine, DSL, compiler, pagination, ergonomics", texts)
        self.assertEqual([f.line for f in check(self.body) if "seamless" in f.message], [24, 28])

    def test_leading_rule_is_not_frontmatter(self):
        (unit,) = markdown_units("s.md", RULE_FIRST)
        self.assertEqual(rules(check(unit)), [(3, "tell")])

    def test_comment_prose_is_scored(self):
        self.assertEqual(rules(check(self.body))[:3], [(24, "tell"), (24, "tell"), (24, "tell")])


class Cli(unittest.TestCase):
    def test_missing_file_is_an_error(self):
        with self.assertRaises(LintError):
            main(["does-not-exist.md"])

    def test_out_of_scope_file_is_skipped(self):
        self.assertEqual(main([__file__]), 0)


class SentenceRules(unittest.TestCase):
    def test_urls_decimals_and_references_do_not_split(self):
        (unit,) = markdown_units("s.md", "See https://docs.gitlab.com/ee/api.html and v0.113.1 or Definition:a.b today.")
        self.assertEqual(len(unit.sentences), 1)

    def test_unpunctuated_list_items_stay_separate(self):
        (unit,) = markdown_units("s.md", "Intro line\n- item one leverages things\n- item two underscores things")
        self.assertEqual([(s.line, s.text) for s in unit.sentences][1:], [(2, "item one leverages things"), (3, "item two underscores things")])
        self.assertEqual(rules(check(unit)), [(2, "tell"), (3, "tell")])

    def test_inline_code_counts_as_one_word(self):
        long_code = "Run `" + " ".join(["flag"] * 40) + "` now."
        (unit,) = markdown_units("s.md", long_code)
        self.assertEqual(unit.sentences[0].words, ["Run", "code", "now"])

    def test_sentence_over_limit_and_average(self):
        long = " ".join(["word"] * 26) + "."
        (unit,) = markdown_units("s.md", "\n\n".join([long] * 3))
        self.assertEqual(rules(check(unit)), [(1, "average"), (1, "sentence"), (3, "sentence"), (5, "sentence")])
        longer = " ".join(["word"] * 35) + "."
        (unit,) = markdown_units("s.md", "Short one.\n\n" + longer + "\n\n" + longer)
        self.assertEqual(rules(check(unit)), [(3, "average"), (3, "sentence"), (5, "sentence")])

    def test_abbreviations_and_ellipses_hold_while_closing_quotes_split(self):
        (unit,) = markdown_units("s.md", 'See e.g. the docs, i.e. this file... Then ask "why not?" (See below.) Stop.')
        self.assertEqual(len(unit.sentences), 3)
        (unit,) = markdown_units("s.md", "Columns get renamed, dropped, etc. New rows arrive. Compare a vs. b, etc. and stop.")
        self.assertEqual(len(unit.sentences), 3)

    def test_closing_emphasis_ends_a_sentence(self):
        (unit,) = markdown_units("s.md", "**Do not mirror it.** Then check the ontology. _Really._ Stop. \u201cQuoted.\u201d Done.")
        self.assertEqual(len(unit.sentences), 6)

    def test_summary_phrase_needs_a_sentence_boundary(self):
        (unit,) = markdown_units("s.md", "Use a colon in summary lines. In summary, stop.")
        self.assertEqual([f.message.split("'")[1] for f in check(unit)], ["In summary"])

    def test_technical_homographs_are_not_tells(self):
        (unit,) = markdown_units("s.md", "CPU utilization rose with elevated privileges; IDs allow an underscore in the realm of names.")
        self.assertEqual([f.rule for f in check(unit)], [])

    def test_negative_parallelism_and_ing_tail(self):
        (unit,) = markdown_units("s.md", "This is not just a cache, but a graph, ensuring speed.")
        self.assertEqual([f.rule for f in check(unit)], ["tell", "tell"])


if __name__ == "__main__":
    unittest.main()
