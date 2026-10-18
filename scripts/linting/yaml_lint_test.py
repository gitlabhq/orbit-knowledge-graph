#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml>=6.0"]
# ///
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

import yaml_lint
from yaml_lint import check_spacing, main, missing_gaps

CRAMPED = """\
name: suite
fixtures:
  - path: a.sh
    content: |
      run() {
          :

      }
      # not a comment
  - path: b.sh
    content: |
      echo b
# the cases
tests:
  - { row_count: 1 }
  - { row_count: 2 }
  - name: one
    query: q
  # second case
  - name: two
    query: q
"""

SPACED = """\
name: suite
description: one line

fixtures:
  - path: a.sh
    content: a

  - path: b.sh
    content: b
tags: [a]
"""


def gaps(text: str) -> list[int]:
    return missing_gaps(text.split("\n"), yaml.compose(text))


class SpacingTest(unittest.TestCase):
    def test_multi_line_siblings_need_a_blank_line(self):
        self.assertEqual(gaps(CRAMPED), [1, 9, 12, 16, 18])

    def test_one_line_siblings_need_no_blank_line(self):
        self.assertEqual(gaps(SPACED), [9])

    def test_fix_keeps_the_value_and_is_stable(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(yaml_lint, "ROOT", Path(tmp)):
            path = Path(tmp) / "suite.yaml"
            path.write_text(CRAMPED)
            check_spacing(path, fix=True)
            fixed = path.read_text()
            self.assertEqual(yaml.safe_load(fixed), yaml.safe_load(CRAMPED))
            self.assertEqual(check_spacing(path, fix=False), [])
            self.assertIn("      }\n      # not a comment\n\n  - path: b.sh", fixed)


    def test_fix_refuses_a_change_to_the_value(self):
        text = "a: |+\n  x\nb:\n  - c\n"
        with tempfile.TemporaryDirectory() as tmp, patch.object(yaml_lint, "ROOT", Path(tmp)):
            path = Path(tmp) / "suite.yaml"
            path.write_text(text)
            self.assertNotEqual(check_spacing(path, fix=True), [])
            self.assertEqual(path.read_text(), text)

    def test_unicode_line_separators_keep_line_numbers(self):
        self.assertEqual(gaps('z: "\u2028"\nl:\n  - c: |\n      one\n      two\n  - c: |\n      three\n'), [1, 5])

    def test_trailing_comments_do_not_make_an_entry_long(self):
        self.assertEqual(gaps("tags:\n  - a\n  - b\n  # note\nother: x\n"), [4])


    def test_dash_first_items_get_the_blank_line_above_the_dash(self):
        self.assertEqual(gaps("l:\n  - name: a\n    q: 1\n  - # b\n    name: b\n    q: 2\n  -\n    name: c\n"), [3, 6])
        self.assertEqual(gaps("l:\n  - x: 1\n    y: 2\n  - # c\n    -x: 1\n    y: 2\n"), [3])

    def test_a_missing_suite_folder_fails_the_full_run(self):
        with patch.object(yaml_lint, "SUITES", (yaml_lint.ROOT / "no-such-suite",)):
            self.assertEqual(main([]), 1)


if __name__ == "__main__":
    unittest.main()
