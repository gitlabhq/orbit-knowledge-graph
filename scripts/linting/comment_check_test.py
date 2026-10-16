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
import contextlib
import io
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import comment_check
from comment_check import digest, main, scan

ROOT = Path(__file__).resolve().parents[2]


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = Path(self._tmp.name)
        self._cwd = os.getcwd()
        os.chdir(self.repo)
        self.addCleanup(os.chdir, self._cwd)
        self.git("init", "-q")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")

    def git(self, *args):
        subprocess.run(["git", *args], check=True, capture_output=True)

    def write(self, name, text):
        path = name.replace("__", "/")
        stem, _, ext = path.rpartition("_")
        path = f"{stem}.{ext}" if stem else path
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def commit(self, **files):
        for path, text in files.items():
            self.write(path, text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "x")

    def stage(self, **files):
        for path, text in files.items():
            self.write(path, text)
        self.git("add", "-A")

    def flagged(self):
        return [(path, text) for path, _, text in scan(None, None)]

    def run_main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(list(argv))
        return code, out.getvalue(), err.getvalue()


class DetectionTest(RepoTestCase):
    def test_new_comment_is_flagged(self):
        self.commit(a_rs="fn a() {}\n")
        self.stage(a_rs="fn a() {}\n// why not\nfn b() {}\n")
        self.assertEqual(self.flagged(), [("a.rs", "// why not")])

    def test_comment_markers_inside_strings_are_not_comments(self):
        self.commit(a_rs="fn a() {}\n")
        self.stage(a_rs='fn a() { let s = "// no"; }\n')
        self.assertEqual(self.flagged(), [])

    def test_rust_attributes_are_not_comments(self):
        self.stage(a_rs="#[derive(Debug)]\nstruct A;\n")
        self.assertEqual(self.flagged(), [])

    def test_trailing_yaml_comment_is_flagged(self):
        self.commit(a_yml="k: 1\n")
        self.stage(a_yml="k: 1 # because\n")
        self.assertEqual(len(self.flagged()), 1)

    def test_adjacent_line_comments_form_one_finding(self):
        self.stage(a_sh="# one\n# two\necho hi\n")
        self.assertEqual(self.flagged(), [("a.sh", "# one\n# two")])

    def test_rustdoc_is_not_flagged(self):
        self.stage(a_rs="/// Item docs.\nfn a() {}\n//! Module docs.\n/** Block docs. */\nfn b() {}\n")
        self.assertEqual(self.flagged(), [])

    def test_four_slashes_is_a_plain_comment(self):
        self.stage(a_rs="//// banner\nfn a() {}\n")
        self.assertEqual(len(self.flagged()), 1)

    def test_tool_directives_are_not_flagged(self):
        self.stage(a_sh="#!/bin/sh\n# shellcheck disable=SC2086\necho $x\n# /// script\n# ///\n", b_rs="// clippy::all\nfn a() {}\n")
        self.assertEqual(self.flagged(), [])

    def test_a_schema_prose_comment_is_flagged(self):
        self.stage(a_yml="# Schema changes need a migration\nk: 1\n")
        self.assertEqual(len(self.flagged()), 1)

    def test_unchanged_comment_next_to_an_edit_is_not_flagged(self):
        self.commit(a_rs="// keep\nfn a() {}\n")
        self.stage(a_rs="// keep\nfn a() { 1; }\n")
        self.assertEqual(self.flagged(), [])

    def test_moved_and_reindented_comment_is_not_flagged(self):
        self.commit(a_rs="fn a() {\n// keep\n}\n")
        self.stage(a_rs="fn b() {}\nfn a() {\n    // keep\n}\n")
        self.assertEqual(self.flagged(), [])

    def test_edited_comment_is_flagged(self):
        self.commit(a_rs="// old words\nfn a() {}\n")
        self.stage(a_rs="// new words\nfn a() {}\n")
        self.assertEqual(self.flagged(), [("a.rs", "// new words")])

    def test_unsupported_and_skipped_paths_are_ignored(self):
        self.stage(a_md="<!-- c -->\n", fixtures__a_rs="// c\n")
        self.write("crates/code-graph-incremental/langs/a.yaml", "# c\n")
        self.git("add", "-A")
        self.assertEqual(self.flagged(), [])

    def test_staged_scan_ignores_unstaged_edits(self):
        self.commit(a_rs="fn a() {}\n")
        self.write("a.rs", "// unstaged\nfn a() {}\n")
        self.assertEqual(self.flagged(), [])


class RenameAndGeneratedTest(RepoTestCase):
    def test_renamed_file_keeps_its_old_comments(self):
        body = "// existing why\nfn a() {}\nfn b() {}\nfn c() {}\n"
        self.commit(old_rs=body)
        self.git("mv", "old.rs", "new.rs")
        self.write("new.rs", body + "// added\n")
        self.git("add", "-A")
        self.assertEqual(self.flagged(), [("new.rs", "// added")])

    def test_generated_files_are_skipped(self):
        self.stage(a_rs="// @generated by prost-build.\n// not flagged\nfn a() {}\n")
        self.assertEqual(self.flagged(), [])


class ModeTest(RepoTestCase):
    def test_range_mode_reports_comments_between_revisions(self):
        self.commit(a_rs="fn a() {}\n")
        base = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        self.commit(a_rs="// why\nfn a() {}\n")
        self.assertEqual([(p, t) for p, _, t in scan(base, "HEAD")], [("a.rs", "// why")])

    def test_blocks_and_prints_the_rule_and_digest(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        code, out, _ = self.run_main("--staged")
        self.assertEqual(code, 1)
        self.assertIn("a.rs:1", out)
        self.assertIn("CONTRIBUTING.md:50", out)
        self.assertIn("COMMENTS_OK=", out)

    def test_matching_digest_acknowledges(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        code = digest(scan(None, None))
        with patch.dict(os.environ, {"COMMENTS_OK": code}):
            self.assertEqual(self.run_main("--staged")[0], 0)

    def test_stale_digest_blocks_again(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        stale = digest(scan(None, None))
        self.stage(a_rs="// why\n// another\nfn a() {}\n")
        with patch.dict(os.environ, {"COMMENTS_OK": stale}):
            self.assertEqual(self.run_main("--staged")[0], 1)

    def test_opt_out_skips_the_check(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        with patch.dict(os.environ, {"COMMENTS_CHECK": "0"}):
            self.assertEqual(self.run_main("--staged"), (0, "", ""))

    def test_merge_in_progress_is_skipped(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        (self.repo / ".git" / "MERGE_HEAD").write_text("0" * 40 + "\n")
        self.assertEqual(self.run_main("--staged")[0], 0)

    def test_internal_error_fails_open(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        with patch.object(comment_check, "scan", side_effect=RuntimeError("boom")):
            code, _, err = self.run_main("--staged")
        self.assertEqual(code, 0)
        self.assertIn("boom", err)

    def test_broken_grammar_skips_the_file_and_fails_open(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        with patch.object(comment_check, "parser_for", side_effect=ImportError("no wheel")):
            code, _, err = self.run_main("--staged")
        self.assertEqual(code, 0)
        self.assertIn("no wheel", err)


class RuleTest(unittest.TestCase):
    def test_quoted_rule_matches_contributing_line_50(self):
        line = (ROOT / "CONTRIBUTING.md").read_text().splitlines()[49]
        self.assertIn(comment_check.RULE, line)


if __name__ == "__main__":
    unittest.main()
