#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

LINTING = Path(__file__).resolve().parent


class NarrationCli(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repository = Path(temporary.name)
        self.linting = self.repository / "ci/linting"
        self.linting.mkdir(parents=True)
        for name in ("check_narration.py", "narration_score.py"):
            shutil.copyfile(LINTING / name, self.linting / name)
        (self.repository / "crates").mkdir()
        self.source = self.repository / 'crates/space [name] "quoted".rs'
        self.source.write_text("// Setup\nfn old() {}\n\nfn changed() {}\n")
        self.git("init")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
             "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=self.repository, text=True, capture_output=True, check=True,
        ).stdout.strip()

    def commit(self):
        self.git("add", "--all")
        self.git("commit", "-m", "fixture")
        return self.git("rev-parse", "HEAD")

    def run_check(self, *args):
        return subprocess.run(
            [sys.executable, str(self.linting / "check_narration.py"), *args],
            cwd=self.linting, text=True, capture_output=True,
        )

    def test_only_changed_comment_lines_are_reported(self):
        self.source.write_text("// Setup\nfn old() {}\n\n// Create\nfn changed() {}\n")
        self.commit()
        result = self.run_check("--diff-base", self.base)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f'{self.source.relative_to(self.repository)}:4\tblock_label\t// Create', result.stdout)
        self.assertNotIn("// Setup", result.stdout)
        self.assertIn("1 new flagged comment(s)", result.stdout)

    def test_changing_only_code_does_not_report_old_comment(self):
        self.source.write_text("// Setup\nfn renamed() {}\n\nfn changed() {}\n")
        self.commit()
        result = self.run_check("--diff-base", self.base)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no new narration comments", result.stdout)

    def test_unreachable_base_fails_without_whole_tree_fallback(self):
        result = self.run_check("--diff-base", "0" * 40)
        self.assertEqual(result.returncode, 2)
        self.assertIn("unreachable", result.stderr)
        self.assertNotIn("// Setup", result.stdout)
        self.assertNotIn("✅", result.stdout)

    def test_deleted_files_are_excluded(self):
        self.source.unlink()
        self.commit()
        result = self.run_check("--diff-base", self.base)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no Rust files changed", result.stdout)

    def test_missing_explicit_files_and_non_rust_paths_are_skipped(self):
        text = self.repository / "notes.txt"
        text.write_text("// Setup\nfn old() {}\n")
        result = self.run_check("crates/missing.rs", "notes.txt", "crates")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no narration comments flagged", result.stdout)

    def test_changed_file_missing_from_worktree_is_skipped(self):
        self.source.write_text("// Create\nfn changed() {}\n")
        self.commit()
        self.source.unlink()
        result = self.run_check("--diff-base", self.base)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no new narration comments", result.stdout)

    def test_rename_does_not_report_unchanged_comments(self):
        self.source.rename(self.repository / "crates/renamed file.rs")
        self.commit()
        result = self.run_check("--diff-base", self.base)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no new narration comments", result.stdout)

    def test_whole_tree_and_explicit_files_use_repository_root(self):
        for args in ((), (str(self.source.relative_to(self.repository)),)):
            with self.subTest(args=args):
                result = self.run_check(*args)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("1 flagged comment(s)", result.stdout)
                self.assertIn("// Setup", result.stdout)


class DescriptionCli(unittest.TestCase):
    def run_check(self, description="", merge_request="1", truncated="false"):
        return subprocess.run(
            [sys.executable, str(LINTING / "check_mr_description.py")],
            text=True, capture_output=True,
            env={**os.environ, "CI_MERGE_REQUEST_IID": merge_request,
                 "CI_MERGE_REQUEST_DESCRIPTION": description,
                 "CI_MERGE_REQUEST_DESCRIPTION_IS_TRUNCATED": truncated},
        )

    def test_non_merge_request_and_empty_descriptions_pass(self):
        for description, merge_request in (("word " * 101, ""), ("", "1"), (" \n", "1")):
            with self.subTest(description=description, merge_request=merge_request):
                result = self.run_check(description, merge_request)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("FAIL", result.stdout)

    def test_each_headline_limit_fails(self):
        for description, failure in (
            ("word " * 101, "words 101>100"),
            ("`one` `two` `three` `four`", "spans 4>3"),
            ("one_name two_name three_name four_name", "bare_idents 4>3"),
        ):
            with self.subTest(failure=failure):
                result = self.run_check(description)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(failure, result.stdout)

    def test_short_headline_ignores_agent_context(self):
        result = self.run_check("### What does this MR do and why?\nFix the check.\n<details>" + "word " * 101)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS", result.stdout)

    def test_truncated_description_without_boundary_skips_scoring(self):
        result = self.run_check("### What does this MR do and why?\n" + "word " * 101, truncated="true")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cannot score reliably", result.stdout)
        self.assertNotIn("FAIL", result.stdout)

    def test_truncated_description_with_boundary_still_fails(self):
        for boundary in ("<details>", "### Agent context"):
            with self.subTest(boundary=boundary):
                result = self.run_check(
                    "### What does this MR do and why?\n" + "word " * 101 + "\n" + boundary,
                    truncated="true",
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("words 101>100", result.stdout)


if __name__ == "__main__":
    unittest.main()
