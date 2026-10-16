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

    def test_pep_723_metadata_block_is_not_flagged(self):
        header = "#!/usr/bin/env -S uv run\n# /// script\n# requires-python = \">=3.10\"\n# dependencies = [\n#   \"x\",\n# ]\n# ///\nimport x\n"
        self.stage(a_py=header)
        self.assertEqual(self.flagged(), [])

    def test_comment_right_after_a_pep_723_block_is_flagged(self):
        self.stage(a_py="# /// script\n# dependencies = []\n# ///\n# why\nimport x\n")
        self.assertEqual(self.flagged(), [("a.py", "# why")])

    def test_unclosed_pep_723_block_is_flagged(self):
        self.stage(a_py="# /// script\n# dependencies = []\nimport x\n")
        self.assertEqual(len(self.flagged()), 1)

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
        self.assertEqual(code, comment_check.EXIT_FLAGGED)
        self.assertIn("a.rs:1", out)
        self.assertIn("CONTRIBUTING.md", out)
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
            self.assertEqual(self.run_main("--staged")[0], comment_check.EXIT_FLAGGED)

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

    def test_range_mode_internal_error_is_not_reported_as_clean(self):
        self.commit(a_rs="fn a() {}\n")
        code, _, err = self.run_main("--range", "deadbeef", "HEAD")
        self.assertEqual(code, comment_check.EXIT_ERROR)
        self.assertIn("internal error", err)

    def test_digest_survives_the_comment_moving_lines(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        before = digest(scan(None, None))
        self.stage(a_rs="fn z() {}\nfn y() {}\n// why\nfn a() {}\n")
        self.assertEqual(digest(scan(None, None)), before)

    def test_range_mode_follows_renames(self):
        body = "// existing why\nfn a() {}\nfn b() {}\nfn c() {}\n"
        self.commit(old_rs=body)
        base = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        self.git("mv", "old.rs", "new.rs")
        self.write("new.rs", body)
        self.commit()
        self.assertEqual(scan(base, "HEAD"), [])
        self.write("new.rs", body + "// added\n")
        self.commit()
        self.assertEqual([(p, t) for p, _, t in scan(base, "HEAD")], [("new.rs", "// added")])

    def test_broken_grammar_skips_the_file_and_fails_open(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        with patch.object(comment_check, "parser_for", side_effect=ImportError("no wheel")):
            code, _, err = self.run_main("--staged")
        self.assertEqual(code, 0)
        self.assertIn("no wheel", err)


class RuleTest(unittest.TestCase):
    def test_quoted_rule_is_in_contributing(self):
        self.assertIn(comment_check.RULE, (ROOT / "CONTRIBUTING.md").read_text())

    def test_checker_does_not_skip_itself_as_generated(self):
        head = (ROOT / "scripts/linting/comment_check.py").read_bytes()[: comment_check.GENERATED_SCAN_BYTES]
        self.assertNotIn(comment_check.GENERATED_MARKER, head)


class HookBootstrapTest(RepoTestCase):
    """Runs the real hook wrapper with a uv that cannot install the wheels."""

    HOOK = ROOT / "scripts/linting/comment_check_hook.sh"
    CHECKER = ROOT / "scripts/linting/comment_check.py"

    def plain_env(self):
        """Put a mise stand-in first on PATH: the scratch repo has no mise config, so real mise would not find uv."""
        shim_dir = tempfile.mkdtemp(dir=self._tmp.name)
        shim = Path(shim_dir) / "mise"
        shim.write_text('#!/bin/sh\nshift 2\nexec "$@"\n')
        shim.chmod(0o755)
        return {**os.environ, "PATH": f"{shim_dir}:{os.environ['PATH']}"}

    def cold_offline_env(self):
        cache = tempfile.mkdtemp(dir=self._tmp.name)
        return {**self.plain_env(), "UV_CACHE_DIR": cache, "UV_OFFLINE": "1"}

    def hook(self, env):
        return subprocess.run([str(self.HOOK)], capture_output=True, text=True, env=env)

    def test_uv_cannot_resolve_the_wheels_and_the_hook_lets_the_commit_through(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        env = self.cold_offline_env()
        direct = subprocess.run(
            ["uv", "run", "--quiet", str(self.CHECKER), "--staged"], capture_output=True, text=True, env=env
        )
        self.assertNotIn(direct.returncode, (0, comment_check.EXIT_FLAGGED))
        result = self.hook(env)
        self.assertEqual(result.returncode, 0)
        self.assertIn("skipped", result.stderr)

    def test_flagged_comments_block_through_the_hook(self):
        self.stage(a_rs="// why\nfn a() {}\n")
        result = self.hook(self.plain_env())
        self.assertEqual(result.returncode, 1)
        self.assertIn("COMMENTS_OK=", result.stdout)


if __name__ == "__main__":
    unittest.main()
