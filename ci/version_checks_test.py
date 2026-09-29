import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class VersionChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("CI", "GIT_", "SKIP_"))
        }
        self.env.update(
            GIT_AUTHOR_NAME="Version test", GIT_AUTHOR_EMAIL="test@example.com",
            GIT_COMMITTER_NAME="Version test", GIT_COMMITTER_EMAIL="test@example.com",
            GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
        )
        for name in ("check-version-bumps.py", "check-skill-version-bump.py", "skip_check.py"):
            self.write(f"ci/{name}", Path(__file__).with_name(name).read_text())
        self.write("config/versions.yaml", "query_dsl: 1.0.0\nraw_output_format: 1.0.0\ngoon_output_format: 1.0.0\n")
        self.write("config/schemas/graph_query.schema.json", "{}\n")
        self.write("crates/query-engine/formatters/src/graph.rs", "original\n")
        self.write("config/prompts/example.yml", "version: 1.0.0\ntext: original\n")
        self.write("skills/example/SKILL.md", "---\nversion: 1.0.0\n---\nOriginal\n")
        self.git("init", "-b", "main")
        self.commit()
        self.base = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("update-ref", "refs/remotes/origin/main", self.base)

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def git(self, *args):
        return subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", *args], cwd=self.root,
            env=self.env, text=True, capture_output=True, check=True,
        )

    def commit(self, message="fixture"):
        self.git("add", ".")
        self.git("commit", "-m", message)

    def check(self, *args, status=0, env=None, standalone=False):
        script = "check-skill-version-bump.py" if standalone else "check-version-bumps.py"
        result = subprocess.run(
            [sys.executable, str(self.root / "ci" / script), *args],
            cwd=self.root / "ci", env=self.env | (env or {}),
            text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, status, result.stdout + result.stderr)
        return result.stdout + result.stderr

    def fake_fetches(self, failures=()):
        executable = shutil.which("git")
        shim = self.write(".git/test-bin/git", f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

if sys.argv[1] == "fetch":
    with Path("fetches.jsonl").open("a") as log:
        log.write(json.dumps(sys.argv[2:]) + "\\n")
    sys.exit(int(sys.argv[3] in {tuple(failures)!r}))
os.execv({executable!r}, [{executable!r}, *sys.argv[1:]])
""")
        shim.chmod(0o755)
        self.env["PATH"] = str(shim.parent) + os.pathsep + self.env["PATH"]
        self.env.update(
            CI="true", CI_MERGE_REQUEST_DIFF_BASE_SHA=self.base,
            CI_MERGE_REQUEST_TARGET_BRANCH_NAME="target",
        )
        self.git("update-ref", "refs/remotes/origin/target", self.base)

    def fetches(self):
        return [json.loads(line) for line in (self.root / "fetches.jsonl").read_text().splitlines()]

    def test_all_reports_each_failure_then_accepts_bumps(self):
        self.write("config/schemas/graph_query.schema.json", '{"changed": true}\n')
        self.write("crates/query-engine/formatters/src/graph.rs", "changed\n")
        self.write("config/prompts/example.yml", "version: 1.0.0\ntext: changed\n")
        self.write("skills/example/reference.md", "Changed\n")
        self.commit()
        output = self.check(status=1)
        for pin in ("query_dsl", "raw_output_format", "goon_output_format"):
            self.assertIn(f"{pin}: covered files changed but the pin was not bumped", output)
        self.assertIn("example.yml changed without a version bump", output)
        self.assertIn("example: version unchanged", output)

        self.write("config/versions.yaml", "query_dsl: 1.0.1\nraw_output_format: 1.0.1\ngoon_output_format: 1.0.1\n")
        self.write("config/prompts/example.yml", "version: 1.0.1\ntext: changed\n")
        self.write("skills/example/SKILL.md", "---\nversion: 1.0.1\n---\nChanged\n")
        self.commit()
        self.check("all", "--base-ref", self.base)

    def test_pinned_ignores_worktree_and_requires_only_an_added_pin_line(self):
        self.write("config/schemas/graph_query.schema.json", "changed\n")
        self.check("pinned")
        self.commit()
        self.check("pinned", status=1)
        self.write("config/versions.yaml", "query_dsl: 1.0.0 # same version\n")
        self.check("pinned", status=1)
        self.commit()
        self.check("pinned")

    def test_pinned_skip_controls(self):
        self.write("config/schemas/graph_query.schema.json", "changed\n")
        self.commit()
        for env in (
            {"SKIP_PINNED_VERSION_CHECK": "1"},
            {"CI_MERGE_REQUEST_DESCRIPTION": "Notes [skip pinned-version-check]"},
            {"CI_MERGE_REQUEST_TITLE": "Title [skip pinned-version-check]"},
        ):
            with self.subTest(env=env):
                self.assertIn("skipping", self.check("pinned", env=env))
        self.write("message.txt", "skip\n")
        self.commit("Change [skip pinned-version-check]")
        self.assertIn("skipping", self.check("pinned"))

    def test_prompt_worktree_version_lines_new_deleted_and_ignored_files(self):
        self.write("config/prompts/example.yml", "version: 1.0.0\ntext: changed\n")
        self.check("prompts", status=1)
        self.write("config/prompts/example.yml", "version: 0.1.0\ntext: changed\n")
        self.check("prompts")
        self.write("config/prompts/example.yml", "text: no version\n")
        self.assertIn("has no top-level version:", self.check("prompts", status=1))
        (self.root / "config/prompts/example.yml").unlink()
        self.write("config/prompts/ignored.yaml", "no version\n")
        self.write("config/prompts/new.yml", "version: anything\n")
        self.git("add", "config/prompts")
        self.check("prompts")

    def test_pinned_three_dot_and_prompt_direct_base_comparison(self):
        self.write("config/schemas/graph_query.schema.json", "target change\n")
        self.write("config/prompts/example.yml", "version: 1.0.0\ntext: target change\n")
        self.commit()
        target = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("checkout", "--detach", self.base)
        self.check("pinned", "--base-ref", target)
        self.assertIn("changed without a version bump", self.check("prompts", "--base-ref", target, status=1))

    def test_staged_skill_uses_index_not_worktree(self):
        self.write("skills/example/reference.md", "changed\n")
        self.git("add", "skills")
        self.write("skills/example/SKILL.md", "---\nversion: 1.0.1\n---\n")
        self.check("skills", "--staged", status=1)
        self.check("skills")
        self.git("add", "skills")
        self.write("skills/example/SKILL.md", "---\nversion: 1.0.0\n---\n")
        self.check("skills", "--staged")
        self.check("--ci", "--staged", standalone=True)

    def test_skill_semver_skip_and_no_worktree_contract(self):
        for version in ("0.9.0", "1.0.0", "invalid"):
            with self.subTest(version=version):
                self.write("skills/example/SKILL.md", f"---\nversion: {version}\n---\nChanged\n")
                self.check("skills", status=1)
        self.check("--ci", "--no-worktree", standalone=True)
        for env in (
            {"SKIP_SKILL_VERSION_BUMP_CHECK": "1"},
            {"CI_MERGE_REQUEST_DESCRIPTION": "[skip skill-version-bump-check]"},
        ):
            with self.subTest(env=env):
                self.assertIn("skipping", self.check("skills", env=env))
        self.check("skills", env={"CI_MERGE_REQUEST_TITLE": "[skip skill-version-bump-check]"}, status=1)
        self.commit("Change [skip skill-version-bump-check]")
        self.check("skills", status=1)

    def test_ci_keeps_target_and_diff_bases_separate(self):
        self.fake_fetches()
        self.write("skills/example/SKILL.md", "---\nversion: 2.0.0\n---\n")
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: target\n")
        self.commit()
        self.git("update-ref", "refs/remotes/origin/target", "HEAD")
        self.git("checkout", "--detach", self.base)
        self.write("skills/example/SKILL.md", "---\nversion: 1.1.0\n---\n")
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: branch\n")
        self.commit()
        self.check()
        fetches = self.fetches()
        self.assertEqual(fetches[0], ["origin", "target", "--depth=1"])
        self.assertIn(["origin", self.base, "--depth=1"], fetches)
        self.assertNotIn(["origin", "target", "--depth=50"], fetches)

    def test_ci_diff_fetch_falls_back_to_target_at_depth_50(self):
        self.fake_fetches(failures=(self.base,))
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: target\n")
        self.write("skills/example/SKILL.md", "---\nversion: 2.0.0\n---\n")
        self.commit()
        self.git("update-ref", "refs/remotes/origin/target", "HEAD")
        self.git("checkout", "--detach", self.base)
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: branch\n")
        self.write("skills/example/SKILL.md", "---\nversion: 1.1.0\n---\n")
        self.commit()
        for selection, failure in (("prompts", "changed without a version bump"), ("skills", "must increase")):
            with self.subTest(selection=selection):
                self.assertIn(failure, self.check(selection, status=1))
        self.assertEqual(self.fetches(), [
            ["origin", self.base, "--depth=1"], ["origin", "target", "--depth=50"],
            ["origin", self.base, "--depth=1"], ["origin", "target", "--depth=50"],
        ])

    def test_ci_pinned_uses_target_even_when_diff_base_is_older(self):
        self.fake_fetches()
        self.write("config/schemas/graph_query.schema.json", "target change\n")
        self.commit()
        self.git("update-ref", "refs/remotes/origin/target", "HEAD")
        self.write("unrelated.txt", "branch change\n")
        self.commit()
        self.check("pinned")
        self.check("pinned", "--base-ref", self.base, status=1)

    def test_local_prompt_defaults_to_ci_diff_base(self):
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: target\n")
        self.commit()
        target = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("checkout", "--detach", self.base)
        self.write("config/prompts/example.yml", "version: 1.1.0\ntext: branch\n")
        self.check("prompts")
        self.check("prompts", env={"CI_MERGE_REQUEST_DIFF_BASE_SHA": target}, status=1)

    def test_failed_fetches_do_not_prevent_other_checks(self):
        self.fake_fetches(failures=("target",))
        self.write("config/prompts/example.yml", "version: 1.0.0\ntext: changed\n")
        output = self.check(status=1)
        self.assertIn("pinned version check failed", output)
        self.assertIn("changed without a version bump", output)
        self.assertIn("No changed files detected", output)

    def test_failed_fallback_is_an_error_even_with_existing_target_ref(self):
        self.fake_fetches(failures=(self.base, "target"))
        for selection in ("prompts", "skills"):
            with self.subTest(selection=selection):
                self.assertIn(f"{selection} version check failed", self.check(selection, status=1))

    def test_explicit_base_avoids_ci_base_preparation(self):
        self.fake_fetches(failures=(self.base, "target"))
        self.check("prompts", "--base-ref", self.base)
        self.check("skills", "--base-ref", self.base)
        self.assertFalse((self.root / "fetches.jsonl").exists())

    def test_staged_rejects_unsupported_selections(self):
        for selection in ("all", "pinned", "prompts"):
            with self.subTest(selection=selection):
                self.assertIn("skills selection", self.check(selection, "--staged", status=2))


if __name__ == "__main__":
    unittest.main()
