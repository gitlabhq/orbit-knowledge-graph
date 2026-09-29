import json
import os
import shutil
import sys

import pytest

CHECK = "check-version-bumps.py"
SKILL = "skills/example/SKILL.md"
PROMPT = "config/prompts/example.yml"
SCHEMA = "config/schemas/graph_query.schema.json"
PINS = ("query_dsl", "raw_output_format", "goon_output_format")


@pytest.fixture(autouse=True)
def base(repo):
    repo.copy(f"ci/{CHECK}", "ci/skip_check.py")
    repo.write("config/versions.yaml", "".join(f"{pin}: 1.0.0\n" for pin in PINS))
    repo.write(SCHEMA, "{}\n")
    repo.write("crates/query-engine/formatters/src/graph.rs", "original\n")
    repo.write(PROMPT, "version: 1.0.0\ntext: original\n")
    repo.write(SKILL, "---\nversion: 1.0.0\n---\nOriginal\n")
    repo.git("init", "-b", "main")
    base = repo.commit()
    repo.git("update-ref", "refs/remotes/origin/main", base)
    return base


@pytest.fixture
def fetches(repo, monkeypatch, base):
    executable = shutil.which("git")
    shim = repo.write(".git/test-bin/git", f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

if sys.argv[1] == 'fetch':
    with Path('.git/fetches.jsonl').open('a') as log:
        log.write(json.dumps(sys.argv[2:]) + '\\n')
    sys.exit(int(sys.argv[3] in json.loads(os.environ['FETCH_FAILURES'])))
os.execv({executable!r}, [{executable!r}, *sys.argv[1:]])
""")
    shim.chmod(0o755)
    for key, value in {
        "PATH": str(shim.parent) + os.pathsep + os.environ["PATH"],
        "CI": "true", "CI_MERGE_REQUEST_DIFF_BASE_SHA": base,
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME": "target", "FETCH_FAILURES": "[]",
    }.items():
        monkeypatch.setenv(key, value)
    repo.git("update-ref", "refs/remotes/origin/target", base)

    def calls():
        path = repo.root / ".git/fetches.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    return calls


def test_all_reports_each_failure_then_accepts_bumps(repo, base):
    repo.write(SCHEMA, '{"changed": true}\n')
    repo.write("crates/query-engine/formatters/src/graph.rs", "changed\n")
    repo.write(PROMPT, "version: 1.0.0\ntext: changed\n")
    repo.write("skills/example/reference.md", "Changed\n")
    repo.commit()
    output = repo.invoke(CHECK, status=1, cwd=repo.root / "ci")
    for pin in PINS:
        assert f"{pin}: covered files changed but the pin was not bumped" in output
    assert "example.yml changed without a version bump" in output
    assert "example: version unchanged" in output
    repo.write("config/versions.yaml", "".join(f"{pin}: 1.0.1\n" for pin in PINS))
    repo.write(PROMPT, "version: 1.0.1\ntext: changed\n")
    repo.write(SKILL, "---\nversion: 1.0.1\n---\nChanged\n")
    repo.commit()
    repo.invoke(CHECK, "all", "--base-ref", base)


def test_pinned_ignores_worktree_and_requires_only_an_added_pin_line(repo):
    repo.write(SCHEMA, "changed\n")
    repo.invoke(CHECK, "pinned")
    repo.commit()
    repo.invoke(CHECK, "pinned", status=1)
    repo.write("config/versions.yaml", "query_dsl: 1.0.0 # same version\n")
    repo.invoke(CHECK, "pinned", status=1)
    repo.commit()
    repo.invoke(CHECK, "pinned")


@pytest.mark.parametrize("field", ["SKIP_PINNED_VERSION_CHECK", "CI_MERGE_REQUEST_DESCRIPTION", "CI_MERGE_REQUEST_TITLE", "commit"])
def test_pinned_skip_controls(repo, monkeypatch, field):
    repo.write(SCHEMA, "changed\n")
    repo.commit()
    if field == "commit":
        repo.git("commit", "--allow-empty", "-m", "Change [skip pinned-version-check]")
    else:
        monkeypatch.setenv(field, "1" if field.startswith("SKIP_") else "Notes [skip pinned-version-check]")
    assert "skipping" in repo.invoke(CHECK, "pinned")


def test_prompt_worktree_version_lines_new_deleted_and_ignored_files(repo):
    repo.write(PROMPT, "version: 1.0.0\ntext: changed\n")
    repo.invoke(CHECK, "prompts", status=1)
    repo.write(PROMPT, "version: 0.1.0\ntext: changed\n")
    repo.invoke(CHECK, "prompts")
    repo.write(PROMPT, "text: no version\n")
    assert "has no top-level version:" in repo.invoke(CHECK, "prompts", status=1)
    (repo.root / PROMPT).unlink()
    repo.write("config/prompts/ignored.yaml", "no version\n")
    repo.write("config/prompts/new.yml", "version: anything\n")
    repo.git("add", "config/prompts")
    repo.invoke(CHECK, "prompts")
    repo.write("config/prompts/new.yml", "text: no version\n")
    repo.invoke(CHECK, "prompts", status=1)


def test_pinned_three_dot_and_prompt_direct_base_comparison(repo, base):
    repo.write(SCHEMA, "target change\n")
    repo.write(PROMPT, "version: 1.0.0\ntext: target change\n")
    target = repo.commit()
    repo.git("checkout", "--detach", base)
    repo.invoke(CHECK, "pinned", "--base-ref", target)
    assert "changed without a version bump" in repo.invoke(CHECK, "prompts", "--base-ref", target, status=1)


def test_staged_skill_uses_index_not_worktree(repo):
    repo.write("skills/example/reference.md", "changed\n")
    repo.git("add", "skills")
    repo.write(SKILL, "---\nversion: 1.0.1\n---\n")
    repo.invoke(CHECK, "skills", "--staged", status=1)
    repo.invoke(CHECK, "skills")
    repo.git("add", "skills")
    repo.write(SKILL, "---\nversion: 1.0.0\n---\n")
    repo.invoke(CHECK, "skills", "--staged")


def test_staged_checks_direct_base_to_index_including_reverts(repo, base):
    repo.write(SKILL, "---\nversion: 2.0.0\n---\n")
    target = repo.commit()
    repo.git("checkout", "--detach", base)
    repo.invoke(CHECK, "skills", "--base-ref", target)
    repo.invoke(CHECK, "skills", "--staged", "--base-ref", target, status=1)
    repo.write("skills/example/reference.md", "changed\n")
    repo.commit()
    repo.git("rm", "skills/example/reference.md")
    repo.invoke(CHECK, "skills", "--staged", "--base-ref", base)


@pytest.mark.parametrize("version,status", [("0.9.0", 1), ("1.0.0", 1), ("invalid", 1), ("1.0.0-rc1", 1), ("1.0.1", 0), ("1.10.0", 0), ("2.0.0", 0)])
def test_skill_numeric_versions_must_increase(repo, version, status):
    repo.write(SKILL, f"---\nversion: {version}\n---\nChanged\n")
    repo.invoke(CHECK, "skills", status=status)


@pytest.mark.parametrize("version,status", [("draft", 1), ("different", 0), ("0.0.1", 0)])
def test_skill_nonnumeric_versions_require_inequality(repo, version, status):
    repo.write(SKILL, "---\nversion: draft\n---\n")
    base = repo.commit()
    repo.write(SKILL, f"---\nversion: {version}\n---\nChanged\n")
    repo.invoke(CHECK, "skills", "--base-ref", base, status=status)


@pytest.mark.parametrize("content,status", [("---\nversion: '1.0.0'\n---\n", 0), ("---\nversion: draft\n---\n", 0), ("---\nname: new\n  version: 1.0.0\n---\n", 1), ("---\nname: new\n---\nversion: 1.0.0\n", 1), ("version: 1.0.0\n", 1)])
def test_new_untracked_skill_requires_frontmatter_version(repo, content, status):
    repo.write("skills/new/SKILL.md", content)
    repo.invoke(CHECK, "skills", status=status)
    repo.invoke(CHECK, "skills", "--staged")


@pytest.mark.parametrize("staged", [False, True])
def test_skill_deletions_still_require_a_bump(repo, staged):
    repo.write("skills/example/reference.md", "original\n")
    base = repo.commit()
    (repo.root / "skills/example/reference.md").unlink()
    repo.git("add", "skills")
    args = ("skills", "--base-ref", base, *(("--staged",) if staged else ()))
    repo.invoke(CHECK, *args, status=1)
    repo.write(SKILL, "---\nversion: 1.0.1\n---\n")
    repo.git("add", "skills")
    repo.invoke(CHECK, *args)
    (repo.root / SKILL).unlink()
    repo.git("add", "skills")
    repo.invoke(CHECK, *args, status=1)


@pytest.mark.parametrize("field,status", [("SKIP_SKILL_VERSION_BUMP_CHECK", 0), ("CI_MERGE_REQUEST_DESCRIPTION", 0), ("CI_MERGE_REQUEST_TITLE", 1), ("commit", 1)])
def test_skill_skip_controls_do_not_expand_to_title_or_commits(repo, monkeypatch, field, status):
    repo.write("skills/example/reference.md", "changed\n")
    repo.commit()
    if field == "commit":
        repo.git("commit", "--allow-empty", "-m", "Change [skip skill-version-bump-check]")
    else:
        monkeypatch.setenv(field, "1" if field.startswith("SKIP_") else "[skip skill-version-bump-check]")
    repo.invoke(CHECK, "skills", status=status)


@pytest.mark.parametrize("selection,skip", [("skills", "SKIP_PINNED_VERSION_CHECK"), ("pinned", "SKIP_SKILL_VERSION_BUMP_CHECK"), ("prompts", "SKIP_PINNED_VERSION_CHECK"), ("prompts", "SKIP_SKILL_VERSION_BUMP_CHECK")])
def test_skip_controls_are_isolated(repo, monkeypatch, selection, skip):
    repo.write(SCHEMA, "changed\n")
    repo.write(PROMPT, "version: 1.0.0\ntext: changed\n")
    repo.write("skills/example/reference.md", "changed\n")
    repo.commit()
    monkeypatch.setenv(skip, "1")
    repo.invoke(CHECK, selection, status=1)


def test_ci_skill_snapshot_ignores_worktree_index_and_untracked(repo, monkeypatch, base):
    monkeypatch.setenv("CI", "true")
    repo.write("skills/example/reference.md", "changed\n")
    repo.invoke(CHECK, "skills", "--base-ref", base)
    repo.git("add", "skills")
    repo.invoke(CHECK, "skills", "--base-ref", base)
    repo.commit()
    repo.write(SKILL, "---\nversion: 1.0.1\n---\n")
    repo.invoke(CHECK, "skills", "--base-ref", base, status=1)
    repo.git("add", "skills")
    repo.invoke(CHECK, "skills", "--base-ref", base, status=1)
    repo.invoke(CHECK, "skills", "--staged", "--base-ref", base)
    repo.commit()
    repo.write(SKILL, "---\nversion: 1.0.0\n---\n")
    repo.invoke(CHECK, "skills", "--base-ref", base)


@pytest.mark.parametrize("fallback", [False, True])
def test_ci_keeps_target_and_diff_bases_separate_with_one_fetch_each(repo, monkeypatch, base, fetches, fallback):
    repo.write(SKILL, "---\nversion: 2.0.0\n---\n")
    repo.write(PROMPT, "version: 1.1.0\ntext: target\n")
    repo.commit()
    repo.git("update-ref", "refs/remotes/origin/target", "HEAD")
    repo.git("checkout", "--detach", base)
    repo.write(SKILL, "---\nversion: 1.1.0\n---\n")
    repo.write(PROMPT, "version: 1.1.0\ntext: branch\n")
    repo.commit()
    monkeypatch.setenv("FETCH_FAILURES", json.dumps([base] if fallback else []))
    output = repo.invoke(CHECK, status=int(fallback))
    if fallback:
        assert "changed without a version bump" in output
        assert "must increase" in output
    assert fetches() == [["origin", base, "--depth=1"], ["origin", "target", f"--depth={50 if fallback else 1}"]]


def test_ci_pinned_uses_target_even_when_diff_base_is_older(repo, base, fetches):
    repo.write(SCHEMA, "target change\n")
    repo.commit()
    repo.git("update-ref", "refs/remotes/origin/target", "HEAD")
    repo.write("unrelated.txt", "branch change\n")
    repo.commit()
    repo.invoke(CHECK, "pinned")
    repo.invoke(CHECK, "pinned", "--base-ref", base, status=1)
    assert fetches() == [["origin", "target", "--depth=1"]]


@pytest.mark.parametrize("selection", ["prompts", "skills"])
def test_local_defaults_use_diff_base_without_ci_fetch(repo, monkeypatch, base, selection):
    repo.write(PROMPT, "version: 1.1.0\ntext: target\n")
    repo.write(SKILL, "---\nversion: 2.0.0\n---\n")
    target = repo.commit()
    repo.git("checkout", "--detach", base)
    repo.write(PROMPT, "version: 1.1.0\ntext: branch\n")
    repo.write(SKILL, "---\nversion: 1.1.0\n---\n")
    repo.invoke(CHECK, selection)
    monkeypatch.setenv("CI_MERGE_REQUEST_DIFF_BASE_SHA", target)
    repo.invoke(CHECK, selection, status=1)


def test_failed_target_fetch_does_not_prevent_other_checks(repo, monkeypatch, base, fetches):
    monkeypatch.setenv("FETCH_FAILURES", '["target"]')
    repo.write(PROMPT, "version: 1.0.0\ntext: changed\n")
    output = repo.invoke(CHECK, status=1)
    assert "pinned version check failed" in output
    assert "changed without a version bump" in output
    assert "No skill files changed" in output
    assert len(fetches()) == 2


def test_failed_fallback_is_an_error_even_with_existing_target_ref(repo, monkeypatch, base, fetches):
    monkeypatch.setenv("FETCH_FAILURES", json.dumps([base, "target"]))
    output = repo.invoke(CHECK, status=1)
    for selection in ("pinned", "prompts", "skills"):
        assert f"{selection} version check failed" in output
    assert fetches() == [["origin", base, "--depth=1"], ["origin", "target", "--depth=50"]]


def test_explicit_base_avoids_all_ci_fetches(repo, monkeypatch, base, fetches):
    monkeypatch.setenv("FETCH_FAILURES", json.dumps([base, "target"]))
    repo.invoke(CHECK, "all", "--base-ref", base)
    assert fetches() == []


@pytest.mark.parametrize("selection,depth", [("all", 50), ("pinned", 1), ("prompts", 50), ("skills", 50)])
def test_ci_without_diff_base_fetches_default_branch_once(repo, monkeypatch, fetches, selection, depth):
    monkeypatch.delenv("CI_MERGE_REQUEST_DIFF_BASE_SHA")
    monkeypatch.delenv("CI_MERGE_REQUEST_TARGET_BRANCH_NAME")
    monkeypatch.setenv("CI_DEFAULT_BRANCH", "target")
    repo.invoke(CHECK, selection)
    assert fetches() == [["origin", "target", f"--depth={depth}"]]


def test_local_skill_default_branch_does_not_change_prompt_or_pinned_base(repo, monkeypatch):
    repo.write(SKILL, "---\nversion: 2.0.0\n---\n")
    repo.write(PROMPT, "version: 2.0.0\ntext: target\n")
    repo.write(SCHEMA, "changed\n")
    target = repo.commit()
    repo.git("update-ref", "refs/remotes/origin/target", target)
    repo.write("skills/example/reference.md", "changed\n")
    monkeypatch.setenv("CI_DEFAULT_BRANCH", "target")
    repo.invoke(CHECK, "skills", status=1)
    repo.invoke(CHECK, "prompts")
    repo.invoke(CHECK, "pinned", status=1)


def test_missing_explicit_base_does_not_fall_back(repo, fetches):
    output = repo.invoke(CHECK, "all", "--base-ref", "missing", status=1)
    for selection in ("pinned", "prompts", "skills"):
        assert f"{selection} version check failed" in output
    assert fetches() == []


@pytest.mark.parametrize("selection", ["all", "pinned", "prompts"])
def test_staged_rejects_unsupported_selections(repo, selection):
    assert "skills selection" in repo.invoke(CHECK, selection, "--staged", status=2)
