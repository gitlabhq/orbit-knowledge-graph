import json
import os
import shutil
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture(params=["e2e", "docs"])
def automation(request, repo, fake_tools, monkeypatch):
    for key in ("AUTOMATION_BOT_TOKEN", "GITLAB_TOKEN", "GLAB_TOKEN", "DRY_RUN",
                "E2E_BUMP_ASSIGNEE", "DOC_PRINCIPLES_ASSIGNEE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    repo.git("init", "-b", "main")
    repo.git("remote", "add", "origin", "https://original.invalid/group/project.git")
    docs = request.param == "docs"
    path = ".ai/principles/distilled/documentation-style.md" if docs else "e2e/config/versions.yaml"
    script = "sync_doc_principles.py" if docs else "open_e2e_bump_mr.py"
    branch = "automation/doc-principles-sync" if docs else "automation/e2e-pin-bump"
    assignee = "zpainter" if docs else "michaelangeloio"
    repo.write(path, "old\n")
    before = repo.commit()
    config = (repo.root / ".git/config").read_bytes()
    real_git = shutil.which("git")
    fake_tools.install("bin/push")
    fake_tools.install("bin/api")
    fake_tools.install("bin/curl")
    repo.write("bin/git", f"""#!{sys.executable}
import os
import sys
assert not set(sys.argv[1:]) & {{'fetch', 'pull', 'clone', 'ls-remote', 'submodule'}}
if 'push' in sys.argv[1:]:
    os.execv({str(repo.root / 'bin/push')!r}, ['push', *sys.argv[1:]])
os.execv({real_git!r}, ['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', *sys.argv[1:]])
"""
    ).chmod(0o755)
    repo.write("bin/glab", f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys
assert os.environ['GITLAB_TOKEN'] == 'test-secret'
with Path('glab.jsonl').open('a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\\n')
method = 'POST' if '--method' in sys.argv else 'GET'
os.execv({str(repo.root / 'bin/api')!r}, ['api', method, sys.argv[4]])
"""
    ).chmod(0o755)
    for component in ("gitlab", "siphon", "orbit"):
        repo.write(f"e2e/scripts/bump-{component}-pins.sh", f"printf '{component}\\n' >> bumps\n")
    upstream = "https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab/repository"
    curl = "curl -sf --max-time 30 --retry 4 --retry-all-errors --retry-connrefused --retry-max-time 120 "
    tree = curl + upstream + "/tree?path=.ai/principles/distilled&ref=master&per_page=100"
    raw = curl + upstream + "/files/.ai%2Fprinciples%2Fdistilled%2Fdocumentation-style.md/raw?ref=master"
    fake_tools.responses[tree] = {"stdout": json.dumps([
        {"type": "blob", "name": "documentation-style.md"},
        {"type": "blob", "name": "other.md"},
        {"type": "tree", "name": "documentation-directory.md"},
    ])}
    fake_tools.responses[raw] = {"stdout": "old\n"}
    endpoint = "projects/42/merge_requests"
    lookup = f"api GET {endpoint}?source_branch={branch.replace('/', '%2F')}&target_branch=main&state=opened"
    fake_tools.responses[lookup] = {"stdout": "[]"}
    fake_tools.responses[f"api GET users?username={assignee}"] = {"stdout": '[{"id": 7}]'}
    fake_tools.responses[f"api POST {endpoint}"] = {"stdout": '{"web_url": "https://example.invalid/new"}'}
    push = f"push push --force https://oauth2:test-secret@gitlab.com/group/project.git HEAD:{branch}"
    fake_tools.responses[push] = {}

    def change(content="new\r\n雪\n"):
        if docs:
            fake_tools.responses[raw] = {"stdout": content}
        else:
            repo.write(path, content)
        return content.encode()

    def credentials():
        for key, value in {"CI_PROJECT_ID": "42", "CI_PROJECT_PATH": "group/project",
                           "AUTOMATION_BOT_TOKEN": "test-secret"}.items():
            monkeypatch.setenv(key, value)

    return SimpleNamespace(docs=docs, path=path, script="automation/" + script, branch=branch,
                           before=before, config=config, change=change, credentials=credentials,
                           tree=tree, raw=raw, lookup=lookup, assignee=assignee, push=push)


def test_no_changes_need_no_credentials(automation, repo, fake_tools):
    assert "No changes" in fake_tools.invoke(automation.script)
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert all(call[0] == "curl" for call in fake_tools.calls())
    if not automation.docs:
        assert (repo.root / "bumps").read_text().splitlines() == ["gitlab", "siphon", "orbit"]


def test_dry_run_updates_files_without_publishing(automation, repo, fake_tools, monkeypatch):
    content = automation.change()
    monkeypatch.setenv("DRY_RUN", "true")
    assert "DRY_RUN=true" in fake_tools.invoke(automation.script)
    assert (repo.root / automation.path).read_bytes() == content
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert repo.git("branch", "--show-current") == "main"
    assert repo.git("diff", "--cached") == ""
    assert (repo.root / ".git/config").read_bytes() == automation.config
    assert all(call[0] == "curl" for call in fake_tools.calls())


@pytest.mark.parametrize("automation", ["e2e"], indirect=True)
@pytest.mark.parametrize("working_change", [False, True])
def test_dry_run_previews_staged_and_working_changes(automation, repo, fake_tools, monkeypatch, working_change):
    automation.change("staged test-secret\n")
    repo.git("add", "--", automation.path)
    if working_change:
        automation.change("staged test-secret\nworking change\n")
    index = (repo.root / ".git/index").read_bytes()
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("AUTOMATION_BOT_TOKEN", "test-secret")
    output = fake_tools.invoke(automation.script)
    assert "DRY_RUN=true" in output and automation.path in output
    assert "-old" in output and "+staged [REDACTED]" in output
    assert ("+working change" in output) == working_change
    assert "test-secret" not in output
    assert (repo.root / ".git/index").read_bytes() == index
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert fake_tools.calls() == []


@pytest.mark.parametrize("automation", ["docs"], indirect=True)
@pytest.mark.parametrize("token", [None, "test-secret"])
def test_dry_run_previews_untracked_docs_with_spaces(automation, repo, fake_tools, monkeypatch, token):
    name = "documentation new guide.md"
    path = ".ai/principles/distilled/" + name
    fake_tools.responses[automation.tree] = {"stdout": json.dumps([{"type": "blob", "name": name}])}
    raw = automation.raw.replace("documentation-style.md", name.replace(" ", "%20"))
    fake_tools.responses[raw] = {"stdout": "new guide test-secret\n雪\n"}
    repo.write("unrelated.txt", "must not appear\n")
    index = (repo.root / ".git/index").read_bytes()
    monkeypatch.setenv("DRY_RUN", "true")
    if token:
        monkeypatch.setenv("AUTOMATION_BOT_TOKEN", token)
    output = fake_tools.invoke(automation.script)
    assert "DRY_RUN=true" in output and path in output
    assert f"new guide {'[REDACTED]' if token else 'test-secret'}\n雪" in output
    assert "must not appear" not in output
    if token:
        assert token not in output
    assert (repo.root / ".git/index").read_bytes() == index
    assert (repo.root / path).read_bytes() == "new guide test-secret\n雪\n".encode()
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert all(call[0] == "curl" for call in fake_tools.calls())


@pytest.mark.parametrize("existing,known_assignee", [(True, True), (False, True), (False, False)])
def test_publish_refreshes_or_creates_once(automation, repo, fake_tools, existing, known_assignee):
    content = automation.change()
    automation.credentials()
    if existing:
        fake_tools.responses[automation.lookup] = {"stdout": '[{"web_url": "https://example.invalid/existing"}]'}
    if not known_assignee:
        fake_tools.responses[f"api GET users?username={automation.assignee}"] = {"stdout": "[]"}
    output = fake_tools.invoke(automation.script)
    assert ("Refreshed existing MR" if existing else "Opened new MR") in output
    assert repo.git("branch", "--show-current") == automation.branch
    assert repo.git("log", "-1", "--format=%an <%ae>") == "Orbit automation bot <orbit-automation-bot@noreply.gitlab.com>"
    assert repo.git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD") == automation.path
    assert (repo.root / automation.path).read_bytes() == content
    assert repo.git("diff", "HEAD", "--", automation.path) == ""
    assert (repo.root / ".git/config").read_bytes() == automation.config
    calls = fake_tools.calls()
    assert sum(call[0] == "push" for call in calls) == 1
    assert sum(call[:2] == ["api", "POST"] for call in calls) == (not existing)
    assert sum(call[0] == "api" for call in calls) == (1 if existing else 3)
    if not existing:
        create = json.loads((repo.root / "glab.jsonl").read_text().splitlines()[-1])
        body = next(field for field in create if field.startswith("description="))
        assert (f"/assign {automation.assignee}" in body) == known_assignee
        assert ("WARNING: assignee" in output) == (not known_assignee)
        assert '/label ~"type::maintenance"' in body
        assert ("/label ~documentation" in body) == automation.docs
    assert "test-secret" not in output


def test_missing_credentials_fail_before_commit(automation, repo, fake_tools):
    automation.change()
    assert "CI_PROJECT_ID is required" in fake_tools.invoke(automation.script, status=1)
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert all(call[0] == "curl" for call in fake_tools.calls())


def test_push_errors_redact_credentials(automation, repo, fake_tools):
    automation.change()
    automation.credentials()
    fake_tools.responses[automation.push] = {"status": 1, "stdout": "test-secret", "stderr": automation.push}
    output = fake_tools.invoke(automation.script, status=1)
    assert "git failed" in output and "[REDACTED]" in output
    assert "test-secret" not in output
    assert not any(call[0] == "api" for call in fake_tools.calls())
    assert (repo.root / ".git/config").read_bytes() == automation.config


@pytest.mark.parametrize("automation", ["docs"], indirect=True)
@pytest.mark.parametrize("failure", ["tree", "raw", "empty"])
def test_docs_upstream_failures_are_nonfatal(automation, repo, fake_tools, failure):
    if failure == "empty":
        fake_tools.responses[automation.tree] = {"stdout": "[]"}
    else:
        fake_tools.responses[getattr(automation, failure)] = {"status": 22, "stdout": "partial"}
    assert "WARNING:" in fake_tools.invoke(automation.script)
    assert (repo.root / automation.path).read_bytes() == b"old\n"
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert all(call[0] == "curl" for call in fake_tools.calls())


@pytest.mark.parametrize("automation", ["docs"], indirect=True)
def test_docs_new_file_is_committed_verbatim(automation, repo, fake_tools):
    (repo.root / automation.path).unlink()
    repo.commit()
    content = automation.change()
    automation.credentials()
    assert "Opened new MR" in fake_tools.invoke(automation.script)
    assert (repo.root / automation.path).read_bytes() == content
    assert repo.git("ls-files", "--", automation.path) == automation.path


def test_failed_mr_lookup_does_not_create_duplicate(automation, fake_tools):
    automation.change()
    automation.credentials()
    fake_tools.responses[automation.lookup] = {"status": 1}
    assert "glab failed" in fake_tools.invoke(automation.script, status=1)
    assert not any(call[:2] == ["api", "POST"] for call in fake_tools.calls())


@pytest.mark.parametrize("automation", ["e2e"], indirect=True)
def test_failed_pin_bump_stops_before_publish(automation, repo, fake_tools):
    repo.write("e2e/scripts/bump-gitlab-pins.sh", "exit 1\n")
    assert "bash failed" in fake_tools.invoke(automation.script, status=1)
    assert not (repo.root / "bumps").exists()
    assert repo.git("rev-parse", "HEAD") == automation.before
    assert fake_tools.calls() == []
