import hashlib
import http.client
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import urllib.error

import pytest
import yaml


@pytest.fixture
def checks(repo, fake_tools, monkeypatch, capsys):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    import check_vendored
    import skip_check

    monkeypatch.setattr(check_vendored, "ROOT", repo.root)
    monkeypatch.setattr(skip_check, "ROOT", repo.root)
    entries = {}

    def run(name="all", status=0):
        repo.write("config/versions.yaml", yaml.safe_dump({"vendored": entries}, sort_keys=False))
        repo.write("responses.json", json.dumps(fake_tools.responses))
        monkeypatch.setattr(sys, "argv", ["check_vendored.py", name])
        result = check_vendored.main()
        output = capsys.readouterr()
        assert result == status, output.out + output.err
        return output.out + output.err

    return entries, run


def entry(name, **pins):
    return {"vendor_dir": f"artifacts/{name}", "check_script": "ci/check_vendored.py", **pins}


@pytest.fixture
def actions(repo, fake_tools, checks):
    checks[0]["gitlab_system_note_actions"] = entry("actions", version="abc123")
    repo.write("artifacts/actions/system_note_metadata.actions", "# actions\n\nadded\nremoved\n")
    fake_tools.install("bin/curl")
    commands = []
    for path, source in (("app/models/system_note_metadata.rb", "ICON_TYPES = %w[added removed]"),
                         ("ee/app/models/ee/system_note_metadata.rb", "EE_ICON_TYPES = %i[removed]")):
        command = ("curl -sf --max-time 30 --retry 4 --retry-all-errors --retry-connrefused "
                   f"--retry-max-time 120 https://gitlab.com/gitlab-org/gitlab/-/raw/abc123/{path}")
        fake_tools.responses[command] = {"stdout": source}
        commands.append(command)
    return commands


@pytest.mark.parametrize("key,value", [
    ("SKIP_SYSTEM_NOTE_ACTIONS_CHECK", "1"),
    ("CI_MERGE_REQUEST_DESCRIPTION", "text [skip system-note-actions-check] text"),
    ("CI_MERGE_REQUEST_TITLE", "text [skip system-note-actions-check]"),
])
def test_skip_controls_avoid_fetch(checks, actions, fake_tools, monkeypatch, key, value):
    monkeypatch.setenv(key, value)
    assert "skipping" in checks[1]()
    assert fake_tools.calls() == []


def test_commit_skip_requires_exported_base_ref(checks, actions, fake_tools, monkeypatch):
    monkeypatch.setenv("CI_MERGE_REQUEST_DIFF_BASE_SHA", "abc123")
    assert "matches upstream (2 actions)" in checks[1]()
    assert len(fake_tools.calls()) == 2
    fake_tools.install("bin/git")
    fake_tools.responses.update({"git fetch origin abc123 --depth=1": {"status": 1},
                               "git log custom base...HEAD --format=%B": {"stdout": "[skip system-note-actions-check]"}})
    monkeypatch.setenv("BASE_REF", "custom base")
    assert "skipping" in checks[1]()
    assert [" ".join(call) for call in fake_tools.calls()[2:]] == [
        "git fetch origin abc123 --depth=1", "git log custom base...HEAD --format=%B",
    ]


@pytest.mark.parametrize("failed_fetch", [0, 1])
def test_upstream_network_failure_is_nonfatal(checks, actions, fake_tools, failed_fetch):
    fake_tools.responses[actions[failed_fetch]] = {"status": 22}
    assert "non-fatal" in checks[1]()
    assert len(fake_tools.calls()) == failed_fetch + 1


@pytest.mark.parametrize("source", ["", "ICON_TYPES = %w[]", "ICON_TYPES = %w[stale]"])
def test_missing_empty_or_changed_actions_fail(checks, actions, fake_tools, source):
    for command in actions:
        fake_tools.responses[command] = {"stdout": source}
    assert "ERROR: gitlab_system_note_actions" in checks[1](status=1)


@pytest.mark.parametrize("name", ["unknown", "../iglu", "iglu;false"])
def test_unknown_or_invalid_selection_does_not_run_other_checks(checks, actions, fake_tools, name):
    assert "Unknown or invalid" in checks[1](name, status=1)
    assert fake_tools.calls() == []


def test_named_selection_and_entries_without_checks(checks, actions):
    checks[0].update({"unknown": entry("unknown"), "no_check": {}, "null_check": {"check_script": None}})
    assert "matches upstream" in checks[1]("gitlab_system_note_actions")
    output = checks[1](status=1)
    assert "unknown" in output and "matches upstream" in output
    assert "no_check" not in output and "null_check" not in output


@pytest.mark.parametrize("field,value", [("vendor_dir", "../outside"), ("vendor_dir", "/tmp"),
                                         ("check_script", "scripts/old.sh"), ("version", "bad pin")])
def test_invalid_entry_does_not_stop_later_checks(checks, actions, field, value):
    checks[0]["duckdb"] = {**entry("duckdb"), field: value}
    checks[0]["gitlab_system_note_actions"] = checks[0].pop("gitlab_system_note_actions")
    output = checks[1](status=1)
    assert "ERROR: duckdb" in output and "matches upstream" in output


def test_empty_selection_succeeds(checks):
    assert checks[1]() == ""


def test_cli_runs_from_another_directory(repo, checks, actions, fake_tools):
    repo.copy("ci/check_vendored.py", "ci/skip_check.py")
    repo.write("config/versions.yaml", yaml.safe_dump({"vendored": checks[0]}))
    assert "matches upstream" in fake_tools.invoke("check_vendored.py", cwd=repo.root / "ci")


@pytest.mark.parametrize("content", ["vendored: [", "null", "vendored: []"])
def test_invalid_versions_file_reports_error(repo, content):
    repo.copy("ci/check_vendored.py", "ci/skip_check.py")
    repo.write("config/versions.yaml", content)
    assert "ERROR: config/versions.yaml" in repo.invoke("check_vendored.py", status=1)


@pytest.fixture
def http_response():
    def response(body, length=None):
        headers = f"HTTP/1.1 200 OK\r\nContent-Length: {len(body) if length is None else length}\r\n\r\n"
        socket = SimpleNamespace(makefile=lambda mode: io.BytesIO(headers.encode() + body))
        result = http.client.HTTPResponse(socket)
        result.begin()
        return result

    return response


@pytest.fixture
def iglu(repo, checks, monkeypatch, http_response):
    checks[0]["iglu"] = entry("iglu", pins={"first": "1-0-0", "second": "1-0-0"})
    responses, requests = {}, []
    for name in ("first", "second"):
        repo.write(f"artifacts/iglu/{name}/1-0-0.json", '{"b": 2, "a": 1}')
        responses[name] = b'{"a":1,"b":2}'

    def fetch(url, timeout):
        name = url.split("/")[-3]
        requests.append(name)
        response = responses[name]
        if isinstance(response, Exception):
            raise response
        return http_response(response) if isinstance(response, bytes) else response

    monkeypatch.setattr("urllib.request.urlopen", fetch)
    return responses, requests


@pytest.mark.parametrize("response", [b'{"a":true,"b":2}', b"invalid", b" " * 1048577,
                                     urllib.error.URLError("offline"), None],
                         ids=["drift", "invalid-json", "oversize", "offline", "missing"])
def test_iglu_checks_every_pin_and_later_dependencies(repo, checks, iglu, actions, response):
    if response is None:
        (repo.root / "artifacts/iglu/first/1-0-0.json").unlink()
    else:
        iglu[0]["first"] = response
    output = checks[1](status=1)
    assert "ERROR: first/1-0-0" in output and "OK: second/1-0-0" in output and "matches upstream" in output
    assert iglu[1] == (["second"] if response is None else ["first", "second"])


@pytest.mark.parametrize("missing_bytes", [0, 1, 1048576])
def test_iglu_rejects_short_transfer_even_with_complete_json(checks, iglu, actions, http_response, missing_bytes):
    body = iglu[0]["first"]
    iglu[0]["first"] = http_response(body, len(body) + missing_bytes)
    output = checks[1](status=int(bool(missing_bytes)))
    assert ("incomplete transfer" in output) == bool(missing_bytes)
    assert "OK: second/1-0-0" in output and "matches upstream" in output


def test_iglu_normalizes_json_and_preserves_versions(repo, checks, iglu, monkeypatch):
    iglu[0]["first"] = iglu[0]["first"].ljust(1048576, b" ")
    loads = []
    load = yaml.safe_load

    def read_once(content):
        loads.append(content)
        return load(content)

    monkeypatch.setattr(yaml, "safe_load", read_once)
    assert "All pinned Iglu schemas verified" in checks[1]()
    assert iglu[1] == ["first", "second"]
    assert loads == [(repo.root / "config/versions.yaml").read_bytes()]


def test_readonly_postcondition_keeps_running(repo, checks, iglu, actions, monkeypatch, http_response):
    def fetch(*args, **kwargs):
        repo.write("config/versions.yaml", "changed: true")
        return http_response(b'{"a":1,"b":2}')

    monkeypatch.setattr("urllib.request.urlopen", fetch)
    output = checks[1](status=1)
    assert "must be read-only" in output and "matches upstream" in output


@pytest.mark.parametrize("failure", [None, "checksum", "sources", "git", "tar", "gzip"])
def test_duckdb_rebuilds_exact_archive_and_aggregates_failures(repo, checks, actions, failure):
    files = {"fts/LICENSE": b"license", "snowball/stem.c": b"snowball"}
    for name in ("fts_extension.cpp", "fts_indexing.cpp", "indexing.sql", "include/fts_extension.hpp", "include/fts_indexing.hpp"):
        files[f"fts/{name}"] = name.encode()
    for name, content in files.items():
        repo.write(f"expected/duckdb-fts-sources/{name}", content).chmod(0o644)
    for path in (repo.root / "expected").rglob("*"):
        if path.is_dir():
            path.chmod(0o755)
    tar = shutil.which("gtar" if sys.platform == "darwin" else "tar")
    archive = subprocess.run(
        [tar, "--sort=name", "--mtime=UTC 1970-01-01", "--owner=0", "--group=0", "--numeric-owner",
         "--format=ustar", "-C", str(repo.root / "expected"), "-cf", "-", "duckdb-fts-sources"],
        capture_output=True, check=True,
    ).stdout
    data = subprocess.run(["gzip", "-n"], input=archive, capture_output=True, check=True).stdout
    repo.write("artifacts/duckdb/duckdb-fts-sources.tar.gz", data)
    checks[0].clear()
    checks[0].update({"duckdb": entry("duckdb", version="v1.5.5", extensions={"fts": {
        "source_revision": "a" * 40, "source_archive_sha256": hashlib.sha256(data).hexdigest() if failure != "checksum" else "0" * 64,
    }}), "gitlab_system_note_actions": entry("actions", version="abc123")})
    for name, content in files.items():
        target = "duckdb/third_party/" + name if name.startswith("snowball/") else "duckdb-fts/" + ("LICENSE" if name == "fts/LICENSE" else "extension/" + name)
        repo.write(f"upstream/{target}", b"changed" if failure == "sources" else content)
    repo.write("upstream/duckdb/third_party/snowball/CMakeLists.txt", "excluded")
    repo.write("bin/git", f"""#!{sys.executable}
from pathlib import Path
import shutil, sys
if {failure == 'git'}: sys.exit(1)
if 'clone' in sys.argv or 'init' in sys.argv:
    target = Path(sys.argv[-1])
    shutil.copytree(Path('upstream') / target.name, target)
""").chmod(0o755)
    for tool in ("tar", "gzip"):
        executable = shutil.which("gtar" if tool == "tar" and sys.platform == "darwin" else tool)
        repo.write(f"bin/{tool}", f"#!{sys.executable}\nimport os, sys\n" + (
            "sys.exit(1)\n" if failure == tool else f"os.execv({executable!r}, [{executable!r}, *sys.argv[1:]])\n"
        )).chmod(0o755)
    output = checks[1](status=int(failure is not None))
    assert "matches upstream (2 actions)" in output
    assert ("matches its pinned upstream" in output) == (failure is None)
