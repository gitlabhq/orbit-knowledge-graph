import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

CI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI / "linting"))


@pytest.fixture
def repo(tmp_path, monkeypatch):
    for key in os.environ:
        if key.startswith(("CI", "GIT_", "SKIP_", "VENDOR_")) or key == "BASE_REF":
            monkeypatch.delenv(key)

    def write(name, content):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode() if isinstance(content, str) else content)
        return path

    def copy(*names):
        for name in names:
            write(name, (CI.parent / name).read_bytes())

    def git(*args):
        return subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
             "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=tmp_path, capture_output=True, text=True, check=True,
        ).stdout.strip()

    def commit():
        git("add", "--all")
        git("commit", "-m", "fixture")
        return git("rev-parse", "HEAD")

    def invoke(script, *args, status=0, cwd=None, stdin=subprocess.DEVNULL):
        path = tmp_path / "ci" / script
        result = subprocess.run(
            [sys.executable, str(path if path.exists() else CI / script), *args],
            cwd=cwd or tmp_path, stdin=stdin, capture_output=True, text=True, timeout=15,
        )
        output = result.stdout + result.stderr
        assert result.returncode == status, output
        return output

    return SimpleNamespace(root=tmp_path, write=write, copy=copy, git=git, commit=commit, invoke=invoke)


@pytest.fixture
def fake_tools(repo, monkeypatch):
    responses = {}
    monkeypatch.setenv("PATH", str(repo.root / "bin") + os.pathsep + os.environ["PATH"])

    def install(name):
        repo.write(name, f"""#!{sys.executable}
import json
from pathlib import Path
import sys

command = [Path(sys.argv[0]).name, *sys.argv[1:]]
with Path('calls.jsonl').open('a') as log:
    log.write(json.dumps(command) + '\\n')
response = json.loads(Path('responses.json').read_text())[' '.join(command)]
sys.stdout.buffer.write(response.get('stdout', '').encode())
sys.stderr.write(response.get('stderr', ''))
sys.exit(response.get('status', 0))
""").chmod(0o755)

    def invoke(*args, **kwargs):
        repo.write("responses.json", json.dumps(responses))
        return repo.invoke(*args, **kwargs)

    def calls():
        path = repo.root / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    return SimpleNamespace(responses=responses, install=install, invoke=invoke, calls=calls)
