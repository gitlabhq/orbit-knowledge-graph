import runpy
import shlex
import subprocess
import sys

import pytest

from conftest import CI

TEST_COMMANDS = ["python -m pytest ci/tests"]
CHECK_COMMANDS = ["python ci/check-repository.py", "python ci/validate-schemas.py all", *TEST_COMMANDS]
GENERATED_COMMANDS = ["python ci/check_generated.py", "git fetch origin release/next --depth=1",
                      "python ci/check_migration_ledger.py --base origin/release/next"]
ADVISORY_COMMANDS = ["python ci/linting/check_narration.py --diff-base abc123",
                     "python ci/linting/check_mr_description.py"]
HYGIENE_COMMANDS = {
    "deps": "cargo shear",
    "fmt": "cargo fmt --all -- --check",
    "newlines": "gitlab-xtasks lint verify-newlines --file-extensions "
                "rs,md,yml,yaml,toml,astro,js,ts,json,mdx,vue,rb,css,mjs --directory . "
                "--exclude-files docs-locale",
}


@pytest.fixture
def run_group(repo, monkeypatch, capsys):
    def run(group, commands, failures=()):
        expected = [[sys.executable if part == "python" else part for part in shlex.split(command)]
                    for command in commands]
        calls = []

        def execute(command, *, cwd):
            assert cwd == CI.parent
            calls.append(command)
            return subprocess.CompletedProcess(command, 7 if len(calls) - 1 in failures else 0)

        monkeypatch.setattr(subprocess, "run", execute)
        monkeypatch.setattr(sys, "argv", ["run_checks.py", group])
        with pytest.raises(SystemExit) as result:
            runpy.run_path(str(CI / "run_checks.py"), run_name="__main__")
        assert result.value.code == int(bool(failures))
        assert calls == expected
        assert capsys.readouterr().err == ("Failed checks:\n" + "\n".join(
            " ".join(expected[index]) for index in failures) + "\n" if failures else "")
    return run


@pytest.mark.parametrize("source,base", [("push", ""), ("merge_request_event", "abc123"), ("merge_request_event", "")])
@pytest.mark.parametrize("group", ["checks", "advisory", "hygiene", "generated", "tests", *HYGIENE_COMMANDS])
@pytest.mark.parametrize("failures", [(), (0,)])
def test_group_selection(run_group, monkeypatch, source, base, group, failures):
    for key, value in dict(CI_PIPELINE_SOURCE=source, CI_MERGE_REQUEST_DIFF_BASE_SHA=base,
                           CI_MERGE_REQUEST_TARGET_BRANCH_NAME="release/next", CI_COMMIT_BRANCH="main").items():
        monkeypatch.setenv(key, value)
    merge_request = source == "merge_request_event"
    groups = {
        "checks": CHECK_COMMANDS + ["python ci/linting/prose_lint.py " + ("--diff-base abc123" if base else "--all")]
                  + (["python ci/check-version-bumps.py"] if merge_request else []),
        "advisory": ["python ci/linting/check_narration.py" + (" --diff-base abc123" if base else "")]
                    + ([ADVISORY_COMMANDS[1]] if merge_request else []),
        "hygiene": list(HYGIENE_COMMANDS.values())[:3 if merge_request else 1],
        "generated": GENERATED_COMMANDS[:3 if merge_request else 1],
        "tests": TEST_COMMANDS,
        **{name: [command] for name, command in HYGIENE_COMMANDS.items()},
    }
    run_group(group, groups[group], failures)


@pytest.mark.parametrize("group,commands,failures", [
    ("advisory", ADVISORY_COMMANDS, (0, 1)), ("generated", GENERATED_COMMANDS, (1,)),
])
def test_all_failures_reported_and_failed_fetch_does_not_stop_ledger(run_group, monkeypatch, group, commands, failures):
    for key, value in dict(CI_PIPELINE_SOURCE="merge_request_event", CI_MERGE_REQUEST_DIFF_BASE_SHA="abc123",
                           CI_MERGE_REQUEST_TARGET_BRANCH_NAME="release/next").items():
        monkeypatch.setenv(key, value)
    run_group(group, commands, failures)
