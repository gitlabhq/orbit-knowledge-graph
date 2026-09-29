import io
import os
from pathlib import Path
import runpy
import shlex
import subprocess
import sys
import unittest
from unittest.mock import call, patch


TEST_COMMANDS = [
    "python -m unittest discover -s ci/linting -p '*_test.py'",
    "python -m unittest discover -s ci/tests -p '*_test.py'",
    "python ci/version_checks_test.py",
]
CHECK_COMMANDS = [
    "python ci/check-repository.py", "python ci/validate-schemas.py all", *TEST_COMMANDS,
]
GENERATED_COMMANDS = [
    "python ci/check_generated.py",
    "git fetch origin release/next --depth=1",
    "python ci/check_migration_ledger.py --base origin/release/next",
]
ADVISORY_COMMANDS = [
    "python ci/linting/check_narration.py --diff-base abc123",
    "python ci/linting/check_mr_description.py",
]


class RunChecksTests(unittest.TestCase):
    def setUp(self):
        self.env = {}

    def run_group(self, group, commands, failures=()):
        root = Path(__file__).resolve().parents[2]
        expected = [
            [sys.executable if part == "python" else part for part in shlex.split(command)]
            for command in commands
        ]
        results = [subprocess.CompletedProcess(command, 7 if index in failures else 0)
                   for index, command in enumerate(expected)]
        with (patch.dict(os.environ, self.env, clear=True),
              patch.object(sys, "argv", ["run_checks.py", group]),
              patch("subprocess.run", side_effect=results) as run,
              patch("sys.stdout", new_callable=io.StringIO),
              patch("sys.stderr", new_callable=io.StringIO) as errors,
              self.assertRaises(SystemExit) as exit_result):
            runpy.run_path(str(root / "ci/run_checks.py"), run_name="__main__")
        self.assertEqual(exit_result.exception.code, int(bool(failures)))
        self.assertEqual(run.call_args_list, [call(command, cwd=root) for command in expected])
        self.assertEqual(errors.getvalue(), "Failed checks:\n" + "\n".join(
            " ".join(expected[index]) for index in failures) + "\n" if failures else "")

    def test_group_selection_on_main_and_merge_requests(self):
        for source, base in (("push", ""), ("merge_request_event", "abc123"), ("merge_request_event", "")):
            self.env.update(CI_PIPELINE_SOURCE=source, CI_MERGE_REQUEST_DIFF_BASE_SHA=base,
                            CI_MERGE_REQUEST_TARGET_BRANCH_NAME="release/next", CI_COMMIT_BRANCH="main")
            merge_request = source == "merge_request_event"
            groups = {
                "checks": CHECK_COMMANDS + ["python ci/linting/prose_lint.py " +
                    ("--diff-base abc123" if base else "--all")] +
                    (["python ci/check-version-bumps.py"] if merge_request else []),
                "advisory": ["python ci/linting/check_narration.py" + (" --diff-base abc123" if base else "")] +
                    ([ADVISORY_COMMANDS[1]] if merge_request else []),
                "hygiene": ["mise lint:deps", "mise lint:fmt", "mise lint:newlines"][:3 if merge_request else 1],
                "generated": GENERATED_COMMANDS[:3 if merge_request else 1],
                "tests": TEST_COMMANDS,
            }
            for group, commands in groups.items():
                for failures in ((), (0,)):
                    with self.subTest(source=source, base=base, group=group, failures=failures):
                        self.run_group(group, commands, failures)

    def test_all_advisory_failures_are_reported(self):
        self.env.update(CI_PIPELINE_SOURCE="merge_request_event", CI_MERGE_REQUEST_DIFF_BASE_SHA="abc123")
        self.run_group("advisory", ADVISORY_COMMANDS, failures=(0, 1))

    def test_failed_fetch_still_runs_ledger_and_fails_group(self):
        self.env.update(CI_PIPELINE_SOURCE="merge_request_event", CI_MERGE_REQUEST_TARGET_BRANCH_NAME="release/next")
        self.run_group("generated", GENERATED_COMMANDS, failures=(1,))
