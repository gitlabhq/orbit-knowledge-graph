import json
import os
from pathlib import Path
import sys

import yaml

from generators_test import CI, GeneratorTests


class VendoredChecksFixture(GeneratorTests):
    def setUp(self):
        super().setUp()
        self.env = {
            key: value for key, value in self.env.items()
            if not key.startswith(("CI", "GIT_", "SKIP_", "VENDOR_")) and key != "BASE_REF"
        }
        for filename in (
            "ci/check_vendored.py", "ci/skip_check.py", "scripts/vendored/run.sh",
            "scripts/vendored/gitlab_system_note_actions/check.sh",
        ):
            self.write(filename, (CI.parent / filename).read_text()).chmod(0o755)
        self.entries = {}

    def vendor(self, name, commands="true"):
        script = f"scripts/vendored/{name}/check.sh"
        self.write(script, f'#!/usr/bin/env bash\nset -euo pipefail\nprintf "%s\\n" "$VENDOR_NAME" >> calls.txt\n{commands}\n').chmod(0o755)
        self.entries[name] = {
            "version": f"{name}-version", "vendor_dir": f"artifacts/{name}", "check_script": script,
        }

    def check(self, *args, status=0):
        self.write("config/versions.yaml", yaml.safe_dump({"vendored": self.entries}, sort_keys=False))
        return self.run_script(str(self.root / "ci/check_vendored.py"), *args, status=status)

    def calls(self):
        path = self.root / "calls.txt"
        return path.read_text().splitlines() if path.exists() else []


class VendoredChecksTests(VendoredChecksFixture):
    def test_failed_first_check_keeps_errexit_and_runs_later_checks(self):
        self.vendor("first", 'false\nprintf "must not run\\n" >> calls.txt')
        self.vendor("second")
        self.vendor("third", "exit 7")
        output = self.check(status=1)
        self.assertEqual(self.calls(), ["first", "second", "third"])
        self.assertIn("first: check failed (exit 1)", output)
        self.assertIn("third: check failed (exit 7)", output)

    def test_all_checks_only_entries_with_check_scripts(self):
        self.entries["no_check"] = {"vendor_dir": "artifacts/no_check"}
        self.entries["null_check"] = {"check_script": None}
        self.vendor("first")
        self.vendor("second")
        self.check("all")
        self.assertEqual(self.calls(), ["first", "second"])

    def test_named_check_preserves_runner_environment_and_active_python(self):
        self.vendor("first", "exit 7")
        self.vendor("second", """python -c 'import json, os, sys; from pathlib import Path; Path("environment.json").write_text(json.dumps({key: value for key, value in os.environ.items() if key.startswith("VENDOR_")} | {"python": sys.executable}))'""")
        self.check("second")
        self.assertEqual(self.calls(), ["second"])
        environment = json.loads((self.root / "environment.json").read_text())
        self.assertEqual(Path(environment.pop("python")).parent, Path(sys.executable).parent)
        self.assertEqual(environment, {
            "VENDOR_NAME": "second",
            "VENDOR_VERSION": "second-version",
            "VENDOR_DIR": str(self.root.resolve() / "artifacts/second"),
            "VENDOR_VERSIONS_FILE": str(self.root.resolve() / "config/versions.yaml"),
        })

    def test_failed_precondition_does_not_prevent_later_check(self):
        self.vendor("first")
        (self.root / self.entries["first"]["check_script"]).chmod(0o644)
        self.vendor("second")
        self.assertIn("is not executable", self.check(status=1))
        self.assertEqual(self.calls(), ["second"])

    def test_read_only_postcondition_failure_does_not_prevent_later_check(self):
        self.vendor("first", 'printf "\\n" >> "$VENDOR_VERSIONS_FILE"')
        self.vendor("second")
        self.assertIn("must be read-only", self.check(status=1))
        self.assertEqual(self.calls(), ["first", "second"])

    def test_unknown_named_dependency_fails_without_running_other_checks(self):
        self.vendor("first")
        self.assertIn("No vendor_dir", self.check("unknown", status=1))
        self.assertEqual(self.calls(), [])

    def test_no_checks_is_success(self):
        self.check()
        self.assertEqual(self.calls(), [])


class SystemNoteActionsChecksTests(VendoredChecksFixture):
    def setUp(self):
        super().setUp()
        self.entries["gitlab_system_note_actions"] = {
            "version": "abc123", "vendor_dir": "config/vendored",
            "check_script": "scripts/vendored/gitlab_system_note_actions/check.sh",
        }
        self.write("config/vendored/system_note_metadata.actions", "added\nremoved\n")
        self.write("bin/curl", f"""#!{sys.executable}
from pathlib import Path
with Path('fetches.txt').open('a') as log:
    log.write('curl\\n')
print('ICON_TYPES = %w[added removed]')
""").chmod(0o755)
        self.env["PATH"] = str(self.root / "bin") + os.pathsep + self.env["PATH"]

    def test_environment_description_and_title_skip_without_upstream_fetch(self):
        for key, value in (
            ("SKIP_SYSTEM_NOTE_ACTIONS_CHECK", "1"),
            ("CI_MERGE_REQUEST_DESCRIPTION", "text [skip system-note-actions-check] text"),
            ("CI_MERGE_REQUEST_TITLE", "text [skip system-note-actions-check]"),
        ):
            with self.subTest(key=key):
                self.env[key] = value
                self.assertIn("skipping", self.check("gitlab_system_note_actions"))
                del self.env[key]
        self.assertFalse((self.root / "fetches.txt").exists())

    def test_commit_skip_requires_exported_base_ref(self):
        self.write("bin/git", f"""#!{sys.executable}
from pathlib import Path
import sys
with Path('git_calls.txt').open('a') as log:
    log.write(' '.join(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'fetch':
    sys.exit(1)
print('[skip system-note-actions-check]')
""").chmod(0o755)
        self.env["CI_MERGE_REQUEST_DIFF_BASE_SHA"] = "abc123"
        self.assertIn("matches upstream", self.check("gitlab_system_note_actions"))
        self.assertFalse((self.root / "git_calls.txt").exists())
        fetches = (self.root / "fetches.txt").read_text()
        self.env["BASE_REF"] = "custom base"
        self.assertIn("skipping", self.check("gitlab_system_note_actions"))
        self.assertEqual((self.root / "fetches.txt").read_text(), fetches)
        self.assertEqual((self.root / "git_calls.txt").read_text().splitlines(), [
            "fetch origin abc123 --depth=1", "log custom base...HEAD --format=%B",
        ])

    def test_upstream_drift_fails_and_later_check_still_runs(self):
        self.write("config/vendored/system_note_metadata.actions", "stale\n")
        self.vendor("later")
        self.assertIn("DRIFT:", self.check(status=1))
        self.assertEqual(self.calls(), ["later"])
