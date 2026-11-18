#!/usr/bin/env python3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_workspace_deps.py")


def run_check(member_manifest, *flags):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/a"]\n')
        (root / "crates/a").mkdir(parents=True)
        (root / "crates/a/Cargo.toml").write_text(member_manifest)
        return subprocess.run(
            [sys.executable, str(SCRIPT), *flags, str(root)], capture_output=True, text=True
        )


class CheckWorkspaceDepsTest(unittest.TestCase):
    def test_inherited_dependency_passes(self):
        result = run_check('[dependencies]\nserde = { workspace = true, features = ["derive"] }\n')
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_crate_local_dependency_fails_in_every_table(self):
        result = run_check(
            '[dependencies]\nserde = "1"\n'
            '[dev-dependencies]\ntempfile = { version = "3" }\n'
            '[build-dependencies]\ncc = "1"\n'
            "[target.'cfg(unix)'.dependencies]\nlibc = \"0.2\"\n"
        )
        self.assertEqual(result.returncode, 1)
        for name in ("serde", "tempfile", "cc", "libc"):
            self.assertIn(f"`{name}`", result.stdout)

    def test_path_dependency_is_ignored_only_in_registry_only_mode(self):
        manifest = '[dependencies]\nsibling = { path = "../sibling" }\n'
        self.assertEqual(run_check(manifest, "--registry-only").returncode, 0)
        self.assertEqual(run_check(manifest).returncode, 1)

    def test_allow_entry_with_reason_passes(self):
        result = run_check(
            '[dependencies]\nzstd = "0.14"\n'
            '[package.metadata.workspace-deps.allow]\nzstd = "needs its own version"\n'
        )
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_allow_entry_without_reason_fails(self):
        result = run_check(
            '[dependencies]\nzstd = "0.14"\n[package.metadata.workspace-deps.allow]\nzstd = ""\n'
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("needs a reason", result.stdout)

    def test_stale_allow_entry_fails(self):
        result = run_check(
            '[dependencies]\nserde = { workspace = true }\n'
            '[package.metadata.workspace-deps.allow]\nzstd = "no longer used"\n'
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("remove it", result.stdout)


if __name__ == "__main__":
    unittest.main()
