import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent.parent
PLUGIN = ROOT / "plugins/orbit"


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


packaging = load_script("package-agent-plugin")
versions = load_script("check-skill-version-bump")


class AgentPluginTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="orbit plugin ")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.archive = self.directory / "plugin.zip"
        packaging.package(self.archive)
        self.install = self.directory / "installed"
        with ZipFile(self.archive) as archive:
            archive.extractall(self.install)
        self.plugin = self.install / "plugins/orbit"

    def test_manifests_and_marketplaces_agree(self):
        portable = json.loads((self.plugin / "plugin.json").read_text())
        claude = json.loads((self.plugin / ".claude-plugin/plugin.json").read_text())
        for key in ("name", "version", "description", "author", "repository"):
            self.assertEqual(portable[key], claude[key], key)
        self.assertEqual(portable["extensions"]["com.openai"]["hooks"], [])
        self.assertEqual(portable["extensions"]["com.openai"]["interface"]["displayName"], "Orbit CLI")
        skill = self.plugin / "skills/orbit-cli/SKILL.md"
        self.assertEqual(versions.parse_version_from_content(skill.read_text()), portable["version"])
        self.assertEqual(list((self.plugin / "skills").iterdir()), [skill.parent])
        for catalog in (".claude-plugin/marketplace.json", ".agents/plugins/marketplace.json"):
            marketplace = json.loads((self.install / catalog).read_text())
            self.assertEqual(marketplace["name"], "gitlab-orbit")
            self.assertEqual(len(marketplace["plugins"]), 1)
            entry = marketplace["plugins"][0]
            self.assertEqual(entry["name"], portable["name"])
            source = entry["source"]
            path = source if isinstance(source, str) else source["path"]
            self.assertEqual((self.install / path).resolve(), self.plugin.resolve())
        self.assertTrue((self.plugin / claude["hooks"]).is_file())
        self.assertFalse((self.plugin / "hooks/hooks.json").exists())
        self.assertFalse((self.plugin / "mcp.json").exists())
        self.assertFalse((self.plugin / ".mcp.json").exists())

    def test_archive_is_reproducible_and_self_contained(self):
        second = self.directory / "second.zip"
        packaging.package(second)
        self.assertEqual(self.archive.read_bytes(), second.read_bytes())
        self.assertEqual((self.install / "LICENSE.md").read_bytes(), (ROOT / "LICENSE.md").read_bytes())
        for source in PLUGIN.rglob("*"):
            if source.is_file():
                self.assertEqual(source.read_bytes(), (self.plugin / source.relative_to(PLUGIN)).read_bytes())

    def test_hooks_forward_input_and_fail_open(self):
        manifest = json.loads((self.plugin / ".claude-plugin/plugin.json").read_text())
        config = json.loads((self.plugin / manifest["hooks"]).read_text())
        self.assertEqual(set(config["hooks"]), {"PreToolUse"})
        bin_dir = self.directory / "bin"
        bin_dir.mkdir()
        (bin_dir / "sh").symlink_to("/bin/sh")
        fake = bin_dir / "orbit"
        payload = json.dumps({"tool_input": {"command": "rg function src"}})
        env = dict(os.environ, PATH=str(bin_dir), CLAUDE_PLUGIN_ROOT=str(self.plugin))
        cases = (
            ("missing", None),
            ("success", f'printf "%s\\n" "$*"\n{shlex.quote(shutil.which("cat"))}\n'),
            ("failure", 'printf "failure" >&2\nexit 1\n'),
        )
        for name, body in cases:
            if body is not None:
                fake.write_text("#!/bin/sh\n" + body)
                fake.chmod(0o755)
            for hook, kind in zip(config["hooks"]["PreToolUse"], ("search", "read"), strict=True):
                with self.subTest(case=name, kind=kind):
                    result = subprocess.run(
                        ["/bin/sh", "-c", hook["hooks"][0]["command"]],
                        input=payload, text=True, capture_output=True, env=env, check=True,
                    )
                    expected = f"hook-guard {kind}\n{payload}" if name == "success" else ""
                    self.assertEqual((result.stdout, result.stderr), (expected, ""))


if __name__ == "__main__":
    unittest.main()
