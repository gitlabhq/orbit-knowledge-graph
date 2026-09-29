import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CI = Path(__file__).resolve().parents[1]


class GeneratorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = os.environ.copy()

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def run_script(self, script, *args, status=0):
        result = subprocess.run(
            [sys.executable, str(CI / script), *args], cwd=self.root,
            env=self.env, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, status, result.stdout + result.stderr)
        return result.stdout + result.stderr


class DashboardsTests(GeneratorTests):
    def test_generate_both_flavors_then_check_bytes_and_report_all_drift(self):
        self.write("dashboards/orbit/b.dashboard.jsonnet", '{z: 2, a: {z: "é", a: std.extVar("flavor")}}')
        self.write("dashboards/orbit/a.dashboard.jsonnet", "{empty: [], object: {}, enabled: true}")
        self.write("dashboards/orbit/helper.jsonnet", "invalid helper ignored")
        self.write("dashboards/orbit/nested/ignored.dashboard.jsonnet", "not recursive")
        output = self.run_script("dashboards.py")
        self.assertLess(output.index("orbit/a.dashboard.json"), output.index("orbit/b.dashboard.json"))
        for directory, flavor in (("orbit", "com"), ("dedicated", "dedicated")):
            expected = '{\n  "a": {\n    "a": "' + flavor + '",\n    "z": "é"\n  },\n  "z": 2\n}\n'
            path = self.root / f"dashboards/{directory}/b.dashboard.json"
            self.assertEqual(path.read_bytes(), expected.encode("utf-8"))
        self.assertIn("2 sources", self.run_script("dashboards.py", "--check"))
        for directory in ("orbit", "dedicated"):
            self.write(f"dashboards/{directory}/b.dashboard.json", "stale\r\n")
        self.assertIn("2 dashboard(s) stale", self.run_script("dashboards.py", "--check", status=1))
        self.assertEqual(path.read_bytes(), b"stale\r\n")

    def test_missing_output_empty_sources_and_jsonnet_failure(self):
        source = self.write("custom/orbit/test.dashboard.jsonnet", "{value: 1}")
        self.run_script("dashboards.py", "-d", "custom/orbit", "--check", status=1)
        self.assertFalse((self.root / "custom/dedicated").exists())
        source.write_text("invalid jsonnet")
        self.assertIn("jsonnet failed", self.run_script("dashboards.py", "--dir", "custom/orbit", status=1))
        source.unlink()
        self.assertIn("no `*.dashboard.jsonnet`", self.run_script("dashboards.py", "-d", "custom/orbit", status=1))


class IntegrationLanesTests(GeneratorTests):
    def setUp(self):
        super().setUp()
        self.config = self.write(".gitlab-ci.yml", """
.template:
  rules: [{when: never}]
integration-test:
  rules: !reference [.template, rules]
  variables:
    NEXTEST_FILTER: >-
      test(first) |
      test(second)
integration-test-data-correctness:
  variables: {NEXTEST_FILTER: 'test(data)'}
corpus-smoke-test:
  variables: {NEXTEST_FILTER: 'test(corpus)'}
""")
        cargo = self.write("bin/cargo", f"""#!{sys.executable}
import json
from pathlib import Path
import sys

arguments = sys.argv[1:]
assert arguments[:9] == ['nextest', 'list', '--all-features', '--test', 'containers', '-p', 'integration-tests', '--message-format', 'oneline'], arguments
assert len(arguments) == 9 or (len(arguments) == 11 and arguments[9] == '-E'), arguments
with Path('calls.jsonl').open('a') as log:
    log.write(json.dumps(arguments) + '\\n')
key = arguments[-1] if len(arguments) == 11 else 'all'
response = json.loads(Path('responses.json').read_text())[key]
sys.stdout.write(response.get('stdout', ''))
sys.stderr.write(response.get('stderr', ''))
sys.exit(response.get('status', 0))
""")
        cargo.chmod(0o755)
        self.env["PATH"] = str(cargo.parent) + os.pathsep + self.env["PATH"]
        self.responses = {
            "all": {"stdout": "containers first\n\ncontainers\tdata\r\ncontainers corpus\ncontainers first\n"},
            "test(first) | test(second)": {"stdout": "containers first\ncontainers first\n"},
            "test(data)": {"stdout": "containers data\n"},
            "test(corpus)": {"stdout": "containers corpus\n"},
        }

    def check(self, status=0):
        self.write("responses.json", json.dumps(self.responses))
        return self.run_script("integration_lanes.py", "--check", status=status)

    def test_exact_partition_and_nextest_arguments(self):
        self.assertIn("All 3 container tests", self.check())
        calls = [json.loads(line) for line in (self.root / "calls.jsonl").read_text().splitlines()]
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[1][-2:], ["-E", "test(first) | test(second)"])

    def test_missing_and_duplicate_assignments_are_reported_in_sorted_order(self):
        self.responses["test(first) | test(second)"]["stdout"] = "containers data\n"
        self.responses["test(corpus)"]["stdout"] = "containers data\ncontainers corpus\n"
        output = self.check(status=1)
        self.assertIn("missing from every integration lane:\n  first\n", output)
        self.assertIn("  data: corpus-smoke-test, integration-test, integration-test-data-correctness\n", output)

    def test_filtered_tests_outside_universe_do_not_affect_partition(self):
        for key in ("test(data)", "test(corpus)"):
            self.responses[key]["stdout"] += "containers extra\n"
        self.check()

    def test_empty_malformed_and_failed_nextest_output(self):
        for response, message in (
            ({"stdout": "\n"}, "listed no container tests"),
            ({"stdout": "missing-separator\n"}, "unexpected cargo nextest list output"),
            ({"stdout": "containers  \n"}, "unexpected cargo nextest list output"),
            ({"status": 7, "stderr": "compiler failed"}, "cargo nextest list failed:\ncompiler failed"),
        ):
            with self.subTest(response=response):
                self.responses["all"] = response
                self.assertIn(message, self.check(status=1))

    def test_requires_check_and_valid_explicit_lane_filters(self):
        self.assertIn("pass --check", self.run_script("integration_lanes.py", status=1))
        original = self.config.read_text()
        for replacement in ("{}", "{NEXTEST_FILTER: 42}", "{NEXTEST_FILTER: '  '}"):
            with self.subTest(replacement=replacement):
                self.config.write_text(original.replace("{NEXTEST_FILTER: 'test(data)'}", replacement))
                self.assertIn("integration-test-data-correctness", self.check(status=1))
        self.config.write_text(original.replace("corpus-smoke-test:", "other-job:"))
        self.assertIn("missing integration lane job corpus-smoke-test", self.check(status=1))

    def test_rejects_unsafe_yaml_tags(self):
        self.config.write_text("danger: !!python/object/apply:os.system ['exit 0']")
        self.assertIn("could not determine a constructor", self.check(status=1))
        self.assertFalse((self.root / "calls.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
