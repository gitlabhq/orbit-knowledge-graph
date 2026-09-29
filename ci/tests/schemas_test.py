import json
import os
import subprocess
import sys

from generators_test import CI, GeneratorTests


BATCHES = (
    ("ontology", "ontology", "config/ontology/nested/entity.yaml"),
    ("named-queries", "named_query", "config/named_queries/nested/reference.yaml"),
    ("versions", "versions", "config/versions.yaml"),
    ("migration-ledger", "schema-migrations", "config/schema-migrations.yaml"),
    ("indexer-scenarios", "indexer_scenario", "crates/integration-tests/tests/indexer/scenarios/nested/reference.yaml"),
    ("setup", "setup_agent", "config/setup/agents/nested/reference.yaml"),
    ("setup", "setup", "config/setup/setup.yaml"),
)


class SchemaTests(GeneratorTests):
    def setUp(self):
        super().setUp()
        self.script = self.write("ci/validate-schemas.py", (CI / "validate-schemas.py").read_text())
        for _, schema, filename in BATCHES:
            self.write(f"config/schemas/{schema}.schema.json", json.dumps({
                "type": "object",
                "properties": {"kind": {"const": schema}},
                "required": ["kind"],
            }))
            self.write(filename, f"kind: {schema}\n")
        for filename in (
            "config/ontology/reference.yaml",
            "config/ontology/nested/reference.yaml",
            "config/ontology/nested/ignored.yml",
            "config/named_queries/nested/ignored.json",
            "config/setup/ignored.yaml",
        ):
            self.write(filename, "invalid: true\n")

    def check(self, *args, status=0, stdin=subprocess.DEVNULL):
        result = subprocess.run(
            [sys.executable, str(self.script), *args], cwd=self.root / "ci",
            env=self.env, stdin=stdin, capture_output=True, text=True, timeout=15,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, status, output)
        return output

    def test_default_and_all_validate_every_batch(self):
        for args in ((), ("all",)):
            with self.subTest(args=args):
                output = self.check(*args)
                self.assertEqual(output.count("ok -- validation done"), len(BATCHES))

    def test_selectors_validate_nested_files_and_only_ontology_excludes_reference(self):
        for _, _, filename in BATCHES:
            self.write(filename, "invalid: true\n")
        for selection in dict.fromkeys(batch[0] for batch in BATCHES):
            with self.subTest(selection=selection):
                output = self.check(selection, status=1)
                for owner, _, filename in BATCHES:
                    if owner == selection:
                        self.assertIn(filename, output)
                    else:
                        self.assertNotIn(filename, output)
                self.assertNotIn("config/ontology/reference.yaml", output)
                self.assertNotIn("config/ontology/nested/reference.yaml", output)

    def test_ontology_schema_size_boundary(self):
        path = self.root / "config/schemas/ontology.schema.json"
        schema = path.read_bytes()
        for size, status in ((65536, 0), (65537, 1)):
            with self.subTest(size=size):
                path.write_bytes(schema.ljust(size, b" "))
                output = self.check("all", status=status)
                self.assertEqual(output.count("ok -- validation done"), len(BATCHES))
                self.assertEqual("must stay at or below 64 KB" in output, bool(status))

    def test_all_reports_multiple_failures_including_size_limit(self):
        for _, _, filename in BATCHES:
            self.write(filename, "invalid: true\n")
        path = self.root / "config/schemas/ontology.schema.json"
        for oversized in (False, True):
            with self.subTest(oversized=oversized):
                if oversized:
                    path.write_bytes(path.read_bytes().ljust(65537, b" "))
                output = self.check("all", status=1)
                for _, _, filename in BATCHES:
                    self.assertIn(filename, output)
                self.assertEqual("must stay at or below 64 KB" in output, oversized)

    def test_empty_batches_fail_without_reading_stdin_and_remaining_batches_run(self):
        for selection, schema, filename in BATCHES:
            with self.subTest(schema=schema):
                path = self.root / filename
                content = path.read_text()
                path.unlink()
                read_pipe, write_pipe = os.pipe()
                with os.fdopen(read_pipe) as stdin, os.fdopen(write_pipe, "w"):
                    output = self.check(selection, status=1, stdin=stdin)
                self.assertIn("No files matched", output)
                self.assertIn(f"{schema}.schema.json", output)
                output = self.check("all", status=1)
                self.assertEqual(output.count("ok -- validation done"), len(BATCHES) - 1)
                self.write(filename, content)
