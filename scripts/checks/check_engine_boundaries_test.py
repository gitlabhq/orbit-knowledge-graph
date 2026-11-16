import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_engine_boundaries import (
    ENGINE,
    ROOT,
    check_engine_dependencies,
    check_flow_isolation,
    check_frozen_files,
)


def package(name: str, manifest: str, path_dependencies: list[str]) -> dict:
    return {
        "name": name,
        "manifest_path": str(ROOT / manifest),
        "dependencies": [{"name": dependency, "path": f"/{dependency}"} for dependency in path_dependencies]
        + [{"name": "serde", "path": None}],
    }


class EngineDependencies(unittest.TestCase):
    def test_allows_infrastructure_crates(self):
        engine = package(ENGINE, "crates/indexing-engine/Cargo.toml", ["nats-client", "clickhouse-client"])

        self.assertEqual(check_engine_dependencies([engine]), [])

    def test_rejects_a_domain_crate(self):
        engine = package(ENGINE, "crates/indexing-engine/Cargo.toml", ["ontology"])

        problems = check_engine_dependencies([engine])

        self.assertEqual(len(problems), 1)
        self.assertIn("depends on ontology", problems[0])

    def test_passes_when_the_engine_does_not_exist_yet(self):
        self.assertEqual(check_engine_dependencies([]), [])


class FlowIsolation(unittest.TestCase):
    def test_rejects_a_flow_that_depends_on_another_flow(self):
        sdlc = package("flow-sdlc", "crates/flows/sdlc/Cargo.toml", [ENGINE])
        code = package("flow-code", "crates/flows/code/Cargo.toml", [ENGINE, "flow-sdlc"])

        problems = check_flow_isolation([sdlc, code])

        self.assertEqual(len(problems), 1)
        self.assertIn("flow-code depends on the flow flow-sdlc", problems[0])

    def test_allows_a_crate_outside_the_flows_to_use_flows(self):
        sdlc = package("flow-sdlc", "crates/flows/sdlc/Cargo.toml", [ENGINE])
        server = package("orbit-server", "crates/orbit-server/Cargo.toml", ["flow-sdlc"])

        self.assertEqual(check_flow_isolation([sdlc, server]), [])


class FrozenFiles(unittest.TestCase):
    def test_rejects_a_new_file(self):
        problems = check_frozen_files({"a.rs", "new.rs"}, {"a.rs"})

        self.assertEqual(len(problems), 1)
        self.assertIn("new.rs is a new file", problems[0])

    def test_asks_to_shrink_the_list_when_a_file_is_gone(self):
        problems = check_frozen_files({"a.rs"}, {"a.rs", "gone.rs"})

        self.assertEqual(len(problems), 1)
        self.assertIn("gone.rs is gone", problems[0])

    def test_allows_edits_to_frozen_files(self):
        self.assertEqual(check_frozen_files({"a.rs"}, {"a.rs"}), [])


if __name__ == "__main__":
    unittest.main()
