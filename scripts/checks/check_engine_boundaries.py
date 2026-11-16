#!/usr/bin/env python3
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINE = "indexing-engine"
ENGINE_MAY_USE = frozenset(
    {"circuit-breaker", "clickhouse-client", "nats-client", "orbit-log-fields", "orbit-observability"}
)
FLOWS_DIR = "crates/flows/"
FROZEN_DIRS = ("crates/indexer/src/engine", "crates/indexer/src/orchestrator")
FROZEN_LIST = "scripts/checks/frozen-v1-indexer-files.txt"
DEPENDENCY_KINDS = ("dependencies", "dev-dependencies", "build-dependencies")


def main() -> int:
    packages = load_workspace_packages()
    problems = (
        check_engine_dependencies(packages)
        + check_flow_isolation(packages)
        + check_frozen_files(list_frozen_dir_files(), read_frozen_list())
    )
    for problem in problems:
        print(f"error: {problem}")
    return 1 if problems else 0


def check_engine_dependencies(packages: list[dict]) -> list[str]:
    engine = next((package for package in packages if package["name"] == ENGINE), None)
    if engine is None:
        return []
    return [
        f"{ENGINE} depends on {name}. The engine may only use infrastructure crates "
        f"({', '.join(sorted(ENGINE_MAY_USE))}). Move domain code into a flow under {FLOWS_DIR}."
        for name in list_path_dependencies(engine)
        if name not in ENGINE_MAY_USE
    ]


def check_flow_isolation(packages: list[dict]) -> list[str]:
    flows = {package["name"] for package in packages if is_flow(package)}
    return [
        f"the flow {package['name']} depends on the flow {name}. Start the other team's work "
        "from its own planner with a done trigger instead (ADR 023, decision 4)."
        for package in packages
        if is_flow(package)
        for name in list_path_dependencies(package)
        if name in flows
    ]


def check_frozen_files(present: set[str], frozen: set[str]) -> list[str]:
    added = [
        f"{path} is a new file in a frozen V1 folder. Build new indexing work as a flow on the "
        f"indexing engine (ADR 023), under {FLOWS_DIR}."
        for path in sorted(present - frozen)
    ]
    removed = [
        f"{path} is gone. Remove it from {FROZEN_LIST} so the list only shrinks."
        for path in sorted(frozen - present)
    ]
    return added + removed


def load_workspace_packages() -> list[dict]:
    workspace = read_manifest(ROOT / "Cargo.toml")["workspace"]
    shared = workspace.get("dependencies", {})
    return [read_package(ROOT / member / "Cargo.toml", shared) for member in workspace["members"]]


def read_package(manifest: Path, shared: dict) -> dict:
    content = read_manifest(manifest)
    sections = [content.get(kind, {}) for kind in DEPENDENCY_KINDS]
    sections += [target.get(kind, {}) for target in content.get("target", {}).values() for kind in DEPENDENCY_KINDS]
    dependencies = [
        {"name": name, "path": find_dependency_path(spec, shared.get(name))}
        for section in sections
        for name, spec in section.items()
    ]
    return {"name": content["package"]["name"], "manifest_path": str(manifest), "dependencies": dependencies}


def find_dependency_path(spec, shared_spec) -> str | None:
    if not isinstance(spec, dict):
        return None
    if spec.get("workspace") and isinstance(shared_spec, dict):
        return shared_spec.get("path")
    return spec.get("path")


def read_manifest(path: Path) -> dict:
    with path.open("rb") as manifest:
        return tomllib.load(manifest)


def list_path_dependencies(package: dict) -> list[str]:
    return sorted({dependency["name"] for dependency in package["dependencies"] if dependency.get("path")})


def is_flow(package: dict) -> bool:
    manifest = Path(package["manifest_path"]).resolve().relative_to(ROOT).as_posix()
    return manifest.startswith(FLOWS_DIR)


def list_frozen_dir_files() -> set[str]:
    return {
        path.relative_to(ROOT).as_posix()
        for folder in FROZEN_DIRS
        for path in (ROOT / folder).rglob("*")
        if path.is_file()
    }


def read_frozen_list() -> set[str]:
    lines = (ROOT / FROZEN_LIST).read_text().splitlines()
    return {line.strip() for line in lines if line.strip()}


if __name__ == "__main__":
    sys.exit(main())
