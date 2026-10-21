#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_CONFIG = REPOSITORY_ROOT / "code-indexing-benchmark.yaml"


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
) -> None:
    subprocess.run(command, cwd=REPOSITORY_ROOT, env=environment, check=True)


def output(command: list[str]) -> str:
    return subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def write_config(config_path: Path, repositories_path: Path) -> None:
    config = SOURCE_CONFIG.read_text(encoding="utf-8")
    config = config.replace("repos_dir: temp", f"repos_dir: {json.dumps(str(repositories_path))}")
    config_path.write_text(config, encoding="utf-8")


def configured_repositories(config_path: Path) -> list[str]:
    repositories = json.loads(
        output(
            [
                "gitlab-xtasks",
                "benchmark",
                "list-repos",
                "--config",
                str(config_path),
                "--format",
                "json",
            ]
        )
    )
    return [repository["name"] for repository in repositories]


def prepare(benchmark_root: Path) -> tuple[Path, Path, dict[str, str]]:
    repositories_path = benchmark_root / "repos"
    target_path = benchmark_root / "target"
    config_path = benchmark_root / "config.yaml"
    benchmark_root.mkdir(parents=True, exist_ok=True)
    write_config(config_path, repositories_path)
    run(
        [
            "gitlab-xtasks",
            "benchmark",
            "download-repos",
            "--config",
            str(config_path),
            "--output-dir",
            str(repositories_path),
        ]
    )
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target_path)
    run(
        [
            "cargo",
            "build",
            "-p",
            "orbit-cli",
            "--bin",
            "orbit",
            "--release",
            "--features",
            "duckdb-client/bundled",
        ],
        environment=environment,
    )
    environment["PATH"] = f"{target_path / 'release'}{os.pathsep}{environment['PATH']}"
    return config_path, repositories_path, environment


def measured_run(
    benchmark_root: Path,
    config_path: Path,
    repositories_path: Path,
    environment: dict[str, str],
) -> None:
    results_path = benchmark_root / "results"
    results_path.mkdir(exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    result_file = results_path / f"{timestamp}-local-cg.json"
    run(
        [
            "gitlab-xtasks",
            "benchmark",
            "hyperfine",
            "--config",
            str(config_path),
            "--architecture",
            "local",
            "--scenario",
            "cg",
            "--repos-dir",
            str(repositories_path),
            "--output-file",
            str(result_file),
        ],
        environment=environment,
    )
    print(f"Benchmark result: {result_file}")


def smoke_run(
    config_path: Path,
    repositories_path: Path,
    environment: dict[str, str],
) -> None:
    executable = repositories_path.parent / "target" / "release" / "orbit"
    if sys.platform == "win32":
        executable = executable.with_suffix(".exe")
    for repository in configured_repositories(config_path):
        print(f"=== Indexing {repository} with code-graph ===", flush=True)
        run(
            [str(executable), "index", str(repositories_path / repository), "--stats"],
            environment=environment,
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["run", "smoke"])
    engine = parser.add_mutually_exclusive_group(required=True)
    engine.add_argument("--cg", action="store_true")
    engine.add_argument("--cgi", action="store_true")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    if arguments.cgi:
        print("code-graph-incremental is not wired yet; skipping.")
        return
    if sys.platform == "win32":
        default_root = Path(os.environ["TEMP"]) / "orbit-code-indexing-benchmark"
    else:
        default_root = Path("/tmp/orbit-code-indexing-benchmark")
    benchmark_root = Path(os.environ.get("ORBIT_BENCH_ROOT", default_root)).expanduser().resolve()
    config_path, repositories_path, environment = prepare(benchmark_root)
    if arguments.action == "run":
        measured_run(benchmark_root, config_path, repositories_path, environment)
    else:
        smoke_run(config_path, repositories_path, environment)


if __name__ == "__main__":
    main()
