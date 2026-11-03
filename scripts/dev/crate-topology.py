#!/usr/bin/env python3
"""Rank workspace crates by how much of the workspace they rebuild.

Usage:
    python3 scripts/dev/crate-topology.py
    python3 scripts/dev/crate-topology.py --exclude code-graph treesitter-visit --days 30
    python3 scripts/dev/crate-topology.py --measure --touch config/ontology/nodes/agents/agent_session.yaml

Commit counts cover each crate's src/, build.rs and Cargo.toml. Files that a build
script watches outside the crate are not counted; pass them to --measure with --touch.
"""

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BUILD_EDGE_KINDS = {None, "build"}


def run(args, **kwargs):
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True, **kwargs)


def load_workspace(excluded):
    metadata = json.loads(run(["cargo", "metadata", "--format-version", "1", "--no-deps"], check=True).stdout)
    packages = {p["name"]: p for p in metadata["packages"] if p["name"] not in excluded}
    dependencies = {
        name: {
            d["name"]
            for d in package["dependencies"]
            if d.get("path") and d["name"] in packages and d["kind"] in BUILD_EDGE_KINDS and not d.get("optional")
        }
        for name, package in packages.items()
    }
    return packages, dependencies


def reverse(graph):
    reversed_graph = defaultdict(set)
    for node, targets in graph.items():
        for target in targets:
            reversed_graph[target].add(node)
    return reversed_graph


def reachable(graph, start):
    seen, stack = set(), [start]
    while stack:
        for nxt in graph.get(stack.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def crate_dir(package):
    return Path(package["manifest_path"]).parent.relative_to(REPO)


def source_lines(package):
    return sum(len(f.read_bytes().splitlines()) for f in (REPO / crate_dir(package) / "src").rglob("*.rs"))


def commits(package, days):
    root = crate_dir(package)
    paths = [str(root / "src"), str(root / "build.rs"), str(root / "Cargo.toml")]
    log = run(["git", "log", f"--since={days}.days", "--no-merges", "--format=%H", "--", *paths])
    return len(log.stdout.split())


def layers(dependencies):
    depth = {}

    def layer(name):
        if name not in depth:
            depth[name] = 1 + max((layer(d) for d in dependencies[name]), default=0)
        return depth[name]

    return {name: layer(name) for name in dependencies}


def static_report(packages, dependencies, days):
    dependents = reverse(dependencies)
    layer = layers(dependencies)
    rows = []
    for name, package in packages.items():
        downstream = reachable(dependents, name)
        changes = commits(package, days)
        rows.append({
            "crate": name,
            "layer": layer[name],
            "deps": len(reachable(dependencies, name)),
            "dependents": len(downstream),
            "lines": source_lines(package),
            "commits": changes,
            "rebuilds": len(downstream) * changes,
            "direct": sorted(dependents[name]),
        })
    rows.sort(key=lambda r: (-r["rebuilds"], -r["dependents"]))
    print(f"Window: {days} days. rebuilds = dependents x commits.\n")
    print(f"{'crate':28}{'layer':>6}{'deps':>6}{'dependents':>11}{'lines':>8}{'commits':>8}{'rebuilds':>9}  direct dependents")
    for r in rows:
        print(
            f"{r['crate']:28}{r['layer']:>6}{r['deps']:>6}{r['dependents']:>11}{r['lines']:>8}"
            f"{r['commits']:>8}{r['rebuilds']:>9}  {', '.join(r['direct'])}"
        )


def entry_file(package):
    skipped = {"test", "bench", "example", "custom-build"}
    return next((t["src_path"] for t in package["targets"] if not skipped & set(t["kind"])), None)


def check(top_crates, packages):
    result = run(["cargo", "check", *(arg for c in top_crates for arg in ("-p", c))])
    if result.returncode != 0:
        raise SystemExit(result.stderr)
    return [
        line.split()[1]
        for line in result.stderr.splitlines()
        if line.strip().startswith(("Checking ", "Compiling ")) and line.split()[1] in packages
    ]


def measure_report(packages, dependencies, extra_touches):
    dependents = reverse(dependencies)
    touches = [(name, entry) for name, package in packages.items() if (entry := entry_file(package))]
    top_crates = sorted(name for name, _ in touches if not dependents[name])
    if (REPO / ".git").is_dir():
        touches.append(("(git HEAD)", str(REPO / ".git" / "HEAD")))
    touches += [(path, str(REPO / path)) for path in extra_touches]

    print(f"cargo check -p {' -p '.join(top_crates)}\n")
    check(top_crates, packages)
    rows = [("(nothing)", check(top_crates, packages))]
    for label, path in touches:
        os.utime(path)
        rows.append((label, check(top_crates, packages)))
    rows.sort(key=lambda r: -len(r[1]))
    print("re-checked counts the touched crate itself.\n")
    print(f"{'touched':48}{'re-checked':>11}  crates")
    for label, rebuilt in rows:
        print(f"{label:48}{len(rebuilt):>11}  {', '.join(rebuilt)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--exclude", nargs="*", default=[], help="crates to leave out of the graph")
    parser.add_argument("--days", type=int, default=90, help="git history window for commit counts")
    parser.add_argument("--measure", action="store_true", help="touch each crate and count what cargo check rebuilds")
    parser.add_argument("--touch", nargs="*", default=[], help="extra repo-relative files to touch in --measure")
    args = parser.parse_args()

    packages, dependencies = load_workspace(set(args.exclude))
    if args.measure:
        measure_report(packages, dependencies, args.touch)
    else:
        static_report(packages, dependencies, args.days)


if __name__ == "__main__":
    main()
