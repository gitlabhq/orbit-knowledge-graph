#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict

LOCK_NAME = re.compile(r'^[+ -]?name = "([^"]+)"')
LOCK_VERSION = re.compile(r'^([+-])version = "([^"]+)"')


def run_query(body):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump({"query": body, "response_format": "raw"}, f)
        path = f.name
    out = subprocess.run(
        ["glab", "orbit", "remote", "query", path],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"query failed:\n{out.stderr}")
    return json.loads(out.stdout)["result"]


def bumped_packages(repo, diff_range, lockfiles=("Cargo.lock", "package-lock.json", "yarn.lock")):
    diff = subprocess.run(
        ["git", "-C", repo, "diff", "-U3", diff_range, "--", *lockfiles],
        capture_output=True, text=True, check=True,
    ).stdout
    bumps = {}
    current = None
    for line in diff.splitlines():
        m = LOCK_NAME.match(line)
        if m:
            current = m.group(1)
            continue
        m = LOCK_VERSION.match(line)
        if m and current:
            old_new = bumps.setdefault(current, [None, None])
            old_new[0 if m.group(1) == "-" else 1] = m.group(2)
    return {k: v for k, v in bumps.items() if v[0] and v[1] and v[0] != v[1]}


def crate_import_prefix(package):
    return package.replace("-", "_")


def usage_sites(project_id, package, limit):
    result = run_query({
        "query_type": "traversal",
        "nodes": [{
            "id": "s",
            "entity": "ImportedSymbol",
            "filters": {
                "project_id": project_id,
                "import_path": {"starts_with": crate_import_prefix(package)},
            },
            "columns": ["identifier_name", "import_path", "file_path", "start_line"],
        }],
        "limit": limit,
    })
    return result.get("nodes", []), result.get("pagination", {}).get("has_more", False)


def main():
    ap = argparse.ArgumentParser(description="Dependency-bump blast radius via Orbit")
    ap.add_argument("project_id", type=int)
    ap.add_argument("--diff-range", default="origin/main...HEAD")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--package", help="report a single package; skips lockfile diff parsing")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--max-files", type=int, default=20)
    args = ap.parse_args()

    if args.package:
        bumps = {args.package: ("?", "?")}
    else:
        bumps = bumped_packages(args.repo, args.diff_range)
    if not bumps:
        print("No lockfile version bumps detected; nothing to report.")
        return

    print("## Dependency bump usage report\n")
    any_usage = False
    for package, (old, new) in sorted(bumps.items()):
        sites, truncated = usage_sites(args.project_id, package, args.limit)
        if not sites:
            continue
        any_usage = True
        per_file = defaultdict(list)
        for s in sites:
            per_file[s["file_path"]].append(s["identifier_name"])
        symbols = sorted({s["identifier_name"] for s in sites})

        print(f"### `{package}` {old} → {new}\n")
        print(f"{len(sites)} import site(s) across {len(per_file)} file(s); "
              f"symbols: {', '.join(f'`{s}`' for s in symbols[:15])}"
              + (f" (+{len(symbols) - 15} more)" if len(symbols) > 15 else "") + "\n")
        print("| File | Imported symbols |")
        print("|---|---|")
        ranked = sorted(per_file.items(), key=lambda kv: -len(kv[1]))
        for path, names in ranked[:args.max_files]:
            print(f"| {path} | {', '.join(sorted(set(names)))} |")
        if len(ranked) > args.max_files:
            print(f"\n…and {len(ranked) - args.max_files} more file(s).")
        if truncated:
            print("\n⚠️ Import sites truncated at query limit.")
        print()

    if not any_usage:
        print("Bumped packages are not imported anywhere in this project "
              "(transitive or non-code dependencies).")


if __name__ == "__main__":
    main()
