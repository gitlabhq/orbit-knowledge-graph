#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import PurePosixPath

LOCK_NAME = re.compile(r'^[+ -]?name = "([^"]+)"')
LOCK_VERSION = re.compile(r'^([+-])version = "([^"]+)"')
LOCKFILES = ("Cargo.lock", "package-lock.json", "yarn.lock", "Gemfile.lock", "go.sum")

EXPORT_PATTERNS = {
    (".js", ".jsx", ".ts", ".tsx", ".mjs", ".vue"): [
        re.compile(r"^\+.*\bexport\s+(?:default\s+)?(?:async\s+)?(?:function|class|const|let|var)\s+(\w+)"),
        re.compile(r"^\+.*\bexport\s+\{([^}]+)\}"),
    ],
    (".rs",): [
        re.compile(r"^\+pub\s+(?:async\s+)?(?:fn|struct|enum|trait|type|const)\s+(\w+)"),
    ],
    (".go",): [
        re.compile(r"^\+(?:func|type|const|var)\s+([A-Z]\w*)"),
    ],
    (".proto",): [
        re.compile(r"^\+\s*(?:message|enum|service)\s+(\w+)"),
        re.compile(r"^\+\s*rpc\s+(\w+)"),
    ],
}

GO_MODULE = re.compile(r"^module\s+(\S+)")
GEMSPEC_NAME = re.compile(r"""\.name\s*=\s*['"]([^'"]+)['"]|name\s*=\s*['"]([^'"]+)['"]""")


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


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", repo, *args], capture_output=True, text=True, check=True,
    ).stdout


def patterns_for(path):
    suffix = PurePosixPath(path).suffix
    for exts, pats in EXPORT_PATTERNS.items():
        if suffix in exts:
            return pats
    return []


def changed_exports(repo, diff_range):
    diff = git(repo, "diff", "-U0", diff_range)
    per_file = defaultdict(set)
    current = None
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            current = line[6:]
            continue
        if current is None or not line.startswith("+"):
            continue
        content = line[1:].lstrip()
        if content.startswith(("//", "/*", "*", "#")):
            continue
        for pat in patterns_for(current):
            m = pat.search(line)
            if not m:
                continue
            for name in re.split(r",", m.group(1)):
                name = name.split(" as ")[-1].strip()
                if name and name != "default" and name[0].isalpha():
                    per_file[current].add(name)
    return per_file


def discover_units(repo):
    units = []
    files = git(repo, "ls-files").splitlines()
    for path in files:
        p = PurePosixPath(path)
        if p.name == "go.mod":
            m = GO_MODULE.search(open(f"{repo}/{path}").read())
            if m:
                units.append({"namespace": m.group(1), "dir": str(p.parent)})
        elif p.suffix == ".gemspec":
            m = GEMSPEC_NAME.search(open(f"{repo}/{path}").read())
            if m:
                units.append({"namespace": m.group(1) or m.group(2), "dir": str(p.parent)})
        elif p.name == "package.json" and "node_modules" not in p.parts:
            try:
                pkg = json.load(open(f"{repo}/{path}"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if pkg.get("name") and not pkg.get("private"):
                units.append({"namespace": pkg["name"], "dir": str(p.parent)})
    return units


def attribute_symbols(per_file, units):
    for unit in units:
        unit["symbols"] = set()
    for path, symbols in per_file.items():
        is_proto = path.endswith(".proto")
        for unit in units:
            in_dir = unit["dir"] == "." or path.startswith(unit["dir"] + "/")
            if in_dir or is_proto:
                unit["symbols"] |= symbols
    return [u for u in units if u["symbols"]]


def bumped_packages(repo, diff_range):
    diff = git(repo, "diff", "-U3", diff_range, "--", *LOCKFILES)
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


def consumer_counts(namespace, symbols, limit):
    result = run_query({
        "query_type": "aggregation",
        "nodes": [{
            "id": "s",
            "entity": "ImportedSymbol",
            "filters": {
                "import_path": {"contains": namespace},
                "identifier_name": {"in": sorted(symbols)[:100]},
            },
        }],
        "relationships": [],
        "group_by": ["s.project_id", "s.identifier_name"],
        "aggregations": [{"count": "s", "as": "usages"}],
        "limit": limit,
    })
    rows = result.get("rows", [])
    truncated = result.get("pagination", {}).get("has_more", False)
    return rows, truncated


def resolve_projects(ids):
    projects = {}
    ids = sorted(ids)
    for i in range(0, len(ids), 100):
        result = run_query({
            "query_type": "traversal",
            "nodes": [{
                "id": "p",
                "entity": "Project",
                "filters": {"id": {"in": ids[i:i + 100]}},
                "columns": ["id", "full_path", "archived", "star_count"],
            }],
            "limit": 100,
        })
        for n in result.get("nodes", []):
            projects[int(n["id"])] = n
    return projects


def format_symbols(symbols, head=15):
    symbols = sorted(symbols)
    shown = ", ".join(f"`{s}`" for s in symbols[:head])
    rest = len(symbols) - head
    return shown + (f" (+{rest} more)" if rest > 0 else "")


def namespace_counts(namespace, limit):
    result = run_query({
        "query_type": "aggregation",
        "nodes": [{
            "id": "s",
            "entity": "ImportedSymbol",
            "filters": {"import_path": {"contains": namespace}},
        }],
        "relationships": [],
        "group_by": ["s.project_id"],
        "aggregations": [{"count": "s", "as": "usages"}],
        "limit": limit,
    })
    rows = result.get("rows", [])
    truncated = result.get("pagination", {}).get("has_more", False)
    return rows, truncated


def print_producer_report(unit, own_project_id, limit, min_stars):
    rows, truncated = consumer_counts(unit["namespace"], unit["symbols"], limit)
    symbol_level = True
    per_project = defaultdict(dict)
    for r in rows:
        pid = int(r["s_project_id"])
        if pid == own_project_id:
            continue
        per_project[pid][r["s_identifier_name"]] = int(r["usages"])

    if not per_project:
        symbol_level = False
        rows, truncated = namespace_counts(unit["namespace"], limit)
        for r in rows:
            pid = int(r["s_project_id"])
            if pid == own_project_id:
                continue
            per_project[pid][unit["namespace"].rsplit("/", 1)[-1]] = int(r["usages"])

    if not per_project:
        return False
    print(f"### Consumers of `{unit['namespace']}`\n")
    print(f"Changed symbols: {format_symbols(unit['symbols'])}\n")
    if not symbol_level:
        print("No symbol-level matches (package-level imports, e.g. Go/Ruby); "
              "listing projects that import the package at all.\n")

    projects = resolve_projects(per_project.keys())
    entries = []
    for pid, syms in per_project.items():
        proj = projects.get(pid)
        if proj is None:
            continue
        if str(proj.get("archived")).lower() == "true":
            continue
        entries.append({
            "path": proj["full_path"],
            "stars": int(proj.get("star_count") or 0),
            "total": sum(syms.values()),
            "syms": syms,
        })

    clusters = defaultdict(list)
    deduped = []
    for e in entries:
        if len(e["syms"]) >= 2 or e["total"] >= 5:
            clusters[tuple(sorted(e["syms"].items()))].append(e)
        else:
            deduped.append(e)
    hidden = 0
    for members in clusters.values():
        members.sort(key=lambda e: (-e["stars"], len(e["path"])))
        keep = members[0]
        keep["siblings"] = len(members) - 1
        hidden += keep["siblings"]
        deduped.append(keep)
    entries = sorted(deduped, key=lambda e: (-e["stars"], -e["total"]))

    def row(e):
        note = f" (+{e['siblings']} likely forks/mirrors)" if e.get("siblings") else ""
        cells = f"| {e['path']}{note} | {e['stars']} | {e['total']} |"
        if symbol_level:
            sym_str = ", ".join(f"{k} ({v})" for k, v in sorted(e["syms"].items(), key=lambda kv: -kv[1]))
            cells += f" {sym_str} |"
        return cells

    header = "| Consumer project | Stars | Total usages |" + (" Symbols used |" if symbol_level else "")
    separator = "|---|---|---|" + ("---|" if symbol_level else "")
    main_rows = [e for e in entries if e["stars"] >= min_stars]
    tail = [e for e in entries if e["stars"] < min_stars]
    print(header)
    print(separator)
    for e in main_rows:
        print(row(e))
    if tail:
        print(f"\n<details><summary>{len(tail)} unstarred consumer(s) — likely forks, mirrors, and demos</summary>\n")
        print(header)
        print(separator)
        for e in tail:
            print(row(e))
        print("\n</details>")
    print(f"\n{len(entries)} external consumer project(s) visible to this token"
          + (f"; {hidden} deduped as likely forks/mirrors." if hidden else "."))
    if truncated:
        print("⚠️ Result truncated at query limit; radius may be larger.")
    print()
    return True


def print_consumer_report(package, old, new, project_id, limit, max_files):
    sites, truncated = usage_sites(project_id, package, limit)
    if not sites:
        return False
    per_file = defaultdict(list)
    for s in sites:
        per_file[s["file_path"]].append(s["identifier_name"])
    symbols = {s["identifier_name"] for s in sites}

    print(f"### `{package}` {old} → {new}\n")
    print(f"{len(sites)} import site(s) across {len(per_file)} file(s); "
          f"symbols: {format_symbols(symbols)}\n")
    print("<details><summary>Import sites by file</summary>\n")
    print("| File | Imported symbols |")
    print("|---|---|")
    ranked = sorted(per_file.items(), key=lambda kv: -len(kv[1]))
    for path, names in ranked[:max_files]:
        print(f"| {path} | {', '.join(sorted(set(names)))} |")
    if len(ranked) > max_files:
        print(f"\n…and {len(ranked) - max_files} more file(s).")
    print("\n</details>")
    if truncated:
        print("\n⚠️ Import sites truncated at query limit.")
    print()
    return True


def main():
    ap = argparse.ArgumentParser(description="Two-way dependency blast radius via Orbit")
    ap.add_argument("project_id", type=int)
    ap.add_argument("--diff-range", default="origin/main...HEAD")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--namespace", action="append", default=[],
                    help="extra import namespace to treat as published (repeatable)")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--max-files", type=int, default=20)
    ap.add_argument("--min-stars", type=int, default=1)
    args = ap.parse_args()

    sections = 0

    bumps = bumped_packages(args.repo, args.diff_range)
    if bumps:
        print("## Dependency bumps: what this project uses\n")
        any_usage = False
        for package, (old, new) in sorted(bumps.items()):
            any_usage |= print_consumer_report(
                package, old, new, args.project_id, args.limit, args.max_files)
        if not any_usage:
            print("Bumped packages are not imported anywhere in this project "
                  "(transitive or non-code dependencies).\n")
        sections += 1

    units = discover_units(args.repo)
    units += [{"namespace": ns, "dir": "."} for ns in args.namespace]
    units = attribute_symbols(changed_exports(args.repo, args.diff_range), units)
    if units:
        print("## Published surface: who consumes this change\n")
        silent = []
        for unit in units:
            if not print_producer_report(unit, args.project_id, args.limit, args.min_stars):
                silent.append(unit["namespace"])
        if silent:
            print("No external consumers import these packages or their changed symbols: "
                  + ", ".join(f"`{n}`" for n in sorted(silent)) + ".\n")
        sections += 1

    if sections == 0:
        print("No dependency bumps or published-surface changes detected; nothing to report.")


if __name__ == "__main__":
    main()
