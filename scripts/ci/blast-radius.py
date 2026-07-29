#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict

EXPORT_PATTERNS = [
    re.compile(r"^\+.*\bexport\s+(?:default\s+)?(?:async\s+)?(?:function|class|const|let|var)\s+(\w+)"),
    re.compile(r"^\+.*\bexport\s+\{([^}]+)\}"),
    re.compile(r"^\+\s*(?:pub|pub\(crate\))\s+(?:async\s+)?(?:fn|struct|enum|trait|type|const)\s+(\w+)"),
]


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


def changed_symbols(repo, diff_range):
    diff = subprocess.run(
        ["git", "-C", repo, "diff", "-U0", diff_range],
        capture_output=True, text=True, check=True,
    ).stdout
    symbols = set()
    for line in diff.splitlines():
        if line.startswith("+++"):
            continue
        content = line[1:].lstrip()
        if content.startswith(("//", "/*", "*", "#")):
            continue
        for pat in EXPORT_PATTERNS:
            m = pat.search(line)
            if not m:
                continue
            for name in re.split(r",", m.group(1)):
                name = name.split(" as ")[-1].strip()
                if name and name != "default" and name[0].isalpha():
                    symbols.add(name)
    return sorted(symbols)


def usage_counts(package, symbols, limit):
    result = run_query({
        "query_type": "aggregation",
        "nodes": [{
            "id": "s",
            "entity": "ImportedSymbol",
            "filters": {
                "import_path": {"contains": package},
                "identifier_name": {"in": symbols},
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


def dependency_versions(package):
    result = run_query({
        "query_type": "traversal",
        "nodes": [{
            "id": "d",
            "entity": "Dependency",
            "filters": {"name": package},
            "columns": ["project_id", "version_pattern"],
        }],
        "limit": 500,
    })
    return {int(n["project_id"]): n["version_pattern"] for n in result.get("nodes", [])}


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
                "columns": ["id", "full_path", "archived", "star_count", "last_activity_at"],
            }],
            "limit": 100,
        })
        for n in result.get("nodes", []):
            projects[int(n["id"])] = n
    return projects


def format_symbols(symbols, head=15):
    shown = ", ".join(f"`{s}`" for s in symbols[:head])
    rest = len(symbols) - head
    return shown + (f" (+{rest} more)" if rest > 0 else "")


def main():
    ap = argparse.ArgumentParser(description="Cross-project blast radius via Orbit")
    ap.add_argument("package")
    ap.add_argument("--symbols", help="comma-separated; skips git-diff extraction")
    ap.add_argument("--diff-range", default="origin/main...HEAD")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--include-archived", action="store_true")
    ap.add_argument("--no-dedupe", action="store_true", help="skip fork/mirror fingerprint dedupe")
    ap.add_argument("--min-stars", type=int, default=1, help="star threshold for the main table; rest go to a collapsed tail")
    args = ap.parse_args()

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = changed_symbols(args.repo, args.diff_range)
    if not symbols:
        print("No changed exported symbols detected; nothing to report.")
        return
    if len(symbols) > 100:
        symbols = symbols[:100]

    rows, truncated = usage_counts(args.package, symbols, args.limit)
    if not rows:
        print(f"## Blast radius: `{args.package}`\n")
        print(f"Changed symbols: {format_symbols(symbols)}\n")
        print("No consumers found importing these symbols. If this repo does not "
              f"publish `{args.package}`, the diff-extracted symbols are unrelated "
              "to the package and an empty result is expected.")
        return

    per_project = defaultdict(dict)
    for r in rows:
        per_project[int(r["s_project_id"])][r["s_identifier_name"]] = int(r["usages"])

    projects = resolve_projects(per_project.keys())
    versions = dependency_versions(args.package)

    entries = []
    for pid, syms in per_project.items():
        proj = projects.get(pid)
        if proj is None:
            continue
        if not args.include_archived and str(proj.get("archived")).lower() == "true":
            continue
        entries.append({
            "path": proj["full_path"],
            "stars": int(proj.get("star_count") or 0),
            "total": sum(syms.values()),
            "syms": syms,
            "version": versions.get(pid),
        })

    hidden = 0
    if not args.no_dedupe:
        clusters = defaultdict(list)
        deduped = []
        for e in entries:
            if len(e["syms"]) >= 2 or e["total"] >= 5:
                clusters[tuple(sorted(e["syms"].items()))].append(e)
            else:
                deduped.append(e)
        for members in clusters.values():
            members.sort(key=lambda e: (-e["stars"], len(e["path"])))
            keep = members[0]
            keep["siblings"] = len(members) - 1
            hidden += keep["siblings"]
            deduped.append(keep)
        entries = deduped

    entries.sort(key=lambda e: (-e["stars"], -e["total"]))

    print(f"## Blast radius: `{args.package}`\n")
    print(f"Changed symbols: {format_symbols(symbols)}\n")
    main = [e for e in entries if e["stars"] >= args.min_stars]
    tail = [e for e in entries if e["stars"] < args.min_stars]

    def row(e):
        sym_str = ", ".join(f"{k} ({v})" for k, v in sorted(e["syms"].items(), key=lambda kv: -kv[1]))
        note = f" (+{e['siblings']} likely forks/mirrors)" if e.get("siblings") else ""
        return f"| {e['path']}{note} | {e['stars']} | {e['total']} | {sym_str} | {e['version'] or '—'} |"

    print("| Consumer project | Stars | Total usages | Symbols used | Declared version |")
    print("|---|---|---|---|---|")
    for e in main:
        print(row(e))
    if tail:
        print(f"\n<details><summary>{len(tail)} unstarred consumer(s) — likely forks, mirrors, and demos</summary>\n")
        print("| Consumer project | Stars | Total usages | Symbols used | Declared version |")
        print("|---|---|---|---|---|")
        for e in tail:
            print(row(e))
        print("\n</details>")
    print(f"\n{len(entries)} consumer project(s) visible to this token"
          + (f"; {hidden} deduped as likely forks/mirrors." if hidden else "."))
    if truncated:
        print("⚠️ Result truncated at query limit; radius may be larger.")
    print("Coverage: default-branch indexes only; symbol match is name-based within "
          f"import paths containing '{args.package}'.")


if __name__ == "__main__":
    main()
