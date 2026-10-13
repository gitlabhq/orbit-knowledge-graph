#!/usr/bin/env python3
"""remote_repo_map.py — remote repo map backed by glab orbit.

Usage:
    remote_repo_map.py [--project-id ID] [--branch B] extends   NAME         [--depth N]
    remote_repo_map.py [--project-id ID] [--branch B] ancestors NAME         [--depth N] [--filter-prefix P]
    remote_repo_map.py [--project-id ID] [--branch B] includes  BASE PREFIX  [--depth N]
    remote_repo_map.py [--project-id ID] [--branch B] class     NAME
    remote_repo_map.py [--project-id ID] [--branch B] api       PATH_PREFIX
    remote_repo_map.py [--project-id ID] [--branch B] callers   NAME

Defaults to gitlab-org/gitlab (project 278964, branch master).
Requires glab >= v1.117.0 authenticated against gitlab.com and Orbit CLI
>= 0.130.0 for inline GQL query text.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_PROJECT_ID = 278964   # gitlab-org/gitlab
DEFAULT_BRANCH     = "master"
QUERY_TIMEOUT_SECONDS = 120
MAX_IDS_PER_QUERY = 500
MAX_DEPTH = 3

TYPE_KINDS = [
    "Class", "Module", "Struct", "Enum", "Trait", "Interface",
    "Type", "TypeAlias", "Record", "Object", "Namespace",
]
CALLABLE_KINDS = [
    "Method", "Function", "AssociatedFunction", "SingletonMethod",
    "StaticMethod", "Constructor", "AsyncFunction",
]
MEMBER_KINDS = TYPE_KINDS + CALLABLE_KINDS + ["Field", "Attribute", "Property"]


# ── Query helpers ─────────────────────────────────────────────────────────────

def _lit(value: str | int) -> str:
    """Render a GQL literal. Strings are single-quoted with `\\` and `'` escaped."""
    if isinstance(value, bool):
        raise TypeError("boolean literals are not used by this helper")
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _list(values: list[str] | list[int]) -> str:
    return "[" + ", ".join(_lit(v) for v in values) + "]"


def _id_list(ids: list[str]) -> str:
    """Render Definition IDs, which the raw response returns as strings, as integers."""
    return "[" + ", ".join(str(int(i)) for i in ids) + "]"


def _scope(var: str, project_id: int, branch: str) -> str:
    return f"{var}.project_id = {_lit(project_id)} AND {var}.branch = {_lit(branch)}"


def _hops(depth: int) -> str:
    return "" if depth == 1 else f"*1..{depth}"


def _returns(var: str, columns: list[str]) -> list[str]:
    return [f"{var}.{column}" for column in columns]


def _gql(match: str, where: list[str], returns: list[str], limit: int) -> str:
    return (
        f"MATCH {match}\n"
        f"WHERE {' AND '.join(where)}\n"
        f"RETURN {', '.join(returns)}\n"
        f"LIMIT {limit}"
    )


def _query(gql: str) -> dict:
    try:
        cp = subprocess.run(
            ["glab", "orbit", "query", "--response-format", "raw", gql],
            capture_output=True, text=True,
            timeout=QUERY_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        sys.exit("glab not found on PATH - install glab >= 1.117.0 (https://gitlab.com/gitlab-org/cli)")
    except subprocess.TimeoutExpired:
        sys.exit(f"Orbit query timed out after {QUERY_TIMEOUT_SECONDS} seconds")
    if cp.returncode != 0:
        sys.exit(f"Orbit query failed:\n{cp.stderr.strip()}")
    try:
        return json.loads(cp.stdout)
    except json.JSONDecodeError as exc:
        sys.exit(f"Orbit query returned non-JSON output:\n{cp.stdout[:500]}\n({exc})")


def _nodes(result: dict, node_type: str | None = None) -> list[dict]:
    nodes = result.get("result", {}).get("nodes", [])
    if node_type:
        return [n for n in nodes if n.get("type") == node_type]
    return nodes


def _edges(result: dict) -> list[dict]:
    """Return the instance-level edges from a raw query response.

    The raw formatter always emits an `edges` array with `from_id`/`to_id`
    string IDs.
    """
    return result.get("result", {}).get("edges", [])


def _merge_results(results: list[dict]) -> dict:
    """Combine chunked responses into one response with deduplicated nodes."""
    nodes: dict[tuple, dict] = {}
    edges: list[dict] = []
    for result in results:
        for n in _nodes(result):
            nodes.setdefault((n.get("type"), n.get("id")), n)
        edges.extend(_edges(result))
    return {"result": {"nodes": list(nodes.values()), "edges": edges}}


# ── Pure helpers (no I/O — unit-testable against canned responses) ─────────────

def _filter_by_prefix(rows: list[dict], prefix: str) -> list[dict]:
    """Keep only rows whose `file_path` starts with `prefix` (no-op if empty)."""
    if not prefix:
        return rows
    return [n for n in rows if (n.get("file_path") or "").startswith(prefix)]


def _concern_ids_under_prefix(nodes: list[dict], prefix: str) -> set[str]:
    """Ids of nodes whose `file_path` is under `prefix`.

    An empty prefix matches nothing — "every file is a concern" is meaningless,
    so we deliberately classify zero concerns rather than treat "" as match-all.
    """
    norm_prefix = prefix.rstrip("/") + "/" if prefix.strip() else None
    if norm_prefix is None:
        return set()
    return {
        n["id"] for n in nodes
        if "id" in n and (n.get("file_path") or "").startswith(norm_prefix)
    }


def _map_descendant_concerns(
    edges: list[dict], desc_ids: set[str], concern_ids: set[str]
) -> dict[str, set[str]]:
    """Build the per-descendant → concern-ids mapping from EXTENDS edges.

    Robust to edge orientation (selectivity reordering can swap `from`/`to`): we
    accept a descendant↔concern edge in either direction. A descendant that is
    also a concern (a concern that extends the base) is never mapped to itself.
    """
    desc_to_concerns: dict[str, set[str]] = {did: set() for did in desc_ids}
    for e in edges:
        from_id, to_id = e.get("from_id"), e.get("to_id")
        if from_id in desc_ids and to_id in concern_ids and from_id != to_id:
            desc_to_concerns[from_id].add(to_id)
        elif to_id in desc_ids and from_id in concern_ids and from_id != to_id:
            desc_to_concerns[to_id].add(from_id)
    return desc_to_concerns


# ── Subcommands ───────────────────────────────────────────────────────────────

def _resolve_name(name: str) -> tuple[str, str]:
    """Return (filter_key, filter_value) for a class/module name lookup.

    Orbit stores nodes by their bare class name in the `name` field, but also
    indexes the fully-qualified name in `fqn`.  When a namespaced name like
    'Ci::Build' is given, filter by `fqn` for an unambiguous match instead of
    falling back to a bare `name` filter that could match unrelated classes.

    Returns a tuple (property_name, value) ready for a property predicate.
    """
    if "::" not in name:
        return ("name", name)
    return ("fqn", name)


def _parse_callers_fqn(raw: str) -> tuple[str, str | None]:
    """Parse a callers subcommand name into (method_name, orbit_fqn | None).

    Accepts bare method names, `ClassName#method`, or `Ns::Class::method`.
    The `#` separator is normalized to `::` to match Orbit's FQN format.
    When `orbit_fqn` is not None the caller should filter by both `name` and
    `fqn` for an exact match; otherwise a bare `name` filter is used.
    """
    if "#" in raw:
        method_name = raw.rsplit("#", 1)[1]
        orbit_fqn = raw.replace("#", "::")
        return (method_name, orbit_fqn)
    if raw.count("::") >= 2:
        method_name = raw.rsplit("::", 1)[1]
        return (method_name, raw)
    return (raw, None)


def _clamp_depth(raw_depth: int) -> int:
    depth = max(1, min(raw_depth, MAX_DEPTH))
    if raw_depth != depth:
        print(f"(depth clamped to {depth}; server limit is {MAX_DEPTH})", file=sys.stderr)
    return depth


def _print_definitions(rows: list[dict]) -> None:
    print(f"{'type':12}  {'fqn':<60}  location")
    print("-" * 110)
    for n in sorted(rows, key=lambda x: x.get("file_path") or ""):
        fqn  = n.get("fqn", n.get("name", "?"))
        kind = n.get("definition_type", "?")
        loc  = f"{n.get('file_path', '')}:{n.get('start_line', '')}"
        print(f"{kind:12}  {fqn[:60]:<60}  {loc}")


def build_extends_query(project_id: int, branch: str, name: str, depth: int) -> str:
    key, value = _resolve_name(name)
    # Request the full row cap. Multi-hop fan-out on wide trees can exceed a few
    # hundred nodes, and the server has no stable order before truncation, so a
    # low limit truncates the result non-deterministically.
    return _gql(
        f"(child:Definition)-[:EXTENDS{_hops(depth)}]->(base:Definition)",
        [_scope("base", project_id, branch), f"base.{key} = {_lit(value)}",
         _scope("child", project_id, branch)],
        _returns("base", ["fqn", "name"])
        + _returns("child", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        1000,
    )


def cmd_extends(args: argparse.Namespace) -> None:
    """Find all descendants of a class/module via EXTENDS in one multi-hop traversal."""
    depth = _clamp_depth(args.depth)
    result = _query(build_extends_query(args.project_id, args.branch, args.name, depth))
    rows = [
        n for n in _nodes(result, "Definition")
        if n.get("name") != args.name and n.get("fqn") != args.name
    ]

    print(f"EXTENDS — descendants of {args.name!r} (depth ≤ {depth})")
    print("=" * 78)
    if not rows:
        print("(no descendants found — class may not be indexed on this branch)")
        return
    _print_definitions(rows)
    print(f"\n{len(rows)} descendant(s) found")


def build_ancestors_query(project_id: int, branch: str, fqn: str, depth: int) -> str:
    # Match the child by fqn, not name: 'Issue' matches app/models/issue.rb and
    # qa/resource/issue.rb.
    return _gql(
        f"(child:Definition)-[:EXTENDS{_hops(depth)}]->(ancestor:Definition)",
        [_scope("child", project_id, branch), f"child.fqn = {_lit(fqn)}",
         _scope("ancestor", project_id, branch)],
        _returns("child", ["fqn", "name"])
        + _returns("ancestor", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        1000,
    )


def cmd_ancestors(args: argparse.Namespace) -> None:
    """Walk EXTENDS edges upward in one multi-hop traversal.

    Pass --filter-prefix app/models/concerns to list only included concerns.
    """
    depth = _clamp_depth(args.depth)
    result = _query(build_ancestors_query(args.project_id, args.branch, args.name, depth))
    rows = [n for n in _nodes(result, "Definition") if n.get("fqn") != args.name]

    prefix = getattr(args, "filter_prefix", None) or ""
    rows = _filter_by_prefix(rows, prefix)

    print(f"ANCESTORS — of {args.name!r} (depth ≤ {depth})"
          + (f"  [filter: {prefix}]" if prefix else ""))
    print("=" * 78)
    if not rows:
        print("(no ancestors found — class may not be indexed on this branch)")
        return
    _print_definitions(rows)
    print(f"\n{len(rows)} ancestor(s) found")


def build_descendants_query(project_id: int, branch: str, base: str, depth: int) -> str:
    key, value = _resolve_name(base)
    return _gql(
        f"(descendant:Definition)-[:EXTENDS{_hops(depth)}]->(base:Definition)",
        [_scope("base", project_id, branch), f"base.{key} = {_lit(value)}",
         _scope("descendant", project_id, branch)],
        _returns("base", ["fqn", "name"])
        + _returns("descendant", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        1000,
    )


def build_concerns_query(project_id: int, branch: str, descendant_ids: list[str], prefix: str) -> str:
    # Direct includes only (single hop, intentional): the concerns each
    # descendant mixes in itself, not transitively.
    return _gql(
        "(descendant:Definition)-[:EXTENDS]->(concern:Definition)",
        [f"descendant.id IN {_id_list(descendant_ids)}",
         _scope("descendant", project_id, branch),
         _scope("concern", project_id, branch),
         f"concern.file_path STARTS WITH {_lit(prefix)}"],
        _returns("descendant", ["fqn", "name"])
        + _returns("concern", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        1000,
    )


def cmd_includes(args: argparse.Namespace) -> None:
    """For each descendant of BASE, list the concerns it includes from PREFIX.

    Example: includes Noteable app/models/concerns
      -> For each Noteable model (Issue, MergeRequest, etc.), the concerns it includes.
    """
    pid    = args.project_id
    branch = args.branch
    depth  = _clamp_depth(getattr(args, "depth", 1))

    norm_prefix = args.prefix.rstrip("/") + "/"
    _, base_val = _resolve_name(args.base)

    # Two traversals instead of one query with two relationships fanning out
    # from a shared node: Orbit Remote returns an empty set for that shape, so
    # we (1) find the descendants of BASE, then (2) map those descendants to the
    # concerns they extend.

    # Query 1 — descendants of BASE.
    desc_nodes = [
        n for n in _nodes(_query(build_descendants_query(pid, branch, args.base, depth)), "Definition")
        if n.get("fqn") != args.base and n.get("name") != args.base
    ]
    id_to_node = {n["id"]: n for n in desc_nodes if "id" in n}
    desc_ids = set(id_to_node)

    print(f"INCLUDES — concerns for descendants of {args.base!r}")
    if not desc_ids:
        print(f"  prefix: {norm_prefix}")
        print("=" * 78)
        print("(no descendants found — class may not be indexed on this branch)")
        return

    # Query 2 — for those descendants, the concerns under PREFIX they extend.
    # The server accepts at most MAX_IDS_PER_QUERY IDs in one list.
    ordered_ids = sorted(desc_ids)
    concern_result = _merge_results([
        _query(build_concerns_query(pid, branch, ordered_ids[i:i + MAX_IDS_PER_QUERY], norm_prefix))
        for i in range(0, len(ordered_ids), MAX_IDS_PER_QUERY)
    ])
    concern_nodes = _nodes(concern_result, "Definition")
    # Add concern nodes without clobbering the richer descendant rows from Q1
    # (the descendant nodes reappear here with fewer columns).
    for n in concern_nodes:
        if "id" in n and n["id"] not in id_to_node:
            id_to_node[n["id"]] = n

    concern_ids = _concern_ids_under_prefix(concern_nodes, args.prefix)
    # Drop the base itself from concerns: when the base lives under PREFIX (a
    # base concern), its descendants extend it directly, but it should not be
    # reported as a concern of its own descendants.
    concern_ids -= {
        n["id"] for n in concern_nodes
        if "id" in n and (n.get("fqn") == args.base or n.get("name") == base_val)
    }
    desc_to_concerns = _map_descendant_concerns(
        _edges(concern_result), desc_ids, concern_ids
    )

    print(f"  prefix: {norm_prefix}  |  descendants: {len(desc_ids)}  |  total concerns: {len(concern_ids)}")
    print("=" * 78)

    for did in sorted(desc_ids, key=lambda d: (id_to_node.get(d, {}).get("file_path") or "")):
        dnode = id_to_node.get(did, {})
        dfqn  = dnode.get("fqn") or dnode.get("name") or "?"
        dloc  = f"{dnode.get('file_path', '')}:{dnode.get('start_line', '')}"
        cids  = desc_to_concerns[did]
        print(f"\n{dfqn}  @ {dloc}")
        if not cids:
            print("  (no concerns from this prefix indexed)")
        else:
            for cn in sorted((id_to_node.get(c, {}) for c in cids), key=lambda x: (x.get("file_path") or "")):
                cfqn = cn.get("fqn") or cn.get("name") or "?"
                cloc = f"{cn.get('file_path', '')}:{cn.get('start_line', '')}"
                print(f"  + {cfqn:<55}  {cloc}")

    total = sum(len(v) for v in desc_to_concerns.values())
    print(f"\n{len(desc_ids)} descendant(s), {total} total concern inclusions")


def build_class_lookup_query(project_id: int, branch: str, name: str) -> str:
    key, value = _resolve_name(name)
    return _gql(
        "(d:Definition)",
        [_scope("d", project_id, branch), f"d.{key} = {_lit(value)}",
         f"d.definition_type IN {_list(TYPE_KINDS)}"],
        _returns("d", ["fqn", "file_path", "start_line", "definition_type"]),
        5,
    )


def build_members_query(project_id: int, branch: str, name: str) -> str:
    key, value = _resolve_name(name)
    # The content column is omitted: it triggers content_resolution_error on
    # large classes.
    return _gql(
        "(parent:Definition)-[:DEFINES]->(member:Definition)",
        [_scope("parent", project_id, branch), f"parent.{key} = {_lit(value)}",
         _scope("member", project_id, branch),
         f"member.definition_type IN {_list(MEMBER_KINDS)}"],
        _returns("parent", ["fqn"])
        + _returns("member", ["name", "fqn", "definition_type", "file_path", "start_line"]),
        200,
    )


def cmd_class(args: argparse.Namespace) -> None:
    """List all members (methods, fields) defined inside a class."""
    pid    = args.project_id
    branch = args.branch

    # Step 1: look up the class itself (the member traversal returns members,
    # and the parent only with the columns it needs for filtering).
    parents = _nodes(_query(build_class_lookup_query(pid, branch, args.name)), "Definition")

    print(f"CLASS — {args.name}")
    print("=" * 78)
    if not parents:
        print("(class not found — may not be indexed on this branch)")
        return
    for p in parents:
        print(f"  {p.get('definition_type','?')}  {p.get('fqn','?')}  @ {p.get('file_path','')}:{p.get('start_line','')}")

    # Step 2: fetch members via DEFINES traversal
    result  = _query(build_members_query(pid, branch, args.name))
    parent_fqns = {p.get("fqn") for p in parents}
    members = [n for n in _nodes(result, "Definition") if n.get("fqn") not in parent_fqns]
    print()
    if not members:
        print("(no members found via DEFINES edge)")
        return
    print(f"{'type':12}  {'name':<40}  {'location'}")
    print("-" * 90)
    for m in sorted(members, key=lambda x: int(x.get("start_line") or 0)):
        name = m.get("name") or "?"
        kind = m.get("definition_type") or "?"
        loc  = f"{m.get('file_path','')}:{m.get('start_line','')}"
        print(f"{kind:12}  {name:<40}  {loc}")
    print(f"\n{len(members)} member(s)")


def build_api_query(project_id: int, branch: str, prefix: str) -> str:
    return _gql(
        "(d:Definition)",
        [_scope("d", project_id, branch),
         f"d.file_path STARTS WITH {_lit(prefix + '/')}",
         f"d.definition_type IN {_list(TYPE_KINDS + CALLABLE_KINDS)}"],
        _returns("d", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        100,
    )


def cmd_api(args: argparse.Namespace) -> None:
    """List types and callables under a file path prefix."""
    prefix = args.prefix.rstrip("/")
    defs   = _nodes(_query(build_api_query(args.project_id, args.branch, prefix)), "Definition")

    print(f"API MAP — under {prefix}/")
    print("=" * 78)
    if not defs:
        print("(no definitions found — path may not be indexed)")
        return

    # Group by file
    by_file: dict[str, list[dict]] = {}
    for d in defs:
        by_file.setdefault(d.get("file_path", "?"), []).append(d)

    for fpath in sorted(by_file):
        print(f"\n{fpath}")
        for d in sorted(by_file[fpath], key=lambda x: int(x.get("start_line") or 0)):
            kind = d.get("definition_type", "?")
            display = d.get("fqn", d.get("name", "?"))
            print(f"  {kind:12}  {display}  [L{d.get('start_line','')}]")

    print(f"\n{len(defs)} definition(s) across {len(by_file)} file(s)")


def build_callers_query(project_id: int, branch: str, method_name: str, orbit_fqn: str | None) -> str:
    target = [_scope("target", project_id, branch), f"target.name = {_lit(method_name)}"]
    if orbit_fqn:
        # Exact FQN match: most precise — returns only the intended target
        target.append(f"target.fqn = {_lit(orbit_fqn)}")
    return _gql(
        "(caller:Definition)-[:CALLS]->(target:Definition)",
        target + [_scope("caller", project_id, branch)],
        _returns("target", ["name", "fqn", "file_path", "start_line"])
        + _returns("caller", ["fqn", "name", "definition_type", "file_path", "start_line"]),
        100,
    )


def cmd_callers(args: argparse.Namespace) -> None:
    """Find definitions that call a given method/function.

    Accepts bare method name ('execute') or qualified 'ClassName#method' /
    'Ns::ClassName::method' syntax to narrow the target when the method name is
    common. A qualified name matches the target's FQN exactly.
    """
    raw = args.name
    method_name, orbit_fqn = _parse_callers_fqn(raw)

    result  = _query(build_callers_query(args.project_id, args.branch, method_name, orbit_fqn))
    all_defs = _nodes(result, "Definition")

    if orbit_fqn:
        targets = [n for n in all_defs if n.get("fqn") == orbit_fqn]
        target_fqns = {t.get("fqn") for t in targets}
        callers = [n for n in all_defs if n.get("fqn") not in target_fqns]
    else:
        targets = [n for n in all_defs if n.get("name") == method_name]
        target_ids = {t.get("id") for t in targets}
        callers = [n for n in all_defs if n.get("id") not in target_ids]

    print(f"CALLERS — of {raw!r}")
    print("=" * 78)
    if not targets:
        print("(method not found — may not be indexed)")
        if orbit_fqn:
            print(f"  Hint: tried name={method_name!r} with fqn filter. "
                  f"Retry with just the bare method name if no qualifier is needed.")
        return
    for t in targets:
        print(f"  target: {t.get('fqn','?')}  @ {t.get('file_path','')}:{t.get('start_line','')}")
    print()
    if not callers:
        print("(no callers found via CALLS edge — CALLS indexing may be incomplete)")
        return
    print(f"{'type':12}  {'fqn':<60}  location")
    print("-" * 100)
    for c in sorted(callers, key=lambda x: x.get("file_path", "")):
        loc = f"{c.get('file_path','')}:{c.get('start_line','')}"
        print(f"{c.get('definition_type','?'):12}  {c.get('fqn','?')[:60]:<60}  {loc}")
    print(f"\n{len(callers)} caller(s) found")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="remote_repo_map.py",
        description="Remote Orbit repo map — code navigation via glab orbit.",
    )
    parser.add_argument("--project-id", type=int, default=DEFAULT_PROJECT_ID,
                        help=f"GitLab project numeric ID (default: {DEFAULT_PROJECT_ID})")
    parser.add_argument("--branch", default=DEFAULT_BRANCH,
                        help=f"Branch name (default: {DEFAULT_BRANCH})")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ext = sub.add_parser("extends", help="descendants of a base class via EXTENDS (single multi-hop query)")
    p_ext.add_argument("name")
    p_ext.add_argument("--depth", type=int, default=2,
                       help="Traversal depth 1-3 (default: 2)")

    p_anc = sub.add_parser("ancestors", help="parent/ancestor classes via EXTENDS edges (single multi-hop query)")
    p_anc.add_argument("name", help="Fully-qualified class name (fqn), e.g. 'Ci::Build'")
    p_anc.add_argument("--depth", type=int, default=3,
                       help="Traversal depth 1-3 (default: 3)")
    p_anc.add_argument("--filter-prefix", default="",
                       help="Only show ancestors whose file_path starts with this prefix "
                            "(e.g. app/models/concerns to list included concerns only).")

    p_inc = sub.add_parser("includes", help="per-descendant concern matrix via two traversals")
    p_inc.add_argument("base", help="Base class/module FQN, e.g. 'Noteable'")
    p_inc.add_argument("prefix", help="File-path prefix to filter concerns, e.g. 'app/models/concerns'")
    p_inc.add_argument("--depth", type=int, default=1,
                       help="Depth for descendant traversal, 1-3 (default: 1)")

    p_cls = sub.add_parser("class", help="all members of a class via DEFINES")
    p_cls.add_argument("name")

    p_api = sub.add_parser("api", help="types + callables under a file path prefix")
    p_api.add_argument("prefix")

    p_cal = sub.add_parser("callers", help="who calls a method via CALLS")
    p_cal.add_argument("name",
                       help="Method/function name, or 'ClassName#method' to narrow by class")

    args = parser.parse_args()
    dispatch = {
        "extends":   cmd_extends,
        "ancestors": cmd_ancestors,
        "includes":  cmd_includes,
        "class":     cmd_class,
        "api":       cmd_api,
        "callers":   cmd_callers,
    }
    dispatch[args.cmd](args)


if __name__ == "__main__":
    main()

