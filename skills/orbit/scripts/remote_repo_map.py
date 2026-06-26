#!/usr/bin/env python3
"""remote_repo_map.py — remote repo map backed by glab orbit remote.

Usage:
    remote_repo_map.py extends   NAME         [--project-id ID] [--branch B] [--depth N]
    remote_repo_map.py ancestors NAME         [--project-id ID] [--branch B] [--depth N] [--filter-prefix P]
    remote_repo_map.py includes  BASE PREFIX  [--project-id ID] [--branch B] [--depth N]
    remote_repo_map.py class     NAME         [--project-id ID] [--branch B]
    remote_repo_map.py api       PATH_PREFIX  [--project-id ID] [--branch B]
    remote_repo_map.py callers   NAME         [--project-id ID] [--branch B]

Defaults to gitlab-org/gitlab (project 278964, branch master).
Requires glab >= v1.94.0 authenticated against gitlab.com.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_PROJECT_ID = 278964   # gitlab-org/gitlab
DEFAULT_BRANCH     = "master"
QUERY_TIMEOUT_SECONDS = 120

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

def _query(body: dict) -> dict:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(body, f)
        tmp = f.name
    try:
        cp = subprocess.run(
            # --format is renamed to --response-format in glab >= 1.105.0;
            # keep --format for back-compat with older glab clients.
            ["glab", "orbit", "remote", "query", "--format", "raw", tmp],
            capture_output=True, text=True,
            timeout=QUERY_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        sys.exit("glab not found on PATH - install glab >= 1.94.0 (https://gitlab.com/gitlab-org/cli)")
    except subprocess.TimeoutExpired:
        sys.exit(f"Orbit query timed out after {QUERY_TIMEOUT_SECONDS} seconds")
    finally:
        os.unlink(tmp)
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
    """Return the instance-level edges from a graph/raw query response.

    The raw formatter always emits an `edges` array (required field in
    config/schemas/query_response.json), with `from_id`/`to_id` string IDs and,
    for variable-length traversals, a `depth` hop count (absent on single-hop
    edges).
    """
    return result.get("result", {}).get("edges", [])


def _base_filters(project_id: int, branch: str) -> dict:
    return {
        "project_id": {"op": "eq", "value": project_id},
        "branch":     {"op": "eq", "value": branch},
    }


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

    Returns a tuple (property_name, value) ready for use in a filters dict.
    """
    if "::" not in name:
        return ("name", name)
    # Namespaced: use fqn filter for precise matching
    return ("fqn", name)


def cmd_extends(args: argparse.Namespace) -> None:
    """Find all descendants of a class/module via EXTENDS — single server-side multi-hop traversal.

    Uses min_hops/max_hops on the RelationshipSelector to collapse the old
    N×M frontier loop (one query per frontier node per hop) into a single
    server-side traversal.  For a class with 16 direct subclasses at depth=2
    the old approach required 16+256=272 sequential API calls (~2.5 min);
    this version issues exactly one query regardless of depth.
    """
    pid    = args.project_id
    branch = args.branch
    depth  = max(1, min(args.depth, 3))  # server caps traversal at 3
    if args.depth != depth:
        print(f"(depth clamped to {depth}; server limit is 3)", file=sys.stderr)

    filter_key, filter_val = _resolve_name(args.name)

    body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "base", "entity": "Definition",
                "filters": {**_base_filters(pid, branch), filter_key: {"op": "eq", "value": filter_val}},
                "columns": ["fqn", "name"],
            },
            {
                "id": "child", "entity": "Definition",
                "filters": _base_filters(pid, branch),
                "columns": ["id", "fqn", "name", "definition_type", "file_path", "start_line"],
            },
        ],
        "relationships": [
            {"type": "EXTENDS", "from": "child", "to": "base", "min_hops": 1, "max_hops": depth}
        ],
        # Max server-side cap. Multi-hop fan-out on wide trees can exceed a few
        # hundred nodes; a low limit truncates the result non-deterministically
        # (the server has no stable order before truncation), so we request the
        # full cap to keep multi-hop results stable and complete.
        "limit": 1000,
    }}

    result = _query(body)
    all_nodes = _nodes(result, "Definition")
    rows = [
        n for n in all_nodes
        if n.get("name") != args.name and n.get("fqn") != args.name
    ]

    print(f"EXTENDS — descendants of {args.name!r} (depth ≤ {depth})")
    print("=" * 78)
    if not rows:
        print("(no descendants found — class may not be indexed on this branch)")
        return
    print(f"{'type':12}  {'fqn':<60}  location")
    print("-" * 110)
    for n in sorted(rows, key=lambda x: x.get("file_path") or ""):
        fqn  = n.get("fqn", n.get("name", "?"))
        kind = n.get("definition_type", "?")
        loc  = f"{n.get('file_path', '')}:{n.get('start_line', '')}"
        print(f"{kind:12}  {fqn[:60]:<60}  {loc}")
    print(f"\n{len(rows)} descendant(s) found")


def cmd_ancestors(args: argparse.Namespace) -> None:
    """Walk EXTENDS edges upward — single server-side multi-hop traversal.

    Uses fqn filter instead of name to avoid ambiguous matches
    (e.g. 'Issue' matches app/models/issue.rb AND qa/resource/issue.rb).
    Pass --filter-prefix app/models/concerns to list only included concerns.

    Uses min_hops/max_hops on the RelationshipSelector to issue a single
    query instead of the old N×M frontier loop.
    """
    pid    = args.project_id
    branch = args.branch
    depth  = max(1, min(args.depth, 3))
    if args.depth != depth:
        print(f"(depth clamped to {depth}; server limit is 3)", file=sys.stderr)

    body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "child", "entity": "Definition",
                "filters": {**_base_filters(pid, branch), "fqn": {"op": "eq", "value": args.name}},
                "columns": ["fqn", "name"],
            },
            {
                "id": "ancestor", "entity": "Definition",
                "filters": _base_filters(pid, branch),
                "columns": ["id", "fqn", "name", "definition_type", "file_path", "start_line"],
            },
        ],
        "relationships": [
            {"type": "EXTENDS", "from": "child", "to": "ancestor", "min_hops": 1, "max_hops": depth}
        ],
        # See cmd_extends: request the full cap so multi-hop ancestor chains are
        # not truncated non-deterministically.
        "limit": 1000,
    }}

    result = _query(body)
    all_nodes = _nodes(result, "Definition")
    rows = [
        n for n in all_nodes
        if n.get("fqn") != args.name
    ]

    prefix = getattr(args, "filter_prefix", None) or ""
    rows = _filter_by_prefix(rows, prefix)

    print(f"ANCESTORS — of {args.name!r} (depth ≤ {depth})"
          + (f"  [filter: {prefix}]" if prefix else ""))
    print("=" * 78)
    if not rows:
        print("(no ancestors found — class may not be indexed on this branch)")
        return
    print(f"{'type':12}  {'fqn':<60}  location")
    print("-" * 110)
    for n in sorted(rows, key=lambda x: x.get("file_path") or ""):
        fqn  = n.get("fqn", n.get("name", "?"))
        kind = n.get("definition_type", "?")
        loc  = f"{n.get('file_path', '')}:{n.get('start_line', '')}"
        print(f"{kind:12}  {fqn[:60]:<60}  {loc}")
    print(f"\n{len(rows)} ancestor(s) found")


def cmd_includes(args: argparse.Namespace) -> None:
    """For each descendant of BASE, list concerns they include from PREFIX — single query.

    More efficient than code_extends + N × code_ancestors: single 3-hop traversal
    (BASE ← EXTENDS ← descendants ← EXTENDS → concerns_in_prefix).
    Returns a per-descendant breakdown grouped by model.

    Example: includes Noteable app/models/concerns
      -> For each Noteable model (Issue, MergeRequest, etc.), the concerns it includes.
    """
    pid    = args.project_id
    branch = args.branch
    depth  = max(1, min(getattr(args, "depth", 1), 3))

    norm_prefix = args.prefix.rstrip("/") + "/"
    base_key, base_val = _resolve_name(args.base)

    # Two 2-node traversals instead of one 3-node branching traversal: Orbit
    # Remote does not currently return results for a single query with two
    # relationships fanning out from a shared node (it returns an empty set), so
    # we (1) find the descendants of BASE, then (2) map those descendants to the
    # concerns they extend. This is still far cheaper than the old
    # "1 extends + N ancestors" pattern: exactly two queries regardless of fan-out.

    # Query 1 — descendants of BASE.
    desc_body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "base", "entity": "Definition",
                "filters": {**_base_filters(pid, branch), base_key: {"op": "eq", "value": base_val}},
                "columns": ["id", "fqn", "name"],
            },
            {
                "id": "descendant", "entity": "Definition",
                "filters": _base_filters(pid, branch),
                "columns": ["id", "fqn", "name", "definition_type", "file_path", "start_line"],
            },
        ],
        "relationships": [
            {"type": "EXTENDS", "from": "descendant", "to": "base",
             "min_hops": 1, "max_hops": depth}
        ],
        "limit": 1000,
    }}
    desc_nodes = [
        n for n in _nodes(_query(desc_body), "Definition")
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
    concern_body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "descendant", "entity": "Definition",
                "filters": _base_filters(pid, branch),
                "node_ids": [int(d) for d in desc_ids],
                "columns": ["id", "fqn", "name"],
            },
            {
                "id": "concern", "entity": "Definition",
                "filters": {**_base_filters(pid, branch),
                            "file_path": {"op": "starts_with", "value": norm_prefix}},
                "columns": ["id", "fqn", "name", "definition_type", "file_path", "start_line"],
            },
        ],
        # Direct includes only (single hop, intentional): the concerns each
        # descendant mixes in itself, not transitively.
        "relationships": [
            {"type": "EXTENDS", "from": "descendant", "to": "concern",
             "min_hops": 1, "max_hops": 1}
        ],
        "limit": 1000,
    }}
    concern_result = _query(concern_body)
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


def cmd_class(args: argparse.Namespace) -> None:
    """List all members (methods, fields) defined inside a class."""
    pid    = args.project_id
    branch = args.branch
    filter_key, filter_val = _resolve_name(args.name)

    # Step 1: look up the class itself (traversal only returns member nodes, not the parent)
    lookup = _query({"query": {
        "query_type": "traversal",
        "node": {
            "id": "d", "entity": "Definition",
            "filters": {**_base_filters(pid, branch),
                        filter_key: {"op": "eq", "value": filter_val},
                        "definition_type": {"op": "in", "value": TYPE_KINDS}},
            "columns": ["id", "fqn", "file_path", "start_line", "definition_type"],
        },
        "limit": 5,
    }})
    parents = _nodes(lookup, "Definition")

    print(f"CLASS — {args.name}")
    print("=" * 78)
    if not parents:
        print("(class not found — may not be indexed on this branch)")
        return
    for p in parents:
        print(f"  {p.get('definition_type','?')}  {p.get('fqn','?')}  @ {p.get('file_path','')}:{p.get('start_line','')}")

    # Step 2: fetch members via DEFINES traversal
    body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "parent", "entity": "Definition",
                "filters": {**_base_filters(pid, branch),
                             filter_key: {"op": "eq", "value": filter_val}},
                "columns": ["id", "fqn"],
            },
            {
                "id": "member", "entity": "Definition",
                # content column omitted — triggers content_resolution_error on large classes
                "filters": {
                    **_base_filters(pid, branch),
                    "definition_type": {"op": "in", "value": MEMBER_KINDS},
                },
                "columns": ["id", "name", "fqn", "definition_type", "file_path", "start_line"],
            },
        ],
        "relationships": [{"type": "DEFINES", "from": "parent", "to": "member"}],
        "limit": 200,
    }}
    result  = _query(body)
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


def cmd_api(args: argparse.Namespace) -> None:
    """List types and callables under a file path prefix."""
    pid    = args.project_id
    branch = args.branch
    prefix = args.prefix.rstrip("/")

    body = {"query": {
        "query_type": "traversal",
        "node": {
            "id": "d", "entity": "Definition",
            "filters": {
                **_base_filters(pid, branch),
                "file_path":       {"op": "starts_with", "value": prefix + "/"},
                "definition_type": {"op": "in", "value": TYPE_KINDS + CALLABLE_KINDS},
            },
            "columns": ["fqn", "name", "definition_type", "file_path", "start_line"],
        },
        "limit": 100,
    }}
    result = _query(body)
    defs   = _nodes(result, "Definition")

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


def cmd_callers(args: argparse.Namespace) -> None:
    """Find definitions that call a given method/function.

    Accepts bare method name ('execute') or qualified 'ClassName#method' /
    'ClassName::method' syntax to narrow the target when the method name is
    common.  The class qualifier is matched via a substring filter on the
    target node's FQN.
    """
    pid    = args.project_id
    branch = args.branch

    # Parse optional class qualifier: "MergeRequests::RefreshService#execute"
    # Orbit FQN uses '::' for all separators — normalize '#' to '::'.
    raw = args.name
    orbit_fqn: str | None = None
    if "#" in raw:
        # "ClassName#method" → Orbit stores as "ClassName::method"
        method_name = raw.rsplit("#", 1)[1]
        orbit_fqn = raw.replace("#", "::")
    elif raw.count("::") >= 2:
        # "Ns::ClassName::method" — last component is the method name
        method_name = raw.rsplit("::", 1)[1]
        orbit_fqn = raw
    else:
        method_name = raw

    if orbit_fqn:
        # Exact FQN match: most precise — returns only the intended target
        target_filters: dict = {
            **_base_filters(pid, branch),
            "name": {"op": "eq", "value": method_name},
            "fqn":  {"op": "eq",  "value": orbit_fqn},
        }
    else:
        target_filters = {**_base_filters(pid, branch), "name": {"op": "eq", "value": method_name}}

    body = {"query": {
        "query_type": "traversal",
        "nodes": [
            {
                "id": "target", "entity": "Definition",
                "filters": target_filters,
                "columns": ["id", "name", "fqn", "file_path", "start_line"],
            },
            {
                "id": "caller", "entity": "Definition",
                "filters": _base_filters(pid, branch),
                "columns": ["id", "fqn", "name", "definition_type", "file_path", "start_line"],
            },
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "target"}],
        "limit": 100,
    }}
    result  = _query(body)
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
        description="Remote Orbit repo map — code navigation via glab orbit remote.",
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

    p_inc = sub.add_parser("includes", help="per-descendant concern matrix via single 3-hop query")
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

