#!/usr/bin/env python3
"""repo_map.py — produce a high-level repo map from Orbit's local DuckDB graph.

Usage:
    repo_map.py overview                  # languages, top dirs, key types
    repo_map.py tree [PATH_PREFIX]        # classes/structs grouped by file
    repo_map.py api  PATH_PREFIX          # types + methods + signatures
    repo_map.py class FQN_OR_NAME         # methods + signatures of one class
    repo_map.py extends NAME              # descendants of a base class/trait
    repo_map.py imports PATTERN           # who imports symbols matching PATTERN

Scoped to the current commit (`git rev-parse HEAD`). If the commit is not
indexed, the script prints the index command and exits.

Output is text aimed at LLM consumption: compact, hierarchical, with file:line
locators. Use this when starting work in an unfamiliar repo, or before
planning a large refactor.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

# How to invoke the Orbit local CLI. Defaults to the glab wrapper; when this
# skill ships bundled with the standalone binary, set ORBIT_CMD=orbit.
ORBIT_CMD = shlex.split(os.environ.get("ORBIT_CMD", "glab orbit local"))

# ---------------------------------------------------------------------------
# Common SQL fragments
# ---------------------------------------------------------------------------

# Path filters to exclude generated / test / vendor noise. Patterns are
# anchored both as top-level (`spec/%`) and nested (`%/spec/%`) because
# monorepos like the GitLab Rails app keep `spec/` at the repo root.
EXCLUDE_PATHS = r"""
  AND file_path NOT LIKE 'spec/%'        AND file_path NOT LIKE '%/spec/%'
  AND file_path NOT LIKE 'ee/spec/%'     AND file_path NOT LIKE 'qa/spec/%'
  AND file_path NOT LIKE 'tests/%'       AND file_path NOT LIKE '%/tests/%'
  AND file_path NOT LIKE 'test/%'        AND file_path NOT LIKE '%/test/%'
  AND file_path NOT LIKE '%_spec.rb'     AND file_path NOT LIKE '%_test.rb'
  AND file_path NOT LIKE '%_test.rs'     AND file_path NOT LIKE '%_test.go'
  AND file_path NOT LIKE '%.spec.%'      AND file_path NOT LIKE '%.test.%'
  AND file_path NOT LIKE '__mocks__/%'   AND file_path NOT LIKE '%/__mocks__/%'
  AND file_path NOT LIKE '__tests__/%'   AND file_path NOT LIKE '%/__tests__/%'
  AND file_path NOT LIKE 'fixtures/%'    AND file_path NOT LIKE '%/fixtures/%'
  AND file_path NOT LIKE 'qa/%'
  AND NOT regexp_matches(file_path, '(^|/)[a-z]+_tests?/')
  AND NOT regexp_matches(file_path, '(^|/)mock_[a-z_]+\.go$')
  AND file_path NOT LIKE 'mocks/%'       AND file_path NOT LIKE '%/mocks/%'
  AND file_path NOT LIKE '%/mocks.go'
  AND file_path NOT LIKE '%.Test/%'      AND file_path NOT LIKE '%.Tests/%'
  AND file_path NOT LIKE '%.pb.go'       AND file_path NOT LIKE '%_pb.rb'
  AND file_path NOT LIKE '%.pb.cc'       AND file_path NOT LIKE '%.pb.h'
  AND file_path NOT LIKE '%/generated/%' AND file_path NOT LIKE 'generated/%'
  AND file_path NOT LIKE 'node_modules/%' AND file_path NOT LIKE '%/node_modules/%'
  AND file_path NOT LIKE 'vendor/%'      AND file_path NOT LIKE '%/vendor/%'
  AND file_path NOT LIKE 'target/%'      AND file_path NOT LIKE '%/target/%'
  AND file_path NOT LIKE 'dist/%'        AND file_path NOT LIKE '%/dist/%'
  AND file_path NOT LIKE 'build/%'       AND file_path NOT LIKE '%/build/%'
"""

# Same filters but against gl_file (uses `path`, not `file_path`).
EXCLUDE_PATHS_FILE = EXCLUDE_PATHS.replace("file_path", "path")

SUBCOMMANDS = {"overview", "tree", "api", "class", "extends", "imports"}


def exclude_paths_for(alias: str) -> str:
    """Render EXCLUDE_PATHS scoped to a given alias (e.g. `d` or `i`).

    The base fragment uses bare `file_path`; in queries with multiple
    aliased tables we need to prefix every occurrence with the table.
    """
    return EXCLUDE_PATHS.replace("file_path", f"{alias}.file_path")

# Universal signature regex (RE2). Matches a structural keyword on the
# first non-comment line in a small window after start_line. Doubled
# backslashes because the string is embedded in SQL.
SIG_REGEX = (
    r"^(pub\s+|export\s+|default\s+|async\s+|unsafe\s+|public\s+|private\s+"
    r"|static\s+|abstract\s+|final\s+|protected\s+|open\s+|override\s+|inline\s+"
    r"|internal\s+|sealed\s+|partial\s+|readonly\s+|virtual\s+|extern\s+)*"
    r"(fn|func|struct|enum|trait|type|mod|impl|const|let|var|class|module|def|function"
    r"|interface|namespace|object|fun|record)\s+[A-Za-z_$<({]"
)

# Source extensions covered by Orbit. Used to build a glob list for
# read_text() since it does not accept lateral column parameters.
SOURCE_EXTS = (
    "rs rb py js ts vue jsx tsx mjs cjs go java kt kts scala "
    "cs cpp c h hpp swift php rake".split()
)

# Definition types we treat as "types" vs "callables" in tree/api output.
TYPE_KINDS = (
    "Class Struct Enum Trait Interface Module Type TypeAlias Record Object "
    "Namespace DataClass ValueClass AnnotationClass".split()
)
CALLABLE_KINDS = (
    "Method Function AssociatedFunction SingletonMethod StaticMethod Constructor "
    "ExtensionFunction AsyncFunction DecoratedFunction DecoratedMethod "
    "DecoratedAsyncFunction DecoratedAsyncMethod Lambda Macro".split()
)
MEMBER_KINDS = CALLABLE_KINDS + "Field Attribute Property ComputedProperty".split()


# ---------------------------------------------------------------------------
# Shell glue
# ---------------------------------------------------------------------------


def sql_lit(s: str) -> str:
    """SQL string literal with single-quote doubling."""
    return "'" + s.replace("'", "''") + "'"


QUERY_TIMEOUT_SECONDS = 120


def run_sql(sql: str, fmt: str = "table") -> str:
    """Invoke the Orbit local CLI `sql -F <fmt> -` with sql on stdin."""
    if not shutil.which(ORBIT_CMD[0]):
        sys.exit(
            f"error: `{ORBIT_CMD[0]}` not on PATH — install the Orbit local CLI "
            f"(or set ORBIT_CMD)"
        )
    try:
        cp = subprocess.run(
            ORBIT_CMD + ["sql", "-F", fmt, "-"],
            input=sql,
            text=True,
            capture_output=True,
            timeout=QUERY_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        sys.exit(f"error: local query timed out after {QUERY_TIMEOUT_SECONDS} seconds")
    if cp.returncode != 0:
        sys.stderr.write(cp.stderr)
        sys.exit(cp.returncode)
    return cp.stdout


def current_sha() -> str:
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True
        )
        return cp.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        sys.exit("error: not inside a git repository")


def apply_extension_filter(raw_extensions: list[str]) -> None:
    """Limit SQL path filters and signature extraction globs to extensions."""
    global EXCLUDE_PATHS, EXCLUDE_PATHS_FILE, SOURCE_EXTS

    extensions = []
    for raw in raw_extensions:
        for value in raw.split(","):
            ext = value.strip().lstrip(".")
            if ext:
                extensions.append(ext)

    if not extensions:
        return

    SOURCE_EXTS = tuple(dict.fromkeys(extensions))
    pattern = "\\.(" + "|".join(re.escape(ext) for ext in extensions) + ")$"
    EXCLUDE_PATHS += f"  AND regexp_matches(file_path, {sql_lit(pattern)})\n"
    EXCLUDE_PATHS_FILE = EXCLUDE_PATHS.replace("file_path", "path")


def split_repo_path_arg(argv: list[str]) -> tuple[Path | None, list[str]]:
    """Accept an optional repository path before the subcommand.

    Examples:
      repo_map.py /path/to/repo
      repo_map.py /path/to/repo api crates/foo
      repo_map.py --ext rs tree crates/foo
    """
    if argv and not argv[0].startswith("-") and argv[0] not in SUBCOMMANDS:
        path = Path(argv[0]).expanduser()
        if path.is_dir() or "/" in argv[0] or argv[0].startswith((".", "~")):
            return path, argv[1:]
    return None, argv


def preflight(sha: str) -> None:
    out = run_sql(
        f"SELECT COUNT(*) AS n FROM gl_file WHERE commit_sha={sql_lit(sha)}",
        fmt="ndjson",
    ).strip()
    try:
        count = json.loads(out).get("n", 0) if out else 0
    except json.JSONDecodeError:
        count = 0
    if count == 0:
        sys.exit(
            f"error: current commit {sha} is not indexed in ~/.orbit/graph.duckdb\n"
            f"       run:  {' '.join(ORBIT_CMD)} index ."
        )


def glob_list(prefix: str) -> str:
    """SQL list literal of recursive globs across all known source extensions."""
    prefix = prefix.rstrip("/")
    parts = [sql_lit(f"{prefix}/**/*.{ext}") for ext in SOURCE_EXTS]
    return "[" + ", ".join(parts) + "]"


def kind_list(kinds: list[str]) -> str:
    return "(" + ", ".join(sql_lit(k) for k in kinds) + ")"


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def cmd_overview(sha: str, args: argparse.Namespace) -> None:
    preflight(sha)
    print(f"REPO MAP — {Path.cwd()} @ {sha}")
    print("=" * 78)
    print("\nLanguages (non-test source files):")
    print(
        run_sql(
            f"""
SELECT language, COUNT(*) AS files
FROM gl_file
WHERE commit_sha={sql_lit(sha)}
{EXCLUDE_PATHS_FILE}
GROUP BY 1 ORDER BY files DESC
"""
        )
    )

    print("Definition totals:")
    print(
        run_sql(
            f"""
SELECT definition_type, COUNT(*) AS n
FROM gl_definition
WHERE commit_sha={sql_lit(sha)}
{EXCLUDE_PATHS}
GROUP BY 1
HAVING n >= 5
ORDER BY n DESC
"""
        )
    )

    print("Top-level structure (files / types / callables per dir):")
    print(
        run_sql(
            f"""
WITH paths AS (
  SELECT
    CASE
      WHEN file_path LIKE 'crates/%/%'   THEN regexp_extract(file_path, '^(crates/[^/]+)', 1)
      WHEN file_path LIKE 'packages/%/%' THEN regexp_extract(file_path, '^(packages/[^/]+)', 1)
      WHEN file_path LIKE 'apps/%/%'     THEN regexp_extract(file_path, '^(apps/[^/]+)', 1)
      WHEN file_path LIKE 'services/%/%' THEN regexp_extract(file_path, '^(services/[^/]+)', 1)
      WHEN file_path LIKE 'ee/app/%/%'   THEN regexp_extract(file_path, '^(ee/app/[^/]+)', 1)
      WHEN file_path LIKE 'app/%/%'      THEN regexp_extract(file_path, '^(app/[^/]+)', 1)
      WHEN file_path LIKE 'lib/%/%'      THEN regexp_extract(file_path, '^(lib/[^/]+)', 1)
      WHEN file_path LIKE 'src/%/%'      THEN regexp_extract(file_path, '^(src/[^/]+)', 1)
      WHEN file_path LIKE '%/%'          THEN regexp_extract(file_path, '^([^/]+)', 1)
      ELSE '<root>'
    END AS dir,
    file_path, definition_type
  FROM gl_definition
  WHERE commit_sha={sql_lit(sha)}
  {EXCLUDE_PATHS}
)
SELECT
  dir,
  COUNT(DISTINCT file_path) AS files,
  COUNT(*) FILTER (WHERE definition_type IN {kind_list(TYPE_KINDS)}) AS types,
  COUNT(*) FILTER (WHERE definition_type IN {kind_list(CALLABLE_KINDS)}) AS callables
FROM paths
GROUP BY dir
ORDER BY files DESC
LIMIT 30
"""
        )
    )

    print("Key abstractions (types with most descendants via EXTENDS):")
    print(
        run_sql(
            f"""
SELECT parent.fqn AS base, parent.definition_type AS kind,
       parent.file_path || ':' || parent.start_line AS loc,
       COUNT(DISTINCT child.id) AS descendants
FROM gl_definition parent
JOIN gl_edge e ON e.target_id = parent.id AND e.relationship_kind='EXTENDS'
JOIN gl_definition child ON child.id = e.source_id
WHERE parent.commit_sha={sql_lit(sha)}
  AND parent.definition_type IN {kind_list(['Class', 'Trait', 'Interface', 'Struct'])}
  {exclude_paths_for("parent")}
  {exclude_paths_for("child")}
GROUP BY 1, 2, 3
HAVING descendants >= 2
ORDER BY descendants DESC
LIMIT 20
"""
        )
    )

    print("Most-imported defined symbols (project-internal anchors):")
    # Inner-join against gl_definition to exclude stdlib/3rd-party names
    # (Arc/HashMap/Result) that dominate raw import counts. Both sides of
    # the join exclude tests so test-only helpers and JS `export default`
    # shenanigans don't show up as repo anchors.
    print(
        run_sql(
            f"""
SELECT i.identifier_name AS symbol,
       COUNT(DISTINCT i.file_path) AS importers,
       any_value(d.file_path || ':' || d.start_line) AS defined_at
FROM gl_imported_symbol i
JOIN gl_definition d
  ON d.commit_sha = i.commit_sha
 AND d.name = i.identifier_name
 {exclude_paths_for("d")}
WHERE i.commit_sha={sql_lit(sha)}
  {exclude_paths_for("i")}
  AND i.identifier_name NOT IN ('default', '_', 'self', 'this', 'super', 'Self')
  AND length(i.identifier_name) >= 3
  AND d.definition_type IN {kind_list(TYPE_KINDS + ['Function', 'AssociatedFunction', 'StaticMethod'])}
GROUP BY 1
HAVING importers >= 3
ORDER BY importers DESC
LIMIT 25
"""
        )
    )

    # Universal anchor fallback. Imports above only surface named-symbol
    # imports — Go's `import "path/to/pkg"` and C#'s `using Namespace;`
    # don't have identifier names, so those repos get an empty section.
    # Most-called callables work everywhere and are a strong "what is this
    # repo built around" signal. We group by (name, file_path) to collapse
    # the parser's polymorphic dispatch (same-named methods on different
    # types in Go are real, but the per-line locator still points at one).
    print("Most-called callables (universal anchor, by CALLS edge count):")
    print(
        run_sql(
            f"""
WITH per_loc AS (
  -- One row per (name, file:line) with its incoming CALLS edge count.
  SELECT d.name, d.definition_type, d.file_path, d.start_line,
         COUNT(*) AS callers
  FROM gl_edge e
  JOIN gl_definition d ON d.id = e.target_id
  WHERE e.relationship_kind='CALLS'
    AND d.commit_sha={sql_lit(sha)}
    AND d.definition_type IN {kind_list(['Function', 'Method', 'AssociatedFunction', 'StaticMethod', 'SingletonMethod', 'AsyncFunction', 'Constructor'])}
    {EXCLUDE_PATHS}
    AND length(d.name) >= 3
    AND d.name NOT IN ('new', 'init', 'main', 'String', 'Error', 'Return', 'Get', 'Set', 'Call', 'Run')
  GROUP BY d.name, d.definition_type, d.file_path, d.start_line
),
ranked AS (
  -- Collapse to the canonical implementation per name (most callers wins;
  -- ties broken by shortest file_path so library code beats helper paths).
  SELECT name, definition_type, file_path, start_line, callers,
         ROW_NUMBER() OVER (PARTITION BY name ORDER BY callers DESC, length(file_path), file_path) AS rn,
         SUM(callers) OVER (PARTITION BY name) AS total_callers
  FROM per_loc
)
SELECT name AS callable, definition_type AS kind,
       file_path || ':' || start_line AS defined_at,
       total_callers AS callers
FROM ranked
WHERE rn = 1
ORDER BY total_callers DESC
LIMIT 20
"""
        )
    )


def cmd_tree(sha: str, args: argparse.Namespace) -> None:
    preflight(sha)
    prefix_filter = ""
    header = "all top-level files (truncated)"
    if args.prefix:
        p = args.prefix.rstrip("/")
        prefix_filter = f"AND file_path LIKE {sql_lit(p + '/%')}"
        header = f"under {p}/"
    print(f"TREE — {header}")
    print("=" * 78)
    print(
        run_sql(
            f"""
WITH t AS (
  SELECT file_path, fqn, name, definition_type, start_line
  FROM gl_definition
  WHERE commit_sha={sql_lit(sha)}
    AND definition_type IN {kind_list(TYPE_KINDS)}
    {prefix_filter}
    {EXCLUDE_PATHS}
)
SELECT
  file_path,
  string_agg(
    definition_type || ' ' || name || ' @L' || start_line,
    chr(10) || '    '  ORDER BY start_line
  ) AS contents
FROM t
GROUP BY file_path
ORDER BY file_path
LIMIT 200
"""
        )
    )


def cmd_api(sha: str, args: argparse.Namespace) -> None:
    if not args.prefix:
        sys.exit("usage: local_repo_map.py api PATH_PREFIX")
    preflight(sha)
    prefix = args.prefix.rstrip("/")
    globs = glob_list(prefix)
    api_kinds = TYPE_KINDS + CALLABLE_KINDS
    print(f"API MAP — under {prefix}/")
    print("=" * 78)
    print(
        run_sql(
            f"""
WITH defs AS (
  -- DISTINCT collapses TypeScript / parser double-defs (one with bare fqn,
  -- one with file-prefixed fqn) that share name + line + type.
  SELECT DISTINCT ON (file_path, name, definition_type, start_line)
         file_path, fqn, name, definition_type, start_line, end_line
  FROM gl_definition
  WHERE commit_sha={sql_lit(sha)}
    AND file_path LIKE {sql_lit(prefix + '/%')}
    AND start_line > 0
    AND definition_type IN {kind_list(api_kinds)}
    {EXCLUDE_PATHS}
  ORDER BY file_path, name, definition_type, start_line, length(fqn)
),
src AS (
  SELECT filename, str_split(content, chr(10)) AS lines
  FROM read_text({globs})
),
windows AS (
  -- Cap the window at end_line so the signature search can't bleed into
  -- an adjacent definition. Many parsers report end_line=start_line for
  -- single-line declarations; in that case fall back to a small fixed span.
  SELECT d.file_path, d.fqn, d.name, d.definition_type, d.start_line,
    array_slice(
      s.lines,
      GREATEST(d.start_line, 1),
      LEAST(
        GREATEST(d.end_line, d.start_line + 3),
        d.start_line + 10,
        length(s.lines)
      )
    ) AS w
  FROM defs d LEFT JOIN src s ON s.filename = d.file_path
),
sigs AS (
  -- Prefer a line that both matches the structural regex AND mentions the
  -- definition name (disambiguates nested `class X` inside `module Y`).
  -- Fall back to the first structural-keyword line, then the def name.
  SELECT file_path, fqn, name, definition_type, start_line,
    coalesce(
      list_filter(w, l -> regexp_matches(trim(l), {sql_lit(SIG_REGEX)}) AND contains(l, name))[1],
      list_filter(w, l -> regexp_matches(trim(l), {sql_lit(SIG_REGEX)}))[1],
      name
    ) AS sig
  FROM windows
)
SELECT
  file_path,
  string_agg(
    rpad(definition_type, 18, ' ') || ' ' || trim(sig) || '  [L' || start_line || ']',
    chr(10) || '    '  ORDER BY start_line
  ) AS api
FROM sigs
GROUP BY file_path
ORDER BY file_path
LIMIT 300
"""
        )
    )


def cmd_class(sha: str, args: argparse.Namespace) -> None:
    if not args.target:
        sys.exit("usage: local_repo_map.py class FQN_OR_NAME")
    preflight(sha)
    t = args.target
    print(f"CLASS — {t}")
    print("=" * 78)

    parents = run_sql(
        f"""
SELECT fqn, definition_type, file_path || ':' || start_line AS loc,
       (end_line - start_line) AS lines
FROM gl_definition
WHERE commit_sha={sql_lit(sha)}
  AND (fqn={sql_lit(t)} OR name={sql_lit(t)})
  AND definition_type IN {kind_list(['Class', 'Struct', 'Enum', 'Trait', 'Interface', 'Module', 'Object', 'Record'])}
  {EXCLUDE_PATHS}
ORDER BY loc
LIMIT 10
"""
    )
    print(parents)

    # Pull the parent rows so we can build a precise glob list scoped to
    # only the files we care about (avoid scanning the whole repo).
    parent_files = run_sql(
        f"""
SELECT DISTINCT file_path
FROM gl_definition
WHERE commit_sha={sql_lit(sha)}
  AND (fqn={sql_lit(t)} OR name={sql_lit(t)})
  AND definition_type IN {kind_list(['Class', 'Struct', 'Enum', 'Trait', 'Interface', 'Module', 'Object', 'Record'])}
  {EXCLUDE_PATHS}
""",
        fmt="ndjson",
    )
    file_paths = [
        json.loads(line)["file_path"]
        for line in parent_files.splitlines()
        if line.strip()
    ]
    if not file_paths:
        print(f"(no class/module/trait named {t})")
        return
    src_list = "[" + ", ".join(sql_lit(p) for p in file_paths) + "]"

    print("\nMembers + signatures:")
    print(
        run_sql(
            f"""
WITH parent AS (
  SELECT id, fqn, file_path, start_line, end_line
  FROM gl_definition
  WHERE commit_sha={sql_lit(sha)}
    AND (fqn={sql_lit(t)} OR name={sql_lit(t)})
    AND definition_type IN {kind_list(['Class', 'Struct', 'Enum', 'Trait', 'Interface', 'Module', 'Object', 'Record'])}
),
contained AS (
  -- Collapse parser double-defs (TypeScript ships two rows per symbol).
  SELECT DISTINCT ON (file_path, name, definition_type, start_line)
         d.fqn, d.name, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d, parent p
  WHERE d.commit_sha={sql_lit(sha)}
    AND d.start_line > 0
    AND d.definition_type IN {kind_list(MEMBER_KINDS)}
    {exclude_paths_for("d")}
    AND (
      d.fqn LIKE p.fqn || '::%'
      OR d.fqn LIKE p.fqn || '.%'
      OR (d.file_path = p.file_path AND d.start_line BETWEEN p.start_line AND p.end_line)
    )
  ORDER BY d.file_path, d.name, d.definition_type, d.start_line, length(d.fqn)
),
src AS (
  SELECT filename, str_split(content, chr(10)) AS lines
  FROM read_text({src_list})
),
windows AS (
  SELECT c.fqn, c.name, c.definition_type, c.file_path, c.start_line,
    array_slice(
      s.lines,
      GREATEST(c.start_line, 1),
      LEAST(GREATEST(c.end_line, c.start_line + 3), c.start_line + 10, length(s.lines))
    ) AS w
  FROM contained c LEFT JOIN src s ON s.filename = c.file_path
),
sigs AS (
  SELECT fqn, name, definition_type, file_path, start_line,
    coalesce(
      list_filter(w, l -> regexp_matches(trim(l), {sql_lit(SIG_REGEX)}) AND contains(l, name))[1],
      list_filter(w, l -> regexp_matches(trim(l), {sql_lit(SIG_REGEX)}))[1],
      name
    ) AS sig
  FROM windows
)
SELECT
  rpad(definition_type, 18, ' ') AS kind,
  trim(sig) AS signature,
  file_path || ':' || start_line AS loc
FROM sigs
ORDER BY file_path, start_line
LIMIT 300
"""
        )
    )


def cmd_extends(sha: str, args: argparse.Namespace) -> None:
    if not args.target:
        sys.exit("usage: local_repo_map.py extends NAME")
    preflight(sha)
    print(f"DESCENDANTS — {args.target}")
    print("=" * 78)
    print(
        run_sql(
            f"""
WITH RECURSIVE chain AS (
  SELECT id, fqn, file_path, start_line, 0 AS depth
  FROM gl_definition
  WHERE commit_sha={sql_lit(sha)}
    AND (fqn={sql_lit(args.target)} OR name={sql_lit(args.target)})
    AND definition_type IN {kind_list(['Class', 'Trait', 'Interface', 'Struct'])}
    {EXCLUDE_PATHS}
  UNION ALL
  SELECT s.id, s.fqn, s.file_path, s.start_line, c.depth + 1
  FROM gl_edge e
  JOIN chain c ON e.target_id = c.id AND e.relationship_kind='EXTENDS'
  JOIN gl_definition s ON s.id = e.source_id
  WHERE c.depth < 6
    {exclude_paths_for("s")}
)
SELECT depth, fqn, file_path || ':' || start_line AS loc
FROM chain
ORDER BY depth, fqn
LIMIT 200
"""
        )
    )


def cmd_imports(sha: str, args: argparse.Namespace) -> None:
    if not args.pattern:
        sys.exit("usage: local_repo_map.py imports PATTERN")
    preflight(sha)
    pat = f"%{args.pattern}%"
    print(f"IMPORTERS — pattern '{args.pattern}'")
    print("=" * 78)
    print(
        run_sql(
            f"""
SELECT identifier_name AS symbol, import_path,
       COUNT(DISTINCT file_path) AS importers
FROM gl_imported_symbol
WHERE commit_sha={sql_lit(sha)}
{EXCLUDE_PATHS}
  AND (identifier_name LIKE {sql_lit(pat)} OR import_path LIKE {sql_lit(pat)})
GROUP BY 1, 2
ORDER BY importers DESC
LIMIT 50
"""
        )
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    repo_path, argv = split_repo_path_arg(sys.argv[1:])
    if repo_path is not None:
        if not repo_path.is_dir():
            sys.exit(f"error: repository path does not exist or is not a directory: {repo_path}")
        os.chdir(repo_path)

    parser = argparse.ArgumentParser(
        prog="local_repo_map.py", description="High-level repo map from the Orbit graph."
    )
    parser.add_argument(
        "--ext",
        "--extension",
        dest="extensions",
        action="append",
        default=[],
        help="limit output to source files with these extensions (for example: --ext .rs; repeat or comma-separate)",
    )
    sub = parser.add_subparsers(dest="cmd", required=False)

    sub.add_parser("overview", help="languages, top dirs, key types")

    p_tree = sub.add_parser("tree", help="classes/structs grouped by file")
    p_tree.add_argument("prefix", nargs="?", default=None)

    p_api = sub.add_parser("api", help="types + methods + signatures under prefix")
    p_api.add_argument("prefix")

    p_class = sub.add_parser("class", help="methods + signatures of a class")
    p_class.add_argument("target")

    p_ext = sub.add_parser("extends", help="descendants of a base class/trait")
    p_ext.add_argument("target")

    p_imp = sub.add_parser("imports", help="who imports a symbol pattern")
    p_imp.add_argument("pattern")

    args = parser.parse_args(argv)
    apply_extension_filter(args.extensions)
    sha = current_sha()
    dispatch = {
        None: cmd_overview,
        "overview": cmd_overview,
        "tree": cmd_tree,
        "api": cmd_api,
        "class": cmd_class,
        "extends": cmd_extends,
        "imports": cmd_imports,
    }
    dispatch[args.cmd](sha, args)


if __name__ == "__main__":
    main()
