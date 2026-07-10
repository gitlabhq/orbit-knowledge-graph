//! Native `orbit repo-map` — a high-level, LLM-oriented map of a locally
//! checked-out repository, built in-process from the DuckDB graph. This is the
//! Rust port of the former `skills/orbit-local/scripts/repo_map.py`: same six
//! subcommands, same compact `file:line` output, but no Python runtime and no
//! shelling back out to `orbit sql`.
//!
//! The definition-kind and path-exclusion vocabularies below mirror the
//! free-text `definition_type` values the code-graph parsers emit (there is no
//! single ontology enum for them) and the generated/test/vendor paths a repo
//! map should hide. They are named constants so the SQL that consumes them
//! stays declarative.

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, RecordBatch, StringArray};

use crate::sql;
use crate::sql_format;
use crate::workspace;

/// `LIKE` patterns excluded from the map: test, mock, fixture, generated,
/// vendored, and build-output paths. Anchored both top-level (`spec/%`) and
/// nested (`%/spec/%`) because monorepos keep `spec/` at the repo root.
const EXCLUDE_LIKE: &[&str] = &[
    "spec/%",
    "%/spec/%",
    "ee/spec/%",
    "qa/spec/%",
    "tests/%",
    "%/tests/%",
    "test/%",
    "%/test/%",
    "%_spec.rb",
    "%_test.rb",
    "%_test.rs",
    "%_test.go",
    "%.spec.%",
    "%.test.%",
    "__mocks__/%",
    "%/__mocks__/%",
    "__tests__/%",
    "%/__tests__/%",
    "fixtures/%",
    "%/fixtures/%",
    "qa/%",
    "mocks/%",
    "%/mocks/%",
    "%/mocks.go",
    "%.Test/%",
    "%.Tests/%",
    "%.pb.go",
    "%_pb.rb",
    "%.pb.cc",
    "%.pb.h",
    "%/generated/%",
    "generated/%",
    "node_modules/%",
    "%/node_modules/%",
    "vendor/%",
    "%/vendor/%",
    "target/%",
    "%/target/%",
    "dist/%",
    "%/dist/%",
    "build/%",
    "%/build/%",
];

/// RE2 patterns whose matches are excluded — catches `foo_tests/` style
/// directories and Go `mock_*.go` files that the `LIKE` list cannot.
const EXCLUDE_REGEX: &[&str] = &[r"(^|/)[a-z]+_tests?/", r"(^|/)mock_[a-z_]+\.go$"];

/// Structural signature keyword regex (RE2), matched against the first
/// non-comment line in the window after `start_line`.
const SIG_REGEX: &str = concat!(
    r"^(pub\s+|export\s+|default\s+|async\s+|unsafe\s+|public\s+|private\s+",
    r"|static\s+|abstract\s+|final\s+|protected\s+|open\s+|override\s+|inline\s+",
    r"|internal\s+|sealed\s+|partial\s+|readonly\s+|virtual\s+|extern\s+)*",
    r"(fn|func|struct|enum|trait|type|mod|impl|const|let|var|class|module|def|function",
    r"|interface|namespace|object|fun|record)\s+[A-Za-z_$<({]"
);

/// Source extensions Orbit indexes; used to build `read_text` globs.
const DEFAULT_SOURCE_EXTS: &[&str] = &[
    "rs", "rb", "py", "js", "ts", "vue", "jsx", "tsx", "mjs", "cjs", "go", "java", "kt", "kts",
    "scala", "cs", "cpp", "c", "h", "hpp", "swift", "php", "rake",
];

const TYPE_KINDS: &[&str] = &[
    "Class",
    "Struct",
    "Enum",
    "Trait",
    "Interface",
    "Module",
    "Type",
    "TypeAlias",
    "Record",
    "Object",
    "Namespace",
    "DataClass",
    "ValueClass",
    "AnnotationClass",
];

const CALLABLE_KINDS: &[&str] = &[
    "Method",
    "Function",
    "AssociatedFunction",
    "SingletonMethod",
    "StaticMethod",
    "Constructor",
    "ExtensionFunction",
    "AsyncFunction",
    "DecoratedFunction",
    "DecoratedMethod",
    "DecoratedAsyncFunction",
    "DecoratedAsyncMethod",
    "Lambda",
    "Macro",
];

const MEMBER_EXTRA_KINDS: &[&str] = &["Field", "Attribute", "Property", "ComputedProperty"];

/// Types treated as containers for `class` / `extends` / `overview` lookups.
const CONTAINER_KINDS: &[&str] = &[
    "Class",
    "Struct",
    "Enum",
    "Trait",
    "Interface",
    "Module",
    "Object",
    "Record",
];

const ABSTRACTION_KINDS: &[&str] = &["Class", "Trait", "Interface", "Struct"];

#[derive(clap::Subcommand)]
pub(crate) enum RepoMapCommand {
    /// Languages, top directories, key types, and repo anchors.
    Overview,
    /// Type-like definitions grouped by file, optionally under a path prefix.
    Tree {
        #[arg(value_name = "PATH_PREFIX")]
        prefix: Option<String>,
    },
    /// Types + callables + extracted signatures under a path prefix.
    Api {
        #[arg(value_name = "PATH_PREFIX")]
        prefix: String,
    },
    /// Members + signatures of one class/module/trait.
    Class {
        #[arg(value_name = "FQN_OR_NAME")]
        target: String,
    },
    /// Descendants of a base class/trait via EXTENDS edges.
    Extends {
        #[arg(value_name = "NAME")]
        target: String,
    },
    /// Files importing symbols or paths matching PATTERN.
    Imports {
        #[arg(value_name = "PATTERN")]
        pattern: String,
    },
}

/// The graph-shape facts a query needs, resolved once per invocation.
struct RepoMap {
    sha: String,
    project_id: i64,
    repo_display: String,
    /// Extension filter from `--ext` (empty means no restriction).
    ext_filter: Vec<String>,
    /// Effective source extensions for `read_text` globs.
    source_exts: Vec<String>,
}

pub(crate) fn run(
    repo: Option<PathBuf>,
    extensions: Vec<String>,
    db: Option<PathBuf>,
    command: RepoMapCommand,
) -> Result<()> {
    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));

    // The db path (from `--db` or `ORBIT_DATA_DIR`) may be relative to the
    // caller's CWD, so resolve it before the chdir below moves the CWD to the
    // repo root — otherwise a relative db resolves against the repo instead.
    let db = workspace::resolve_db_path(db)?;

    // `orbit index` stores paths relative to the git working-tree root, so a
    // `--repo` pointing at a subdirectory must anchor there too, not at the
    // subdirectory itself.
    let top_level = workspace::git_toplevel(&repo_path)
        .with_context(|| format!("failed to find git top-level for {}", repo_path.display()))?;
    let git = workspace::git_info(&top_level)
        .with_context(|| format!("failed to read git info for {}", top_level.display()))?;

    // `read_text` in the api/class queries resolves its globs against the
    // process CWD, and the graph stores repo-relative file paths, so anchor
    // the CWD at the repo root before running any query.
    std::env::set_current_dir(&git.repo_path)
        .with_context(|| format!("failed to enter {}", git.repo_path.display()))?;

    let ext_filter = parse_extensions(&extensions);
    let source_exts: Vec<String> = if ext_filter.is_empty() {
        DEFAULT_SOURCE_EXTS.iter().map(|s| s.to_string()).collect()
    } else {
        ext_filter.clone()
    };

    let map = RepoMap {
        sha: git.commit_sha.clone(),
        project_id: git.project_id,
        repo_display: git.repo_path.display().to_string(),
        ext_filter,
        source_exts,
    };

    let client = sql::open_graph(Some(db))?;
    map.preflight(&client)?;

    let mut out = std::io::stdout().lock();
    match command {
        RepoMapCommand::Overview => map.overview(&client, &mut out),
        RepoMapCommand::Tree { prefix } => map.tree(&client, &mut out, prefix.as_deref()),
        RepoMapCommand::Api { prefix } => map.api(&client, &mut out, &prefix),
        RepoMapCommand::Class { target } => map.class(&client, &mut out, &target),
        RepoMapCommand::Extends { target } => map.extends(&client, &mut out, &target),
        RepoMapCommand::Imports { pattern } => map.imports(&client, &mut out, &pattern),
    }
}

impl RepoMap {
    fn preflight(&self, client: &duckdb_client::DuckDbClient) -> Result<()> {
        let batches = sql::query(
            client,
            &format!(
                "SELECT COUNT(*) AS n FROM gl_file WHERE project_id = {} AND commit_sha = {}",
                self.project_id,
                sql_lit(&self.sha)
            ),
        )?;
        if scalar_i64(&batches) == 0 {
            anyhow::bail!(
                "current commit {} is not indexed in the local graph\n       run:  orbit index .",
                self.sha
            );
        }
        Ok(())
    }

    /// Render the exclusion clause for a column expression (bare `file_path`,
    /// `d.file_path`, or `path` for `gl_file`).
    fn exclude(&self, col: &str) -> String {
        let mut s = String::from("\n");
        for pat in EXCLUDE_LIKE {
            s.push_str(&format!("  AND {col} NOT LIKE {}\n", sql_lit(pat)));
        }
        for re in EXCLUDE_REGEX {
            s.push_str(&format!(
                "  AND NOT regexp_matches({col}, {})\n",
                sql_lit(re)
            ));
        }
        if !self.ext_filter.is_empty() {
            s.push_str(&format!(
                "  AND regexp_matches({col}, {})\n",
                sql_lit(&ext_regex(&self.ext_filter))
            ));
        }
        s
    }

    fn glob_list(&self, prefix: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let parts: Vec<String> = self
            .source_exts
            .iter()
            .map(|ext| sql_lit(&format!("{prefix}/**/*.{ext}")))
            .collect();
        format!("[{}]", parts.join(", "))
    }

    fn overview<W: Write>(&self, client: &duckdb_client::DuckDbClient, out: &mut W) -> Result<()> {
        let sha = sql_lit(&self.sha);
        let pid = self.project_id;
        writeln!(out, "REPO MAP — {} @ {}", self.repo_display, self.sha)?;
        writeln!(out, "{}", "=".repeat(78))?;

        writeln!(out, "\nLanguages (non-test source files):")?;
        section(
            client,
            out,
            &format!(
                "SELECT language, COUNT(*) AS files
FROM gl_file
WHERE project_id={pid} AND commit_sha={sha}{exclude}
GROUP BY 1 ORDER BY files DESC",
                exclude = self.exclude("path")
            ),
        )?;

        writeln!(out, "Definition totals:")?;
        section(
            client,
            out,
            &format!(
                "SELECT definition_type, COUNT(*) AS n
FROM gl_definition
WHERE project_id={pid} AND commit_sha={sha}{exclude}
GROUP BY 1
HAVING n >= 5
ORDER BY n DESC",
                exclude = self.exclude("file_path")
            ),
        )?;

        writeln!(
            out,
            "Top-level structure (files / types / callables per dir):"
        )?;
        section(
            client,
            out,
            &format!(
                "WITH paths AS (
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
  WHERE project_id={pid} AND commit_sha={sha}{exclude}
)
SELECT
  dir,
  COUNT(DISTINCT file_path) AS files,
  COUNT(*) FILTER (WHERE definition_type IN {types}) AS types,
  COUNT(*) FILTER (WHERE definition_type IN {callables}) AS callables
FROM paths
GROUP BY dir
ORDER BY files DESC
LIMIT 30",
                exclude = self.exclude("file_path"),
                types = kind_list(TYPE_KINDS),
                callables = kind_list(CALLABLE_KINDS),
            ),
        )?;

        writeln!(
            out,
            "Key abstractions (types with most descendants via EXTENDS):"
        )?;
        section(
            client,
            out,
            &format!(
                "SELECT parent.fqn AS base, parent.definition_type AS kind,
       parent.file_path || ':' || parent.start_line AS loc,
       COUNT(DISTINCT child.id) AS descendants
FROM gl_definition parent
JOIN gl_edge e ON e.target_id = parent.id AND e.relationship_kind='EXTENDS'
JOIN gl_definition child ON child.id = e.source_id
WHERE parent.project_id={pid} AND parent.commit_sha={sha}
  AND child.project_id={pid} AND child.commit_sha={sha}
  AND parent.definition_type IN {abstractions}{exclude_parent}{exclude_child}
GROUP BY 1, 2, 3
HAVING descendants >= 2
ORDER BY descendants DESC
LIMIT 20",
                abstractions = kind_list(ABSTRACTION_KINDS),
                exclude_parent = self.exclude("parent.file_path"),
                exclude_child = self.exclude("child.file_path"),
            ),
        )?;

        writeln!(
            out,
            "Most-imported defined symbols (project-internal anchors):"
        )?;
        let anchor_kinds: Vec<&str> = TYPE_KINDS
            .iter()
            .chain(["Function", "AssociatedFunction", "StaticMethod"].iter())
            .copied()
            .collect();
        section(
            client,
            out,
            &format!(
                "SELECT i.identifier_name AS symbol,
       COUNT(DISTINCT i.file_path) AS importers,
       any_value(d.file_path || ':' || d.start_line) AS defined_at
FROM gl_imported_symbol i
JOIN gl_definition d
  ON d.project_id = i.project_id
 AND d.commit_sha = i.commit_sha
 AND d.name = i.identifier_name{exclude_d}
WHERE i.project_id={pid} AND i.commit_sha={sha}{exclude_i}
  AND i.identifier_name NOT IN ('default', '_', 'self', 'this', 'super', 'Self')
  AND length(i.identifier_name) >= 3
  AND d.definition_type IN {anchors}
GROUP BY 1
HAVING importers >= 3
ORDER BY importers DESC
LIMIT 25",
                exclude_d = self.exclude("d.file_path"),
                exclude_i = self.exclude("i.file_path"),
                anchors = kind_list(&anchor_kinds),
            ),
        )?;

        writeln!(
            out,
            "Most-called callables (universal anchor, by CALLS edge count):"
        )?;
        section(
            client,
            out,
            &format!(
                "WITH per_loc AS (
  SELECT d.name, d.definition_type, d.file_path, d.start_line,
         COUNT(*) AS callers
  FROM gl_edge e
  JOIN gl_definition d ON d.id = e.target_id
  WHERE e.relationship_kind='CALLS'
    AND d.project_id={pid} AND d.commit_sha={sha}
    AND d.definition_type IN {called_kinds}{exclude}
    AND length(d.name) >= 3
    AND d.name NOT IN ('new', 'init', 'main', 'String', 'Error', 'Return', 'Get', 'Set', 'Call', 'Run')
  GROUP BY d.name, d.definition_type, d.file_path, d.start_line
),
ranked AS (
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
LIMIT 20",
                called_kinds = kind_list(&[
                    "Function",
                    "Method",
                    "AssociatedFunction",
                    "StaticMethod",
                    "SingletonMethod",
                    "AsyncFunction",
                    "Constructor",
                ]),
                exclude = self.exclude("d.file_path"),
            ),
        )
    }

    fn tree<W: Write>(
        &self,
        client: &duckdb_client::DuckDbClient,
        out: &mut W,
        prefix: Option<&str>,
    ) -> Result<()> {
        let (prefix_filter, header) = match prefix {
            Some(p) => {
                let p = p.trim_end_matches('/');
                (
                    format!("AND file_path LIKE {}", sql_lit(&format!("{p}/%"))),
                    format!("under {p}/"),
                )
            }
            None => (String::new(), "all top-level files (truncated)".to_string()),
        };
        writeln!(out, "TREE — {header}")?;
        writeln!(out, "{}", "=".repeat(78))?;
        section(
            client,
            out,
            &format!(
                "WITH t AS (
  SELECT file_path, fqn, name, definition_type, start_line
  FROM gl_definition
  WHERE project_id={pid} AND commit_sha={sha}
    AND definition_type IN {types}
    {prefix_filter}{exclude}
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
LIMIT 200",
                pid = self.project_id,
                sha = sql_lit(&self.sha),
                types = kind_list(TYPE_KINDS),
                exclude = self.exclude("file_path"),
            ),
        )
    }

    fn api<W: Write>(
        &self,
        client: &duckdb_client::DuckDbClient,
        out: &mut W,
        prefix: &str,
    ) -> Result<()> {
        let prefix = prefix.trim_end_matches('/');
        let api_kinds: Vec<&str> = TYPE_KINDS
            .iter()
            .chain(CALLABLE_KINDS.iter())
            .copied()
            .collect();
        writeln!(out, "API MAP — under {prefix}/")?;
        writeln!(out, "{}", "=".repeat(78))?;
        section(
            client,
            out,
            &format!(
                "WITH defs AS (
  SELECT DISTINCT ON (file_path, name, definition_type, start_line)
         file_path, fqn, name, definition_type, start_line, end_line
  FROM gl_definition
  WHERE project_id={pid} AND commit_sha={sha}
    AND file_path LIKE {prefix_like}
    AND start_line > 0
    AND definition_type IN {api_kinds}{exclude}
  ORDER BY file_path, name, definition_type, start_line, length(fqn)
),
src AS (
  SELECT filename, str_split(content, chr(10)) AS lines
  FROM read_text({globs})
),
windows AS (
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
  SELECT file_path, fqn, name, definition_type, start_line,
    coalesce(
      list_filter(w, l -> regexp_matches(trim(l), {sig}) AND contains(l, name))[1],
      list_filter(w, l -> regexp_matches(trim(l), {sig}))[1],
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
LIMIT 300",
                pid = self.project_id,
                sha = sql_lit(&self.sha),
                prefix_like = sql_lit(&format!("{prefix}/%")),
                api_kinds = kind_list(&api_kinds),
                exclude = self.exclude("file_path"),
                globs = self.glob_list(prefix),
                sig = sql_lit(SIG_REGEX),
            ),
        )
    }

    fn class<W: Write>(
        &self,
        client: &duckdb_client::DuckDbClient,
        out: &mut W,
        target: &str,
    ) -> Result<()> {
        let sha = sql_lit(&self.sha);
        let pid = self.project_id;
        let t = sql_lit(target);
        let containers = kind_list(CONTAINER_KINDS);
        writeln!(out, "CLASS — {target}")?;
        writeln!(out, "{}", "=".repeat(78))?;

        section(
            client,
            out,
            &format!(
                "SELECT fqn, definition_type, file_path || ':' || start_line AS loc,
       (end_line - start_line) AS lines
FROM gl_definition
WHERE project_id={pid} AND commit_sha={sha}
  AND (fqn={t} OR name={t})
  AND definition_type IN {containers}{exclude}
ORDER BY loc
LIMIT 10",
                exclude = self.exclude("file_path"),
            ),
        )?;

        let file_batches = sql::query(
            client,
            &format!(
                "SELECT DISTINCT file_path
FROM gl_definition
WHERE project_id={pid} AND commit_sha={sha}
  AND (fqn={t} OR name={t})
  AND definition_type IN {containers}{exclude}",
                exclude = self.exclude("file_path"),
            ),
        )?;
        let file_paths = string_column(&file_batches, "file_path");
        if file_paths.is_empty() {
            writeln!(out, "(no class/module/trait named {target})")?;
            return Ok(());
        }
        let src_list = format!(
            "[{}]",
            file_paths
                .iter()
                .map(|p| sql_lit(p))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let member_kinds: Vec<&str> = CALLABLE_KINDS
            .iter()
            .chain(MEMBER_EXTRA_KINDS.iter())
            .copied()
            .collect();

        writeln!(out, "\nMembers + signatures:")?;
        section(
            client,
            out,
            &format!(
                "WITH parent AS (
  SELECT id, fqn, file_path, start_line, end_line
  FROM gl_definition
  WHERE project_id={pid} AND commit_sha={sha}
    AND (fqn={t} OR name={t})
    AND definition_type IN {containers}
),
contained AS (
  SELECT DISTINCT ON (file_path, name, definition_type, start_line)
         d.fqn, d.name, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d, parent p
  WHERE d.project_id={pid} AND d.commit_sha={sha}
    AND d.start_line > 0
    AND d.definition_type IN {members}{exclude}
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
      list_filter(w, l -> regexp_matches(trim(l), {sig}) AND contains(l, name))[1],
      list_filter(w, l -> regexp_matches(trim(l), {sig}))[1],
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
LIMIT 300",
                members = kind_list(&member_kinds),
                exclude = self.exclude("d.file_path"),
                sig = sql_lit(SIG_REGEX),
            ),
        )
    }

    fn extends<W: Write>(
        &self,
        client: &duckdb_client::DuckDbClient,
        out: &mut W,
        target: &str,
    ) -> Result<()> {
        writeln!(out, "DESCENDANTS — {target}")?;
        writeln!(out, "{}", "=".repeat(78))?;
        section(
            client,
            out,
            &format!(
                "WITH RECURSIVE chain AS (
  SELECT id, fqn, file_path, start_line, 0 AS depth
  FROM gl_definition
  WHERE project_id={pid} AND commit_sha={sha}
    AND (fqn={t} OR name={t})
    AND definition_type IN {abstractions}{exclude}
  UNION ALL
  SELECT s.id, s.fqn, s.file_path, s.start_line, c.depth + 1
  FROM gl_edge e
  JOIN chain c ON e.target_id = c.id AND e.relationship_kind='EXTENDS'
  JOIN gl_definition s ON s.id = e.source_id
  WHERE s.project_id={pid} AND s.commit_sha={sha} AND c.depth < 6{exclude_s}
)
SELECT depth, fqn, file_path || ':' || start_line AS loc
FROM chain
ORDER BY depth, fqn
LIMIT 200",
                pid = self.project_id,
                sha = sql_lit(&self.sha),
                t = sql_lit(target),
                abstractions = kind_list(ABSTRACTION_KINDS),
                exclude = self.exclude("file_path"),
                exclude_s = self.exclude("s.file_path"),
            ),
        )
    }

    fn imports<W: Write>(
        &self,
        client: &duckdb_client::DuckDbClient,
        out: &mut W,
        pattern: &str,
    ) -> Result<()> {
        let like = sql_lit(&format!("%{pattern}%"));
        writeln!(out, "IMPORTERS — pattern '{pattern}'")?;
        writeln!(out, "{}", "=".repeat(78))?;
        section(
            client,
            out,
            &format!(
                "SELECT identifier_name AS symbol, import_path,
       COUNT(DISTINCT file_path) AS importers
FROM gl_imported_symbol
WHERE project_id={pid} AND commit_sha={sha}{exclude}
  AND (identifier_name LIKE {like} OR import_path LIKE {like})
GROUP BY 1, 2
ORDER BY importers DESC
LIMIT 50",
                pid = self.project_id,
                sha = sql_lit(&self.sha),
                exclude = self.exclude("file_path"),
            ),
        )
    }
}

fn section<W: Write>(client: &duckdb_client::DuckDbClient, out: &mut W, query: &str) -> Result<()> {
    let batches = sql::query(client, query)?;
    sql_format::write_table(&mut *out, &batches)
}

/// SQL string literal with single-quote doubling.
fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn kind_list(kinds: &[&str]) -> String {
    format!(
        "({})",
        kinds
            .iter()
            .map(|k| sql_lit(k))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Split `--ext` values on commas, trim, and strip a leading dot, preserving
/// order while de-duplicating.
fn parse_extensions(raw: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for entry in raw {
        for value in entry.split(',') {
            let ext = value.trim().trim_start_matches('.');
            if !ext.is_empty() && seen.insert(ext.to_string()) {
                out.push(ext.to_string());
            }
        }
    }
    out
}

fn ext_regex(exts: &[String]) -> String {
    let alt = exts
        .iter()
        .map(|e| regex_escape(e))
        .collect::<Vec<_>>()
        .join("|");
    format!(r"\.({alt})$")
}

fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|arr| arr.value(0))
        .unwrap_or(0)
}

fn string_column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .flat_map(|arr| arr.iter().flatten().map(String::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_lit_doubles_single_quotes() {
        assert_eq!(sql_lit("O'Brien"), "'O''Brien'");
        assert_eq!(sql_lit("plain"), "'plain'");
    }

    #[test]
    fn kind_list_quotes_each_element() {
        assert_eq!(kind_list(&["Class", "Enum"]), "('Class', 'Enum')");
    }

    #[test]
    fn parse_extensions_normalizes_dedups_and_preserves_order() {
        let parsed = parse_extensions(&[".rs".into(), "rs,toml".into(), " py ".into()]);
        assert_eq!(parsed, vec!["rs", "toml", "py"]);
    }

    #[test]
    fn parse_extensions_empty_when_no_values() {
        assert!(parse_extensions(&[]).is_empty());
        assert!(parse_extensions(&["".into(), " , ".into()]).is_empty());
    }

    #[test]
    fn ext_regex_escapes_and_anchors() {
        assert_eq!(ext_regex(&["rs".into()]), r"\.(rs)$");
        assert_eq!(ext_regex(&["c++".into()]), r"\.(c\+\+)$");
    }

    // The map hides tests/vendored/generated paths, so the clause must render
    // one filter per pattern against the requested column.
    #[test]
    fn exclude_renders_for_the_given_column() {
        let map = RepoMap {
            sha: "abc".into(),
            project_id: 1,
            repo_display: "/repo".into(),
            ext_filter: vec![],
            source_exts: DEFAULT_SOURCE_EXTS.iter().map(|s| s.to_string()).collect(),
        };
        let clause = map.exclude("d.file_path");
        assert!(clause.contains("d.file_path NOT LIKE 'spec/%'"));
        assert!(clause.contains("d.file_path NOT LIKE '%/vendor/%'"));
        assert!(!clause.contains("regexp_matches(d.file_path, '\\.("));
    }

    #[test]
    fn exclude_appends_extension_filter_when_present() {
        let map = RepoMap {
            sha: "abc".into(),
            project_id: 1,
            repo_display: "/repo".into(),
            ext_filter: vec!["rs".into()],
            source_exts: vec!["rs".into()],
        };
        let clause = map.exclude("file_path");
        assert!(clause.contains(r"regexp_matches(file_path, '\.(rs)$')"));
    }

    #[test]
    fn glob_list_covers_each_source_extension() {
        let map = RepoMap {
            sha: "abc".into(),
            project_id: 1,
            repo_display: "/repo".into(),
            ext_filter: vec!["rs".into(), "toml".into()],
            source_exts: vec!["rs".into(), "toml".into()],
        };
        assert_eq!(
            map.glob_list("crates/foo/"),
            "['crates/foo/**/*.rs', 'crates/foo/**/*.toml']"
        );
    }
}
