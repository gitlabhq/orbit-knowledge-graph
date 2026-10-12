use std::fmt::Write as _;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use duckdb_client::search::{NodeHydrator, NodeValue, excluded_path_predicate};
use duckdb_client::{DuckDbClient, bool_column, i64_column, sql_lit, string_column};

use crate::commands::{context, setup::spec};

const FILE_CONNECTION_LIMIT: usize = 10;
use crate::workspace;

fn labels_cte(definition: &NodeHydrator) -> Result<String> {
    Ok(format!(
        "labels AS (
  SELECT {id} AS id, {fqn} AS label,
         {file} || ':' || CAST({start} AS VARCHAR) AS loc, {file} AS path,
         'Definition:' || CAST({id} AS VARCHAR) AS reference
  FROM {table} WHERE {project} = ?1 AND {commit} = ?2
  UNION ALL
  SELECT id, path, '', path, 'File:' || CAST(id AS VARCHAR)
  FROM gl_file WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT id, path, '', path, '' FROM gl_directory WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT i.id, concat_ws(' from ', NULLIF(i.identifier_name, ''), NULLIF(i.import_path, '')),
         COALESCE(d.{file} || ':' || CAST(d.{start} AS VARCHAR), ''),
         COALESCE(d.{file}, i.file_path),
         COALESCE('Definition:' || CAST(d.{id} AS VARCHAR), 'external/unresolved')
  FROM gl_imported_symbol i
  LEFT JOIN gl_edge e ON e.source_id = i.id AND e.relationship_kind = 'IMPORTS'
  LEFT JOIN {table} d ON d.{id} = e.target_id AND d.{project} = ?1 AND d.{commit} = ?2
  WHERE i.project_id = ?1 AND i.commit_sha = ?2
), import_files AS (
  SELECT i.id, f.id AS file_id FROM gl_imported_symbol i
  JOIN gl_file f ON f.path = i.file_path AND f.project_id = ?1 AND f.commit_sha = ?2
  WHERE i.project_id = ?1 AND i.commit_sha = ?2
)",
        id = definition.column("id")?,
        fqn = definition.column("fqn")?,
        file = definition.column("file_path")?,
        start = definition.column("start_line")?,
        table = definition.table(),
        project = definition.column("project_id")?,
        commit = definition.column("commit_sha")?,
    ))
}

struct Row {
    target_id: i64,
    kind: String,
    dir: String,
    label: String,
    reference: String,
    loc: String,
    via: String,
    hidden: bool,
    direct: bool,
}

fn rows_from(batches: &[RecordBatch], direct: bool) -> Vec<Row> {
    let target_ids = i64_column(batches, "target_id");
    let kinds = string_column(batches, "kind");
    let dirs = string_column(batches, "dir");
    let labels = string_column(batches, "label");
    let references = string_column(batches, "reference");
    let locs = string_column(batches, "loc");
    let vias = string_column(batches, "via");
    let hidden = bool_column(batches, "hidden");
    (0..kinds.len())
        .map(|i| Row {
            target_id: target_ids[i],
            kind: kinds[i].clone(),
            dir: dirs[i].clone(),
            label: labels[i].clone(),
            reference: references[i].clone(),
            loc: locs[i].clone(),
            via: vias[i].clone(),
            hidden: hidden[i],
            direct,
        })
        .collect()
}

pub(crate) fn render(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    nodes: &[NodeValue],
    file_members: &[context::SourceRange],
) -> Result<String> {
    if nodes.is_empty() {
        return Ok(String::new());
    }
    let labels_cte = labels_cte(hydrator)?;
    let definition_id = hydrator.column("id")?;
    let definition_file = hydrator.column("file_path")?;
    let definition_table = hydrator.table();
    let hidden_expr = format!("COALESCE({}, FALSE)", excluded_path_predicate("l.path"));
    let targets = nodes
        .iter()
        .enumerate()
        .map(|(order, node)| {
            let path = node
                .properties
                .get("path")
                .and_then(serde_json::Value::as_str);
            format!(
                "({order}, {}, {})",
                node.id,
                path.map(sql_lit).unwrap_or_else(|| "NULL".into())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let params = [git.project_id.into(), git.commit_sha.clone().into()];
    let edges = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id, path) AS (VALUES {targets})
SELECT DISTINCT t.ord, t.target_id, e.relationship_kind AS kind,
       CASE WHEN e.source_id = t.target_id THEN '-->' ELSE '<--' END AS dir,
       l.label, l.reference, l.loc, '' AS via, {hidden_expr} AS hidden
FROM targets t
JOIN gl_edge e ON e.source_id = t.target_id OR e.target_id = t.target_id
LEFT JOIN import_files i ON i.id = e.source_id
JOIN labels l ON l.id = CASE WHEN e.source_id = t.target_id THEN e.target_id ELSE COALESCE(i.file_id, e.source_id) END
WHERE e.relationship_kind <> 'DEFINES'
  AND (t.path IS NULL OR l.path <> t.path OR l.reference = 'external/unresolved')
ORDER BY t.ord, kind, dir DESC, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let via = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id, path) AS (VALUES {targets}),
members AS (
  SELECT t.ord, t.target_id, t.path, e.target_id AS id FROM targets t
  JOIN gl_edge e ON e.source_id = t.target_id AND e.relationship_kind = 'DEFINES'
  WHERE t.path IS NULL
  UNION ALL
  SELECT t.ord, t.target_id, t.path, d.{definition_id} FROM targets t
  JOIN {definition_table} d ON d.{definition_file} = t.path
  WHERE d.project_id = ?1 AND d.commit_sha = ?2
)
SELECT members.target_id, e.relationship_kind AS kind,
       CASE WHEN e.source_id = members.id THEN '-->' ELSE '<--' END AS dir,
       l.label, l.reference, l.loc,
       string_agg(DISTINCT def_name(m.label), ', ' ORDER BY def_name(m.label)) AS via,
       {hidden_expr} AS hidden
FROM gl_edge e
JOIN members ON members.id = e.target_id OR (members.path IS NOT NULL AND members.id = e.source_id)
JOIN labels m ON m.id = members.id
LEFT JOIN import_files i ON i.id = e.source_id
JOIN labels l ON l.id = CASE WHEN e.source_id = members.id THEN e.target_id ELSE COALESCE(i.file_id, e.source_id) END
WHERE e.relationship_kind <> 'DEFINES'
  AND l.id <> members.target_id
  AND (members.path IS NULL OR l.path <> members.path OR l.reference = 'external/unresolved')
  AND l.id NOT IN (
    SELECT own.id FROM members own WHERE own.target_id = members.target_id
  )
GROUP BY members.ord, members.target_id, kind, dir, l.label, l.reference, l.loc, l.path
ORDER BY members.ord, kind, dir DESC, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let edges = rows_from(&edges, true);
    let via = rows_from(&via, false);
    let mut out = String::new();
    for node in nodes {
        if !out.is_empty() {
            out.push('\n');
        }
        let is_file = node.entity_type == "File";
        if is_file {
            let path = node.properties["path"].as_str().unwrap();
            let members: Vec<_> = file_members
                .iter()
                .filter(|member| member.file == path)
                .cloned()
                .collect();
            writeln!(
                out,
                "File:{}  {}  [{}]  ({} definitions)",
                node.id,
                path,
                node.properties["language"].as_str().unwrap_or(""),
                members.len()
            )?;
            if let Some(reason) = node.properties["reason"]
                .as_str()
                .filter(|reason| !reason.is_empty())
            {
                writeln!(out, "Indexing reason: {reason}")?;
            }
            if members.is_empty() {
                writeln!(out, "No indexed definitions.")?;
            }
            for member in &members {
                writeln!(
                    out,
                    "  Definition:{}  {}  [{}]  L{}-{}",
                    member.id, member.fqn, member.kind, member.start, member.end
                )?;
            }
            if let Some(member) = members.first() {
                writeln!(
                    out,
                    "\nFor complete source and definition-specific connections, choose a Definition ID above.\nnext: {} context Definition:{}",
                    spec::launcher(),
                    member.id
                )?;
            }
        } else {
            let range = context::source_range(node)?;
            writeln!(
                out,
                "Definition:{}  {}  [{}]  {}:{}-{}",
                node.id, range.fqn, range.kind, range.file, range.start, range.end
            )?;
        }
        let links: Vec<_> = edges
            .iter()
            .filter(|row| row.target_id == node.id && !row.hidden)
            .collect();
        let used_via: Vec<_> = via
            .iter()
            .filter(|row| row.target_id == node.id && !row.hidden)
            .collect();
        let tests: Vec<_> = edges
            .iter()
            .chain(&via)
            .filter(|row| row.target_id == node.id && row.hidden)
            .collect();
        if links.is_empty() && used_via.is_empty() && tests.is_empty() {
            writeln!(out, "\nNo indexed connections.")?;
            continue;
        }
        for (title, rows) in [
            ("Connections", links),
            (
                if is_file {
                    "Connections via definitions"
                } else {
                    "Used via members"
                },
                used_via,
            ),
            ("Test, fixture, or generated connections", tests),
        ] {
            if rows.is_empty() {
                continue;
            }
            writeln!(out, "\n{title} ({} indexed):", rows.len())?;
            let mut prev_path = String::new();
            let limit = if is_file {
                FILE_CONNECTION_LIMIT
            } else {
                rows.len()
            };
            for row in rows.iter().take(limit) {
                let via = if row.via.is_empty() {
                    String::new()
                } else {
                    let names: Vec<_> = row.via.split(", ").collect();
                    if is_file && names.len() > 3 {
                        format!(
                            "  via {} (+{} names)",
                            names[..3].join(", "),
                            names.len() - 3
                        )
                    } else {
                        format!("  via {}", row.via)
                    }
                };
                writeln!(
                    out,
                    "  {} {}  [{}]  {}{via}{}",
                    row.dir,
                    row.label,
                    row.kind.to_lowercase(),
                    row.reference,
                    loc_suffix(&row.loc, &mut prev_path)
                )?;
            }
            let omitted = rows.len().saturating_sub(limit);
            if omitted > 0 {
                let omitted_rows = &rows[limit..];
                if omitted_rows.iter().any(|row| row.direct) {
                    writeln!(
                        out,
                        "  … {omitted} connections omitted; list all file edges: {} sql \"SELECT * FROM gl_edge WHERE source_id = {} OR target_id = {}\"",
                        spec::launcher(),
                        node.id,
                        node.id
                    )?;
                    if omitted_rows.iter().any(|row| !row.direct) {
                        writeln!(
                            out,
                            "  For omitted definition connections, use a Definition ID from the map for its complete connections."
                        )?;
                    }
                } else {
                    writeln!(
                        out,
                        "  … {omitted} connections omitted; use a Definition ID from the map for its complete connections."
                    )?;
                }
            }
        }
    }
    Ok(out)
}

/// Edges of the definitions a line range shows, each with a ready `path:start-end`, so the next
/// hop (who extends, calls, or is called) can be read in one batch. Subclasses and superclasses
/// come first, then code, with at most a couple of test links; vendored code is left out.
const RANGE_LINKS_PER_SIDE: usize = 6;
const RANGE_TEST_LINKS_PER_SIDE: usize = 2;

pub(crate) fn range_links(
    client: &DuckDbClient,
    path: &str,
    start: usize,
    end: usize,
    members: &[context::SourceRange],
) -> Result<String> {
    let inside: Vec<&context::SourceRange> = members
        .iter()
        .filter(|m| m.start >= start && m.end <= end)
        .collect();
    let shown: Vec<&context::SourceRange> = match inside.is_empty() {
        true => members
            .iter()
            .min_by_key(|m| m.end - m.start)
            .into_iter()
            .collect(),
        false => inside,
    };
    if shown.is_empty() {
        return Ok(String::new());
    }
    let ids = shown
        .iter()
        .map(|m| m.id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let batches = client.query_arrow_json(
        &format!(
            "WITH m AS (SELECT unnest([{ids}]::BIGINT[]) AS id),
links AS (
  SELECT '-->' AS dir, e.relationship_kind AS kind, COALESCE(ie.target_id, e.target_id) AS other
  FROM gl_edge e
  LEFT JOIN gl_edge ie ON e.target_kind = 'ImportedSymbol' AND ie.source_id = e.target_id
    AND ie.relationship_kind = 'IMPORTS' AND ie.target_kind = 'Definition'
  WHERE e.source_id IN (SELECT id FROM m) AND e.relationship_kind IN ('CALLS', 'EXTENDS')
  UNION ALL
  SELECT '<--', e.relationship_kind, e.source_id FROM gl_edge e
  WHERE e.target_id IN (SELECT id FROM m) AND e.relationship_kind IN ('CALLS', 'EXTENDS')
    AND e.source_kind = 'Definition'
)
SELECT DISTINCT dir, kind, d.fqn AS fqn, d.file_path AS file,
       CAST(d.start_line AS BIGINT) AS start_line, CAST(d.end_line AS BIGINT) AS end_line
FROM (SELECT DISTINCT dir, kind, other FROM links) l JOIN gl_definition d ON d.id = l.other
WHERE d.id NOT IN (SELECT id FROM m)
  AND NOT (d.file_path = ?1 AND d.start_line <= ?3 AND d.end_line >= ?2)
  AND NOT regexp_matches(d.file_path, '(^|/)(vendor|node_modules|third_party)/')
ORDER BY dir, kind = 'CALLS', d.file_path, d.start_line"
        ),
        &[path.into(), (start as i64).into(), (end as i64).into()],
    )?;
    let (dirs, kinds, fqns, files) = (
        string_column(&batches, "dir"),
        string_column(&batches, "kind"),
        string_column(&batches, "fqn"),
        string_column(&batches, "file"),
    );
    let (starts, ends) = (
        i64_column(&batches, "start_line"),
        i64_column(&batches, "end_line"),
    );
    let test = |i: usize| crate::commands::grep::is_test(&files[i]);
    let mut order: Vec<usize> = (0..dirs.len()).collect();
    order.sort_by_key(|&i| (dirs[i] == "-->", kinds[i] != "EXTENDS", test(i)));
    let (mut out, mut listed, mut tests) = (String::new(), [0usize; 2], [0usize; 2]);
    for i in order {
        let side = usize::from(dirs[i] == "-->");
        if listed[side] >= RANGE_LINKS_PER_SIDE
            || (test(i) && tests[side] >= RANGE_TEST_LINKS_PER_SIDE)
        {
            continue;
        }
        listed[side] += 1;
        tests[side] += usize::from(test(i));
        let mut name: Vec<&str> = fqns[i]
            .rsplit(['.', ':', '#', '/'])
            .filter(|p| !p.is_empty())
            .take(2)
            .collect();
        name.reverse();
        let kind = match (side, kinds[i].as_str()) {
            (0, "EXTENDS") => "extended by",
            (1, "EXTENDS") => "extends",
            (0, _) => "called by",
            _ => "calls",
        };
        writeln!(
            out,
            "  {} {kind} {}  {}:{}-{}",
            dirs[i],
            name.join("."),
            files[i],
            starts[i],
            ends[i]
        )?;
    }
    let more: Vec<String> = [("<--", 0), ("-->", 1)]
        .iter()
        .filter_map(|&(dir, side)| {
            let total = dirs.iter().filter(|d| *d == dir).count();
            (total > listed[side]).then(|| format!("{dir} +{}", total - listed[side]))
        })
        .collect();
    if !more.is_empty() {
        let target = match shown.as_slice() {
            [only] => only.fqn.as_str(),
            _ => "<name>",
        };
        writeln!(
            out,
            "  … {}; all of them: {} context {target}",
            more.join(", "),
            spec::launcher()
        )?;
    }
    Ok(match out.is_empty() {
        true => out,
        false => format!("Connections:\n{out}"),
    })
}

fn loc_suffix(loc: &str, prev_path: &mut String) -> String {
    if loc.is_empty() {
        return String::new();
    }
    let (path, line) = loc.rsplit_once(':').unwrap_or((loc, ""));
    let suffix = if path == prev_path {
        format!("  (:{line})")
    } else {
        format!("  ({loc})")
    };
    *prev_path = path.to_string();
    suffix
}
