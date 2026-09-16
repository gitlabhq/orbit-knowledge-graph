use std::fmt::Write as _;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use duckdb_client::search::{NodeHydrator, NodeValue, excluded_path_predicate};
use duckdb_client::{DuckDbClient, bool_column, i64_column, string_column};

use crate::commands::context;
use crate::workspace;

const CONNECTION_LIMIT: usize = 10;
const TEST_CONNECTION_LIMIT: usize = 3;

fn labels_cte(definition: &NodeHydrator) -> Result<String> {
    Ok(format!(
        "labels AS (
  SELECT {id} AS id, {fqn} AS label,
         {file} || ':' || CAST({start} AS VARCHAR) AS loc, {file} AS path,
         'Definition:' || CAST({id} AS VARCHAR) AS reference
  FROM {table} WHERE {project} = ?1 AND {commit} = ?2
  UNION ALL
  SELECT id, path, '', path, '' FROM gl_file WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT id, path, '', path, '' FROM gl_directory WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT id, identifier_name, '', file_path, 'external/unresolved' FROM gl_imported_symbol
  WHERE project_id = ?1 AND commit_sha = ?2
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
}

fn rows_from(batches: &[RecordBatch]) -> Vec<Row> {
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
        })
        .collect()
}

pub(crate) fn render(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    defs: &[NodeValue],
    show_tests: bool,
) -> Result<String> {
    if defs.is_empty() {
        return Ok(String::new());
    }
    let labels_cte = labels_cte(hydrator)?;
    let definition_id = hydrator.column("id")?;
    let definition_fqn = hydrator.column("fqn")?;
    let definition_table = hydrator.table();
    let hidden_expr = format!("COALESCE({}, FALSE)", excluded_path_predicate("l.path"));
    let targets = defs
        .iter()
        .enumerate()
        .map(|(order, def)| format!("({order}, {})", def.id))
        .collect::<Vec<_>>()
        .join(", ");
    let params = [git.project_id.into(), git.commit_sha.clone().into()];
    let edges = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id) AS (VALUES {targets})
SELECT DISTINCT t.ord, t.target_id, e.relationship_kind AS kind,
       CASE WHEN e.source_id = t.target_id THEN '-->' ELSE '<--' END AS dir,
       l.label, l.reference, l.loc, '' AS via, {hidden_expr} AS hidden
FROM targets t
JOIN gl_edge e ON e.source_id = t.target_id OR e.target_id = t.target_id
JOIN labels l ON l.id = CASE WHEN e.source_id = t.target_id THEN e.target_id ELSE e.source_id END
WHERE e.relationship_kind <> 'DEFINES'
ORDER BY t.ord, kind, dir DESC, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let via = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id) AS (VALUES {targets}),
members AS (
  SELECT t.ord, t.target_id, e.target_id AS id FROM targets t
  JOIN gl_edge e ON e.source_id = t.target_id AND e.relationship_kind = 'DEFINES'
)
SELECT members.target_id, e.relationship_kind AS kind, '<--' AS dir, l.label, l.reference, l.loc,
       string_agg(DISTINCT def_name(m.{definition_fqn}), ', ' ORDER BY def_name(m.{definition_fqn})) AS via,
       {hidden_expr} AS hidden
FROM gl_edge e
JOIN members ON members.id = e.target_id
JOIN {definition_table} m ON m.{definition_id} = e.target_id
JOIN labels l ON l.id = e.source_id
WHERE e.relationship_kind <> 'DEFINES'
  AND e.source_id <> members.target_id
  AND e.source_id NOT IN (
    SELECT own.id FROM members own WHERE own.target_id = members.target_id
  )
GROUP BY members.ord, members.target_id, kind, l.label, l.reference, l.loc, l.path
ORDER BY members.ord, kind, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let edges = rows_from(&edges);
    let via = rows_from(&via);
    let mut out = String::new();
    for def in defs {
        if !out.is_empty() {
            out.push('\n');
        }
        let range = context::source_range(def)?;
        writeln!(
            out,
            "Definition:{}  {}  [{}]  {}:{}-{}",
            def.id, range.fqn, range.kind, range.file, range.start, range.end
        )?;
        let links: Vec<_> = edges
            .iter()
            .filter(|row| row.target_id == def.id && !row.hidden)
            .collect();
        let used_via: Vec<_> = via
            .iter()
            .filter(|row| row.target_id == def.id && !row.hidden)
            .collect();
        let tests: Vec<_> = edges
            .iter()
            .chain(&via)
            .filter(|row| row.target_id == def.id && row.hidden)
            .collect();
        if links.is_empty() && used_via.is_empty() && tests.is_empty() {
            writeln!(out, "\nNo indexed connections.")?;
            continue;
        }
        for (title, rows, limit) in [
            ("Connections", links, CONNECTION_LIMIT),
            ("Used via members", used_via, CONNECTION_LIMIT),
            (
                "Test, fixture, or generated connections",
                tests,
                if show_tests {
                    CONNECTION_LIMIT
                } else {
                    TEST_CONNECTION_LIMIT
                },
            ),
        ] {
            if rows.is_empty() {
                continue;
            }
            writeln!(
                out,
                "\n{title} (showing {} of {} indexed):",
                rows.len().min(limit),
                rows.len()
            )?;
            let mut prev_path = String::new();
            for row in rows.iter().take(limit) {
                let via = if row.via.is_empty() {
                    String::new()
                } else {
                    format!("  via {}", row.via)
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
            if rows.len() > limit {
                writeln!(
                    out,
                    "  … {} omitted; follow a Definition:<id> for focused context{}.",
                    rows.len() - limit,
                    if title.starts_with("Test") && !show_tests {
                        "; --tests expands this sample"
                    } else {
                        ""
                    }
                )?;
            }
        }
    }
    Ok(out)
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
