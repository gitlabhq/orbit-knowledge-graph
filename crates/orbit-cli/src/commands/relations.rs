use anyhow::Result;
use arrow::record_batch::RecordBatch;
use duckdb_client::search::{NodeHydrator, excluded_path_predicate};
use duckdb_client::{bool_column, string_column};

use crate::commands::{context, definition};
use crate::workspace;

fn labels_cte(definition: &NodeHydrator) -> Result<String> {
    Ok(format!(
        "labels AS (
  SELECT {id} AS id, {fqn} AS label,
         {file} || ':' || CAST({start} AS VARCHAR) AS loc, {file} AS path
  FROM {table} WHERE {project} = ?2 AND {commit} = ?3
  UNION ALL
  SELECT id, path, '', path FROM gl_file WHERE project_id = ?2 AND commit_sha = ?3
  UNION ALL
  SELECT id, path, '', path FROM gl_directory WHERE project_id = ?2 AND commit_sha = ?3
  UNION ALL
  SELECT id, identifier_name, '', file_path FROM gl_imported_symbol
  WHERE project_id = ?2 AND commit_sha = ?3
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
    kind: String,
    dir: String,
    label: String,
    loc: String,
    via: String,
}

fn rows_from(batches: &[RecordBatch], show_tests: bool) -> (Vec<Row>, usize) {
    let kinds = string_column(batches, "kind");
    let dirs = string_column(batches, "dir");
    let labels = string_column(batches, "label");
    let locs = string_column(batches, "loc");
    let vias = string_column(batches, "via");
    let hidden = bool_column(batches, "hidden");
    let rows: Vec<Row> = (0..kinds.len())
        .filter(|&j| show_tests || !hidden[j])
        .map(|j| Row {
            kind: kinds[j].clone(),
            dir: dirs[j].clone(),
            label: labels[j].clone(),
            loc: locs[j].clone(),
            via: vias[j].clone(),
        })
        .collect();
    let hidden = kinds.len() - rows.len();
    (rows, hidden)
}

pub(crate) fn run(target: crate::ContextArgs) -> Result<()> {
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(target.repo, target.db)?;
    let (file, ids) = context::resolve_targets(&git.repo_path, &target.target)?;
    anyhow::ensure!(
        file.is_none(),
        "relationships require Definition:<id> targets"
    );
    let hydrator = NodeHydrator::embedded("Definition")?;
    let defs = definition::resolve_ids(&client, &git, &hydrator, &ids)?;
    let labels_cte = labels_cte(&hydrator)?;
    let definition_id = hydrator.column("id")?;
    let definition_fqn = hydrator.column("fqn")?;
    let definition_table = hydrator.table();
    let hidden_expr = format!("COALESCE({}, FALSE)", excluded_path_predicate("l.path"));
    for (i, def) in defs.iter().enumerate() {
        if i > 0 {
            println!();
        }
        let range = context::source_range(def)?;
        let params = [
            def.id.into(),
            git.project_id.into(),
            git.commit_sha.clone().into(),
        ];
        let edges = client.query_arrow_json(
            &format!(
                "WITH {labels_cte}
SELECT DISTINCT e.relationship_kind AS kind,
       CASE WHEN e.source_id = ?1 THEN '-->' ELSE '<--' END AS dir,
       l.label, l.loc, '' AS via, {hidden_expr} AS hidden
FROM gl_edge e
JOIN labels l ON l.id = CASE WHEN e.source_id = ?1 THEN e.target_id ELSE e.source_id END
WHERE e.source_id = ?1 OR e.target_id = ?1
ORDER BY kind, dir DESC, l.path, l.label, l.loc"
            ),
            &params,
        )?;
        let (links, links_hidden) = rows_from(&edges, target.tests);

        let via = client.query_arrow_json(
            &format!(
                "WITH {labels_cte},
members AS (
  SELECT target_id AS id FROM gl_edge
  WHERE source_id = ?1 AND relationship_kind = 'DEFINES'
)
SELECT e.relationship_kind AS kind, '<--' AS dir, l.label, l.loc,
       string_agg(DISTINCT def_name(m.{definition_fqn}), ', ' ORDER BY def_name(m.{definition_fqn})) AS via,
       {hidden_expr} AS hidden
FROM gl_edge e
JOIN members ON members.id = e.target_id
JOIN {definition_table} m ON m.{definition_id} = e.target_id
JOIN labels l ON l.id = e.source_id
WHERE e.relationship_kind <> 'DEFINES'
  AND e.source_id <> ?1
  AND e.source_id NOT IN (SELECT id FROM members)
GROUP BY kind, l.label, l.loc, l.path
ORDER BY kind, l.path, l.label, l.loc"
            ),
            &params,
        )?;
        let (via, via_hidden) = rows_from(&via, target.tests);

        println!(
            "Definition:{}  {}  [{}]  {}:{}-{}  (links {}, via members {})",
            def.id,
            range.fqn,
            range.kind,
            range.file,
            range.start,
            range.end,
            links.len(),
            via.len(),
        );
        let hidden = links_hidden + via_hidden;
        if links.is_empty() && via.is_empty() {
            if hidden > 0 {
                println!(
                    "\nNo connections outside test, fixture, or generated files \
                     ({hidden} hidden; pass --tests to show them)."
                );
            } else {
                println!("\nNo connections.");
            }
            continue;
        }
        let via_title = format!(
            "Used via members ({}) — callers of this definition's fields, methods, or items:",
            via.len()
        );
        let sections = [
            (
                format!("Connections ({}):", links.len()),
                links,
                links_hidden,
            ),
            (via_title, via, via_hidden),
        ];
        for (title, rows, hidden) in sections {
            if rows.is_empty() && hidden == 0 {
                continue;
            }
            println!("\n{title}");
            let mut prev_path = String::new();
            for row in &rows {
                let via = if row.via.is_empty() {
                    String::new()
                } else {
                    format!("  via {}", row.via)
                };
                println!(
                    "  {} {}  [{}]{via}{}",
                    row.dir,
                    row.label,
                    row.kind.to_lowercase(),
                    loc_suffix(&row.loc, &mut prev_path)
                );
            }
            if hidden > 0 {
                println!(
                    "  … {hidden} more in test, fixture, or generated files — pass --tests to show them"
                );
            }
        }
    }
    Ok(())
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
