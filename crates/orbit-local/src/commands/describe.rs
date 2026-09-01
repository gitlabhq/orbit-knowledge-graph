use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::{i64_column, string_column};

use crate::{sql, workspace};

pub(crate) fn run(fqn: String, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let top_level = workspace::git_toplevel(&repo.unwrap_or_else(|| PathBuf::from(".")))?;
    let git = workspace::git_info(&top_level)?;
    let client = sql::open_graph(db)?;
    let defs = client.query_arrow_json(
        "SELECT id, definition_type, file_path, start_line, end_line
         FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 AND fqn = ?3
         ORDER BY file_path, start_line",
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            fqn.clone().into(),
        ],
    )?;
    let ids = i64_column(&defs, "id");
    let kinds = string_column(&defs, "definition_type");
    let files = string_column(&defs, "file_path");
    let starts = i64_column(&defs, "start_line");
    let ends = i64_column(&defs, "end_line");
    if ids.is_empty() {
        anyhow::bail!(
            "no definition {fqn:?} for commit {} — pass the exact fqn printed by \
             `orbit local ask`, and make sure the commit is indexed (`orbit index <path>`)",
            git.commit_sha
        );
    }
    for i in 0..ids.len() {
        if i > 0 {
            println!();
        }
        let edges = client.query_arrow_json(
            "WITH labels AS (
  SELECT id, fqn AS label,
         file_path || ':' || CAST(start_line AS VARCHAR) AS loc
  FROM gl_definition WHERE project_id = ?2 AND commit_sha = ?3
  UNION ALL
  SELECT id, path, '' FROM gl_file WHERE project_id = ?2 AND commit_sha = ?3
  UNION ALL
  SELECT id, path, '' FROM gl_directory WHERE project_id = ?2 AND commit_sha = ?3
  UNION ALL
  SELECT id, identifier_name, '' FROM gl_imported_symbol
  WHERE project_id = ?2 AND commit_sha = ?3
)
SELECT DISTINCT e.relationship_kind AS kind,
       CASE WHEN e.source_id = ?1 THEN '-->' ELSE '<--' END AS dir,
       l.label, l.loc
FROM gl_edge e
JOIN labels l ON l.id = CASE WHEN e.source_id = ?1 THEN e.target_id ELSE e.source_id END
WHERE e.source_id = ?1 OR e.target_id = ?1
ORDER BY kind, dir DESC, l.label, l.loc",
            &[
                ids[i].into(),
                git.project_id.into(),
                git.commit_sha.clone().into(),
            ],
        )?;
        let edge_kinds = string_column(&edges, "kind");
        let dirs = string_column(&edges, "dir");
        let labels = string_column(&edges, "label");
        let locs = string_column(&edges, "loc");
        println!(
            "{fqn}  [{}]  {}:{}-{}  (links {})",
            kinds[i],
            files[i],
            starts[i],
            ends[i],
            edge_kinds.len()
        );
        if edge_kinds.is_empty() {
            println!("\nNo connections.");
            continue;
        }
        println!("\nConnections ({}):", edge_kinds.len());
        for j in 0..edge_kinds.len() {
            let loc = if locs[j].is_empty() {
                String::new()
            } else {
                format!("  ({})", locs[j])
            };
            println!(
                "  {} {}  [{}]{loc}",
                dirs[j],
                labels[j],
                edge_kinds[j].to_lowercase()
            );
        }
    }
    Ok(())
}
