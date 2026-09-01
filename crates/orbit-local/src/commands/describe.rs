use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::string_column;

use crate::commands::fqn;

pub(crate) fn run(fqn: String, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let resolved = fqn::resolve(&fqn, repo, db)?;
    for i in 0..resolved.ids.len() {
        if i > 0 {
            println!();
        }
        let edges = resolved.client.query_arrow_json(
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
                resolved.ids[i].into(),
                resolved.git.project_id.into(),
                resolved.git.commit_sha.clone().into(),
            ],
        )?;
        let edge_kinds = string_column(&edges, "kind");
        let dirs = string_column(&edges, "dir");
        let labels = string_column(&edges, "label");
        let locs = string_column(&edges, "loc");
        println!(
            "{fqn}  [{}]  {}:{}-{}  (links {})",
            resolved.kinds[i],
            resolved.files[i],
            resolved.starts[i],
            resolved.ends[i],
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
