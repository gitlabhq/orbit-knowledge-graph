use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::string_column;

use crate::commands::fqn;

const LABELS_CTE: &str = "labels AS (
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
)";

pub(crate) fn run(fqn: String, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let resolved = fqn::resolve(&fqn, repo, db)?;
    for i in 0..resolved.ids.len() {
        if i > 0 {
            println!();
        }
        let params = [
            resolved.ids[i].into(),
            resolved.git.project_id.into(),
            resolved.git.commit_sha.clone().into(),
        ];
        let edges = resolved.client.query_arrow_json(
            &format!(
                "WITH {LABELS_CTE}
SELECT DISTINCT e.relationship_kind AS kind,
       CASE WHEN e.source_id = ?1 THEN '-->' ELSE '<--' END AS dir,
       l.label, l.loc
FROM gl_edge e
JOIN labels l ON l.id = CASE WHEN e.source_id = ?1 THEN e.target_id ELSE e.source_id END
WHERE e.source_id = ?1 OR e.target_id = ?1
ORDER BY kind, dir DESC, l.label, l.loc"
            ),
            &params,
        )?;
        let edge_kinds = string_column(&edges, "kind");
        let dirs = string_column(&edges, "dir");
        let labels = string_column(&edges, "label");
        let locs = string_column(&edges, "loc");

        let via = resolved.client.query_arrow_json(
            &format!(
                "WITH {LABELS_CTE},
members AS (
  SELECT target_id AS id FROM gl_edge
  WHERE source_id = ?1 AND relationship_kind = 'DEFINES'
)
SELECT e.relationship_kind AS kind, l.label, l.loc,
       string_agg(DISTINCT def_name(m.fqn), ', ' ORDER BY def_name(m.fqn)) AS via
FROM gl_edge e
JOIN members ON members.id = e.target_id
JOIN gl_definition m ON m.id = e.target_id
JOIN labels l ON l.id = e.source_id
WHERE e.relationship_kind <> 'DEFINES'
  AND e.source_id <> ?1
  AND e.source_id NOT IN (SELECT id FROM members)
GROUP BY kind, l.label, l.loc
ORDER BY kind, l.label, l.loc"
            ),
            &params,
        )?;
        let via_kinds = string_column(&via, "kind");
        let via_labels = string_column(&via, "label");
        let via_locs = string_column(&via, "loc");
        let via_members = string_column(&via, "via");

        println!(
            "{fqn}  [{}]  {}:{}-{}  (links {}, via members {})",
            resolved.kinds[i],
            resolved.files[i],
            resolved.starts[i],
            resolved.ends[i],
            edge_kinds.len(),
            via_kinds.len(),
        );
        if edge_kinds.is_empty() && via_kinds.is_empty() {
            println!("\nNo connections.");
            continue;
        }
        if !edge_kinds.is_empty() {
            println!("\nConnections ({}):", edge_kinds.len());
            for j in 0..edge_kinds.len() {
                println!(
                    "  {} {}  [{}]{}",
                    dirs[j],
                    labels[j],
                    edge_kinds[j].to_lowercase(),
                    loc_suffix(&locs[j])
                );
            }
        }
        if !via_kinds.is_empty() {
            println!(
                "\nUsed via members ({}) — callers of this definition's fields, methods, or items:",
                via_kinds.len()
            );
            for j in 0..via_kinds.len() {
                println!(
                    "  <-- {}  [{}]  via {}{}",
                    via_labels[j],
                    via_kinds[j].to_lowercase(),
                    via_members[j],
                    loc_suffix(&via_locs[j])
                );
            }
        }
    }
    Ok(())
}

fn loc_suffix(loc: &str) -> String {
    if loc.is_empty() {
        String::new()
    } else {
        format!("  ({loc})")
    }
}
