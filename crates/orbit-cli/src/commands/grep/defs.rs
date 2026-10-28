use std::collections::{BTreeSet, HashMap};

use anyhow::Result;
use duckdb_client::search::NodeHydrator;
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};

use super::scan::Hit;
use super::term::Term;
use crate::workspace::GitInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Def {
    pub(super) name: String,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) id: i64,
    pub(super) kind: String,
}

type ImportsByLine = HashMap<(String, usize), Vec<(String, String)>>;

const DEF_FILES_PER_QUERY: usize = 500;

struct Candidate {
    def: Def,
    kind: String,
    id: i64,
}

pub(super) fn attach_definitions(
    client: &DuckDbClient,
    git: &GitInfo,
    hits: &mut Vec<Hit>,
    alternatives: &[Term],
    kinds: &[String],
    edited: &BTreeSet<String>,
) -> Result<()> {
    let node = NodeHydrator::embedded("Definition")?;
    let mut files: Vec<&str> = hits
        .iter()
        .map(|h| h.file.as_str())
        .filter(|file| !edited.contains(*file))
        .collect();
    files.dedup();
    let mut by_file: HashMap<String, Vec<Candidate>> = HashMap::new();
    for chunk in files.chunks(DEF_FILES_PER_QUERY) {
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {file} AS file_path, {name} AS name, {kind} AS kind, {id} AS id,
       CAST({start} AS BIGINT) AS def_start, CAST({end} AS BIGINT) AS def_end
FROM {table}
WHERE {project} = ?1 AND {commit} = ?2 AND {fqn} NOT LIKE '%@%' AND {file} IN ({list})",
                file = node.column("file_path")?,
                name = node.column("name")?,
                kind = node.column("definition_type")?,
                id = node.column("id")?,
                start = node.column("start_line")?,
                end = node.column("end_line")?,
                table = node.table(),
                project = node.column("project_id")?,
                commit = node.column("commit_sha")?,
                fqn = node.column("fqn")?,
                list = chunk
                    .iter()
                    .map(|f| sql_lit(f))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            &[git.project_id.into(), git.commit_sha.clone().into()],
        )?;
        let (paths, names, kinds_col) = (
            string_column(&batches, "file_path"),
            string_column(&batches, "name"),
            string_column(&batches, "kind"),
        );
        let (ids, starts, ends) = (
            i64_column(&batches, "id"),
            i64_column(&batches, "def_start"),
            i64_column(&batches, "def_end"),
        );
        for i in 0..paths.len() {
            by_file
                .entry(paths[i].clone())
                .or_default()
                .push(Candidate {
                    def: Def {
                        name: names[i].clone(),
                        start: starts[i] as usize,
                        end: ends[i] as usize,
                        id: ids[i],
                        kind: kinds_col[i].clone(),
                    },
                    kind: kinds_col[i].to_lowercase(),
                    id: ids[i],
                });
        }
    }
    let imports = imports_by_line(client, git, &files)?;
    let wanted: Vec<String> = kinds.iter().map(|k| k.to_lowercase()).collect();
    hits.retain_mut(|hit| {
        let best = by_file.get(&hit.file).and_then(|defs| {
            defs.iter()
                .filter(|c| c.def.start <= hit.line && hit.line <= c.def.end)
                .min_by_key(|c| {
                    let named = c.def.start == hit.line
                        && alternatives.iter().any(|a| a.names(&c.def.name));
                    (
                        !named,
                        c.def.end <= c.def.start,
                        c.def.end - c.def.start,
                        c.id,
                    )
                })
        });
        let keep =
            hit.context || wanted.is_empty() || best.is_some_and(|c| wanted.contains(&c.kind));
        hit.def = best.map(|c| c.def.clone());
        if hit.def.is_none()
            && let Some(symbols) = imports.get(&(hit.file.clone(), hit.line))
        {
            let named: Vec<String> = symbols
                .iter()
                .filter(|(name, _)| alternatives.iter().any(|a| a.mentions(name)))
                .map(|(_, label)| label.clone())
                .collect();
            hit.imports = match named.is_empty() {
                true => symbols.iter().map(|(_, label)| label.clone()).collect(),
                false => named,
            };
        }
        keep
    });
    let matched: std::collections::HashSet<String> = hits
        .iter()
        .filter(|hit| !hit.context)
        .map(|hit| hit.file.clone())
        .collect();
    hits.retain(|hit| matched.contains(&hit.file));
    Ok(())
}

fn imports_by_line(client: &DuckDbClient, git: &GitInfo, files: &[&str]) -> Result<ImportsByLine> {
    let mut by_line = ImportsByLine::new();
    for chunk in files.chunks(DEF_FILES_PER_QUERY) {
        let list = chunk
            .iter()
            .map(|f| sql_lit(f))
            .collect::<Vec<_>>()
            .join(", ");
        let batches = client.query_arrow_json(
            &format!(
                "SELECT i.file_path AS file_path, CAST(i.start_line AS BIGINT) AS line,
       COALESCE(NULLIF(i.identifier_alias, ''), NULLIF(i.identifier_name, ''), i.import_path, '') AS name,
       COALESCE(i.import_path, '') AS path,
       COALESCE(any_value(d.file_path), '') AS target_file,
       CAST(COALESCE(any_value(d.start_line), 0) AS BIGINT) AS target_line
FROM gl_imported_symbol i
LEFT JOIN gl_edge e ON e.source_id = i.id AND e.source_kind = 'ImportedSymbol'
  AND e.relationship_kind = 'IMPORTS' AND e.target_kind = 'Definition'
LEFT JOIN gl_definition d ON d.id = e.target_id
WHERE i.project_id = ?1 AND i.commit_sha = ?2 AND i.file_path IN ({list})
GROUP BY ALL
ORDER BY 1, 2, min(i.start_byte)"
            ),
            &[git.project_id.into(), git.commit_sha.clone().into()],
        )?;
        let (paths, lines, names, imported, targets, target_lines) = (
            string_column(&batches, "file_path"),
            i64_column(&batches, "line"),
            string_column(&batches, "name"),
            string_column(&batches, "path"),
            string_column(&batches, "target_file"),
            i64_column(&batches, "target_line"),
        );
        for i in 0..paths.len() {
            let mut label = match names[i] == imported[i] {
                true => names[i].clone(),
                false => format!("{} ({})", names[i], imported[i]),
            };
            if !targets[i].is_empty() {
                label.push_str(&format!(" → {}:{}", targets[i], target_lines[i]));
            }
            let entry = by_line
                .entry((paths[i].clone(), lines[i] as usize))
                .or_default();
            if !entry.iter().any(|(_, existing)| *existing == label) {
                entry.push((names[i].clone(), label));
            }
        }
    }
    Ok(by_line)
}
