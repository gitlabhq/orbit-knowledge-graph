use std::collections::{BTreeSet, HashMap};

use anyhow::Result;
use duckdb_client::search::NodeHydrator;
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};

use super::rank::is_test;
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

/// Callers and callees of each listed definition, by definition id.
pub(super) type Connections = HashMap<i64, (Vec<String>, Vec<String>)>;

const CONNECTION_NAMES: usize = 8;
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

pub(super) fn connections(client: &DuckDbClient, hits: &[Hit]) -> Result<Connections> {
    let mut ids: Vec<i64> = hits
        .iter()
        .filter_map(|h| h.def.as_ref().map(|d| d.id))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    type Named = Vec<(bool, String)>;
    let mut raw: HashMap<i64, (Named, Named)> = HashMap::new();
    if ids.is_empty() {
        return Ok(Connections::new());
    }
    let list = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
    client.execute(
        &format!(
            "CREATE OR REPLACE TEMP TABLE grep_defs AS SELECT unnest([{list}]::BIGINT[]) AS id"
        ),
        &[],
    )?;
    {
        let callers = client.query_arrow_json(
            "SELECT DISTINCT e.target_id AS def, d.name AS name, d.file_path AS file
FROM gl_edge e JOIN gl_definition d ON d.id = e.source_id
WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition'
  AND e.target_kind = 'Definition' AND e.target_id IN (SELECT id FROM grep_defs)
  AND e.source_id <> e.target_id",
            &[],
        )?;
        let callees = client.query_arrow_json(
            "WITH calls AS (
  SELECT e.source_id, e.target_id, e.target_kind FROM gl_edge e
  JOIN grep_defs g ON g.id = e.source_id
  WHERE e.relationship_kind = 'CALLS' AND e.source_id <> e.target_id
)
SELECT DISTINCT c.source_id AS def, d.name AS name, d.file_path AS file
FROM calls c JOIN gl_definition d ON d.id = c.target_id
WHERE c.target_kind = 'Definition'
UNION
SELECT DISTINCT c.source_id AS def,
       COALESCE(NULLIF(i.identifier_alias, ''), NULLIF(i.identifier_name, ''), i.import_path) AS name,
       i.file_path AS file
FROM calls c JOIN gl_imported_symbol i ON i.id = c.target_id
WHERE c.target_kind = 'ImportedSymbol'",
            &[],
        )?;
        for (batches, callers_side) in [(callers, true), (callees, false)] {
            let (defs, names, files) = (
                i64_column(&batches, "def"),
                string_column(&batches, "name"),
                string_column(&batches, "file"),
            );
            for ((def, name), file) in defs.into_iter().zip(names).zip(files) {
                if name.is_empty() {
                    continue;
                }
                let entry = raw.entry(def).or_default();
                match callers_side {
                    true => entry.0.push((is_test(&file), name)),
                    false => entry.1.push((is_test(&file), name)),
                }
            }
        }
    }
    let names = |mut list: Named| {
        list.sort();
        let mut seen = std::collections::HashSet::new();
        list.into_iter()
            .filter_map(|(_, name)| seen.insert(name.clone()).then_some(name))
            .collect::<Vec<_>>()
    };
    Ok(raw
        .into_iter()
        .map(|(def, (callers, callees))| (def, (names(callers), names(callees))))
        .collect())
}

pub(super) fn connection_label(def: &Def, connections: &Connections) -> String {
    let Some((callers, callees)) = connections.get(&def.id) else {
        return String::new();
    };
    let side = |arrow: &str, names: &[String], noun: &str| match names.len() {
        0 => String::new(),
        n if n > CONNECTION_NAMES => format!("{arrow}{n} {noun} "),
        _ => format!("{arrow}{} ", names.join(",")),
    };
    format!(
        "{}{}",
        side("←", callers, "callers"),
        side("→", callees, "callees")
    )
}
