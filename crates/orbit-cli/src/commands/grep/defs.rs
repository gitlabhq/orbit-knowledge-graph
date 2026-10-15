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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Links {
    pub(super) callers: Vec<String>,
    pub(super) callees: Vec<String>,
    pub(super) siblings: Vec<String>,
}

pub(super) type Connections = HashMap<i64, Links>;

/// Import symbols by `(file, line)`, each as `(name, label)`.
type ImportsByLine = HashMap<(String, usize), Vec<(String, String)>>;

const CONNECTION_NAMES: usize = 8;
const SIBLINGS_SHOWN: usize = 4;
const SIBLINGS_MAX: usize = 25;
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

/// Each import line's symbols as `(name, label)`, the label being `name (path)` with
/// `→ file:line` when the graph resolves the import to a local definition.
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
    let mut links: Connections = raw
        .into_iter()
        .map(|(def, (callers, callees))| {
            let links = Links {
                callers: names(callers),
                callees: names(callees),
                siblings: Vec::new(),
            };
            (def, links)
        })
        .collect();
    let shown: std::collections::HashSet<&str> = hits.iter().map(|h| h.file.as_str()).collect();
    for (def, siblings) in siblings(client, &shown)? {
        links.entry(def).or_default().siblings = siblings;
    }
    Ok(links)
}

fn siblings(
    client: &DuckDbClient,
    shown: &std::collections::HashSet<&str>,
) -> Result<HashMap<i64, Vec<String>>> {
    let family = |column: &str| {
        format!(
            "CASE WHEN regexp_matches({column}, 'Function|Method|Constructor') THEN 1 \
             WHEN regexp_matches({column}, 'Class|Struct|Interface|Trait|Record|Enum|Object') THEN 2 END"
        )
    };
    let batches = client.query_arrow_json(
        &format!(
            "SELECT g.id AS def, d.file_path AS file, CAST(d.start_line AS BIGINT) AS line
FROM grep_defs g
JOIN gl_definition s ON s.id = g.id
JOIN gl_definition d ON d.project_id = s.project_id AND d.commit_sha = s.commit_sha
  AND d.name = s.name AND d.file_path <> s.file_path AND d.fqn NOT LIKE '%@%'
WHERE {source} IS NOT NULL AND {source} = {target}",
            source = family("s.definition_type"),
            target = family("d.definition_type"),
        ),
        &[],
    )?;
    let (defs, files, lines) = (
        i64_column(&batches, "def"),
        string_column(&batches, "file"),
        i64_column(&batches, "line"),
    );
    let mut found: HashMap<i64, Vec<(bool, String, i64)>> = HashMap::new();
    for ((def, file), line) in defs.into_iter().zip(files).zip(lines) {
        found
            .entry(def)
            .or_default()
            .push((is_test(&file), file, line));
    }
    Ok(found
        .into_iter()
        .filter(|(_, list)| list.len() <= SIBLINGS_MAX)
        .map(|(def, mut list)| {
            list.retain(|(_, file, _)| !shown.contains(file.as_str()));
            list.sort();
            list.dedup();
            (
                def,
                list.into_iter()
                    .map(|(_, file, line)| format!("{file}:{line}"))
                    .collect(),
            )
        })
        .filter(|(_, list): &(i64, Vec<String>)| !list.is_empty())
        .collect())
}

pub(super) fn connection_label(def: &Def, connections: &Connections) -> String {
    let Some(links) = connections.get(&def.id) else {
        return String::new();
    };
    let side = |arrow: &str, names: &[String]| match names.len() {
        0 => String::new(),
        n if n > CONNECTION_NAMES => {
            let shown = names[..CONNECTION_NAMES].join(",");
            format!("{arrow}{shown},+{} ", n - CONNECTION_NAMES)
        }
        _ => format!("{arrow}{} ", names.join(",")),
    };
    let siblings = match links.siblings.len() {
        0 => String::new(),
        n if n > SIBLINGS_SHOWN => format!(
            "also {} in {},+{} ",
            def.name,
            links.siblings[..SIBLINGS_SHOWN].join(","),
            n - SIBLINGS_SHOWN
        ),
        _ => format!("also {} in {} ", def.name, links.siblings.join(",")),
    };
    format!(
        "{}{}{siblings}",
        side("←", &links.callers),
        side("→", &links.callees)
    )
}
