//! `orbit workspace graph`: the local graph of a checkout as one JSON document, for tools that analyse the
//! whole graph rather than one symbol (reachability, comparing two trees, call sets).
//!
//! The command brings the index up to date first: it indexes a checkout that has no rows for its commit,
//! and refreshes the files edited since, so the output always describes the working tree as it is. The
//! document is versioned and its fields are fixed; it does not expose the database's table layout.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::{Array, Int64Array, ListArray, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use serde::Serialize;

use crate::workspace;

pub const FORMAT_VERSION: u32 = 1;

const DEFAULT_KINDS: &[&str] = &[
    "Function",
    "Method",
    "Constructor",
    "Variable",
    "Constant",
    "Struct",
    "Enum",
    "TypeAlias",
    "Class",
    "Interface",
    "Trait",
];
const DEFAULT_EDGES: &[&str] = &["CALLS", "USES_VALUE", "USES_TYPE"];

#[derive(Serialize)]
pub struct Graph {
    pub version: u32,
    pub repository: String,
    pub commit: String,
    /// Files that differed from the commit when the graph was read, and are in it as they are on disk.
    pub edited: Vec<String>,
    pub definitions: Vec<Definition>,
    pub edges: Vec<Edge>,
    /// Calls to names the graph did not resolve to a definition, such as C macros and library functions:
    /// the name, per calling definition.
    pub unresolved_calls: Vec<UnresolvedCall>,
}

#[derive(Serialize)]
pub struct Definition {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub language: String,
    pub file: String,
    pub start_line: i64,
    pub end_line: i64,
    pub start_byte: i64,
    pub end_byte: i64,
    pub signature: String,
    pub parameters: Vec<String>,
    pub doc_comment: String,
}

#[derive(Serialize)]
pub struct Edge {
    pub source: String,
    pub target: String,
    pub kind: String,
    /// Source lines of the call or use, in the source definition's file.
    pub lines: Vec<i64>,
    /// For USES_TYPE, how the type is used: parameter, return, field, generic, or cast.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub usage: Vec<String>,
}

#[derive(Serialize)]
pub struct UnresolvedCall {
    pub source: String,
    pub name: String,
}

pub struct Options {
    pub repo: Option<PathBuf>,
    pub db: Option<PathBuf>,
    pub kinds: Vec<String>,
    pub edges: Vec<String>,
}

pub fn run(options: Options) -> Result<()> {
    let graph = read(options)?;
    let stdout = std::io::stdout().lock();
    serde_json::to_writer(stdout, &graph)?;
    println!();
    Ok(())
}

pub fn read(options: Options) -> Result<Graph> {
    let repo = options.repo.clone().unwrap_or_else(|| PathBuf::from("."));
    let top = workspace::git_toplevel(&repo)
        .with_context(|| format!("failed to find git top-level for {}", repo.display()))?;
    workspace::open_indexed(Some(top.clone()), options.db.clone())?;
    let edited: Vec<String> = workspace::git_working_changes(&top)
        .map(|changes| changes.paths().cloned().collect())
        .unwrap_or_default();
    if !edited.is_empty() {
        crate::commands::index::incremental::refresh_now(
            top.clone(),
            options.db.clone(),
            edited.clone(),
        )
        .context("failed to refresh the files edited since the index")?;
    }
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(Some(top), options.db)?;
    let kinds = list_or(&options.kinds, DEFAULT_KINDS);
    let edge_kinds = list_or(&options.edges, DEFAULT_EDGES);

    let definitions = definitions(&client, &kinds)?;
    let wanted: std::collections::HashSet<&str> =
        definitions.iter().map(|d| d.id.as_str()).collect();
    let edges = edges(&client, &edge_kinds)?
        .into_iter()
        .filter(|e| wanted.contains(e.source.as_str()) && wanted.contains(e.target.as_str()))
        .collect();
    let unresolved_calls = unresolved_calls(&client)?
        .into_iter()
        .filter(|c| wanted.contains(c.source.as_str()))
        .collect();
    Ok(Graph {
        version: FORMAT_VERSION,
        repository: git.repo_path.display().to_string(),
        commit: git.commit_sha,
        edited,
        definitions,
        edges,
        unresolved_calls,
    })
}

fn list_or(given: &[String], default: &[&str]) -> Vec<String> {
    if given.is_empty() {
        default.iter().map(|s| s.to_string()).collect()
    } else {
        given.to_vec()
    }
}

fn quoted(values: &[String]) -> String {
    values
        .iter()
        .map(|v| format!("'{}'", v.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

fn definitions(client: &crate::database::GraphReader, kinds: &[String]) -> Result<Vec<Definition>> {
    let sql = format!(
        "SELECT CAST(id AS VARCHAR) AS id, name, kind, language, file_path, start_line, end_line, \
         start_byte, end_byte, signature, parameters, doc_comment FROM gl_definition WHERE kind IN ({}) \
         ORDER BY file_path, start_byte",
        quoted(kinds)
    );
    let mut out = Vec::new();
    for batch in crate::sql::query(client, &sql)? {
        let text = |name: &str| strings(&batch, name);
        let (id, name, kind, language, file, signature) = (
            text("id")?,
            text("name")?,
            text("kind")?,
            text("language")?,
            text("file_path")?,
            text("signature")?,
        );
        let doc = text("doc_comment")?;
        let int = |name: &str| ints(&batch, name);
        let (start_line, end_line, start_byte, end_byte) = (
            int("start_line")?,
            int("end_line")?,
            int("start_byte")?,
            int("end_byte")?,
        );
        let parameters = string_lists(&batch, "parameters")?;
        for row in 0..batch.num_rows() {
            out.push(Definition {
                id: id[row].clone(),
                name: name[row].clone(),
                kind: kind[row].clone(),
                language: language[row].clone(),
                file: file[row].clone(),
                start_line: start_line[row],
                end_line: end_line[row],
                start_byte: start_byte[row],
                end_byte: end_byte[row],
                signature: signature[row].clone(),
                parameters: parameters[row].clone(),
                doc_comment: doc[row].clone(),
            });
        }
    }
    Ok(out)
}

fn edges(client: &crate::database::GraphReader, kinds: &[String]) -> Result<Vec<Edge>> {
    let sql = format!(
        "SELECT CAST(source_id AS VARCHAR) AS source, CAST(target_id AS VARCHAR) AS target, \
         relationship_kind AS kind, lines, usage FROM gl_edge WHERE source_kind = 'Definition' \
         AND target_kind = 'Definition' AND relationship_kind IN ({})",
        quoted(kinds)
    );
    let mut merged: BTreeMap<(String, String, String), (Vec<i64>, Vec<String>)> = BTreeMap::new();
    for batch in crate::sql::query(client, &sql)? {
        let (source, target, kind) = (
            strings(&batch, "source")?,
            strings(&batch, "target")?,
            strings(&batch, "kind")?,
        );
        let lines = int_lists(&batch, "lines")?;
        let usage = strings(&batch, "usage")?;
        for row in 0..batch.num_rows() {
            if source[row] == target[row] {
                continue;
            }
            let entry = merged
                .entry((source[row].clone(), target[row].clone(), kind[row].clone()))
                .or_default();
            entry.0.extend(&lines[row]);
            if !usage[row].is_empty() {
                entry.1.push(usage[row].clone());
            }
        }
    }
    Ok(merged
        .into_iter()
        .map(|((source, target, kind), (mut lines, mut usage))| {
            lines.sort_unstable();
            lines.dedup();
            usage.sort_unstable();
            usage.dedup();
            Edge {
                source,
                target,
                kind,
                lines,
                usage,
            }
        })
        .collect())
}

fn unresolved_calls(client: &crate::database::GraphReader) -> Result<Vec<UnresolvedCall>> {
    let sql = "SELECT DISTINCT CAST(e.source_id AS VARCHAR) AS source, i.name AS name FROM gl_edge e \
               JOIN gl_import i ON i.id = e.target_id WHERE e.source_kind = 'Definition' \
               AND e.target_kind = 'Import' AND e.relationship_kind = 'CALLS' AND i.name <> '*' \
               ORDER BY 1, 2";
    let mut out = Vec::new();
    for batch in crate::sql::query(client, sql)? {
        let (source, name) = (strings(&batch, "source")?, strings(&batch, "name")?);
        for row in 0..batch.num_rows() {
            out.push(UnresolvedCall {
                source: source[row].clone(),
                name: name[row].clone(),
            });
        }
    }
    Ok(out)
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array> {
    batch
        .column_by_name(name)
        .map(|c| c.as_ref())
        .with_context(|| format!("graph query returned no column {name}"))
}

fn strings(batch: &RecordBatch, name: &str) -> Result<Vec<String>> {
    let array = column(batch, name)?;
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {name} is not text"))?;
    Ok((0..array.len())
        .map(|i| {
            if array.is_null(i) {
                String::new()
            } else {
                array.value(i).to_string()
            }
        })
        .collect())
}

fn ints(batch: &RecordBatch, name: &str) -> Result<Vec<i64>> {
    let array = column(batch, name)?;
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..a.len())
            .map(|i| if a.is_null(i) { 0 } else { a.value(i) })
            .collect());
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok((0..a.len())
            .map(|i| if a.is_null(i) { 0 } else { a.value(i) as i64 })
            .collect());
    }
    anyhow::bail!("column {name} is not an integer")
}

fn lists(batch: &RecordBatch, name: &str) -> Result<Vec<Option<std::sync::Arc<dyn Array>>>> {
    let array = column(batch, name)?;
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .with_context(|| format!("column {name} is not a list"))?;
    Ok((0..list.len())
        .map(|i| (!list.is_null(i)).then(|| list.value(i)))
        .collect())
}

fn string_lists(batch: &RecordBatch, name: &str) -> Result<Vec<Vec<String>>> {
    lists(batch, name)?
        .into_iter()
        .map(|item| {
            let Some(item) = item else {
                return Ok(Vec::new());
            };
            let values = item
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| format!("column {name} does not hold text"))?;
            Ok((0..values.len())
                .filter(|&i| !values.is_null(i))
                .map(|i| values.value(i).to_string())
                .collect())
        })
        .collect()
}

fn int_lists(batch: &RecordBatch, name: &str) -> Result<Vec<Vec<i64>>> {
    lists(batch, name)?
        .into_iter()
        .map(|item| {
            let Some(item) = item else {
                return Ok(Vec::new());
            };
            if let Some(values) = item.as_any().downcast_ref::<Int64Array>() {
                return Ok((0..values.len())
                    .filter(|&i| !values.is_null(i))
                    .map(|i| values.value(i))
                    .collect());
            }
            anyhow::bail!("column {name} does not hold integers")
        })
        .collect()
}
