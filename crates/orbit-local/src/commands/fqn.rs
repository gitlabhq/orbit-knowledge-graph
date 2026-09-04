use anyhow::Result;
use arrow::record_batch::RecordBatch;
use duckdb_client::search::{GLOB_CHARS, kind_scope};
use duckdb_client::{DuckDbClient, i64_column, string_column};

use crate::commands::setup::spec;
use crate::workspace;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Def {
    pub id: i64,
    pub fqn: String,
    pub kind: String,
    pub file: String,
    pub start: i64,
    pub end: i64,
}

pub(crate) fn defs_from(batches: &[RecordBatch]) -> Vec<Def> {
    let ids = i64_column(batches, "id");
    let fqns = string_column(batches, "fqn");
    let kinds = string_column(batches, "definition_type");
    let files = string_column(batches, "file_path");
    let starts = i64_column(batches, "start_line");
    let ends = i64_column(batches, "end_line");
    (0..ids.len())
        .map(|i| Def {
            id: ids[i],
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            file: files[i].clone(),
            start: starts[i],
            end: ends[i],
        })
        .collect()
}

const SEPARATORS: [char; 4] = [':', '.', '#', '/'];
const SUGGESTION_LIMIT: usize = 5;
const MAX_EDIT_DISTANCE: usize = 2;

pub(crate) fn resolve(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    fqn: &str,
    file: Option<&str>,
    kinds: &[String],
) -> Result<Vec<Def>> {
    let mut params: Vec<serde_json::Value> = vec![
        git.project_id.into(),
        git.commit_sha.clone().into(),
        fqn.into(),
    ];
    let file_predicate = file_predicate(file, &mut params);
    let kind_predicate = kind_scope("definition_type", kinds);
    let name_predicate = if fqn.contains(GLOB_CHARS) {
        "fqn GLOB ?3".to_string()
    } else {
        format!(
            "(fqn = ?3 OR name = ?3
              OR (ends_with(fqn, ?3)
                  AND substr(fqn, length(fqn) - length(?3), 1) IN ({})))",
            SEPARATORS
                .iter()
                .map(|c| format!("'{c}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let batches = client.query_arrow_json(
        &format!(
            "SELECT id, fqn, definition_type, file_path, start_line, end_line
             FROM gl_definition
             WHERE project_id = ?1 AND commit_sha = ?2 AND {name_predicate} {file_predicate}
             {kind_predicate}
             ORDER BY file_path, start_line, end_line DESC, fqn"
        ),
        &params,
    )?;
    let defs = defs_from(&batches);
    let launcher = spec::launcher();
    if defs.is_empty() {
        let scope = format!(
            "{}{}",
            file.map(|p| format!(" in {p:?}")).unwrap_or_default(),
            kind_suffix(kinds)
        );
        anyhow::bail!(
            "no definition {fqn:?}{scope} for commit {} — pass the fqn printed by \
             `{launcher} grep`, its unqualified tail such as `Type::method`, or a glob \
             such as `crate::module::*`, and make sure the commit is indexed \
             (`{launcher} index <path>`){}",
            git.commit_sha,
            did_you_mean(client, git, fqn, file)?
        );
    }
    if defs.iter().any(|d| d.fqn == fqn) {
        return Ok(defs.into_iter().filter(|d| d.fqn == fqn).collect());
    }
    let mut distinct: Vec<&str> = defs.iter().map(|d| d.fqn.as_str()).collect();
    distinct.sort_unstable();
    distinct.dedup();
    if fqn.contains(GLOB_CHARS) || distinct.len() == 1 {
        return Ok(defs);
    }
    let more = distinct.len().saturating_sub(SUGGESTION_LIMIT);
    anyhow::bail!(
        "{fqn:?} matches {} definitions — pass one full fqn as printed by \
         `{launcher} grep`:\n  {}",
        distinct.len(),
        distinct
            .iter()
            .take(SUGGESTION_LIMIT)
            .map(|s| s.to_string())
            .chain((more > 0).then(|| format!("… {more} more")))
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

fn file_predicate(file: Option<&str>, params: &mut Vec<serde_json::Value>) -> String {
    match file {
        Some(path) => {
            params.push(path.into());
            format!("AND file_path = ?{}", params.len())
        }
        None => String::new(),
    }
}

pub(crate) fn kind_suffix(kinds: &[String]) -> String {
    if kinds.is_empty() {
        String::new()
    } else {
        format!(" of kind {}", kinds.join("|"))
    }
}

fn did_you_mean(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    fqn: &str,
    file: Option<&str>,
) -> Result<String> {
    if fqn.contains(GLOB_CHARS) {
        return Ok(String::new());
    }
    let mut segments: Vec<&str> = fqn.split(SEPARATORS).filter(|s| !s.is_empty()).collect();
    let Some(tail) = segments.pop() else {
        return Ok(String::new());
    };
    let lower = tail.to_lowercase();
    let mut params: Vec<serde_json::Value> = vec![
        git.project_id.into(),
        git.commit_sha.clone().into(),
        lower.clone().into(),
        format!("%{lower}").into(),
        tail.into(),
    ];
    let qualifier_hits = segments
        .iter()
        .map(|seg| {
            params.push(format!("%{}%", seg.to_lowercase()).into());
            format!(
                "(CASE WHEN lower(fqn) LIKE ?{} THEN 1 ELSE 0 END)",
                params.len()
            )
        })
        .collect::<Vec<_>>()
        .join(" + ");
    let qualifier_hits = if qualifier_hits.is_empty() {
        "TRUE".to_string()
    } else {
        format!("({qualifier_hits})")
    };
    let file_predicate = file_predicate(file, &mut params);
    let rows = client.query_arrow_json(
        &format!(
            "SELECT fqn, definition_type, file_path, start_line
             FROM gl_definition
             WHERE project_id = ?1 AND commit_sha = ?2
               AND (lower(name) = ?3
                    OR lower(fqn) LIKE ?4
                    OR (abs(length(name) - length(?3)) <= {MAX_EDIT_DISTANCE}
                        AND levenshtein(lower(name), ?3) <= {MAX_EDIT_DISTANCE}))
               {file_predicate}
             ORDER BY {qualifier_hits} DESC,
                      name = ?5 DESC,
                      lower(name) = ?3 DESC,
                      levenshtein(lower(name), ?3),
                      length(fqn), fqn, file_path, start_line
             LIMIT {SUGGESTION_LIMIT}"
        ),
        &params,
    )?;
    let fqns = string_column(&rows, "fqn");
    if fqns.is_empty() {
        return Ok(String::new());
    }
    let kinds = string_column(&rows, "definition_type");
    let files = string_column(&rows, "file_path");
    let starts = i64_column(&rows, "start_line");
    let mut out = String::from("\nDid you mean:");
    for i in 0..fqns.len() {
        out.push_str(&format!(
            "\n  {}  [{}]  {}:{}",
            fqns[i], kinds[i], files[i], starts[i]
        ));
    }
    Ok(out)
}
