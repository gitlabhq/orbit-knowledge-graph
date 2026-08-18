use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{CorpusRow, Edge};
use crate::commands::repo_map::{scalar_i64, sql_lit, string_column};
use crate::search;
use crate::sql;
use crate::workspace;

const LOCAL_CANDIDATES: usize = 2000;
const EDGE_LIMIT: usize = 40;

pub(super) struct LocalBackend {
    client: duckdb_client::DuckDbClient,
    pid: i64,
    sha: String,
    header: String,
}

impl LocalBackend {
    pub(super) fn open(repo_path: &Path, db: Option<PathBuf>) -> Result<Self> {
        let db = workspace::resolve_db_path(db)?;
        let top_level = workspace::git_toplevel(repo_path)
            .with_context(|| format!("failed to find git top-level for {}", repo_path.display()))?;
        let git = workspace::git_info(&top_level)
            .with_context(|| format!("failed to read git info for {}", top_level.display()))?;

        let mut client = sql::open_graph(Some(db.clone()))?;
        let pid = git.project_id;
        let sha = sql_lit(&git.commit_sha);

        let indexed_count = |client: &duckdb_client::DuckDbClient| -> Result<i64> {
            let batches = sql::query(
                client,
                &format!(
                    "SELECT COUNT(*) AS n FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}"
                ),
            )?;
            Ok(scalar_i64(&batches))
        };

        if indexed_count(&client)? == 0 {
            eprintln!(
                "current commit {} is not indexed — indexing {} first",
                git.commit_sha.get(..8).unwrap_or(&git.commit_sha),
                git.repo_path.display()
            );
            drop(client);
            crate::index_collect(git.repo_path.clone(), 0, false, Some(db.clone()))
                .context("failed to index the repository for ask")?;
            client = sql::open_graph(Some(db))?;
            if indexed_count(&client)? == 0 {
                anyhow::bail!(
                    "indexing finished but commit {} still has no rows in the local graph",
                    git.commit_sha
                );
            }
        }

        Ok(Self {
            client,
            pid,
            sha,
            header: format!("{} @ {}", git.repo_path.display(), git.commit_sha),
        })
    }
}

impl LocalBackend {
    pub(super) fn header(&self) -> &str {
        &self.header
    }

    pub(super) fn search(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        let use_postings = search::has_postings(&self.client, self.pid, &self.sha)?;
        let (total, per_term) = if use_postings {
            postings_term_counts(&self.client, self.pid, &self.sha, terms)?
        } else {
            corpus_term_counts(&self.client, self.pid, &self.sha, terms)?
        };
        let n = total.max(1) as f64;
        let weights: Vec<f64> = per_term
            .iter()
            .map(|df| (1.0 + n / (1.0 + *df as f64)).ln())
            .collect();
        let corpus = if use_postings {
            let sql = search::bm25_candidates_sql(
                self.pid,
                &self.sha,
                &search::query_tokens(terms),
                LOCAL_CANDIDATES,
            );
            rows_from_batches(&sql::query(&self.client, &sql)?)
        } else {
            fetch_corpus(
                &self.client,
                self.pid,
                &self.sha,
                terms,
                &weights,
                LOCAL_CANDIDATES,
            )?
        };
        Ok((corpus, Some(weights)))
    }

    pub(super) fn expand(&self, seeds: &[&CorpusRow], focus: Option<&str>) -> Result<Vec<Edge>> {
        let ids: Vec<&str> = seeds.iter().map(|s| s.id.as_str()).collect();
        let batches = sql::query(&self.client, &expand_sql(self.pid, &self.sha, &ids, focus))?;
        let kinds = string_column(&batches, "relationship_kind");
        let sources = string_column(&batches, "source_label");
        let targets = string_column(&batches, "target_label");
        Ok((0..kinds.len())
            .map(|i| Edge {
                kind: kinds[i].clone(),
                source: sources[i].clone(),
                target: targets[i].clone(),
            })
            .collect())
    }
}

fn term_needles(term: &str) -> Vec<String> {
    let lower = term.to_lowercase();
    let mut needles = vec![lower.clone()];
    let stemmed = search::stem(&lower);
    if stemmed.len() >= 3 && stemmed != lower {
        needles.push(stemmed);
    }
    needles
}

fn columns_match_sql(term: &str, columns: &[&str]) -> String {
    let clauses: Vec<String> = term_needles(term)
        .iter()
        .flat_map(|needle| {
            let pat = sql_lit(&format!("%{needle}%"));
            columns
                .iter()
                .map(move |col| format!("lower({col}) LIKE {pat}"))
                .collect::<Vec<_>>()
        })
        .collect();
    format!("({})", clauses.join(" OR "))
}

fn term_match_sql(term: &str) -> String {
    columns_match_sql(term, &["d.fqn", "d.file_path"])
}

fn term_predicate(terms: &[String]) -> String {
    if terms.is_empty() {
        return "TRUE".to_string();
    }
    let clauses: Vec<String> = terms.iter().map(|t| term_match_sql(t)).collect();
    format!("({})", clauses.join(" OR "))
}

fn relevance_sql(terms: &[String], weights: &[f64]) -> String {
    if terms.is_empty() {
        return "0".to_string();
    }
    let clauses: Vec<String> = terms
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            format!(
                "CASE WHEN {name} THEN {double:.6} WHEN {any} THEN {weight:.6} ELSE 0 END",
                name = columns_match_sql(t, &["d.name"]),
                double = weight * 2.0,
                any = term_match_sql(t),
            )
        })
        .collect();
    format!("({})", clauses.join(" + "))
}

fn postings_term_counts(
    client: &duckdb_client::DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
) -> Result<(i64, Vec<i64>)> {
    let counters: Vec<String> = terms
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let tokens = search::query_tokens(std::slice::from_ref(t));
            if tokens.is_empty() {
                return format!("CAST(0 AS VARCHAR) AS df_{i}");
            }
            let list: Vec<String> = tokens.iter().map(|tok| sql_lit(tok)).collect();
            format!(
                "CAST(COUNT(DISTINCT def_id) FILTER (WHERE token IN ({})) AS VARCHAR) AS df_{i}",
                list.join(", ")
            )
        })
        .collect();
    let selects = if counters.is_empty() {
        String::new()
    } else {
        format!(", {}", counters.join(", "))
    };
    let batches = sql::query(
        client,
        &format!(
            "SELECT CAST(COUNT(DISTINCT def_id) AS VARCHAR) AS total{selects} FROM gl_search_token WHERE project_id = {pid} AND commit_sha = {sha}"
        ),
    )?;
    let count_of = |name: &str| {
        string_column(&batches, name)
            .first()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let total = count_of("total");
    let per_term = (0..terms.len())
        .map(|i| count_of(&format!("df_{i}")))
        .collect();
    Ok((total, per_term))
}

fn corpus_term_counts(
    client: &duckdb_client::DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
) -> Result<(i64, Vec<i64>)> {
    let counters: Vec<String> = terms
        .iter()
        .enumerate()
        .map(|(i, t)| {
            format!(
                "CAST(COUNT(*) FILTER (WHERE {}) AS VARCHAR) AS df_{i}",
                term_match_sql(t)
            )
        })
        .collect();
    let selects = if counters.is_empty() {
        String::new()
    } else {
        format!(", {}", counters.join(", "))
    };
    let batches = sql::query(
        client,
        &format!(
            "SELECT CAST(COUNT(*) AS VARCHAR) AS total{selects} FROM gl_definition d WHERE {pred}",
            pred = search::corpus_predicate(pid, sha),
        ),
    )?;
    let count_of = |name: &str| {
        string_column(&batches, name)
            .first()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let total = count_of("total");
    let per_term = (0..terms.len())
        .map(|i| count_of(&format!("df_{i}")))
        .collect();
    Ok((total, per_term))
}

fn fetch_corpus(
    client: &duckdb_client::DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
    weights: &[f64],
    cap: usize,
) -> Result<Vec<CorpusRow>> {
    let batches = sql::query(
        client,
        &format!(
            "WITH cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line
  FROM gl_definition d
  WHERE {pred}
  AND {terms}
  ORDER BY {relevance} DESC, length(d.fqn) ASC
  LIMIT {cap}
),
deg AS (
  SELECT id, COUNT(*) AS degree FROM (
    SELECT source_id AS id FROM gl_edge WHERE source_id IN (SELECT id FROM cand)
    UNION ALL
    SELECT target_id FROM gl_edge WHERE target_id IN (SELECT id FROM cand)
  ) GROUP BY 1
)
SELECT CAST(c.id AS VARCHAR) AS id, c.fqn, c.definition_type,
       c.file_path || ':' || CAST(c.start_line AS VARCHAR) AS loc,
       CAST(COALESCE(deg.degree, 0) AS VARCHAR) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id",
            pred = search::corpus_predicate(pid, sha),
            terms = term_predicate(terms),
            relevance = relevance_sql(terms, weights),
        ),
    )?;
    Ok(rows_from_batches(&batches))
}

fn rows_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<CorpusRow> {
    let ids = string_column(batches, "id");
    let fqns = string_column(batches, "fqn");
    let kinds = string_column(batches, "definition_type");
    let locs = string_column(batches, "loc");
    let degrees = string_column(batches, "degree");
    (0..ids.len())
        .map(|i| CorpusRow {
            id: ids[i].clone(),
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: locs[i].clone(),
            degree: degrees[i].clone(),
        })
        .collect()
}

fn expand_sql(pid: i64, sha: &str, seed_ids: &[&str], focus: Option<&str>) -> String {
    let ids = seed_ids.join(", ");
    let order = match focus {
        Some(kind) => {
            let lit = sql_lit(kind);
            format!(
                "CASE WHEN h.relationship_kind = {lit} THEN 0 ELSE 1 END,
         h.relationship_kind,
         CASE WHEN h.relationship_kind = {lit} AND h.target_id IN ({ids}) THEN 0 ELSE 1 END,
         source_label, target_label"
            )
        }
        None => "h.relationship_kind, source_label, target_label".to_string(),
    };
    format!(
        "WITH hood AS (
  SELECT DISTINCT relationship_kind, source_id, target_id
  FROM gl_edge
  WHERE source_id IN ({ids}) OR target_id IN ({ids})
),
labels AS (
  SELECT id, fqn AS label FROM gl_definition
  WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path FROM gl_directory WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, identifier_name FROM gl_imported_symbol
  WHERE project_id = {pid} AND commit_sha = {sha}
)
SELECT h.relationship_kind,
       COALESCE(s.label, CAST(h.source_id AS VARCHAR)) AS source_label,
       COALESCE(t.label, CAST(h.target_id AS VARCHAR)) AS target_label
FROM hood h
LEFT JOIN labels s ON s.id = h.source_id
LEFT JOIN labels t ON t.id = h.target_id
ORDER BY {order}
LIMIT {EDGE_LIMIT}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_match_sql_adds_a_stem_pattern_for_inflected_terms() {
        let sql = term_match_sql("validated");
        assert!(sql.contains("%validated%"), "sql was {sql}");
        assert!(sql.contains("%valid%"), "sql was {sql}");
    }

    #[test]
    fn term_match_sql_skips_the_stem_when_it_matches_the_term() {
        let sql = term_match_sql("dlq");
        assert_eq!(sql.matches("LIKE").count(), 2, "sql was {sql}");
    }

    #[test]
    fn relevance_sql_weights_each_term_independently() {
        let sql = relevance_sql(&["send".to_string(), "dlq".to_string()], &[0.5, 3.0]);
        assert!(sql.contains("0.500000"), "sql was {sql}");
        assert!(sql.contains("3.000000"), "sql was {sql}");
    }
}
