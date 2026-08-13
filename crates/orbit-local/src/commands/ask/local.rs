use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{CorpusRow, Edge};
use crate::commands::repo_map::{
    DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex, scalar_i64, sql_lit, string_column,
};
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

        let client = sql::open_graph(Some(db))?;
        let pid = git.project_id;
        let sha = sql_lit(&git.commit_sha);

        let indexed = sql::query(
            &client,
            &format!(
                "SELECT COUNT(*) AS n FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}"
            ),
        )?;
        if scalar_i64(&indexed) == 0 {
            anyhow::bail!(
                "current commit {} is not indexed in the local graph\n       run:  orbit index .",
                git.commit_sha
            );
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
        let (total, per_term) = corpus_term_counts(&self.client, self.pid, &self.sha, terms)?;
        let n = total.max(1) as f64;
        let weights: Vec<f64> = per_term
            .iter()
            .map(|df| (1.0 + n / (1.0 + *df as f64)).ln())
            .collect();
        let corpus = fetch_corpus(
            &self.client,
            self.pid,
            &self.sha,
            terms,
            &weights,
            LOCAL_CANDIDATES,
        )?;
        Ok((corpus, Some(weights)))
    }

    pub(super) fn expand(&self, seeds: &[&CorpusRow]) -> Result<Vec<Edge>> {
        let ids: Vec<&str> = seeds.iter().map(|s| s.id.as_str()).collect();
        let batches = sql::query(&self.client, &expand_sql(self.pid, &self.sha, &ids))?;
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

fn corpus_predicate(pid: i64, sha: &str) -> String {
    format!(
        "d.project_id = {pid} AND d.commit_sha = {sha}
  AND regexp_matches(d.file_path, {source_only})
  AND NOT regexp_matches(d.name, '^[0-9]+$')
{exclude}",
        source_only = sql_lit(&ext_regex(
            &DEFAULT_SOURCE_EXTS
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        )),
        exclude = exclusions("d.file_path"),
    )
}

fn term_match_sql(term: &str) -> String {
    let lower = term.to_lowercase();
    let mut needles = vec![lower.clone()];
    let stemmed = super::stem(&lower);
    if stemmed.len() >= 3 && stemmed != lower {
        needles.push(stemmed);
    }
    let clauses: Vec<String> = needles
        .iter()
        .map(|needle| {
            let pat = sql_lit(&format!("%{needle}%"));
            format!("lower(d.fqn) LIKE {pat} OR lower(d.file_path) LIKE {pat}")
        })
        .collect();
    format!("({})", clauses.join(" OR "))
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
                "CASE WHEN {} THEN {weight:.6} ELSE 0 END",
                term_match_sql(t)
            )
        })
        .collect();
    format!("({})", clauses.join(" + "))
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
            pred = corpus_predicate(pid, sha),
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
  ORDER BY {relevance} DESC
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
            pred = corpus_predicate(pid, sha),
            terms = term_predicate(terms),
            relevance = relevance_sql(terms, weights),
        ),
    )?;
    let ids = string_column(&batches, "id");
    let fqns = string_column(&batches, "fqn");
    let kinds = string_column(&batches, "definition_type");
    let locs = string_column(&batches, "loc");
    let degrees = string_column(&batches, "degree");
    Ok((0..ids.len())
        .map(|i| CorpusRow {
            id: ids[i].clone(),
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: locs[i].clone(),
            degree: degrees[i].clone(),
        })
        .collect())
}

fn exclusions(col: &str) -> String {
    let mut s = String::new();
    for pat in EXCLUDE_LIKE {
        s.push_str(&format!("  AND {col} NOT LIKE {}\n", sql_lit(pat)));
    }
    for re in EXCLUDE_REGEX {
        s.push_str(&format!(
            "  AND NOT regexp_matches({col}, {})\n",
            sql_lit(re)
        ));
    }
    s
}

fn expand_sql(pid: i64, sha: &str, seed_ids: &[&str]) -> String {
    let ids = seed_ids.join(", ");
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
ORDER BY h.relationship_kind, source_label, target_label
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
