use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

use crate::{DuckDbClient, scalar_i64, sql_lit, string_column};
use orbit_search::ask::{AskSource, ask};
use orbit_search::corpus::{DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex};
use orbit_search::expand::{NeighborhoodSource, NodeLabel};
use orbit_search::ppr::NeighborhoodEdge;
use orbit_search::{AskOutcome, BM25_B, BM25_K1, CorpusRow, SearchVocab, query_tokens};
use std::collections::HashMap;

const LOCAL_CANDIDATES: usize = 2000;

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
}

impl DuckDbSearch {
    pub fn new(client: DuckDbClient, project_id: i64, commit_sha: &str) -> Self {
        Self {
            client,
            pid: project_id,
            sha: sql_lit(commit_sha),
        }
    }

    pub fn ask(
        &self,
        question: &str,
        limit: usize,
        vocab: &SearchVocab,
        kind_weights: &HashMap<String, f64>,
    ) -> Result<AskOutcome> {
        ask(self, question, limit, vocab, kind_weights).map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub fn search(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        if !has_search_text(&self.client, self.pid, &self.sha)? {
            anyhow::bail!(
                "local graph has no ranked-search data for this commit; \
                 re-index the repository (`orbit index <path>`)"
            );
        }
        let (total, per_term) = search_text_term_counts(&self.client, self.pid, &self.sha, terms)?;
        let n = total.max(1) as f64;
        let weights: Vec<f64> = per_term
            .iter()
            .map(|df| (1.0 + n / (1.0 + *df as f64)).ln())
            .collect();
        let sql = bm25_candidates_sql(self.pid, &self.sha, terms, &weights, LOCAL_CANDIDATES);
        let corpus = rows_from_batches(&query(&self.client, &sql)?);
        Ok((corpus, Some(weights)))
    }
}

impl AskSource for DuckDbSearch {
    fn corpus(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        self.search(terms)
    }

    fn token_df(&self, tokens: &[String]) -> Result<Vec<i64>> {
        let counters: Vec<String> = tokens
            .iter()
            .enumerate()
            .map(|(i, t)| match token_like_sql(t) {
                Some(matched) => {
                    format!("CAST(COUNT(*) FILTER (WHERE {matched}) AS VARCHAR) AS df_{i}")
                }
                None => format!("CAST(0 AS VARCHAR) AS df_{i}"),
            })
            .collect();
        let (_, dfs) = term_counts(&self.client, self.pid, &self.sha, tokens, &counters)?;
        Ok(dfs)
    }

    fn rows_by_ids(&self, ids: &[&str]) -> Result<Vec<CorpusRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let list = ids.join(", ");
        let batches = query(
            &self.client,
            &format!(
                "WITH cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d
  WHERE d.project_id = {pid} AND d.commit_sha = {sha} AND d.id IN ({list})
),
deg AS (
  SELECT id, COUNT(*) AS degree FROM (
    SELECT source_id AS id FROM gl_edge WHERE source_id IN ({list})
    UNION ALL
    SELECT target_id FROM gl_edge WHERE target_id IN ({list})
  ) GROUP BY 1
)
SELECT CAST(c.id AS VARCHAR) AS id, c.fqn, c.definition_type,
       c.file_path || ':' || CAST(c.start_line AS VARCHAR) AS loc,
       CAST(c.end_line AS VARCHAR) AS end_line,
       CAST(COALESCE(deg.degree, 0) AS VARCHAR) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id",
                pid = self.pid,
                sha = self.sha,
            ),
        )?;
        Ok(rows_from_batches(&batches))
    }
}

impl NeighborhoodSource for DuckDbSearch {
    type Error = anyhow::Error;

    fn hop(&self, ids: &[&str], cap: usize) -> Result<Vec<NeighborhoodEdge>> {
        let list = ids.join(", ");
        let batches = query(
            &self.client,
            &format!(
                "SELECT DISTINCT relationship_kind, CAST(source_id AS VARCHAR) AS source_id,
       CAST(target_id AS VARCHAR) AS target_id
FROM gl_edge
WHERE source_id IN ({list}) OR target_id IN ({list})
LIMIT {cap}"
            ),
        )?;
        let kinds = string_column(&batches, "relationship_kind");
        let sources = string_column(&batches, "source_id");
        let targets = string_column(&batches, "target_id");
        Ok((0..kinds.len())
            .map(|i| NeighborhoodEdge {
                kind: kinds[i].clone(),
                source: sources[i].clone(),
                target: targets[i].clone(),
            })
            .collect())
    }

    fn degrees(&self, ids: &[&str]) -> Result<HashMap<String, u64>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let list = ids.join(", ");
        let batches = query(
            &self.client,
            &format!(
                "SELECT CAST(id AS VARCHAR) AS id, CAST(COUNT(*) AS VARCHAR) AS degree FROM (
  SELECT source_id AS id FROM gl_edge WHERE source_id IN ({list})
  UNION ALL
  SELECT target_id FROM gl_edge WHERE target_id IN ({list})
) GROUP BY 1"
            ),
        )?;
        let node_ids = string_column(&batches, "id");
        let counts = string_column(&batches, "degree");
        Ok((0..node_ids.len())
            .map(|i| (node_ids[i].clone(), counts[i].parse().unwrap_or(0)))
            .collect())
    }

    fn labels(&self, ids: &[&str]) -> Result<HashMap<String, NodeLabel>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let list = ids.join(", ");
        let pid = self.pid;
        let sha = &self.sha;
        let batches = query(
            &self.client,
            &format!(
                "SELECT CAST(id AS VARCHAR) AS id, label, loc, CAST(scoped AS VARCHAR) AS scoped FROM (
  SELECT id, fqn AS label,
         file_path || ':' || CAST(start_line AS VARCHAR) AS loc,
         fqn LIKE '%@%' AS scoped
  FROM gl_definition
  WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path, '', FALSE FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path, '', FALSE FROM gl_directory WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, identifier_name, '', FALSE FROM gl_imported_symbol
  WHERE project_id = {pid} AND commit_sha = {sha}
)
WHERE id IN ({list})"
            ),
        )?;
        let node_ids = string_column(&batches, "id");
        let node_labels = string_column(&batches, "label");
        let locs = string_column(&batches, "loc");
        let scoped = string_column(&batches, "scoped");
        Ok((0..node_ids.len())
            .map(|i| {
                (
                    node_ids[i].clone(),
                    NodeLabel {
                        label: node_labels[i].clone(),
                        loc: locs[i].clone(),
                        scoped: scoped[i] == "true",
                    },
                )
            })
            .collect())
    }
}

fn query(client: &DuckDbClient, sql: &str) -> Result<Vec<RecordBatch>> {
    client.query_arrow(sql).with_context(|| {
        let preview: String = sql.chars().take(120).collect();
        let suffix = if sql.chars().count() > 120 { "…" } else { "" };
        format!("query failed: {preview}{suffix}")
    })
}

fn has_search_text(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<bool> {
    let columns = client.query_arrow(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM information_schema.columns
 WHERE table_name = 'gl_definition' AND column_name = 'search_text'",
    )?;
    if scalar_i64(&columns) == 0 {
        return Ok(false);
    }
    let batches = client.query_arrow(&format!(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM gl_definition
  WHERE project_id = {project_id} AND commit_sha = {sha} AND token_count > 0
  LIMIT 1
)"
    ))?;
    Ok(scalar_i64(&batches) > 0)
}

fn corpus_predicate(pid: i64, sha: &str) -> String {
    format!(
        "d.project_id = {pid} AND d.commit_sha = {sha}
  AND regexp_matches(d.file_path, {source_only})
  AND NOT regexp_matches(d.name, '^[0-9]+$')
  AND d.fqn NOT LIKE '%@%'
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

fn token_like_sql(term: &str) -> Option<String> {
    let tokens = query_tokens(std::slice::from_ref(&term.to_string()));
    if tokens.is_empty() {
        return None;
    }
    let clauses: Vec<String> = tokens
        .iter()
        .map(|tok| {
            format!(
                "' ' || d.search_text || ' ' LIKE {}",
                sql_lit(&format!("% {tok} %"))
            )
        })
        .collect();
    Some(format!("({})", clauses.join(" OR ")))
}

fn token_tf_sql(term: &str) -> Option<String> {
    let tokens = query_tokens(std::slice::from_ref(&term.to_string()));
    if tokens.is_empty() {
        return None;
    }
    let clauses: Vec<String> = tokens
        .iter()
        .map(|tok| {
            format!(
                "len(list_filter(string_split(d.search_text, ' '), x -> x = {}))",
                sql_lit(tok)
            )
        })
        .collect();
    Some(format!("({})", clauses.join(" + ")))
}

fn bm25_candidates_sql(
    pid: i64,
    sha: &str,
    terms: &[String],
    weights: &[f64],
    cap: usize,
) -> String {
    let match_clauses: Vec<String> = terms.iter().filter_map(|t| token_like_sql(t)).collect();
    let any_match = if match_clauses.is_empty() {
        "FALSE".to_string()
    } else {
        format!("({})", match_clauses.join(" OR "))
    };
    let score_parts: Vec<String> = terms
        .iter()
        .enumerate()
        .filter_map(|(i, t)| {
            let tf = token_tf_sql(t)?;
            let idf = weights.get(i).copied().unwrap_or(1.0);
            Some(format!(
                "{idf:.6} * {tf} * ({k1} + 1)
           / ({tf} + {k1} * (1 - {b} + {b} * d.token_count / s.avgdl))",
                k1 = BM25_K1,
                b = BM25_B,
            ))
        })
        .collect();
    let score = if score_parts.is_empty() {
        "0".to_string()
    } else {
        score_parts.join("\n         + ")
    };
    format!(
        "WITH stats AS (
  SELECT GREATEST(AVG(d.token_count), 1) AS avgdl
  FROM gl_definition d
  WHERE {pred}
),
cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d
  CROSS JOIN stats s
  WHERE {pred}
  AND {any_match}
  ORDER BY ({score}) DESC, length(d.fqn) ASC
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
       CAST(c.end_line AS VARCHAR) AS end_line,
       CAST(COALESCE(deg.degree, 0) AS VARCHAR) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id",
        pred = corpus_predicate(pid, sha),
    )
}

fn search_text_term_counts(
    client: &DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
) -> Result<(i64, Vec<i64>)> {
    let counters: Vec<String> = terms
        .iter()
        .enumerate()
        .map(|(i, t)| match token_like_sql(t) {
            Some(matched) => {
                format!("CAST(COUNT(*) FILTER (WHERE {matched}) AS VARCHAR) AS df_{i}")
            }
            None => format!("CAST(0 AS VARCHAR) AS df_{i}"),
        })
        .collect();
    term_counts(client, pid, sha, terms, &counters)
}

fn term_counts(
    client: &DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
    counters: &[String],
) -> Result<(i64, Vec<i64>)> {
    let selects = if counters.is_empty() {
        String::new()
    } else {
        format!(", {}", counters.join(", "))
    };
    let batches = query(
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

fn rows_from_batches(batches: &[RecordBatch]) -> Vec<CorpusRow> {
    let ids = string_column(batches, "id");
    let fqns = string_column(batches, "fqn");
    let kinds = string_column(batches, "definition_type");
    let locs = string_column(batches, "loc");
    let end_lines = string_column(batches, "end_line");
    let degrees = string_column(batches, "degree");
    (0..ids.len())
        .map(|i| CorpusRow {
            id: ids[i].clone(),
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: locs[i].clone(),
            end_line: end_lines[i].clone(),
            degree: degrees[i].clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_sql_embeds_padded_token_matches() {
        let sql = bm25_candidates_sql(1, "'abc'", &["valid".to_string()], &[1.5], 10);
        assert!(sql.contains("LIKE '% valid %'"), "sql was {sql}");
        assert!(sql.contains("1.500000"), "sql was {sql}");
        assert!(sql.contains("ORDER BY ("), "sql was {sql}");
    }

    #[test]
    fn bm25_tf_counts_exact_tokens_not_substrings() {
        let sql = token_tf_sql("validated").unwrap();
        assert!(sql.contains("x = 'valid'"), "sql was {sql}");
        assert!(!sql.contains('%'), "sql was {sql}");
    }
}
