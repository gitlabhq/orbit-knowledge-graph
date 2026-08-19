use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use duckdb_client::{DuckDbClient, scalar_i64, sql_lit, string_column};
use rayon::prelude::*;

use crate::corpus::{DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex};
use crate::{
    AskMatch, AskOutcome, BM25_B, BM25_K1, CorpusRow, Edge, SearchVocab, content_words,
    query_tokens, rank_and_trim, seed_rows, split_words, stem, token_counts,
};

const POSTINGS_FLUSH_ROWS: usize = 500_000;
const POSTINGS_CHUNK_ROWS: usize = 65_536;
const LOCAL_CANDIDATES: usize = 2000;
const EDGE_LIMIT: usize = 40;

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

    pub fn ask(&self, question: &str, limit: usize, vocab: &SearchVocab) -> Result<AskOutcome> {
        let terms = content_words(question);
        if terms.is_empty() {
            anyhow::bail!("no usable search terms in question: {question:?}");
        }
        let (corpus, weights) = self.search(&terms)?;
        let hits = rank_and_trim(&terms, &corpus, limit, weights.as_deref(), vocab);
        let focus = vocab.focus_edge_kind(&terms);
        let (seed_count, edges) = if hits.is_empty() {
            (0, Vec::new())
        } else {
            let seeds = seed_rows(&hits, &corpus);
            let edges = self.expand(&seeds, focus.as_deref())?;
            (seeds.len(), edges)
        };
        let matches = hits
            .into_iter()
            .map(|h| AskMatch {
                row: corpus[h.index].clone(),
                score: h.score,
            })
            .collect();
        Ok(AskOutcome {
            terms,
            matches,
            seed_count,
            focus,
            edges,
        })
    }

    pub fn search(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        let use_postings = has_postings(&self.client, self.pid, &self.sha)?;
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
            let sql =
                bm25_candidates_sql(self.pid, &self.sha, &query_tokens(terms), LOCAL_CANDIDATES);
            rows_from_batches(&query(&self.client, &sql)?)
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

    pub fn expand(&self, seeds: &[&CorpusRow], focus: Option<&str>) -> Result<Vec<Edge>> {
        let ids: Vec<&str> = seeds.iter().map(|s| s.id.as_str()).collect();
        let batches = query(&self.client, &expand_sql(self.pid, &self.sha, &ids, focus))?;
        let kinds = string_column(&batches, "relationship_kind");
        let sources = string_column(&batches, "source_label");
        let source_locs = string_column(&batches, "source_loc");
        let targets = string_column(&batches, "target_label");
        let target_locs = string_column(&batches, "target_loc");
        Ok((0..kinds.len())
            .map(|i| Edge {
                kind: kinds[i].clone(),
                source: sources[i].clone(),
                source_loc: source_locs[i].clone(),
                target: targets[i].clone(),
                target_loc: target_locs[i].clone(),
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

pub fn build_postings(client: &DuckDbClient, project_id: i64, commit_sha: &str) -> Result<usize> {
    let sha = sql_lit(commit_sha);
    client.execute(
        &format!("DELETE FROM gl_search_token WHERE project_id = {project_id}"),
        &[],
    )?;

    let batches = client
        .query_arrow(&format!(
            "SELECT CAST(d.id AS VARCHAR) AS id, d.fqn, d.file_path FROM gl_definition d WHERE {}",
            corpus_predicate(project_id, &sha)
        ))
        .context("failed to read definitions for the search index")?;

    let mut def_ids: Vec<i64> = Vec::new();
    let mut tokens: Vec<String> = Vec::new();
    let mut tfs: Vec<i32> = Vec::new();
    let mut total = 0usize;

    let flush =
        |def_ids: &mut Vec<i64>, tokens: &mut Vec<String>, tfs: &mut Vec<i32>| -> Result<()> {
            if def_ids.is_empty() {
                return Ok(());
            }
            let rows = def_ids.len();
            let batch = RecordBatch::try_new(
                postings_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![project_id; rows])),
                    Arc::new(StringArray::from(vec![commit_sha; rows])),
                    Arc::new(Int64Array::from(std::mem::take(def_ids))),
                    Arc::new(StringArray::from(std::mem::take(tokens))),
                    Arc::new(Int32Array::from(std::mem::take(tfs))),
                ],
            )?;
            client.insert_batch("gl_search_token", &batch)?;
            Ok(())
        };

    let ids = string_column(&batches, "id");
    let fqns = string_column(&batches, "fqn");
    let paths = string_column(&batches, "file_path");

    let path_counts: HashMap<&str, HashMap<String, i32>> = paths
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>()
        .into_par_iter()
        .map(|path| (path, token_counts(split_words(path))))
        .collect();

    for start in (0..ids.len()).step_by(POSTINGS_CHUNK_ROWS) {
        let end = (start + POSTINGS_CHUNK_ROWS).min(ids.len());
        let counted: Vec<(i64, HashMap<String, i32>)> = (start..end)
            .into_par_iter()
            .filter_map(|i| {
                let def_id = ids[i].parse::<i64>().ok()?;
                let mut counts = token_counts(split_words(&fqns[i]));
                if let Some(path) = path_counts.get(paths[i].as_str()) {
                    for (token, tf) in path {
                        *counts.entry(token.clone()).or_insert(0) += tf;
                    }
                }
                Some((def_id, counts))
            })
            .collect();
        for (def_id, counts) in counted {
            for (token, tf) in counts {
                def_ids.push(def_id);
                tokens.push(token);
                tfs.push(tf);
                total += 1;
            }
            if def_ids.len() >= POSTINGS_FLUSH_ROWS {
                flush(&mut def_ids, &mut tokens, &mut tfs)?;
            }
        }
    }
    flush(&mut def_ids, &mut tokens, &mut tfs)?;
    Ok(total)
}

fn postings_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("project_id", DataType::Int64, false),
        Field::new("commit_sha", DataType::Utf8, false),
        Field::new("def_id", DataType::Int64, false),
        Field::new("token", DataType::Utf8, false),
        Field::new("tf", DataType::Int32, false),
    ]))
}

fn has_postings(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<bool> {
    let tables = client.query_arrow(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM information_schema.tables WHERE table_name = 'gl_search_token'",
    )?;
    if scalar_i64(&tables) == 0 {
        return Ok(false);
    }
    let batches = client.query_arrow(&format!(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM gl_search_token
  WHERE project_id = {project_id} AND commit_sha = {sha}
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

fn bm25_candidates_sql(pid: i64, sha: &str, tokens: &[String], cap: usize) -> String {
    let values: Vec<String> = tokens.iter().map(|t| format!("({})", sql_lit(t))).collect();
    format!(
        "WITH docs AS (
  SELECT def_id, SUM(tf) AS dl
  FROM gl_search_token
  WHERE project_id = {pid} AND commit_sha = {sha}
  GROUP BY def_id
),
stats AS (SELECT COUNT(*) AS n, AVG(dl) AS avgdl FROM docs),
q(token) AS (VALUES {values}),
df AS (
  SELECT t.token, COUNT(*) AS df
  FROM gl_search_token t
  JOIN q USING (token)
  WHERE t.project_id = {pid} AND t.commit_sha = {sha}
  GROUP BY t.token
),
scores AS (
  SELECT t.def_id,
         SUM(ln(1 + (s.n - f.df + 0.5) / (f.df + 0.5))
             * t.tf * ({k1} + 1)
             / (t.tf + {k1} * (1 - {b} + {b} * docs.dl / s.avgdl))) AS score
  FROM gl_search_token t
  JOIN q USING (token)
  JOIN df f USING (token)
  JOIN docs ON docs.def_id = t.def_id
  CROSS JOIN stats s
  WHERE t.project_id = {pid} AND t.commit_sha = {sha}
  GROUP BY t.def_id
),
cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM scores sc
  JOIN gl_definition d ON d.id = sc.def_id
  WHERE d.project_id = {pid} AND d.commit_sha = {sha}
  ORDER BY sc.score DESC, length(d.fqn) ASC
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
        values = values.join(", "),
        k1 = BM25_K1,
        b = BM25_B,
    )
}

fn term_needles(term: &str) -> Vec<String> {
    let lower = term.to_lowercase();
    let mut needles = vec![lower.clone()];
    let stemmed = stem(&lower);
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
    client: &DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
) -> Result<(i64, Vec<i64>)> {
    let counters: Vec<String> = terms
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let tokens = query_tokens(std::slice::from_ref(t));
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
    let batches = query(
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
    client: &DuckDbClient,
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

fn fetch_corpus(
    client: &DuckDbClient,
    pid: i64,
    sha: &str,
    terms: &[String],
    weights: &[f64],
    cap: usize,
) -> Result<Vec<CorpusRow>> {
    let batches = query(
        client,
        &format!(
            "WITH cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
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
       CAST(c.end_line AS VARCHAR) AS end_line,
       CAST(COALESCE(deg.degree, 0) AS VARCHAR) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id",
            pred = corpus_predicate(pid, sha),
            terms = term_predicate(terms),
            relevance = relevance_sql(terms, weights),
        ),
    )?;
    Ok(rows_from_batches(&batches))
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
SELECT h.relationship_kind,
       COALESCE(s.label, CAST(h.source_id AS VARCHAR)) AS source_label,
       COALESCE(s.loc, '') AS source_loc,
       COALESCE(t.label, CAST(h.target_id AS VARCHAR)) AS target_label,
       COALESCE(t.loc, '') AS target_loc
FROM hood h
LEFT JOIN labels s ON s.id = h.source_id
LEFT JOIN labels t ON t.id = h.target_id
WHERE NOT COALESCE(s.scoped, FALSE) AND NOT COALESCE(t.scoped, FALSE)
ORDER BY {order}
LIMIT {EDGE_LIMIT}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_sql_embeds_tokens_as_values() {
        let sql = bm25_candidates_sql(1, "'abc'", &["valid".to_string()], 10);
        assert!(sql.contains("VALUES ('valid')"), "sql was {sql}");
        assert!(sql.contains("ORDER BY sc.score DESC"), "sql was {sql}");
    }

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
