use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

use crate::{DuckDbClient, bool_column, i64_column, scalar_i64, sql_lit, string_column};
use orbit_search::ask::{AskError, AskSource, ask};
use orbit_search::corpus::{DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex};
use orbit_search::expand::{NeighborhoodSource, NodeLabel};
use orbit_search::ppr::NeighborhoodEdge;
use orbit_search::{AskOutcome, BM25_B, BM25_K1, CorpusRow, KindRates, SearchVocab, query_tokens};
use std::collections::HashMap;

const LOCAL_CANDIDATES: usize = 2000;

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
}

impl DuckDbSearch {
    pub fn new(client: DuckDbClient, project_id: i64, commit_sha: &str) -> Result<Self> {
        let sha = sql_lit(commit_sha);
        ensure_search_text(&client, project_id, &sha)?;
        client.execute(&corpus_view_sql(project_id, &sha), &[])?;
        Ok(Self {
            client,
            pid: project_id,
            sha,
        })
    }

    pub fn ask(
        &self,
        question: &str,
        limit: usize,
        vocab: &SearchVocab,
        kind_rates: &HashMap<String, KindRates>,
    ) -> Result<AskOutcome> {
        ask(self, question, limit, vocab, kind_rates).map_err(|e| match e {
            AskError::Source(e) => e,
            e => anyhow::anyhow!("{e}"),
        })
    }

    pub fn search(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        let (total, per_term) = term_counts(&self.client, terms)?;
        let n = total.max(1) as f64;
        let weights: Vec<f64> = per_term
            .iter()
            .map(|df| (1.0 + n / (1.0 + *df as f64)).ln())
            .collect();
        let sql = bm25_candidates_sql(terms, &weights, LOCAL_CANDIDATES);
        let corpus = rows_from_batches(&query(&self.client, &sql)?);
        Ok((corpus, Some(weights)))
    }
}

impl AskSource for DuckDbSearch {
    fn corpus(&self, terms: &[String]) -> Result<(Vec<CorpusRow>, Option<Vec<f64>>)> {
        self.search(terms)
    }

    fn token_df(&self, tokens: &[String]) -> Result<Vec<i64>> {
        let (_, dfs) = term_counts(&self.client, tokens)?;
        Ok(dfs)
    }

    fn rows_by_ids(&self, ids: &[&str]) -> Result<Vec<CorpusRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = corpus_rows_sql(&format!(
            "cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d
  WHERE d.project_id = {pid} AND d.commit_sha = {sha} AND d.id IN ({list})
)",
            pid = self.pid,
            sha = self.sha,
            list = ids.join(", "),
        ));
        Ok(rows_from_batches(&query(&self.client, &sql)?))
    }
}

impl NeighborhoodSource for DuckDbSearch {
    type Error = anyhow::Error;

    fn hop(&self, ids: &[&str], cap: usize) -> Result<Vec<NeighborhoodEdge>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
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
        Ok(kinds
            .into_iter()
            .zip(sources)
            .zip(targets)
            .map(|((kind, source), target)| NeighborhoodEdge {
                kind,
                source,
                target,
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
                "SELECT CAST(id AS VARCHAR) AS id, COUNT(*) AS degree FROM (
  SELECT source_id AS id FROM gl_edge WHERE source_id IN ({list})
  UNION ALL
  SELECT target_id FROM gl_edge WHERE target_id IN ({list})
) GROUP BY 1"
            ),
        )?;
        let node_ids = string_column(&batches, "id");
        let counts = i64_column(&batches, "degree");
        Ok(node_ids
            .into_iter()
            .zip(counts)
            .map(|(id, c)| (id, c as u64))
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
                "SELECT CAST(id AS VARCHAR) AS id, label, loc, scoped FROM (
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
        let scoped = bool_column(&batches, "scoped");
        Ok((0..node_ids.len())
            .map(|i| {
                (
                    node_ids[i].clone(),
                    NodeLabel {
                        label: node_labels[i].clone(),
                        loc: locs[i].clone(),
                        scoped: scoped[i],
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

fn ensure_search_text(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<()> {
    let columns = client.query_arrow(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM information_schema.columns
 WHERE table_name = 'gl_definition' AND column_name = 'search_text'",
    )?;
    let indexed = scalar_i64(&columns) > 0
        && scalar_i64(&client.query_arrow(&format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM gl_definition
  WHERE project_id = {project_id} AND commit_sha = {sha} AND token_count > 0
  LIMIT 1
)"
        ))?) > 0;
    if !indexed {
        anyhow::bail!(
            "local graph has no ranked-search data for this commit; \
             re-index the repository (`orbit index <path>`)"
        );
    }
    Ok(())
}

fn corpus_view_sql(pid: i64, sha: &str) -> String {
    format!(
        "CREATE OR REPLACE TEMP VIEW search_corpus AS
SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line,
       d.search_text, d.token_count
FROM gl_definition d
WHERE d.project_id = {pid} AND d.commit_sha = {sha}
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
    let tokens = query_tokens(&[term.to_string()]);
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
    let tokens = query_tokens(&[term.to_string()]);
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

fn corpus_rows_sql(cand_ctes: &str) -> String {
    format!(
        "WITH {cand_ctes},
deg AS (
  SELECT id, COUNT(*) AS degree FROM (
    SELECT source_id AS id FROM gl_edge WHERE source_id IN (SELECT id FROM cand)
    UNION ALL
    SELECT target_id FROM gl_edge WHERE target_id IN (SELECT id FROM cand)
  ) GROUP BY 1
)
SELECT CAST(c.id AS VARCHAR) AS id, c.fqn, c.definition_type,
       c.file_path || ':' || CAST(c.start_line AS VARCHAR) AS loc,
       c.end_line,
       COALESCE(deg.degree, 0) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id"
    )
}

fn bm25_candidates_sql(terms: &[String], weights: &[f64], cap: usize) -> String {
    let match_clauses: Vec<String> = terms.iter().filter_map(|t| token_like_sql(t)).collect();
    let any_match = if match_clauses.is_empty() {
        "FALSE".to_string()
    } else {
        format!("({})", match_clauses.join(" OR "))
    };
    let mut tf_cols = String::new();
    let mut score_parts: Vec<String> = Vec::new();
    for (i, term) in terms.iter().enumerate() {
        let Some(tf) = token_tf_sql(term) else {
            continue;
        };
        let idf = weights.get(i).copied().unwrap_or(1.0);
        tf_cols.push_str(&format!(",\n         {tf} AS tf_{i}"));
        score_parts.push(format!(
            "{idf:.6} * tf_{i} * ({k1} + 1)
           / (tf_{i} + {k1} * (1 - {b} + {b} * token_count / avgdl))",
            k1 = BM25_K1,
            b = BM25_B,
        ));
    }
    let score = if score_parts.is_empty() {
        "0".to_string()
    } else {
        score_parts.join("\n         + ")
    };
    corpus_rows_sql(&format!(
        "stats AS (
  SELECT GREATEST(AVG(token_count), 1) AS avgdl FROM search_corpus
),
matched AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line,
         d.token_count{tf_cols}
  FROM search_corpus d
  WHERE {any_match}
),
cand AS (
  SELECT id, fqn, definition_type, file_path, start_line, end_line
  FROM matched CROSS JOIN stats
  ORDER BY ({score}) DESC, length(fqn) ASC
  LIMIT {cap}
)"
    ))
}

fn term_counts(client: &DuckDbClient, terms: &[String]) -> Result<(i64, Vec<i64>)> {
    let counters: String = terms
        .iter()
        .enumerate()
        .map(|(i, t)| match token_like_sql(t) {
            Some(matched) => format!(", COUNT(*) FILTER (WHERE {matched}) AS df_{i}"),
            None => format!(", CAST(0 AS BIGINT) AS df_{i}"),
        })
        .collect();
    let batches = query(
        client,
        &format!("SELECT COUNT(*) AS total{counters} FROM search_corpus d"),
    )?;
    let count_of = |name: &str| i64_column(&batches, name).first().copied().unwrap_or(0);
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
    let end_lines = i64_column(batches, "end_line");
    let degrees = i64_column(batches, "degree");
    (0..ids.len())
        .map(|i| CorpusRow {
            id: ids[i].clone(),
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: locs[i].clone(),
            end_line: end_lines[i],
            degree: degrees[i] as u64,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_sql_embeds_padded_token_matches() {
        let sql = bm25_candidates_sql(&["valid".to_string()], &[1.5], 10);
        assert!(sql.contains("LIKE '% valid %'"), "sql was {sql}");
        assert!(sql.contains("1.500000"), "sql was {sql}");
        assert!(sql.contains("ORDER BY ("), "sql was {sql}");
        assert_eq!(
            sql.matches("list_filter").count(),
            1,
            "tf must be computed once as a column, sql was {sql}"
        );
    }

    #[test]
    fn bm25_tf_counts_exact_tokens_not_substrings() {
        let sql = token_tf_sql("validated").unwrap();
        assert!(sql.contains("x = 'valid'"), "sql was {sql}");
        assert!(!sql.contains('%'), "sql was {sql}");
    }
}
