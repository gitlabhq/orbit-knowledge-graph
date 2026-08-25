use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

use crate::{DuckDbClient, f64_column, i64_column, scalar_i64, sql_lit, string_column};
use orbit_search::ask::{AskError, AskSource, ask};
use orbit_search::corpus::{DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex};
use orbit_search::expand::{GraphSource, NodeLabel};
use orbit_search::{AskOutcome, CorpusRow, Graph, GraphEdge, KindRates, SearchVocab, TermRecall};
use std::collections::HashMap;

pub const RECALL_FLOOR: f64 = 0.5;
pub const CONTEXT_SIM_WEIGHT: f64 = 0.4;
pub const RECALL_LIMIT: usize = 2000;
pub const LENGTH_NORM_B: f64 = 0.75;

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
}

impl DuckDbSearch {
    pub fn new(client: DuckDbClient, project_id: i64, commit_sha: &str) -> Result<Self> {
        let sha = sql_lit(commit_sha);
        ensure_trigram_index(&client, project_id, &sha)?;
        client.execute(&corpus_table_sql(project_id, &sha), &[])?;
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
}

impl AskSource for DuckDbSearch {
    fn recall(&self, terms: &[String]) -> Result<Vec<TermRecall>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let sql = recall_sql(self.pid, &self.sha, terms.len());
        let params: Vec<serde_json::Value> = terms
            .iter()
            .map(|t| serde_json::Value::String(t.clone()))
            .collect();
        let batches = self
            .client
            .query_arrow_json(&sql, &params)
            .with_context(|| format!("trigram recall failed for terms {terms:?}"))?;
        let term_idx = i64_column(&batches, "term_idx");
        let ids = i64_column(&batches, "id");
        let sims = f64_column(&batches, "sim");
        let dfs = i64_column(&batches, "df");
        let totals = i64_column(&batches, "total");
        let mut recalls: Vec<TermRecall> = terms
            .iter()
            .map(|_| TermRecall {
                hits: Vec::new(),
                matched: 0,
                corpus: 0,
            })
            .collect();
        for i in 0..term_idx.len() {
            let Some(recall) = recalls.get_mut(term_idx[i] as usize) else {
                continue;
            };
            recall.matched = dfs[i] as u64;
            recall.corpus = totals[i] as u64;
            if ids[i] != 0 {
                recall.hits.push((ids[i], sims[i]));
            }
        }
        Ok(recalls)
    }

    fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<CorpusRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = corpus_rows_sql(
            &format!(
                "cand AS (
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
  FROM gl_definition d
  WHERE d.project_id = {pid} AND d.commit_sha = {sha} AND d.id IN ({list})
)",
                pid = self.pid,
                sha = self.sha,
                list = id_list(ids),
            ),
            self.pid,
            &self.sha,
        );
        Ok(rows_from_batches(&query(&self.client, &sql)?))
    }
}

fn id_list(ids: &[i64]) -> String {
    ids.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl GraphSource for DuckDbSearch {
    type Error = anyhow::Error;

    fn graph(&self, _seeds: &[i64]) -> Result<Graph> {
        let pid = self.pid;
        let sha = &self.sha;
        let batches = query(
            &self.client,
            &format!(
                "WITH nodes AS (
  SELECT id FROM gl_definition
  WHERE project_id = {pid} AND commit_sha = {sha} AND fqn NOT LIKE '%@%'
  UNION ALL
  SELECT id FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id FROM gl_directory WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id FROM gl_imported_symbol WHERE project_id = {pid} AND commit_sha = {sha}
)
SELECT relationship_kind, source_id, target_id
FROM gl_edge
WHERE source_id IN (SELECT id FROM nodes)
  AND target_id IN (SELECT id FROM nodes)"
            ),
        )?;
        let kind_names = string_column(&batches, "relationship_kind");
        let sources = i64_column(&batches, "source_id");
        let targets = i64_column(&batches, "target_id");
        let mut kinds: Vec<String> = Vec::new();
        let mut kind_index: HashMap<String, u16> = HashMap::new();
        let edges = (0..kind_names.len())
            .map(|i| {
                let kind = *kind_index.entry(kind_names[i].clone()).or_insert_with(|| {
                    kinds.push(kind_names[i].clone());
                    (kinds.len() - 1) as u16
                });
                GraphEdge {
                    kind,
                    source: sources[i],
                    target: targets[i],
                }
            })
            .collect();
        Ok(Graph { kinds, edges })
    }

    fn labels(&self, ids: &[i64]) -> Result<HashMap<i64, NodeLabel>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let list = id_list(ids);
        let pid = self.pid;
        let sha = &self.sha;
        let batches = query(
            &self.client,
            &format!(
                "SELECT id, label, loc FROM (
  SELECT id, fqn AS label,
         file_path || ':' || CAST(start_line AS VARCHAR) AS loc
  FROM gl_definition
  WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path, '' FROM gl_file WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, path, '' FROM gl_directory WHERE project_id = {pid} AND commit_sha = {sha}
  UNION ALL
  SELECT id, identifier_name, '' FROM gl_imported_symbol
  WHERE project_id = {pid} AND commit_sha = {sha}
)
WHERE id IN ({list})"
            ),
        )?;
        let node_ids = i64_column(&batches, "id");
        let node_labels = string_column(&batches, "label");
        let locs = string_column(&batches, "loc");
        Ok((0..node_ids.len())
            .map(|i| {
                (
                    node_ids[i],
                    NodeLabel {
                        label: node_labels[i].clone(),
                        loc: locs[i].clone(),
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

fn ensure_trigram_index(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<()> {
    let indexed = scalar_i64(&client.query_arrow(&format!(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM gl_def_trigram
  WHERE project_id = {project_id} AND commit_sha = {sha}
  LIMIT 1
)"
    ))?) > 0;
    if !indexed {
        anyhow::bail!(
            "local graph has no search index for this commit; \
             re-index the repository (`orbit index <path>`)"
        );
    }
    Ok(())
}

fn recall_sql(pid: i64, sha: &str, term_count: usize) -> String {
    let q_arms: Vec<String> = (0..term_count)
        .map(|i| {
            format!(
                "SELECT {i} AS term_idx, UNNEST(trigrams(?{p})) AS gram",
                p = i + 1
            )
        })
        .collect();
    format!(
        "WITH q AS ({q}),
qn AS (SELECT term_idx, GREATEST(COUNT(*), 1) AS n FROM q GROUP BY term_idx),
corpus_n AS (SELECT GREATEST(COUNT(*), 1) AS total FROM search_corpus),
field_sims AS (
  SELECT t.def_id, q.term_idx,
         CAST(COUNT(DISTINCT CASE WHEN t.field = 'name' THEN t.gram END) AS DOUBLE)
           / ANY_VALUE(qn.n) AS name_sim,
         CAST(COUNT(DISTINCT CASE WHEN t.field = 'context' THEN t.gram END) AS DOUBLE)
           / ANY_VALUE(qn.n) AS ctx_sim
  FROM gl_def_trigram t
  JOIN q ON q.gram = t.gram
  JOIN qn ON qn.term_idx = q.term_idx
  WHERE t.project_id = {pid} AND t.commit_sha = {sha}
    AND t.def_id IN (SELECT id FROM search_corpus)
  GROUP BY t.def_id, q.term_idx
),
hits AS (
  SELECT def_id, term_idx,
         GREATEST(name_sim, {CONTEXT_SIM_WEIGHT} * ctx_sim) AS sim
  FROM field_sims
  WHERE GREATEST(name_sim, ctx_sim) >= {RECALL_FLOOR}
),
df AS (SELECT term_idx, COUNT(*) AS df FROM hits GROUP BY term_idx),
lens AS (
  SELECT t.def_id, CAST(COUNT(DISTINCT t.gram) AS DOUBLE) AS len
  FROM gl_def_trigram t
  WHERE t.project_id = {pid} AND t.commit_sha = {sha} AND t.field = 'context'
    AND t.def_id IN (SELECT def_id FROM hits)
  GROUP BY t.def_id
),
avgdl AS (SELECT GREATEST(AVG(len), 1) AS avg_len FROM lens),
top_defs AS (
  SELECT h.def_id
  FROM hits h
  JOIN df ON df.term_idx = h.term_idx
  JOIN lens ON lens.def_id = h.def_id
  CROSS JOIN corpus_n
  CROSS JOIN avgdl
  GROUP BY h.def_id
  ORDER BY SUM(h.sim * LN(1 + corpus_n.total / (1.0 + df.df))
               / (1 - {LENGTH_NORM_B} + {LENGTH_NORM_B} * lens.len / avgdl.avg_len)) DESC,
           h.def_id
  LIMIT {RECALL_LIMIT}
),
surviving AS (
  SELECT h.def_id, h.term_idx, h.sim
  FROM hits h
  JOIN top_defs ON top_defs.def_id = h.def_id
)
SELECT CAST(df.term_idx AS BIGINT) AS term_idx,
       COALESCE(s.def_id, 0) AS id,
       COALESCE(s.sim, 0.0) AS sim,
       df.df,
       corpus_n.total
FROM df
CROSS JOIN corpus_n
LEFT JOIN surviving s ON s.term_idx = df.term_idx
ORDER BY df.term_idx, sim DESC, id",
        q = q_arms.join(" UNION ALL ")
    )
}

fn corpus_table_sql(pid: i64, sha: &str) -> String {
    format!(
        "CREATE OR REPLACE TEMP TABLE search_corpus AS
SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line
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

fn corpus_rows_sql(cand_ctes: &str, pid: i64, sha: &str) -> String {
    format!(
        "WITH {cand_ctes},
deg AS (
  SELECT id, COUNT(*) AS degree FROM (
    SELECT source_id AS id FROM gl_edge WHERE source_id IN (SELECT id FROM cand)
    UNION ALL
    SELECT target_id FROM gl_edge WHERE target_id IN (SELECT id FROM cand)
  ) GROUP BY 1
),
lens AS (
  SELECT def_id, COUNT(DISTINCT gram) AS grams FROM gl_def_trigram
  WHERE project_id = {pid} AND commit_sha = {sha} AND field = 'context'
    AND def_id IN (SELECT id FROM cand)
  GROUP BY def_id
)
SELECT c.id, c.fqn, c.definition_type,
       c.file_path || ':' || CAST(c.start_line AS VARCHAR) AS loc,
       c.end_line,
       COALESCE(deg.degree, 0) AS degree,
       COALESCE(lens.grams, 0) AS grams
FROM cand c
LEFT JOIN deg ON deg.id = c.id
LEFT JOIN lens ON lens.def_id = c.id"
    )
}

fn rows_from_batches(batches: &[RecordBatch]) -> Vec<CorpusRow> {
    let ids = i64_column(batches, "id");
    let fqns = string_column(batches, "fqn");
    let kinds = string_column(batches, "definition_type");
    let locs = string_column(batches, "loc");
    let end_lines = i64_column(batches, "end_line");
    let degrees = i64_column(batches, "degree");
    let grams = i64_column(batches, "grams");
    (0..ids.len())
        .map(|i| CorpusRow {
            id: ids[i],
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: locs[i].clone(),
            end_line: end_lines[i],
            degree: degrees[i] as u64,
            grams: grams[i] as u64,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_sql_grams_every_term_and_caps_on_the_combined_idf_weighted_score() {
        let sql = recall_sql(7, "'sha'", 2);
        assert!(sql.contains("trigrams(?1)"), "sql was {sql}");
        assert!(
            sql.contains("1 AS term_idx, UNNEST(trigrams(?2))"),
            "sql was {sql}"
        );
        assert!(
            sql.contains("IN (SELECT id FROM search_corpus)"),
            "sql was {sql}"
        );
        assert!(
            sql.contains("CASE WHEN t.field = 'name' THEN t.gram END"),
            "sql was {sql}"
        );
        assert!(
            sql.contains(&format!(
                "GREATEST(name_sim, {CONTEXT_SIM_WEIGHT} * ctx_sim)"
            )),
            "sql was {sql}"
        );
        assert!(
            sql.contains(&format!("GREATEST(name_sim, ctx_sim) >= {RECALL_FLOOR}")),
            "sql was {sql}"
        );
        assert!(
            sql.contains("SUM(h.sim * LN(1 + corpus_n.total / (1.0 + df.df))"),
            "sql was {sql}"
        );
        assert!(sql.contains("lens.len / avgdl.avg_len"), "sql was {sql}");
        assert!(
            sql.contains(&format!("LIMIT {RECALL_LIMIT}")),
            "sql was {sql}"
        );
    }
}
