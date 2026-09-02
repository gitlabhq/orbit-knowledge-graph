use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

use crate::{DuckDbClient, f64_column, i64_column, scalar_i64, sql_lit, string_column};
use orbit_search::ask::{AskError, AskSource, Caller, CallerEdge, ask};
use orbit_search::corpus::{DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex};
use orbit_search::expand::{GraphSource, NodeLabel};
use orbit_search::{AskOutcome, CorpusRow, Graph, GraphEdge, KindRates, SearchVocab, TermRecall};
use std::collections::HashMap;

pub const CONTEXT_SIM_CAP: f64 = 0.99;
pub const NAME_SIM_FLOOR: f64 = 0.999;

pub const FTS_STEMMER: &str = "english";

pub fn def_doc_table(project_id: i64) -> String {
    format!("gl_def_doc_{project_id}")
}

pub fn create_fts_index_sql(doc_table: &str) -> String {
    format!(
        "PRAGMA create_fts_index('{doc_table}', 'def_id', 'name', 'context', stemmer='{FTS_STEMMER}', stopwords='none', overwrite=1)"
    )
}

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
}

impl DuckDbSearch {
    pub fn new(client: DuckDbClient, project_id: i64, commit_sha: &str) -> Result<Self> {
        let sha = sql_lit(commit_sha);
        client.load_extension("fts")?;
        ensure_search_index(&client, project_id, &sha)?;
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
    fn stem(&self, words: &[String]) -> Result<Vec<String>> {
        if words.is_empty() {
            return Ok(Vec::new());
        }
        let values = words
            .iter()
            .enumerate()
            .map(|(i, w)| format!("({i}, {})", sql_lit(&w.to_lowercase())))
            .collect::<Vec<_>>()
            .join(", ");
        let batches = query(
            &self.client,
            &format!(
                "SELECT stem(w, '{FTS_STEMMER}') AS s FROM (VALUES {values}) t(i, w) ORDER BY i"
            ),
        )?;
        Ok(string_column(&batches, "s"))
    }

    fn recall(&self, terms: &[String]) -> Result<Vec<TermRecall>> {
        let sql = recall_sql(self.pid, &self.sha);
        terms
            .iter()
            .map(|term| {
                let batches = self
                    .client
                    .query_arrow_json(&sql, &[serde_json::Value::String(term.clone())])
                    .with_context(|| format!("fts recall failed for term {term:?}"))?;
                let ids = i64_column(&batches, "id");
                let sims = f64_column(&batches, "sim");
                let dfs = i64_column(&batches, "df");
                let totals = i64_column(&batches, "total");
                let mut recall = TermRecall {
                    hits: Vec::new(),
                    matched: 0,
                    corpus: 0,
                };
                for i in 0..ids.len() {
                    recall.matched = dfs[i] as u64;
                    recall.corpus = totals[i] as u64;
                    if ids[i] != 0 {
                        recall.hits.push((ids[i], sims[i]));
                    }
                }
                Ok(recall)
            })
            .collect()
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

    fn callers(&self, ids: &[i64]) -> Result<Vec<CallerEdge>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let batches = query(
            &self.client,
            &format!(
                "WITH callers AS (
  SELECT DISTINCT e.target_id, s.fqn,
         s.file_path || ':' || CAST(s.start_line AS VARCHAR) AS loc
  FROM gl_edge e
  JOIN gl_definition s ON s.id = e.source_id
  WHERE e.relationship_kind = 'CALLS'
    AND e.target_id IN ({list})
    AND s.project_id = {pid} AND s.commit_sha = {sha}
)
SELECT target_id, fqn, loc,
       COUNT(*) OVER (PARTITION BY target_id) AS total
FROM callers
QUALIFY row_number() OVER (PARTITION BY target_id ORDER BY fqn) <= {cap}
ORDER BY target_id, fqn",
                list = id_list(ids),
                pid = self.pid,
                sha = self.sha,
                cap = orbit_search::ask::CALLERS_SHOWN,
            ),
        )?;
        let callees = i64_column(&batches, "target_id");
        let fqns = string_column(&batches, "fqn");
        let locs = string_column(&batches, "loc");
        let totals = i64_column(&batches, "total");
        Ok((0..callees.len())
            .map(|i| CallerEdge {
                callee: callees[i],
                caller: Caller {
                    label: fqns[i].clone(),
                    loc: locs[i].clone(),
                },
                total: usize::try_from(totals[i]).unwrap_or(0),
            })
            .collect())
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

fn ensure_search_index(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<()> {
    let doc_table = def_doc_table(project_id);
    let table_exists = scalar_i64(&client.query_arrow(&format!(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_tables()
  WHERE table_name = {}",
        sql_lit(&doc_table)
    ))?) > 0;
    let indexed = table_exists
        && scalar_i64(&client.query_arrow(&format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM {doc_table}
  WHERE commit_sha = {sha}
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

fn recall_sql(pid: i64, sha: &str) -> String {
    let doc_table = def_doc_table(pid);
    format!(
        "WITH scored AS (
  SELECT def_id AS id,
         fts_main_{doc_table}.match_bm25(def_id, ?1, fields := 'name,context') AS score
  FROM {doc_table}
  WHERE commit_sha = {sha}
    AND def_id IN (SELECT id FROM search_corpus)
),
hits AS (
  SELECT s.id, s.score,
         list_contains(
           list_transform(string_split_regex(lower(d.name), '[^0-9a-z]+'), t -> stem(t, '{FTS_STEMMER}')),
           stem(lower(?1), '{FTS_STEMMER}')) AS name_hit
  FROM scored s
  JOIN {doc_table} d ON d.def_id = s.id AND d.commit_sha = {sha}
  WHERE s.score IS NOT NULL
  ORDER BY s.score DESC, s.id
),
df AS (SELECT COUNT(*) AS df FROM scored WHERE score IS NOT NULL),
mx AS (SELECT MAX(score) AS m FROM hits),
corpus_n AS (SELECT GREATEST(COUNT(*), 1) AS total FROM search_corpus)
SELECT COALESCE(h.id, 0) AS id,
       COALESCE(CASE WHEN h.name_hit THEN {NAME_SIM_FLOOR} + (1.0 - {NAME_SIM_FLOOR}) * h.score / mx.m
                     ELSE LEAST(h.score / mx.m, {CONTEXT_SIM_CAP}) END, 0.0) AS sim,
       CAST(df.df AS BIGINT) AS df,
       CAST(corpus_n.total AS BIGINT) AS total
FROM df
CROSS JOIN corpus_n
CROSS JOIN mx
LEFT JOIN hits h ON TRUE
ORDER BY sim DESC, id"
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
    let doc_table = def_doc_table(pid);
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
  SELECT def_id, CAST(len(string_split(context, ' ')) AS BIGINT) AS grams
  FROM {doc_table}
  WHERE commit_sha = {sha}
    AND def_id IN (SELECT id FROM cand)
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
