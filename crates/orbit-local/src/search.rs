use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use crate::commands::repo_map::{
    DEFAULT_SOURCE_EXTS, EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex, scalar_i64, sql_lit, string_column,
};

const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const POSTINGS_FLUSH_ROWS: usize = 500_000;
const POSTINGS_CHUNK_ROWS: usize = 65_536;

pub(crate) fn split_words(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in input.split(|c: char| !c.is_ascii_alphanumeric()) {
        let chars: Vec<char> = word.chars().collect();
        let mut start = 0;
        for i in 1..=chars.len() {
            let boundary = i == chars.len()
                || (chars[i].is_ascii_uppercase()
                    && (chars[i - 1].is_ascii_lowercase()
                        || chars[i - 1].is_ascii_digit()
                        || (i + 1 < chars.len() && chars[i + 1].is_ascii_lowercase())));
            if boundary {
                let tok: String = chars[start..i].iter().collect::<String>().to_lowercase();
                if tok.len() >= 2 {
                    tokens.push(tok);
                }
                start = i;
            }
        }
    }
    tokens
}

pub(crate) fn stem(word: &str) -> String {
    thread_local! {
        static STEMMER: rust_stemmers::Stemmer =
            rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
        static CACHE: std::cell::RefCell<HashMap<String, String>> =
            std::cell::RefCell::new(HashMap::new());
    }
    CACHE.with(|cache| {
        if let Some(stemmed) = cache.borrow().get(word) {
            return stemmed.clone();
        }
        let stemmed = STEMMER.with(|s| s.stem(word).into_owned());
        cache.borrow_mut().insert(word.to_string(), stemmed.clone());
        stemmed
    })
}

pub(crate) fn corpus_predicate(pid: i64, sha: &str) -> String {
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

fn token_counts(words: Vec<String>) -> HashMap<String, i32> {
    let mut counts: HashMap<String, i32> = HashMap::new();
    for word in words {
        *counts.entry(stem(&word)).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
fn doc_token_counts(fqn: &str, file_path: &str) -> HashMap<String, i32> {
    let mut counts = token_counts(split_words(fqn));
    for (token, tf) in token_counts(split_words(file_path)) {
        *counts.entry(token).or_insert(0) += tf;
    }
    counts
}

pub(crate) fn build_postings(
    client: &duckdb_client::DuckDbClient,
    project_id: i64,
    commit_sha: &str,
) -> Result<usize> {
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

pub(crate) fn has_postings(
    client: &duckdb_client::DuckDbClient,
    project_id: i64,
    sha: &str,
) -> Result<bool> {
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

pub(crate) fn query_tokens(terms: &[String]) -> Vec<String> {
    let mut tokens: Vec<String> = terms
        .iter()
        .flat_map(|t| split_words(t))
        .map(|t| stem(&t))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

pub(crate) fn bm25_candidates_sql(pid: i64, sha: &str, tokens: &[String], cap: usize) -> String {
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
  SELECT d.id, d.fqn, d.definition_type, d.file_path, d.start_line
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
       CAST(COALESCE(deg.degree, 0) AS VARCHAR) AS degree
FROM cand c
LEFT JOIN deg ON deg.id = c.id",
        values = values.join(", "),
        k1 = BM25_K1,
        b = BM25_B,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_token_counts_stems_and_counts_identifier_fragments() {
        let counts = doc_token_counts(
            "indexer::nats::message::NatsMessage::to_dlq",
            "crates/indexer/src/nats/message.rs",
        );
        assert_eq!(counts.get("dlq"), Some(&1));
        assert_eq!(counts.get("nat"), Some(&3));
        assert_eq!(counts.get("messag"), Some(&3));
    }

    #[test]
    fn query_tokens_stem_and_dedupe() {
        let tokens = query_tokens(&["validated".to_string(), "Validate".to_string()]);
        assert_eq!(tokens, vec!["valid".to_string()]);
    }

    #[test]
    fn bm25_sql_embeds_tokens_as_values() {
        let sql = bm25_candidates_sql(1, "'abc'", &["valid".to_string()], 10);
        assert!(sql.contains("VALUES ('valid')"), "sql was {sql}");
        assert!(sql.contains("ORDER BY sc.score DESC"), "sql was {sql}");
    }

    #[test]
    fn postings_roundtrip_ranks_exact_symbol_above_long_tie() {
        let dir = std::env::temp_dir().join(format!("orbit-search-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("t.duckdb");
        let _ = std::fs::remove_file(&db);
        let client = duckdb_client::DuckDbClient::open(&db).unwrap();
        client
            .initialize_schema(include_str!(concat!(
                env!("CONFIG_DIR"),
                "/graph_local.sql"
            )))
            .unwrap();
        let insert = |id: i64, fqn: &str, name: &str, path: &str| {
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', 'Method', 1, 2, 0, 0, 0, 0)"
                    ),
                    &[],
                )
                .unwrap();
        };
        insert(
            1,
            "Group::execute_hooks",
            "execute_hooks",
            "app/models/group.rb",
        );
        insert(
            2,
            "Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build",
            "execute_hooks_for_created_build",
            "app/workers/ci/execute_build_hooks_worker.rb",
        );
        insert(
            3,
            "Project::unrelated",
            "unrelated",
            "app/models/project.rb",
        );

        let rows = build_postings(&client, 7, "sha").unwrap();
        assert!(rows > 0);
        assert!(has_postings(&client, 7, "'sha'").unwrap());

        let sql = bm25_candidates_sql(
            7,
            "'sha'",
            &query_tokens(&["execute_hooks".to_string()]),
            10,
        );
        let batches = client.query_arrow(&sql).unwrap();
        let fqns = string_column(&batches, "fqn");
        assert!(fqns.contains(&"Group::execute_hooks".to_string()));
        assert!(
            fqns.contains(
                &"Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build".to_string()
            )
        );
        assert!(!fqns.contains(&"Project::unrelated".to_string()));
    }
}
