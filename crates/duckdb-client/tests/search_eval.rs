//! Golden-set retrieval eval: homebrew search (tokenizer + BM25 SQL + rerank)
//! versus DuckDB's FTS extension as the drop-in comparator, over a frozen
//! corpus of real gitlab-org/gitlab definitions. Guards ranking quality the
//! way unit tests cannot: a tokenizer or scoring change that hurts recall
//! fails here, and if the drop-in ever beats the custom brain the assertion
//! at the bottom forces that conversation.
//!
//! Corpus provenance: `orbit local index` over gitlab-org/gitlab, then a dump
//! of five subsystems (merge requests, users, issues, ci pipelines, auth):
//! `orbit local sql -F csv "SELECT id, fqn, name, definition_type, file_path,
//! start_line, end_line FROM gl_definition WHERE <subsystem path filters>"`.
//! The fixture is frozen on purpose — golden expectations are written against
//! this snapshot, not against the moving upstream repo.

use duckdb_client::search::DuckDbSearch;
use duckdb_client::{DuckDbClient, sql_lit, string_column};
use orbit_search::{SearchVocab, content_words, rank_and_trim, search_document};

const CORPUS: &str = include_str!("fixtures/search_eval_corpus.csv");
const GOLDEN: &str = include_str!("fixtures/search_eval_golden.json");
const PROJECT_ID: i64 = 1;
const COMMIT: &str = "eval";
const TOP_K: usize = 10;

struct GoldenCase {
    query: String,
    expect: Vec<String>,
}

fn golden_cases() -> Vec<GoldenCase> {
    let parsed: serde_json::Value = serde_json::from_str(GOLDEN).expect("golden json");
    parsed
        .as_array()
        .expect("golden array")
        .iter()
        .map(|case| GoldenCase {
            query: case["query"].as_str().expect("query").to_string(),
            expect: case["expect"]
                .as_array()
                .expect("expect")
                .iter()
                .map(|e| e.as_str().expect("expect entry").to_string())
                .collect(),
        })
        .collect()
}

/// Only the columns the search SQL touches; the authoritative DDL lives in
/// `config/graph_local.sql`, which duckdb-client cannot embed (no CONFIG_DIR).
const EVAL_DDL: &str = "
CREATE TABLE gl_definition (
    id BIGINT NOT NULL,
    traversal_path VARCHAR NOT NULL DEFAULT '',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL DEFAULT 'main',
    commit_sha VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    fqn VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    definition_type VARCHAR NOT NULL,
    start_line BIGINT NOT NULL,
    end_line BIGINT NOT NULL,
    search_text VARCHAR NOT NULL DEFAULT '',
    token_count BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE gl_edge (
    source_id BIGINT NOT NULL,
    source_kind VARCHAR NOT NULL,
    relationship_kind VARCHAR NOT NULL,
    target_id BIGINT NOT NULL,
    target_kind VARCHAR NOT NULL,
    traversal_path VARCHAR NOT NULL
);
";

fn load_corpus(client: &DuckDbClient) {
    client.execute(EVAL_DDL, &[]).expect("eval ddl");
    let mut rows = Vec::new();
    // Definition ids derive from the fqn, so a symbol indexed under two
    // definition_types collides; FTS requires unique document ids.
    let mut seen_ids = std::collections::HashSet::new();
    for line in CORPUS.lines().skip(1) {
        let fields: Vec<&str> = line.split(',').collect();
        assert_eq!(fields.len(), 7, "unquoted csv row: {line}");
        let (id, fqn, name, def_type, path, start, end) = (
            fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6],
        );
        if !seen_ids.insert(id) {
            continue;
        }
        let (search_text, token_count) = search_document(fqn, path);
        rows.push(format!(
            "({id}, {PROJECT_ID}, {sha}, {path}, {fqn}, {name}, {def_type}, {start}, {end}, {st}, {tc})",
            sha = sql_lit(COMMIT),
            path = sql_lit(path),
            fqn = sql_lit(fqn),
            name = sql_lit(name),
            def_type = sql_lit(def_type),
            st = sql_lit(&search_text),
            tc = token_count,
        ));
    }
    for chunk in rows.chunks(1000) {
        client
            .execute(
                &format!(
                    "INSERT INTO gl_definition (id, project_id, commit_sha, file_path, fqn, name, \
                     definition_type, start_line, end_line, search_text, token_count) VALUES {}",
                    chunk.join(", ")
                ),
                &[],
            )
            .expect("corpus insert");
    }
}

fn homebrew_top_k(search: &DuckDbSearch, vocab: &SearchVocab, query: &str) -> Vec<String> {
    let terms = content_words(query);
    let (corpus, weights) = search.search(&terms).expect("search");
    rank_and_trim(&terms, &corpus, TOP_K, weights.as_deref(), vocab)
        .into_iter()
        .map(|h| corpus[h.index].fqn.clone())
        .collect()
}

fn fts_top_k(client: &DuckDbClient, query: &str) -> Vec<String> {
    let batches = client
        .query_arrow(&format!(
            "SELECT fqn FROM (
  SELECT fqn, fts_main_gl_definition.match_bm25(id, {q}) AS score FROM gl_definition
) WHERE score IS NOT NULL ORDER BY score DESC LIMIT {TOP_K}",
            q = sql_lit(query),
        ))
        .expect("fts query");
    string_column(&batches, "fqn")
}

struct Metrics {
    recall: f64,
    mrr: f64,
    misses: Vec<String>,
}

fn score(cases: &[GoldenCase], mut top_k: impl FnMut(&str) -> Vec<String>) -> Metrics {
    let mut hits = 0usize;
    let mut reciprocal_sum = 0.0;
    let mut misses = Vec::new();
    for case in cases {
        let results = top_k(&case.query);
        let rank = results
            .iter()
            .position(|fqn| case.expect.iter().any(|e| fqn.contains(e.as_str())));
        match rank {
            Some(r) => {
                hits += 1;
                reciprocal_sum += 1.0 / (r as f64 + 1.0);
            }
            None => misses.push(case.query.clone()),
        }
    }
    Metrics {
        recall: hits as f64 / cases.len() as f64,
        mrr: reciprocal_sum / cases.len() as f64,
        misses,
    }
}

#[test]
fn golden_set_recall_beats_the_fts_drop_in() {
    let dir = std::env::temp_dir().join(format!("search-eval-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("eval.duckdb");
    let _ = std::fs::remove_file(&db);
    let client = DuckDbClient::open(&db).expect("open");
    load_corpus(&client);
    let cases = golden_cases();

    let fts_metrics = match client.execute(
        "INSTALL fts; LOAD fts;
         PRAGMA create_fts_index('gl_definition', 'id', 'fqn', 'file_path', stemmer='porter')",
        &[],
    ) {
        Ok(_) => Some(score(&cases, |q| fts_top_k(&client, q))),
        Err(e) => {
            eprintln!("fts extension unavailable, skipping baseline lane: {e}");
            None
        }
    };

    let vocab = SearchVocab::new(std::iter::empty::<String>());
    let search = DuckDbSearch::new(client, PROJECT_ID, COMMIT);
    let ours = score(&cases, |q| homebrew_top_k(&search, &vocab, q));

    println!(
        "homebrew: recall@{TOP_K} {:.3}  mrr {:.3}  misses {:?}",
        ours.recall, ours.mrr, ours.misses
    );
    if let Some(fts) = &fts_metrics {
        println!(
            "fts     : recall@{TOP_K} {:.3}  mrr {:.3}  misses {:?}",
            fts.recall, fts.mrr, fts.misses
        );
    }

    assert!(
        ours.recall >= 0.9,
        "homebrew recall@{TOP_K} regressed to {:.3}; misses: {:?}",
        ours.recall,
        ours.misses
    );
    assert!(ours.mrr >= 0.6, "homebrew mrr regressed to {:.3}", ours.mrr);
    if let Some(fts) = &fts_metrics {
        assert!(
            ours.recall >= fts.recall,
            "the FTS drop-in beats the custom brain on recall ({:.3} vs {:.3}); \
             per the search design contract, revisit whether the custom brain still earns its keep",
            fts.recall,
            ours.recall
        );
    }
}
