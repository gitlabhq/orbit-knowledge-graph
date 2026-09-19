use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::search::DuckDbSearch;
use orbit_search::{GrepOutcome, RecallFilter};

use crate::workspace;

pub(super) struct LocalBackend {
    search: DuckDbSearch,
    header: String,
}

impl LocalBackend {
    pub(super) fn open(
        repo: Option<PathBuf>,
        db: Option<PathBuf>,
        paths: &[String],
    ) -> Result<Self> {
        let workspace::IndexedRepo { git, client } = workspace::open_indexed(repo, db)?;
        Ok(Self {
            search: DuckDbSearch::scoped(client, git.project_id, &git.commit_sha, paths)?,
            header: git.short_sha().to_string(),
        })
    }

    pub(super) fn header(&self) -> &str {
        &self.header
    }

    pub(super) fn search(&self) -> &DuckDbSearch {
        &self.search
    }

    pub(super) fn grep(
        &self,
        query: &str,
        limit: usize,
        filter: &RecallFilter,
    ) -> Result<(GrepOutcome, Vec<duckdb_client::search::NodeValue>)> {
        self.search.grep(query, limit, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestGraph {
        client: duckdb_client::DuckDbClient,
    }

    impl TestGraph {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("orbit-{name}-{}", std::process::id()));
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
            Self { client }
        }

        fn def(&self, id: i64, fqn: &str, name: &str, path: &str) {
            self.typed_def(id, fqn, name, path, "Method");
        }

        fn typed_def(&self, id: i64, fqn: &str, name: &str, path: &str, kind: &str) {
            self.client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', '{kind}', 1, 2, 0, 0, 0, 0)"
                    ),
                    &[],
                )
                .unwrap();
        }

        fn edge(&self, source: i64, kind: &str, target: i64) {
            self.client
                .execute(
                    &format!(
                        "INSERT INTO gl_edge VALUES ({source}, 'Definition', '{kind}', {target}, 'Definition', '')"
                    ),
                    &[],
                )
                .unwrap();
        }

        fn search(self) -> DuckDbSearch {
            self.scoped_search(&[])
        }

        fn scoped_search(self, paths: &[&str]) -> DuckDbSearch {
            self.search_with_sources(paths, &[])
        }

        fn search_with_sources(self, paths: &[&str], sources: &[(i64, &str)]) -> DuckDbSearch {
            self.client.load_extension("fts").unwrap();
            self.client
                .execute(
                    &duckdb_client::search::def_doc_sql(
                        "gl_def_doc_7",
                        &ontology::Ontology::load_embedded().unwrap(),
                    )
                    .unwrap(),
                    &[serde_json::json!(7), serde_json::json!("sha")],
                )
                .unwrap();
            for (id, source) in sources {
                self.client
                    .execute(
                        "UPDATE gl_def_doc_7 SET source = ?1 WHERE def_id = ?2",
                        &[serde_json::json!(source), serde_json::json!(id)],
                    )
                    .unwrap();
            }
            self.client
                .execute(
                    &duckdb_client::search::create_fts_index_sql("gl_def_doc_7"),
                    &[],
                )
                .unwrap();
            let paths: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            DuckDbSearch::scoped(self.client, 7, "sha", &paths).unwrap()
        }
    }

    fn kinds(kinds: &[&str]) -> RecallFilter {
        RecallFilter {
            kinds: kinds.iter().map(|k| k.to_string()).collect(),
        }
    }

    #[test]
    fn source_population_skips_unreadable_files() {
        let root = tempfile::tempdir().unwrap();
        let content = "fn example() {}";
        std::fs::write(root.path().join("a.rs"), content).unwrap();
        std::fs::write(root.path().join("b.rs"), [0xff]).unwrap();
        std::fs::create_dir(root.path().join("tests")).unwrap();
        std::fs::write(root.path().join("tests/example.rs"), content).unwrap();
        std::fs::write(root.path().join("z.txt"), content).unwrap();
        let g = TestGraph::new("source-read-errors");
        for (i, path) in ["a.rs", "b.rs", "missing.rs", "tests/example.rs", "z.txt"]
            .iter()
            .enumerate()
        {
            g.def(i as i64 + 1, path, "example", path);
        }
        g.client
            .execute(
                "UPDATE gl_definition SET end_byte = ?1",
                &[serde_json::json!(content.len())],
            )
            .unwrap();
        let search = g.search();
        duckdb_client::search::populate_def_doc_sources(
            search.client(),
            "gl_def_doc_7",
            &ontology::Ontology::load_embedded().unwrap(),
            root.path(),
            7,
            "sha",
        )
        .unwrap();
        let rows = search
            .client()
            .query_arrow("SELECT source FROM gl_def_doc_7 ORDER BY def_id")
            .unwrap();
        assert_eq!(
            duckdb_client::string_column(&rows, "source"),
            vec![content, "", "", content, ""]
        );
    }

    #[test]
    fn grep_runs_end_to_end_against_a_real_local_graph() {
        let g = TestGraph::new("grep-e2e");
        g.def(1, "Dlq::publish", "publish", "app/services/dlq.rb");
        g.def(2, "Dlq::encode", "encode", "app/services/dlq.rb");
        g.def(
            3,
            "Setup::initialize_defaults",
            "initialize_defaults",
            "app/c.rb",
        );
        g.edge(1, "CALLS", 2);
        g.edge(2, "CALLS", 3);

        let search = g.search();
        let (_, nodes) = search
            .grep("dlq publish", 5, &RecallFilter::default())
            .unwrap();
        assert_eq!(nodes[0].entity_type, "Definition");
        assert_eq!(nodes[0].id, 1);
        assert_eq!(nodes[0].properties["fqn"], "Dlq::publish");
        assert_eq!(nodes[0].properties["name"], "publish");
        assert_eq!(nodes[0].properties["commit_sha"], "sha");
    }

    #[test]
    fn path_scope_limits_recall_to_the_given_subtree() {
        let g = TestGraph::new("grep-path-scope");
        g.def(1, "resources.limits", "limits", "e2e/charts/values.yaml");
        g.def(2, "Input::limit", "limit", "crates/compiler/src/input.rs");
        g.def(3, "Cli::limit", "limit", "crates/cli/src/main.rs");

        let search = g.scoped_search(&["crates/compiler"]);
        let (outcome, _) = search.grep("limit", 5, &RecallFilter::default()).unwrap();
        let ids: Vec<i64> = outcome.matches.iter().map(|hit| hit.id).collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn path_scope_accepts_globs_and_multiple_paths() {
        let g = TestGraph::new("grep-path-glob");
        g.def(1, "resources.limits", "limits", "e2e/charts/values.yaml");
        g.def(2, "Input::limit", "limit", "crates/compiler/src/input.rs");
        g.def(3, "Cli::limit", "limit", "crates/cli/src/main.rs");

        let search = g.scoped_search(&["crates/*/src/main.rs", "e2e/"]);
        let (outcome, _) = search.grep("limit", 5, &RecallFilter::default()).unwrap();
        let mut ids: Vec<i64> = outcome.matches.iter().map(|hit| hit.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn kind_scope_matches_definition_type_case_insensitively() {
        let g = TestGraph::new("grep-kind-scope");
        g.typed_def(
            1,
            "Input::limit",
            "limit",
            "crates/compiler/src/input.rs",
            "Field",
        );
        g.typed_def(
            2,
            "MAX_LIMIT",
            "MAX_LIMIT",
            "crates/compiler/src/lib.rs",
            "Constant",
        );
        g.typed_def(3, "Cli::limit", "limit", "crates/cli/src/main.rs", "Method");

        let search = g.search();
        let (outcome, _) = search
            .grep("limit", 5, &kinds(&["constant", "Field"]))
            .unwrap();
        let mut ids: Vec<i64> = outcome.matches.iter().map(|hit| hit.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn bm25_order_is_not_overridden_by_exact_names_or_graph_degree() {
        let g = TestGraph::new("grep-bm25-order");
        g.def(1, "Module::compile", "compile", "src/a.rs");
        g.def(2, "Module::compiled", "compiled", "src/a.rs");
        for caller in 10..60 {
            g.def(caller, &format!("Caller::c{caller}"), "c", "src/c.rs");
            g.edge(caller, "CALLS", 1);
        }
        let long_source = "unrelated ".repeat(400);
        let search =
            g.search_with_sources(&[], &[(1, &long_source), (2, "compile compile compile")]);
        let raw = search.client().query_arrow_json(
            "SELECT def_id AS id, fts_main_gl_def_doc_7.match_bm25(def_id, ?1, fields := 'name,context,source', conjunctive := true) AS score
             FROM gl_def_doc_7 WHERE score IS NOT NULL ORDER BY score DESC, id",
            &[serde_json::json!("compile")]).unwrap();
        let expected = duckdb_client::i64_column(&raw, "id");
        assert_eq!(expected, [2, 1]);
        search.client().execute("DROP TABLE gl_edge", &[]).unwrap();
        let (outcome, nodes) = search.grep("compile", 2, &RecallFilter::default()).unwrap();
        assert_eq!(
            outcome.matches.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            outcome
                .matches
                .iter()
                .map(|hit| hit.score)
                .collect::<Vec<_>>(),
            duckdb_client::f64_column(&raw, "score")
        );
        assert!(!outcome.matches[0].exact_name);
        assert!(outcome.matches[1].exact_name);
        assert_eq!(nodes.len(), 2);
        let (limited, nodes) = search.grep("compile", 1, &RecallFilter::default()).unwrap();
        assert_eq!(limited.total, 2);
        assert_eq!(limited.exact_alternatives, ["compile"]);
        assert_eq!(nodes.len(), 1);
        assert!(!limited.matches[0].exact_name);
    }

    #[test]
    fn full_query_conjunction_rejects_partial_body_hits_and_missing_vocabulary() {
        let g = TestGraph::new("grep-conjunction");
        g.def(1, "with_metaclass", "with_metaclass", "src/compat.py");
        g.def(
            2,
            "prepare_multipart",
            "prepare_multipart",
            "src/request.py",
        );
        g.def(3, "copy", "copy", "src/models.py");
        let search = g.search_with_sources(
            &[],
            &[
                (1, "def with_metaclass(): prepare()"),
                (3, "# clone this object"),
            ],
        );
        let (outcome, _) = search
            .grep("prepare_multipart", 5, &RecallFilter::default())
            .unwrap();
        assert_eq!(
            outcome.matches.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            [2]
        );
        assert!(outcome.matches[0].name_match);
        let (outcome, _) = search.grep("clone", 5, &RecallFilter::default()).unwrap();
        assert_eq!(outcome.matches.len(), 1);
        assert_eq!(outcome.matches[0].id, 3);
        assert!(!outcome.matches[0].name_match);
        assert!(!outcome.matches[0].exact_name);
        for query in [
            "prepare_nonexistenttoken",
            "unfindable",
            "' OR true --",
            "clone'; DROP TABLE gl_definition; --",
        ] {
            let (outcome, nodes) = search.grep(query, 5, &RecallFilter::default()).unwrap();
            assert_eq!(outcome.total, 0, "{query}");
            assert!(outcome.matches.is_empty());
            assert!(nodes.is_empty());
        }
        assert!(search.grep("!!!", 5, &RecallFilter::default()).is_err());
        assert!(search.grep("clone", 0, &RecallFilter::default()).is_err());
        assert_eq!(
            search.list_corpus(&RecallFilter::default()).unwrap().len(),
            3
        );
    }

    #[test]
    fn exact_metadata_uses_raw_names_and_respects_scope_before_limits() {
        let g = TestGraph::new("grep-exact-scopes");
        g.def(1, "markInSync", "markInSync", "src/a.rs");
        g.def(2, "mark_in_sync", "mark_in_sync", "src/a.rs");
        g.typed_def(3, "Other::markInSync", "markInSync", "src/a.rs", "Class");
        g.def(4, "outside", "outside", "other/a.rs");
        let search = g.scoped_search(&["src"]);
        let (outcome, _) = search
            .grep(
                "markInSync|MARKINSYNC|mark_in_sync|outside",
                1,
                &kinds(&["method"]),
            )
            .unwrap();
        assert_eq!(
            outcome.alternatives,
            ["markInSync", "mark_in_sync", "outside"]
        );
        assert_eq!(outcome.exact_alternatives, ["markInSync", "mark_in_sync"]);
        assert_eq!(outcome.total, 2);
        assert_eq!(outcome.matches.len(), 1);
        let (camel, _) = search.grep("markInSync", 5, &kinds(&["method"])).unwrap();
        assert_eq!(camel.matches.len(), 1);
        assert!(camel.matches[0].exact_name);
        let (words, _) = search.grep("mark in sync", 5, &kinds(&["method"])).unwrap();
        assert!(words.exact_alternatives.is_empty());
        assert!(words.matches.iter().all(|hit| !hit.exact_name));
        let (scoped, _) = search.grep("outside", 5, &RecallFilter::default()).unwrap();
        assert!(scoped.exact_alternatives.is_empty());
        let (scoped, _) = search.grep("mark_in_sync", 5, &kinds(&["class"])).unwrap();
        assert!(scoped.exact_alternatives.is_empty());
    }

    #[test]
    fn or_scores_use_best_alternative_and_ties_use_id() {
        let g = TestGraph::new("grep-or-scores");
        g.def(2, "clone", "clone", "src/a.rs");
        g.def(1, "clone", "clone", "src/a.rs");
        g.def(3, "prepare_clone", "prepare_clone", "src/b.rs");
        let search = g.search();
        let (clones, _) = search.grep("clone", 5, &RecallFilter::default()).unwrap();
        let (prepares, _) = search.grep("prepare", 5, &RecallFilter::default()).unwrap();
        let (combined, _) = search
            .grep("clone|prepare|CLONE", 5, &RecallFilter::default())
            .unwrap();
        assert_eq!(combined.total, 3);
        for hit in &combined.matches {
            let expected = clones
                .matches
                .iter()
                .chain(&prepares.matches)
                .filter(|other| other.id == hit.id)
                .map(|other| other.score)
                .max_by(f64::total_cmp)
                .unwrap();
            assert_eq!(hit.score, expected);
        }
        let tied: Vec<_> = combined
            .matches
            .iter()
            .filter(|hit| hit.id < 3)
            .map(|hit| hit.id)
            .collect();
        assert_eq!(tied, [1, 2]);
    }

    #[test]
    fn hyphenated_whole_name_anchors() {
        let g = TestGraph::new("grep-hyphen");
        g.def(1, "mr-title-check", "mr-title-check", ".gitlab-ci.yml");
        g.def(2, "Mr::title", "title", "app/mr.rb");

        let search = g.search();
        let (_, nodes) = search
            .grep("mr-title-check", 5, &RecallFilter::default())
            .unwrap();
        assert_eq!(nodes[0].properties["fqn"], "mr-title-check");
    }

    #[test]
    fn conjunctive_fts_ignores_empty_boundary_tokens_but_keeps_real_terms() {
        let g = TestGraph::new("grep-boundary-tokens");
        let names = [
            ("__init__", "init"),
            ("_private", "private"),
            ("save!", "save"),
            ("valid?", "valid"),
            ("entry_0", "entry"),
            ("__café__", "cafe"),
            ("_naïve2", "naive"),
            ("_東京_init_", "東京 init"),
            ("__東京__", "東京"),
        ];
        for (i, (name, _)) in names.iter().enumerate() {
            g.def(i as i64 + 1, name, name, "src/definitions.rs");
        }
        g.def(99, "0__", "0__", "src/definitions.rs");
        let search = g.search();
        let mut missing = Vec::new();
        for (i, (name, normalized)) in names.iter().enumerate() {
            let (outcome, _) = search.grep(name, 20, &RecallFilter::default()).unwrap();
            assert_eq!(outcome.exact_alternatives, [*name]);
            if !outcome
                .matches
                .iter()
                .any(|hit| hit.id == i as i64 + 1 && hit.exact_name && hit.name_match)
            {
                missing.push(*name);
                continue;
            }
            let raw = search.client().query_arrow_json(
                "SELECT def_id AS id, fts_main_gl_def_doc_7.match_bm25(def_id, ?1, fields := 'name,context,source', conjunctive := true) AS score
                 FROM gl_def_doc_7 WHERE score IS NOT NULL ORDER BY score DESC, id",
                &[serde_json::json!(normalized)]).unwrap();
            assert_eq!(
                outcome.matches.iter().map(|hit| hit.id).collect::<Vec<_>>(),
                duckdb_client::i64_column(&raw, "id")
            );
            assert_eq!(
                outcome
                    .matches
                    .iter()
                    .map(|hit| hit.score)
                    .collect::<Vec<_>>(),
                duckdb_client::f64_column(&raw, "score")
            );
        }
        assert!(
            missing.is_empty(),
            "boundary identifiers not retrieved: {missing:?}"
        );
        for query in [
            "_entry_nonexistenttoken_0",
            "__init_nonexistenttoken__",
            "_entry_未登録_0",
            "0__",
            "123",
        ] {
            let (outcome, nodes) = search.grep(query, 20, &RecallFilter::default()).unwrap();
            assert_eq!(outcome.total, 0, "{query}");
            assert!(nodes.is_empty(), "{query}");
        }
        let (outcome, nodes) = search
            .grep("__init__|__INIT__|save!", 1, &RecallFilter::default())
            .unwrap();
        assert_eq!(outcome.alternatives, ["__init__", "save!"]);
        assert_eq!(outcome.exact_alternatives, ["__init__", "save!"]);
        assert_eq!(outcome.total, 3);
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn fts_stopword_identifiers_are_findable() {
        let g = TestGraph::new("grep-stopword");
        g.def(1, "Repo::find", "find", "app/finders/repo.rb");
        g.def(2, "Dlq::publish", "publish", "app/services/dlq.rb");

        let search = g.search();
        let (outcome, _) = search.grep("find", 5, &RecallFilter::default()).unwrap();
        assert_eq!(outcome.matches[0].id, 1);
    }
}
