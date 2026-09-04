use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb_client::search::DuckDbSearch;
use orbit_search::{GrepOutcome, SearchVocab};

use duckdb_client::scalar_i64;

use crate::sql;
use crate::workspace;

pub(super) struct LocalBackend {
    search: DuckDbSearch,
    header: String,
}

impl LocalBackend {
    pub(super) fn open(repo_path: &Path, db: Option<PathBuf>, paths: &[String]) -> Result<Self> {
        let db = workspace::resolve_db_path(db)?;
        let top_level = workspace::git_toplevel(repo_path)
            .with_context(|| format!("failed to find git top-level for {}", repo_path.display()))?;
        let git = workspace::git_info(&top_level)
            .with_context(|| format!("failed to read git info for {}", top_level.display()))?;

        let mut client = sql::open_graph(Some(db.clone()))?;
        let pid = git.project_id;
        let sha = duckdb_client::sql_lit(&git.commit_sha);

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
                .context("failed to index the repository for grep")?;
            client = sql::open_graph(Some(db))?;
            if indexed_count(&client)? == 0 {
                anyhow::bail!(
                    "indexing finished but commit {} still has no rows in the local graph",
                    git.commit_sha
                );
            }
        }

        Ok(Self {
            search: DuckDbSearch::scoped(client, pid, &git.commit_sha, paths)?,
            header: format!("{} @ {}", git.repo_path.display(), git.commit_sha),
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
        vocab: &SearchVocab,
    ) -> Result<GrepOutcome> {
        self.search.grep(query, limit, vocab)
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
            self.client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', 'Method', 1, 2, 0, 0, 0, 0)"
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
            self.client.load_extension("fts").unwrap();
            self.client
                .execute(
                    "CREATE OR REPLACE TABLE gl_def_doc_7 AS
                     SELECT DISTINCT commit_sha, id AS def_id,
                            fts_doc(def_name(fqn)) AS name,
                            fts_doc(fqn || ' ' || file_path) AS context
                     FROM gl_definition",
                    &[],
                )
                .unwrap();
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

    fn vocab(search: &DuckDbSearch) -> SearchVocab {
        super::super::build_vocab(search).unwrap()
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
        let vocab = vocab(&search);
        let outcome = search.grep("dlq publish", 5, &vocab).unwrap();
        assert!(!outcome.matches.is_empty());
        assert!(!outcome.weak);
        assert!(outcome.unmatched_terms.is_empty());
        assert!(
            !outcome.edges.is_empty(),
            "edges among the matches must be listed"
        );
    }

    #[test]
    fn path_scope_limits_recall_to_the_given_subtree() {
        let g = TestGraph::new("grep-path-scope");
        g.def(1, "resources.limits", "limits", "e2e/charts/values.yaml");
        g.def(2, "Input::limit", "limit", "crates/compiler/src/input.rs");
        g.def(3, "Cli::limit", "limit", "crates/cli/src/main.rs");

        let search = g.scoped_search(&["crates/compiler"]);
        let vocab = vocab(&search);
        let outcome = search.grep("limit", 5, &vocab).unwrap();
        let ids: Vec<i64> = outcome.matches.iter().map(|m| m.row.id).collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn path_scope_accepts_globs_and_multiple_paths() {
        let g = TestGraph::new("grep-path-glob");
        g.def(1, "resources.limits", "limits", "e2e/charts/values.yaml");
        g.def(2, "Input::limit", "limit", "crates/compiler/src/input.rs");
        g.def(3, "Cli::limit", "limit", "crates/cli/src/main.rs");

        let search = g.scoped_search(&["crates/*/src/main.rs", "e2e/"]);
        let vocab = vocab(&search);
        let outcome = search.grep("limit", 5, &vocab).unwrap();
        let mut ids: Vec<i64> = outcome.matches.iter().map(|m| m.row.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn exact_name_hit_outranks_stem_hit_on_a_hub() {
        let g = TestGraph::new("grep-exact-name");
        g.def(
            1,
            "compiler::compile",
            "compile",
            "crates/compiler/src/lib.rs",
        );
        g.def(
            2,
            "code_graph::compiled_labels",
            "compiled_labels",
            "crates/code-graph/src/edge.rs",
        );
        for caller in 10..60 {
            g.def(
                caller,
                &format!("Caller::c{caller}"),
                "c",
                "crates/x/src/c.rs",
            );
            g.edge(caller, "CALLS", 2);
        }

        let search = g.search();
        let vocab = vocab(&search);
        let outcome = search.grep("compile", 5, &vocab).unwrap();
        assert_eq!(outcome.matches[0].row.id, 1);
        assert_eq!(outcome.matches[1].row.id, 2);
        assert!(!outcome.weak);
    }

    #[test]
    fn hyphenated_whole_name_anchors() {
        let g = TestGraph::new("grep-hyphen");
        g.def(1, "mr-title-check", "mr-title-check", ".gitlab-ci.yml");
        g.def(2, "Mr::title", "title", "app/mr.rb");

        let search = g.search();
        let vocab = vocab(&search);
        let outcome = search.grep("mr-title-check", 5, &vocab).unwrap();
        assert_eq!(outcome.matches[0].row.fqn, "mr-title-check");
        assert!(!outcome.weak);
    }

    #[test]
    fn fts_stopword_identifiers_are_findable() {
        let g = TestGraph::new("grep-stopword");
        g.def(1, "Repo::find", "find", "app/finders/repo.rb");
        g.def(2, "Dlq::publish", "publish", "app/services/dlq.rb");

        let search = g.search();
        let vocab = vocab(&search);
        let outcome = search.grep("find", 5, &vocab).unwrap();
        assert!(
            outcome.unmatched_terms.is_empty(),
            "identifiers colliding with English stopwords must recall"
        );
        assert_eq!(outcome.matches[0].row.id, 1);
    }

    #[test]
    fn vocab_maps_question_verbs_through_the_db_stemmer() {
        use orbit_search::content_words;
        use orbit_search::grep::GrepSource;

        let g = TestGraph::new("vocab-stem");
        g.def(1, "Dlq::publish", "publish", "app/services/dlq.rb");
        let search = g.search();
        let vocab = vocab(&search);

        let stem_all = |q: &str| search.stem(&content_words(q)).unwrap();
        assert!(vocab.is_relational(&stem_all("calling")[0]));
        assert!(!vocab.is_relational(&stem_all("hooks")[0]));
        let stems = stem_all("who calls execute_hooks");
        assert_eq!(stems.iter().filter(|s| vocab.is_relational(s)).count(), 1);
    }
}
