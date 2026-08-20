use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb_client::search::DuckDbSearch;
use orbit_search::{AskOutcome, SearchVocab};

use duckdb_client::scalar_i64;

use crate::sql;
use crate::workspace;

pub(super) struct LocalBackend {
    search: DuckDbSearch,
    header: String,
    root: PathBuf,
}

impl LocalBackend {
    pub(super) fn open(repo_path: &Path, db: Option<PathBuf>) -> Result<Self> {
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
                .context("failed to index the repository for ask")?;
            client = sql::open_graph(Some(db))?;
            if indexed_count(&client)? == 0 {
                anyhow::bail!(
                    "indexing finished but commit {} still has no rows in the local graph",
                    git.commit_sha
                );
            }
        }

        Ok(Self {
            search: DuckDbSearch::new(client, pid, &git.commit_sha),
            header: format!("{} @ {}", git.repo_path.display(), git.commit_sha),
            root: git.repo_path,
        })
    }

    pub(super) fn header(&self) -> &str {
        &self.header
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn ask(
        &self,
        question: &str,
        limit: usize,
        vocab: &SearchVocab,
        kind_weights: &std::collections::HashMap<String, f64>,
    ) -> Result<AskOutcome> {
        self.search.ask(question, limit, vocab, kind_weights)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_text_roundtrip_ranks_exact_symbol_above_long_tie() {
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
            let (search_text, token_count) = orbit_search::search_document(fqn, path);
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', 'Method', 1, 2, 0, 0, 0, 0, '{search_text}', {token_count})"
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

        let search = DuckDbSearch::new(client, 7, "sha");
        let (corpus, weights) = search.search(&["execute_hooks".to_string()]).unwrap();
        assert!(weights.is_some());
        let fqns: Vec<&str> = corpus.iter().map(|r| r.fqn.as_str()).collect();
        assert!(fqns.contains(&"Group::execute_hooks"));
        assert!(fqns.contains(&"Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build"));
        assert!(!fqns.contains(&"Project::unrelated"));
    }

    #[test]
    fn ask_surfaces_a_two_hop_call_chain_over_hub_noise() {
        let dir = std::env::temp_dir().join(format!("orbit-ask-test-{}", std::process::id()));
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
            let (search_text, token_count) = orbit_search::search_document(fqn, path);
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', 'Method', 1, 2, 0, 0, 0, 0, '{search_text}', {token_count})"
                    ),
                    &[],
                )
                .unwrap();
        };
        insert(1, "Dlq::publish", "publish", "app/services/dlq.rb");
        insert(2, "Dlq::encode", "encode", "app/services/dlq.rb");
        insert(3, "Dlq::checksum", "checksum", "app/services/dlq.rb");
        insert(4, "Util::log", "log", "app/util.rb");
        for i in 5..40 {
            insert(i, &format!("Other::fn_{i}"), &format!("fn_{i}"), "app/o.rb");
        }
        let edge = |source: i64, kind: &str, target: i64| {
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_edge VALUES ({source}, 'Definition', '{kind}', {target}, 'Definition', '')"
                    ),
                    &[],
                )
                .unwrap();
        };
        edge(1, "CALLS", 2);
        edge(2, "CALLS", 3);
        edge(1, "CALLS", 4);
        for i in 5..40 {
            edge(i, "CALLS", 4);
        }

        let search = DuckDbSearch::new(client, 7, "sha");
        let vocab = SearchVocab::new(["Calls", "Imports", "Extends", "Contains", "Defines"]);
        let weights = std::collections::HashMap::from([("CALLS".to_string(), 1.0)]);
        let outcome = search.ask("dlq publish", 5, &vocab, &weights).unwrap();

        assert!(!outcome.matches.is_empty());
        assert!(!outcome.weak);
        assert!(outcome.unmatched_terms.is_empty());

        let vague = search
            .ask("first repository setup initialization", 5, &vocab, &weights)
            .unwrap();
        assert!(
            vague.matches.is_empty() || vague.weak || !vague.unmatched_terms.is_empty(),
            "a vocabulary-mismatched question must not present as confident"
        );
        let rendered: Vec<String> = outcome
            .edges
            .iter()
            .map(|e| format!("{} {} {}", e.source, e.kind, e.target))
            .collect();
        assert!(
            rendered.contains(&"Dlq::publish CALLS Dlq::encode".to_string()),
            "edges were {rendered:?}"
        );
        assert!(
            rendered.contains(&"Dlq::encode CALLS Dlq::checksum".to_string()),
            "two-hop edge missing: {rendered:?}"
        );
        let hub_first = rendered
            .iter()
            .position(|e| e == "Dlq::publish CALLS Util::log");
        let chain_first = rendered
            .iter()
            .position(|e| e == "Dlq::publish CALLS Dlq::encode")
            .unwrap();
        if let Some(hub_pos) = hub_first {
            assert!(chain_first < hub_pos, "hub edge outranked the chain");
        }
    }

    #[test]
    fn structurally_central_node_surfaces_without_matching_any_term() {
        let dir =
            std::env::temp_dir().join(format!("orbit-objectrank-test-{}", std::process::id()));
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
            let (search_text, token_count) = orbit_search::search_document(fqn, path);
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_definition VALUES ({id}, '', 7, 'main', 'sha', '{path}', '{fqn}', '{name}', 'Method', 1, 2, 0, 0, 0, 0, '{search_text}', {token_count})"
                    ),
                    &[],
                )
                .unwrap();
        };
        insert(1, "Repo::commit_created", "commit_created", "app/a.rb");
        insert(2, "Project::branch_created", "branch_created", "app/b.rb");
        insert(
            3,
            "Setup::initialize_defaults",
            "initialize_defaults",
            "app/c.rb",
        );
        let edge = |source: i64, target: i64| {
            client
                .execute(
                    &format!(
                        "INSERT INTO gl_edge VALUES ({source}, 'Definition', 'CALLS', {target}, 'Definition', '')"
                    ),
                    &[],
                )
                .unwrap();
        };
        edge(1, 3);
        edge(2, 3);

        let search = DuckDbSearch::new(client, 7, "sha");
        let vocab = SearchVocab::new(["Calls", "Imports", "Extends", "Contains", "Defines"]);
        let weights = std::collections::HashMap::from([("CALLS".to_string(), 1.0)]);
        let outcome = search.ask("commit branch", 5, &vocab, &weights).unwrap();

        assert!(
            outcome
                .surfaced
                .iter()
                .any(|m| m.row.fqn == "Setup::initialize_defaults"),
            "the node both weak anchors call must surface; surfaced were {:?}",
            outcome
                .surfaced
                .iter()
                .map(|m| m.row.fqn.as_str())
                .collect::<Vec<_>>()
        );
    }
}
