use gitalisk_core::repository::testing::local::LocalGitRepository;
use std::path::Path;

use crate::analysis::types::GraphData;
use crate::graph::RelationshipType;
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;

fn init_js_fixture_repository(relative_fixture: &str) -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = Path::new(env!("FIXTURES_DIR"))
        .join("code")
        .join("typescript")
        .join(relative_fixture);
    local_repo.copy_dir(&fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with JS reference examples");
    local_repo
}

pub struct JsReferenceTestSetup {
    pub _local_repo: LocalGitRepository,
    pub graph_data: GraphData,
}

impl JsReferenceTestSetup {
    fn get_definition_fqn_by_id(&self, id: u32) -> Option<String> {
        self.graph_data
            .definition_nodes
            .get(id as usize)
            .map(|node| node.fqn.to_string())
    }

    fn imported_definition_targets_from(&self, file_path: &str) -> Vec<(String, String)> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::ImportedSymbolToDefinition)
            .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some(file_path))
            .filter_map(|rel| {
                Some((
                    rel.target_path.as_ref()?.to_string(),
                    self.get_definition_fqn_by_id(rel.target_id?)?,
                ))
            })
            .collect()
    }

    fn find_calls_from_method(&self, method_fqn: &str) -> Vec<String> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                let source_fqn = self.get_definition_fqn_by_id(rel.source_id?)?;
                if source_fqn == method_fqn {
                    self.get_definition_fqn_by_id(rel.target_id?)
                } else {
                    None
                }
            })
            .collect()
    }
}

pub async fn setup_js_reference_pipeline(relative_fixture: &str) -> JsReferenceTestSetup {
    let local_repo = init_js_fixture_repository(relative_fixture);
    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "js-references-test".to_string(),
        repo_path_str.to_string(),
        1,
        "main".to_string(),
    );
    let file_source = DirectoryFileSource::new(repo_path_str.to_string());

    let config = IndexingConfig {
        worker_threads: 1,
        max_file_size: 5_000_000,
        respect_gitignore: false,
    };

    let indexing_result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index repository");
    let graph_data = indexing_result.graph_data.expect("Should have graph data");

    JsReferenceTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[cfg(test)]
mod integration_tests {
    use super::setup_js_reference_pipeline;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test_js_cross_file_import_resolution_uses_fixture_repo() {
        let setup = setup_js_reference_pipeline("references").await;
        let import_targets = setup.imported_definition_targets_from("src/consumer.ts");

        assert!(
            import_targets
                .iter()
                .any(|(path, fqn)| path == "src/direct.ts" && fqn == "normalize"),
            "Named import through a re-export should resolve to the originating definition"
        );
        assert!(
            import_targets
                .iter()
                .any(|(path, fqn)| path == "src/default_formatter.ts" && fqn == "defaultFormat"),
            "Default import should resolve to the exported definition"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_cross_file_calls_use_fixture_repo() {
        let setup = setup_js_reference_pipeline("references").await;
        let calls = setup.find_calls_from_method("run");

        assert!(
            calls.iter().any(|fqn| fqn == "normalize"),
            "run should call normalize across files through a re-export"
        );
        assert!(
            calls.iter().any(|fqn| fqn == "defaultFormat"),
            "run should call a default-imported function across files"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_definition_ids_are_unique_per_file_in_fixture_repo() {
        let setup = setup_js_reference_pipeline("references").await;
        let foo_defs: Vec<_> = setup
            .graph_data
            .definition_nodes
            .iter()
            .filter(|node| node.fqn.to_string() == "foo")
            .collect();

        assert_eq!(
            foo_defs.len(),
            2,
            "Fixture should include duplicate top-level names"
        );
        assert_ne!(foo_defs[0].id, foo_defs[1].id);
    }
}
