use gitalisk_core::repository::testing::local::LocalGitRepository;
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::analysis::types::GraphData;
use crate::graph::RelationshipType;
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;
use crate::parsing::processor::{FileProcessingResult, FileProcessor, ProcessingResult};

pub fn fixture_root(relative_fixture: &str) -> PathBuf {
    Path::new(env!("FIXTURES_DIR"))
        .join("code")
        .join("typescript")
        .join(relative_fixture)
}

pub fn read_fixture_file(relative_fixture: &str, file_path: &str) -> String {
    fs::read_to_string(fixture_root(relative_fixture).join(file_path))
        .expect("Should read JS fixture file")
}

pub fn process_fixture_file(relative_fixture: &str, file_path: &str) -> Box<FileProcessingResult> {
    let source = read_fixture_file(relative_fixture, file_path);
    let result = FileProcessor::new(file_path.to_string(), &source).process();
    match result {
        ProcessingResult::Success(result) => result,
        ProcessingResult::Skipped(skipped) => {
            panic!(
                "Fixture {file_path} was skipped unexpectedly: {}",
                skipped.reason
            )
        }
        ProcessingResult::Error(error) => {
            panic!(
                "Fixture {file_path} failed unexpectedly: {}",
                error.error_message
            )
        }
    }
}

pub fn collect_discovered_paths(root_dir: &Path) -> Vec<String> {
    fn walk(current: &Path, root: &Path, paths: &mut Vec<String>) {
        let mut entries = fs::read_dir(current)
            .expect("Should read fixture directory")
            .map(|entry| entry.expect("Should read fixture dir entry"))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, paths);
            } else if path.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .expect("Fixture path should be under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                paths.push(relative);
            }
        }
    }

    let mut paths = Vec::new();
    walk(root_dir, root_dir, &mut paths);
    paths
}

pub struct JsFixtureTestSetup {
    pub _local_repo: LocalGitRepository,
    pub graph_data: GraphData,
}

impl JsFixtureTestSetup {
    pub fn get_definition_fqn_by_id(&self, id: u32) -> Option<String> {
        self.graph_data
            .definition_nodes
            .get(id as usize)
            .map(|node| node.fqn.to_string())
    }

    pub fn imported_definition_targets_from(&self, file_path: &str) -> Vec<(String, String)> {
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

    pub fn find_calls_from_method(&self, method_fqn: &str) -> Vec<String> {
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

    pub fn has_definition(&self, fqn: &str) -> bool {
        self.graph_data
            .definition_nodes
            .iter()
            .any(|n| n.fqn.to_string() == fqn)
    }
}

pub async fn setup_js_fixture_pipeline(relative_fixture: &str) -> JsFixtureTestSetup {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = fixture_root(relative_fixture);
    local_repo.copy_dir(&fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with JS fixture examples");

    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "js-fixture-test".to_string(),
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

    JsFixtureTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}
