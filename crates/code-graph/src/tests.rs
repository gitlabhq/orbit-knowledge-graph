use std::path::Path;

use crate::analysis::types::{DefinitionType, GraphData};
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;

use crate::graph::{RelationshipKind, RelationshipType};
use gitalisk_core::repository::testing::local::LocalGitRepository;
use parser_core::SupportedLanguage;
use tracing_test::traced_test;

fn init_local_git_repository(language: SupportedLanguage) -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    if language == SupportedLanguage::Ruby {
        let fixtures_path = Path::new(concat!(env!("FIXTURES_DIR"), "/code/test-repo"));
        local_repo.copy_dir(fixtures_path);
    } else if language == SupportedLanguage::TypeScript {
        let fixtures_path = Path::new(concat!(env!("FIXTURES_DIR"), "/code/typescript/test-repo"));
        local_repo.copy_dir(fixtures_path);
    }
    local_repo.add_all().commit("Initial commit");
    local_repo
}

/// Test setup structure for indexing tests
struct IndexingTestSetup {
    _local_repo: LocalGitRepository,
    graph_data: GraphData,
}

impl IndexingTestSetup {
    /// Find all callers of a method by FQN
    #[allow(dead_code)]
    fn find_calls_to_method(&self, method_fqn: &str) -> Vec<String> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                let target_fqn = self.get_definition_fqn_by_id(rel.target_id?)?;
                if target_fqn == method_fqn {
                    self.get_definition_fqn_by_id(rel.source_id?)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find all methods called from a method by FQN
    #[allow(dead_code)]
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

    /// Get definition FQN by node ID
    fn get_definition_fqn_by_id(&self, id: u32) -> Option<String> {
        self.graph_data
            .definition_nodes
            .get(id as usize)
            .map(|node| node.fqn.to_string())
    }

    /// Count relationships of a specific type
    fn count_relationships_of_type(&self, rel_type: RelationshipType) -> usize {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == rel_type)
            .count()
    }

    /// Get call relationship with location info
    fn get_call_with_location(
        &self,
        source_fqn: &str,
        target_fqn: &str,
    ) -> Option<(i32, i32, i32, i32)> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .find_map(|rel| {
                let src_fqn = self.get_definition_fqn_by_id(rel.source_id?)?;
                let tgt_fqn = self.get_definition_fqn_by_id(rel.target_id?)?;
                if src_fqn == source_fqn && tgt_fqn == target_fqn {
                    Some((
                        rel.source_range.start.line as i32,
                        rel.source_range.end.line as i32,
                        rel.source_range.start.column as i32,
                        rel.source_range.end.column as i32,
                    ))
                } else {
                    None
                }
            })
    }
}

async fn setup_indexing_test(language: SupportedLanguage) -> IndexingTestSetup {
    let local_repo = init_local_git_repository(language);
    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "test-repo".to_string(),
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

    IndexingTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[traced_test]
#[tokio::test]
async fn test_new_indexer_with_directory_file_source() {
    let temp_repo = init_local_git_repository(SupportedLanguage::Ruby);
    let repo_path = temp_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "test-repo".to_string(),
        repo_path.to_string(),
        1,
        "main".to_string(),
    );
    let file_source = DirectoryFileSource::new(repo_path.to_string());

    let config = IndexingConfig {
        worker_threads: 1,
        max_file_size: 5_000_000,
        respect_gitignore: false,
    };

    let result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index files");

    // Verify we have graph data
    let graph_data = result.graph_data.expect("Should have graph data");

    assert!(
        !graph_data.file_nodes.is_empty(),
        "Should have processed some files"
    );
    assert!(graph_data.file_nodes.iter().all(|node| node.id.is_some()));
    assert!(
        graph_data
            .definition_nodes
            .iter()
            .all(|node| node.id.is_some())
    );
    assert_eq!(result.errored_files.len(), 0, "Should have no errors");

    println!("✅ New indexer test completed successfully!");
    println!("📊 Processed {} files", graph_data.file_nodes.len());
}

#[traced_test]
#[tokio::test]
async fn test_indexer_with_directory_file_source() {
    let temp_repo = init_local_git_repository(SupportedLanguage::Ruby);
    let repo_path = temp_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::new("test-repo".to_string(), repo_path.to_string());
    let file_source = DirectoryFileSource::new(repo_path.to_string());

    let config = IndexingConfig {
        worker_threads: 1,
        max_file_size: 5_000_000,
        respect_gitignore: false,
    };

    let result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index files");

    // Verify we have graph data
    let graph_data = result.graph_data.expect("Should have graph data");

    assert!(
        !graph_data.file_nodes.is_empty(),
        "Should have processed some files"
    );
    assert_eq!(result.errored_files.len(), 0, "Should have no errors");

    println!("✅ Directory file source test completed successfully!");
    println!("📊 Processed {} files", graph_data.file_nodes.len());
}

#[traced_test]
#[tokio::test]
async fn test_full_indexing_pipeline() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Check we have the expected file nodes
    assert!(
        graph_data.file_nodes.len() >= 6,
        "Should have at least 6 file nodes"
    );

    // Check we have definition nodes
    assert!(
        !graph_data.definition_nodes.is_empty(),
        "Should have definition nodes"
    );

    // Check that we have file-definition relationships
    let file_def_rels = graph_data
        .relationships
        .iter()
        .filter(|r| r.kind == RelationshipKind::FileToDefinition)
        .collect::<Vec<_>>();
    assert!(
        !file_def_rels.is_empty(),
        "Should have file-definition relationships"
    );

    // Check that we have definition relationships (parent-child)
    let def_rels = graph_data
        .relationships
        .iter()
        .filter(|r| r.kind == RelationshipKind::DefinitionToDefinition)
        .collect::<Vec<_>>();
    assert!(!def_rels.is_empty(), "Should have definition relationships");

    println!("✅ Test completed successfully!");
    println!(
        "📊 Created {} definition nodes",
        graph_data.definition_nodes.len()
    );
    println!(
        "📊 Created {} file-definition relationships",
        file_def_rels.len()
    );
    println!("📊 Created {} definition relationships", def_rels.len());
}

#[traced_test]
#[tokio::test]
async fn test_inheritance_relationships() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Find BaseModel and UserModel classes
    let base_model = graph_data
        .definition_nodes
        .iter()
        .find(|def| def.fqn.to_string() == "BaseModel")
        .expect("Should find BaseModel class");

    let user_model = graph_data
        .definition_nodes
        .iter()
        .find(|def| def.fqn.to_string() == "UserModel")
        .expect("Should find UserModel class");

    assert_eq!(
        base_model.definition_type,
        DefinitionType::Ruby(parser_core::ruby::types::RubyDefinitionType::Class)
    );
    assert_eq!(
        user_model.definition_type,
        DefinitionType::Ruby(parser_core::ruby::types::RubyDefinitionType::Class)
    );

    // Verify we have class-to-method relationships
    let class_method_rels: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::ClassToMethod)
        .collect();

    assert!(
        !class_method_rels.is_empty(),
        "Should have CLASS_TO_METHOD relationships"
    );

    // Check for methods in BaseModel
    let base_model_methods: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| {
            rel.relationship_type == RelationshipType::ClassToMethod
                && rel.source_path.as_ref().map(|p| p.as_ref().as_str())
                    == Some("app/models/base_model.rb")
        })
        .collect();

    let mut match_count = 0;
    let base_model_range = base_model.range;
    for rel in &base_model_methods {
        println!("Rel target range: {:?}", rel.target_range);
        if rel.target_range.is_contained_within(base_model_range) {
            match_count += 1;
        }
    }

    assert!(match_count > 0, "BaseModel should have methods");

    println!("✅ Inheritance relationships test completed successfully!");
    println!(
        "📊 Found {} class-to-method relationships",
        class_method_rels.len()
    );
    println!("📊 BaseModel has {} methods", base_model_methods.len());
}

#[traced_test]
#[tokio::test]
async fn test_ruby_definition_counts() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Verify definition count
    let definition_count = graph_data.definition_nodes.len();
    println!("Definition node count: {definition_count}");
    assert_eq!(
        definition_count, 96,
        "Should have 96 definitions (includes modules and improved parsing)"
    );

    // Verify file count
    let file_count = graph_data.file_nodes.len();
    println!("File node count: {file_count}");
    assert_eq!(file_count, 7, "Should have 7 file nodes");

    // Verify directory count
    let dir_count = graph_data.directory_nodes.len();
    println!("Directory node count: {dir_count}");
    assert_eq!(dir_count, 4, "Should have 4 directory nodes");

    // Verify class-to-method relationship count
    let class_method_rel_count = setup.count_relationships_of_type(RelationshipType::ClassToMethod);
    println!("Class -> method relationship count: {class_method_rel_count}");
    assert_eq!(
        class_method_rel_count, 50,
        "Should have 50 class-to-method relationships"
    );

    // Verify file defines relationship count
    let file_defn_rel_count = setup.count_relationships_of_type(RelationshipType::FileDefines);
    println!("File defines relationship count: {file_defn_rel_count}");
    assert_eq!(
        file_defn_rel_count, 96,
        "Should have 96 file-defines relationships"
    );

    // Verify directory contains file relationship count
    let dir_file_rel_count = setup.count_relationships_of_type(RelationshipType::DirContainsFile);
    println!("Directory -> file relationship count: {dir_file_rel_count}");
    assert_eq!(
        dir_file_rel_count, 6,
        "Should have 6 dir-contains-file relationships"
    );

    // Verify directory contains directory relationship count
    let dir_dir_rel_count = setup.count_relationships_of_type(RelationshipType::DirContainsDir);
    println!("Directory -> directory relationship count: {dir_dir_rel_count}");
    assert_eq!(
        dir_dir_rel_count, 2,
        "Should have 2 dir-contains-dir relationships"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_class_to_method_relationships() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Original Cypher query (commented out for reference):
    // MATCH (d:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(c:DefinitionNode)
    // WHERE r.type = 'ClassToMethod' RETURN d, c, r.type

    // Find class-to-method relationships and verify specific ones
    let class_method_rels: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::ClassToMethod)
        .filter_map(|rel| {
            let from_fqn = setup.get_definition_fqn_by_id(rel.source_id?)?;
            let to_fqn = setup.get_definition_fqn_by_id(rel.target_id?)?;
            Some((from_fqn, to_fqn))
        })
        .collect();

    // Verify LdapProvider methods
    let ldap_methods: Vec<_> = class_method_rels
        .iter()
        .filter(|(from, _)| from == "Authentication::Providers::LdapProvider")
        .collect();

    assert!(
        ldap_methods
            .iter()
            .any(|(_, to)| to == "Authentication::Providers::LdapProvider#verify_credentials"),
        "LdapProvider should have verify_credentials method"
    );
    assert!(
        ldap_methods
            .iter()
            .any(|(_, to)| to == "Authentication::Providers::LdapProvider#authenticate"),
        "LdapProvider should have authenticate method"
    );

    // Verify OAuthProvider methods
    let oauth_methods: Vec<_> = class_method_rels
        .iter()
        .filter(|(from, _)| from == "Authentication::Providers::OAuthProvider")
        .collect();

    assert!(
        oauth_methods
            .iter()
            .any(|(_, to)| to == "Authentication::Providers::OAuthProvider#exchange_code_for_token"),
        "OAuthProvider should have exchange_code_for_token method"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_file_defines_relationships() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Original Cypher query (commented out for reference):
    // MATCH (f:FileNode)-[r:FILE_RELATIONSHIPS]->(d:DefinitionNode)
    // WHERE r.type = 'FileDefines' RETURN f, d, r.type

    // Find file-defines relationships
    let file_def_rels: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::FileDefines)
        .collect();

    // Check specific file-definition relationships
    // main.rb should define Application::test_authentication_providers
    let main_definitions: Vec<_> = file_def_rels
        .iter()
        .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some("main.rb"))
        .filter_map(|rel| setup.get_definition_fqn_by_id(rel.target_id?))
        .collect();

    assert!(
        main_definitions
            .iter()
            .any(|fqn| fqn == "Application#test_authentication_providers"),
        "main.rb should define Application#test_authentication_providers"
    );

    // user_model.rb should define UserModel::valid?
    let user_model_definitions: Vec<_> = file_def_rels
        .iter()
        .filter(|rel| {
            rel.source_path.as_ref().map(|p| p.as_ref().as_str())
                == Some("app/models/user_model.rb")
        })
        .filter_map(|rel| setup.get_definition_fqn_by_id(rel.target_id?))
        .collect();

    assert!(
        user_model_definitions
            .iter()
            .any(|fqn| fqn == "UserModel#valid?"),
        "user_model.rb should define UserModel#valid?"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_directory_relationships() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    // Original Cypher query (commented out for reference):
    // MATCH (d:DirectoryNode)-[r:DIRECTORY_RELATIONSHIPS]->(f:FileNode)
    // WHERE r.type = 'DirContainsFile' RETURN d, f, r.type

    // Check directory contains file relationships
    let dir_file_rels: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::DirContainsFile)
        .collect();

    // Verify app/models contains user_model.rb
    let app_models_files: Vec<_> = dir_file_rels
        .iter()
        .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some("app/models"))
        .filter_map(|rel| rel.target_path.as_ref().map(|p| p.as_ref().clone()))
        .collect();

    assert!(
        app_models_files
            .iter()
            .any(|path| path == "app/models/user_model.rb"),
        "app/models should contain user_model.rb"
    );

    // Verify lib/authentication contains providers.rb
    let lib_auth_files: Vec<_> = dir_file_rels
        .iter()
        .filter(|rel| {
            rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some("lib/authentication")
        })
        .filter_map(|rel| rel.target_path.as_ref().map(|p| p.as_ref().clone()))
        .collect();

    assert!(
        lib_auth_files
            .iter()
            .any(|path| path == "lib/authentication/providers.rb"),
        "lib/authentication should contain providers.rb"
    );

    // Original Cypher query (commented out for reference):
    // MATCH (d1:DirectoryNode)-[r:DIRECTORY_RELATIONSHIPS]->(d2:DirectoryNode)
    // WHERE r.type = 'DirContainsDir' RETURN d1, d2, r.type

    // Check directory contains directory relationships
    let dir_dir_rels: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::DirContainsDir)
        .collect();

    // Verify lib contains lib/authentication
    let lib_subdirs: Vec<_> = dir_dir_rels
        .iter()
        .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some("lib"))
        .filter_map(|rel| rel.target_path.as_ref().map(|p| p.as_ref().clone()))
        .collect();

    assert!(
        lib_subdirs.iter().any(|path| path == "lib/authentication"),
        "lib should contain lib/authentication"
    );

    // Verify app contains app/models
    let app_subdirs: Vec<_> = dir_dir_rels
        .iter()
        .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some("app"))
        .filter_map(|rel| rel.target_path.as_ref().map(|p| p.as_ref().clone()))
        .collect();

    assert!(
        app_subdirs.iter().any(|path| path == "app/models"),
        "app should contain app/models"
    );
}

#[traced_test]
#[tokio::test]
async fn test_detailed_data_inspection() {
    let setup = setup_indexing_test(SupportedLanguage::Ruby).await;
    let graph_data = &setup.graph_data;

    println!("\n🔍 === DETAILED DATA INSPECTION ===");

    // Verify specific expected definitions exist
    println!("\n📊 Expected Definitions Verification:");
    let expected_definitions = vec![
        ("Authentication::Providers::LdapProvider", "Class"),
        ("Authentication::Token", "Class"),
        ("UserManagement::User", "Class"),
        ("BaseModel", "Class"),
        ("UserModel", "Class"),
    ];

    for (expected_fqn, expected_type) in expected_definitions {
        if let Some(def) = graph_data
            .definition_nodes
            .iter()
            .find(|d| d.fqn.to_string() == expected_fqn)
        {
            println!("  ✅ Found: {} ({:?})", expected_fqn, def.definition_type);
        } else {
            panic!("  ❌ Missing: {expected_fqn} ({expected_type})");
        }
    }

    println!("✅ All verification checks passed!");
}

#[traced_test]
#[tokio::test]
async fn test_typescript_definition_counts() {
    let setup = setup_indexing_test(SupportedLanguage::TypeScript).await;
    let graph_data = &setup.graph_data;

    // Verify definition count
    let definition_count = graph_data.definition_nodes.len();
    println!("Definition node count: {definition_count}");
    assert_eq!(
        definition_count, 84,
        "Should have 84 definitions (with mandatory FQN)"
    );

    // Verify imported symbol count
    let imported_symbol_count = graph_data.imported_symbol_nodes.len();
    println!("Imported symbol count: {imported_symbol_count}");
    assert_eq!(imported_symbol_count, 9, "Should have 9 imported symbols");

    // Verify main.ts imports
    let main_ts_imports: Vec<_> = graph_data
        .imported_symbol_nodes
        .iter()
        .filter(|node| node.location.file_path == "main.ts")
        .collect();

    assert_eq!(main_ts_imports.len(), 3, "main.ts should have 3 imports");
}

#[traced_test]
#[tokio::test]
async fn test_typescript_call_relationship_has_location() {
    let setup = setup_indexing_test(SupportedLanguage::TypeScript).await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'Application::run' AND target.fqn = 'Application::testAuthenticationProviders' AND r.type = 'Calls'
    // RETURN r.source_start_line, r.source_end_line

    // Validate known call location: Application::run -> Application::testAuthenticationProviders
    let location = setup.get_call_with_location(
        "Application::run",
        "Application::testAuthenticationProviders",
    );

    assert!(
        location.is_some(),
        "Should find call from Application::run to Application::testAuthenticationProviders"
    );

    let (start_line, end_line, _, _) = location.unwrap();
    // Line numbers should be around line 18-21 (0-based indexing)
    assert!(
        (18..=21).contains(&start_line),
        "Call should be around line 18-21, got {start_line}"
    );
    assert_eq!(start_line, end_line, "Call should be on a single line");
}
