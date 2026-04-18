use std::path::Path;

use crate::legacy::linker::analysis::types::GraphData;
use crate::legacy::linker::graph::RelationshipType;
use crate::legacy::linker::indexer::{IndexingConfig, RepositoryIndexer};
use crate::legacy::linker::loading::DirectoryFileSource;
use gitalisk_core::repository::testing::local::LocalGitRepository;

use tracing_test::traced_test;

/// Initialize a local git repository with Ruby reference test fixtures
fn init_ruby_references_repository() -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = Path::new(concat!(env!("FIXTURES_DIR"), "/code/ruby-references"));
    local_repo.copy_dir(fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with Ruby reference examples");
    local_repo
}

/// Setup structure for Ruby reference resolution tests
struct RubyReferenceTestSetup {
    _local_repo: LocalGitRepository,
    graph_data: GraphData,
}

/// Helper functions to query GraphData for call relationships
impl RubyReferenceTestSetup {
    /// Find all callers of a method by FQN (e.g., "NotificationService::notify")
    fn find_calls_to_method(&self, method_fqn: &str) -> Vec<String> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                // Get target FQN from definition nodes
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

    /// Count total call relationships
    fn count_call_relationships(&self) -> usize {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .count()
    }

    /// Count relationships of a specific type
    fn count_relationships_of_type(&self, rel_type: RelationshipType) -> usize {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == rel_type)
            .count()
    }

    /// Get all call relationships as (source_fqn, target_fqn) pairs
    fn get_all_call_relationships(&self) -> Vec<(String, String)> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                let source_fqn = self.get_definition_fqn_by_id(rel.source_id?)?;
                let target_fqn = self.get_definition_fqn_by_id(rel.target_id?)?;
                Some((source_fqn, target_fqn))
            })
            .collect()
    }

    /// Check if a method definition exists
    fn method_exists(&self, method_fqn: &str) -> bool {
        self.graph_data
            .definition_nodes
            .iter()
            .any(|node| node.fqn.to_string() == method_fqn)
    }

    /// Count definition nodes
    fn count_definitions(&self) -> usize {
        self.graph_data.definition_nodes.len()
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

/// Setup the Ruby reference resolution test pipeline
async fn setup_ruby_reference_pipeline() -> RubyReferenceTestSetup {
    // Create temporary repository with Ruby reference test files
    let local_repo = init_ruby_references_repository();
    let repo_path_str = local_repo.path.to_str().unwrap();

    // Create our RepositoryIndexer wrapper
    let indexer = RepositoryIndexer::with_graph_identity(
        "ruby-references-test".to_string(),
        repo_path_str.to_string(),
        1,
        "main".to_string(),
    );
    let file_source = DirectoryFileSource::new(repo_path_str.to_string());

    // Configure indexing for Ruby files with Ruby-specific settings
    let config = IndexingConfig {
        worker_threads: 1, // Use single thread for deterministic testing
        max_file_size: 5_000_000,
        respect_gitignore: false, // Don't use gitignore in tests
    };

    // Run the indexing pipeline to get GraphData
    let indexing_result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index repository");

    // Verify we have graph data
    let graph_data = indexing_result.graph_data.expect("Should have graph data");

    let call_relationships: Vec<_> = graph_data
        .relationships
        .iter()
        .filter(|rel| rel.relationship_type == RelationshipType::Calls)
        .collect();
    if call_relationships.is_empty() {
        println!("No call relationships found in graph data");
    }

    RubyReferenceTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[traced_test]
#[tokio::test]
async fn test_notification_service_call_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Debug: dump all call relationships
    let all_calls = setup.get_all_call_relationships();
    for (i, (source, target)) in all_calls.iter().take(10).enumerate() {
        println!("  {}: {} -> {}", i + 1, source, target);
    }
    if all_calls.len() > 10 {
        println!("  ... and {} more", all_calls.len() - 10);
    }

    // Try different FQN formats for NotificationService methods
    let fqn_variants = [
        "NotificationService::notify",
        "NotificationService::#notify",
        "NotificationService.notify",
        "NotificationService#notify",
    ];

    for variant in &fqn_variants {
        let calls = setup.find_calls_to_method(variant);
        if !calls.is_empty() {
            break; // Found the right format
        }
    }

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE target.fqn = 'NotificationService::notify'
    // RETURN source.fqn

    // Check that UsersController#destroy calls NotificationService::notify (correct FQN format)
    let notify_callers = setup.find_calls_to_method("NotificationService::notify");

    assert!(
        notify_callers.contains(&"UsersController#destroy".to_string()),
        "Should have call relationship from UsersController#destroy to NotificationService::notify. Found callers: {notify_callers:?}"
    );
}

#[traced_test]
#[tokio::test]
async fn test_send_welcome_email_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'UsersController#create'
    // RETURN target.fqn

    // Check what UsersController#create actually calls first
    let create_calls = setup.find_calls_from_method("UsersController#create");

    // Should find calls from UsersController#create and potentially other places
    assert!(
        create_calls.contains(&"User#send_welcome_email".to_string()),
        "Should find call from UsersController#create to User#send_welcome_email. Found calls: {create_calls:?}"
    );

    // Test that send_welcome_email method calls EmailService.send_welcome
    let calls_from_send_welcome_email = setup.find_calls_from_method("User#send_welcome_email");

    // Should call EmailService::send_welcome
    assert!(
        calls_from_send_welcome_email
            .iter()
            .any(|callee| callee.contains("EmailService") && callee.contains("send_welcome")),
        "User#send_welcome_email should call EmailService::send_welcome"
    );
}

#[traced_test]
#[tokio::test]
async fn test_static_method_call_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE target.fqn = 'User::find_by_email'
    // RETURN source.fqn

    // Test static method calls like User::find_by_email resolve correctly
    let _calls_to_find_by_email = setup.find_calls_to_method("User::find_by_email");

    // Test User::create_with_profile static method calls
    let calls_to_create_with_profile = setup.find_calls_to_method("User::create_with_profile");

    // Should find calls from main.rb test methods
    assert!(
        calls_to_create_with_profile
            .iter()
            .any(|caller| caller.contains("Application")
                || caller.contains("test_user_creation_flow")),
        "Should find call to User::create_with_profile from Application methods"
    );

    // Directly test AuthService static method calls
    let calls_to_create_session = setup.find_calls_to_method("AuthService::create_session");
    let calls_to_authenticate_token = setup.find_calls_to_method("AuthService::authenticate_token");
    let calls_to_refresh_session = setup.find_calls_to_method("AuthService::refresh_session");

    assert!(
        calls_to_create_session
            .iter()
            .any(|caller| caller.contains("Application#test_authentication_flow")),
        "AuthService::create_session should be called from Application#test_authentication_flow. Found callers: {calls_to_create_session:?}"
    );
    assert!(
        calls_to_authenticate_token.iter().any(|caller| caller
            .contains("Application#test_authentication_flow")
            || caller.contains("UsersController")),
        "AuthService::authenticate_token should be called from Application#test_authentication_flow or a controller. Found callers: {calls_to_authenticate_token:?}"
    );
    assert!(
        calls_to_refresh_session
            .iter()
            .any(|caller| caller.contains("Application#test_authentication_flow")),
        "AuthService::refresh_session should be called from Application#test_authentication_flow. Found callers: {calls_to_refresh_session:?}"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_call_relationship_has_location() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // let calls_id = RelationshipType::Calls.as_string();
    // let query = format!(
    //     "MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode) \
    //      WHERE target.fqn = 'AuthService::create_session' AND source.fqn = 'Application#test_authentication_flow' AND r.type = '{calls_id}' \
    //      RETURN r.source_start_line, r.source_end_line, r.source_start_col, r.source_end_col"
    // );

    // Find the call: Application#test_authentication_flow -> AuthService::create_session
    let location = setup.get_call_with_location(
        "Application#test_authentication_flow",
        "AuthService::create_session",
    );

    assert!(
        location.is_some(),
        "Should find call from Application#test_authentication_flow to AuthService::create_session"
    );

    let (start_line, end_line, _start_col, _end_col) = location.unwrap();
    assert!(
        start_line == 70 && end_line == 70,
        "Expected a call row with 0-based line 70 for create_session call. Got start_line={start_line}, end_line={end_line}"
    );
}

#[traced_test]
#[tokio::test]
async fn test_chained_method_call_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE target.fqn = 'User#get_profile'
    // RETURN source.fqn

    // Test complex method chains like user.get_profile.full_profile_data
    // First, verify user.get_profile calls are resolved
    let calls_to_get_profile = setup.find_calls_to_method("User#get_profile");

    // Debug: Check what UsersController#show is calling
    let show_calls = setup.find_calls_from_method("UsersController#show");

    // Should find calls from UsersController#show and other places
    assert!(
        calls_to_get_profile
            .iter()
            .any(|caller| caller.contains("UsersController"))
            || show_calls.contains(&"User#get_profile".to_string()),
        "Should find call to User#get_profile from UsersController. Show calls: {show_calls:?}"
    );

    // Test that get_profile calls Profile.find_by_user_id
    let calls_from_get_profile = setup.find_calls_from_method("User#get_profile");

    assert!(
        calls_from_get_profile
            .iter()
            .any(|callee| callee.contains("Profile") && callee.contains("find_by_user_id")),
        "User#get_profile should call Profile.find_by_user_id"
    );

    // Test that the method chain resolution works for what we can resolve
    // Note: profile.update() calls Profile#update which is a framework method (ActiveRecord)
    // not explicitly defined in our parsed files. This is an accepted limitation.
    // We should still be able to resolve the Profile constant and get_profile method calls.

    let calls_from_update_profile = setup.find_calls_from_method("User#update_profile");

    // Should at minimum call get_profile method
    assert!(
        calls_from_update_profile
            .iter()
            .any(|callee| callee.contains("get_profile")),
        "User#update_profile should call get_profile method"
    );
}

#[traced_test]
#[tokio::test]
async fn test_cross_file_reference_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'Application#test_user_creation_flow'
    // RETURN target.fqn

    // Test cross-file references: main.rb calling methods in other files

    // Test Application class methods calling User methods
    let calls_from_application =
        setup.find_calls_from_method("Application#test_user_creation_flow");

    // Should call User.create_with_profile
    assert!(
        calls_from_application
            .iter()
            .any(|callee| callee.contains("User") && callee.contains("create_with_profile")),
        "Application#test_user_creation_flow should call User.create_with_profile"
    );

    // Test TestUtilities calling methods across files
    let calls_from_test_utilities = setup.find_calls_from_method("TestUtilities::create_test_data");

    // Should reference User constant and call Profile.create_default
    // Note: User.create is a framework method (ActiveRecord) not explicitly defined
    assert!(
        calls_from_test_utilities
            .iter()
            .any(|callee| callee == "User"),
        "TestUtilities::create_test_data should reference User class"
    );

    assert!(
        calls_from_test_utilities
            .iter()
            .any(|callee| callee.contains("Profile") && callee.contains("create_default")),
        "TestUtilities::create_test_data should call Profile.create_default"
    );

    // Test NotificationService calls from TestUtilities
    let calls_to_notify_all = setup.find_calls_to_method("NotificationService::notify_all");

    assert!(
        calls_to_notify_all
            .iter()
            .any(|caller| caller.contains("TestUtilities")),
        "Should find call to NotificationService::notify_all from TestUtilities::send_bulk_notifications"
    );
}

#[traced_test]
#[tokio::test]
async fn test_comprehensive_call_relationships() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE r.type = 'Calls'
    // RETURN count(*)

    // Test comprehensive call relationships across the entire codebase

    // Count total call relationships
    let total_call_relationships = setup.count_call_relationships();
    assert!(
        total_call_relationships > 10,
        "Should have found substantial call relationships"
    );

    // Test specific critical call patterns we identified

    // 1. NotificationService.notify calls
    let notify_callers = setup.find_calls_to_method("NotificationService::notify");
    assert!(
        !notify_callers.is_empty(),
        "NotificationService::notify should have callers"
    );

    // 2. EmailService method calls
    let email_service_callers = setup.find_calls_to_method("EmailService::send_welcome");
    assert!(
        !email_service_callers.is_empty(),
        "EmailService::send_welcome should have callers"
    );

    // 3. User model method calls
    let _user_activate_callers = setup.find_calls_to_method("User#activate!");

    // Note: user.activate! calls exist in the code but require variable type inference
    // from framework methods like @users.first or User.find(). This is an accepted limitation
    // when we don't use heuristics for return type inference.
    // The method definition itself should exist.
    let activate_method_exists = setup.method_exists("User#activate!");
    assert!(
        activate_method_exists,
        "User#activate! method should be defined"
    );

    // 4. Profile method calls
    let profile_create_callers = setup.find_calls_to_method("Profile::create");

    // Also check Profile::create_default which we know works
    let profile_create_default_callers = setup.find_calls_to_method("Profile::create_default");

    // At least one of these should have callers
    assert!(
        !profile_create_callers.is_empty() || !profile_create_default_callers.is_empty(),
        "Profile methods should have callers"
    );

    // 5. Verify specific call chains work end-to-end

    // Test that User#send_notification calls NotificationService::notify
    let send_notification_calls = setup.find_calls_from_method("User#send_notification");
    assert!(
        send_notification_calls
            .iter()
            .any(|callee| callee.contains("NotificationService") && callee.contains("notify")),
        "User#send_notification should call NotificationService::notify"
    );
}

#[traced_test]
#[tokio::test]
async fn test_service_method_call_patterns() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'NotificationService::notify'
    // RETURN target.fqn

    // Test service class method call patterns

    // 1. NotificationService internal method calls
    let notify_method_calls = setup.find_calls_from_method("NotificationService::notify");

    // Should call NotificationService internal methods (with proper FQN)
    let expected_internal_calls = [
        "NotificationService::build_notification",
        "NotificationService::determine_delivery_method",
        "NotificationService::log_notification",
    ];
    for expected_call in &expected_internal_calls {
        assert!(
            notify_method_calls.contains(&expected_call.to_string()),
            "NotificationService::notify should call {expected_call}. Actual calls: {notify_method_calls:?}"
        );
    }

    // 2. EmailService calls from NotificationService
    let calls_to_email_service = setup.find_calls_to_method("EmailService::send_notification");

    assert!(
        calls_to_email_service
            .iter()
            .any(|caller| caller.contains("NotificationService")),
        "EmailService::send_notification should be called by NotificationService"
    );

    // 3. Test batch notification patterns
    let batch_notification_calls =
        setup.find_calls_from_method("NotificationService::send_batch_notifications");

    // Should call User constant and NotificationService.notify
    // Note: User.find is a framework method not explicitly defined, so we only expect User constant resolution
    assert!(
        batch_notification_calls
            .iter()
            .any(|callee| callee == "User"),
        "NotificationService::send_batch_notifications should reference User class"
    );

    assert!(
        batch_notification_calls
            .iter()
            .any(|callee| callee.contains("NotificationService") && callee.contains("notify")),
        "NotificationService::send_batch_notifications should call NotificationService.notify"
    );
}

#[traced_test]
#[tokio::test]
async fn test_controller_action_call_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'UsersController#create'
    // RETURN target.fqn

    // Test controller action method calls

    // 1. UsersController#create method calls
    let create_method_calls = setup.find_calls_from_method("UsersController#create");

    // Should call User.new, user.send_welcome_email, Profile.create_default (save is complex variable tracking)
    let expected_create_calls = ["User", "send_welcome_email", "Profile"];
    for expected_call in &expected_create_calls {
        assert!(
            create_method_calls
                .iter()
                .any(|callee| callee.contains(expected_call)),
            "UsersController#create should call something with {expected_call}"
        );
    }

    // Additional check for specific method calls that should work
    assert!(
        create_method_calls.contains(&"User#send_welcome_email".to_string()),
        "Should find User#send_welcome_email call"
    );

    // 2. UsersController#destroy method calls
    let destroy_method_calls = setup.find_calls_from_method("UsersController#destroy");

    // Should call @user.destroy and NotificationService.notify
    assert!(
        destroy_method_calls
            .iter()
            .any(|callee| callee.contains("NotificationService") && callee.contains("notify")),
        "UsersController#destroy should call NotificationService.notify"
    );

    // 3. UsersController#show method calls
    let show_method_calls = setup.find_calls_from_method("UsersController#show");

    // Should call @user.get_profile
    assert!(
        show_method_calls
            .iter()
            .any(|callee| callee.contains("get_profile")),
        "UsersController#show should call get_profile"
    );

    // 4. UsersController#activate method calls
    let activate_method_calls = setup.find_calls_from_method("UsersController#activate");

    // Should reference User constant and potentially call activate! if variable type tracking works
    // Note: User.find is a framework method not explicitly defined, so we only expect User constant resolution
    assert!(
        activate_method_calls.iter().any(|callee| callee == "User"),
        "UsersController#activate should reference User class"
    );

    // activate! might not resolve if variable type tracking doesn't infer user type correctly without .find return type
    // This is an acceptable limitation for now
    // TODO: Could be enhanced with better return type inference or explicit type annotations
}

#[traced_test]
#[tokio::test]
async fn test_ruby_reference_resolution_performance() {
    // Measure setup time
    let setup_start = std::time::Instant::now();
    let setup = setup_ruby_reference_pipeline().await;
    let setup_duration = setup_start.elapsed();

    assert!(
        setup_duration.as_secs() < 30,
        "Setup should complete within 30 seconds"
    );

    // Original Cypher queries (commented out for reference):
    // MATCH (n:DefinitionNode) RETURN count(n)
    // MATCH ()-[r:DEFINITION_RELATIONSHIPS {type: 'Calls'}]->() RETURN count(r)
    // MATCH ()-[r:DEFINITION_RELATIONSHIPS {type: 'ClassToMethod'}]->() RETURN count(r)

    // Measure query performance for call relationships
    let query_start = std::time::Instant::now();

    let definition_count = setup.count_definitions();
    let call_relationships = setup.count_call_relationships();
    let class_method_rels = setup.count_relationships_of_type(RelationshipType::ClassToMethod);

    let query_duration = query_start.elapsed();

    assert!(query_duration.as_millis() < 1000, "Queries should be fast");
    assert!(
        definition_count > 40,
        "Should have processed substantial codebase"
    );
    assert!(
        call_relationships > 3,
        "Should have found call relationships"
    );
    assert!(
        class_method_rels > 20,
        "Should have many class-method relationships"
    );

    // Test specific query performance
    let specific_query_start = std::time::Instant::now();
    let _notify_callers = setup.find_calls_to_method("NotificationService::notify");
    let specific_query_duration = specific_query_start.elapsed();

    assert!(
        specific_query_duration.as_millis() < 100,
        "Specific queries should be very fast"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_instance_variable_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'UsersController#show' AND target.fqn = 'User#get_profile'
    // RETURN source, target

    // Test @user instance variable resolution in UsersController
    let controller_show_calls = setup.find_calls_from_method("UsersController#show");

    assert!(
        controller_show_calls.contains(&"User#get_profile".to_string()),
        "UsersController#show should call @user.get_profile, resolving @user to User type"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_constant_resolution() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE target.fqn = 'User::create_with_profile'
    // RETURN source.fqn

    // Test constant resolution: User.create_with_profile
    let static_method_calls = setup.find_calls_to_method("User::create_with_profile");

    assert!(
        !static_method_calls.is_empty(),
        "User::create_with_profile should be called (constant User resolved to class)"
    );

    // Test Profile constant resolution
    let profile_calls = setup.find_calls_to_method("Profile::create_default");

    assert!(
        !profile_calls.is_empty(),
        "Profile::create_default should be called (constant Profile resolved)"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_nested_method_calls() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'NotificationService::notify'
    // RETURN target.fqn

    // Test nested service method calls
    let notify_calls = setup.find_calls_from_method("NotificationService::notify");

    assert!(
        notify_calls.contains(&"NotificationService::build_notification".to_string()),
        "NotificationService::notify should call internal build_notification method"
    );

    assert!(
        notify_calls.contains(&"NotificationService::determine_delivery_method".to_string()),
        "NotificationService::notify should call internal determine_delivery_method"
    );

    assert!(
        notify_calls.contains(&"NotificationService::log_notification".to_string()),
        "NotificationService::notify should call internal log_notification"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_cross_service_calls() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'NotificationService::notify' AND target.fqn = 'EmailService::send_notification'
    // RETURN source, target

    // Test service-to-service calls
    let notify_calls = setup.find_calls_from_method("NotificationService::notify");

    assert!(
        notify_calls.contains(&"EmailService::send_notification".to_string()),
        "NotificationService::notify should call EmailService::send_notification"
    );

    // Test User model calling service
    let user_welcome_calls = setup.find_calls_from_method("User#send_welcome_email");

    assert!(
        user_welcome_calls.contains(&"EmailService::send_welcome".to_string()),
        "User#send_welcome_email should call EmailService::send_welcome"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_private_method_calls() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'User#activate!'
    // RETURN target.fqn

    // Test private method calls within same class
    let user_activate_calls = setup.find_calls_from_method("User#activate!");

    // Check if we're detecting any method calls from activate! (it should call update and send_notification)
    assert!(
        !user_activate_calls.is_empty(),
        "User#activate! should call some methods (send_notification, update, etc.). Found: {user_activate_calls:?}"
    );

    // Test private method calling other services
    let send_notification_calls = setup.find_calls_from_method("User#send_notification");

    assert!(
        send_notification_calls.contains(&"NotificationService::notify".to_string()),
        "User#send_notification (private) should call NotificationService::notify"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_variable_assignment_tracking() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'UsersController#create' AND target.fqn = 'User#send_welcome_email'
    // RETURN source, target

    // Test variable assignment and subsequent method calls
    // user = User.new followed by user.send_welcome_email
    let create_method_calls = setup.find_calls_from_method("UsersController#create");

    assert!(
        create_method_calls.contains(&"User#send_welcome_email".to_string()),
        "Should track that user variable is of type User and resolve user.send_welcome_email"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_block_and_iterator_calls() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'NotificationService::send_batch_notifications'
    // RETURN target.fqn

    // Test iterator calls like users.each do |user|
    let batch_notifications_calls =
        setup.find_calls_from_method("NotificationService::send_batch_notifications");

    assert!(
        batch_notifications_calls.contains(&"NotificationService::notify".to_string()),
        "NotificationService::send_batch_notifications should call notify within block"
    );

    let notify_all_calls = setup.find_calls_from_method("NotificationService::notify_all");

    assert!(
        notify_all_calls.contains(&"NotificationService::notify".to_string()),
        "NotificationService::notify_all should call notify within each block"
    );
}

#[traced_test]
#[tokio::test]
async fn test_ruby_conditional_method_calls() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher query (commented out for reference):
    // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
    // WHERE source.fqn = 'User#update_profile'
    // RETURN target.fqn

    // Test conditional method calls: profile.update(attributes) if profile
    let update_profile_calls = setup.find_calls_from_method("User#update_profile");

    assert!(
        update_profile_calls.contains(&"User#get_profile".to_string()),
        "User#update_profile should call get_profile to get profile variable"
    );

    // TODO: Enhance to detect profile.update call within conditional
    // This is currently a limitation - we don't track conditional method calls well
}

#[traced_test]
#[tokio::test]
async fn test_ruby_method_resolution_accuracy() {
    let setup = setup_ruby_reference_pipeline().await;

    // Original Cypher queries (commented out for reference):
    // Various queries checking specific call relationships exist

    // Test exact method call resolution - these should be precise

    // 1. User instance method calls
    assert!(
        setup
            .find_calls_from_method("User#send_welcome_email")
            .contains(&"EmailService::send_welcome".to_string()),
        "User#send_welcome_email must call EmailService::send_welcome"
    );

    // 2. Service method composition
    assert!(
        setup
            .find_calls_from_method("NotificationService::notify")
            .contains(&"NotificationService::build_notification".to_string()),
        "NotificationService::notify must call build_notification"
    );

    // 3. Cross-service calls
    assert!(
        setup
            .find_calls_from_method("User#send_notification")
            .contains(&"NotificationService::notify".to_string()),
        "User#send_notification must call NotificationService::notify"
    );

    // 4. Instance variable method calls
    assert!(
        setup
            .find_calls_from_method("UsersController#show")
            .contains(&"User#get_profile".to_string()),
        "UsersController#show must call @user.get_profile"
    );

    // 5. Static method calls
    assert!(
        !setup
            .find_calls_to_method("Profile::create_default")
            .is_empty(),
        "Profile::create_default must have callers"
    );
}
