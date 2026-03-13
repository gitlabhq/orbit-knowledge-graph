use gitalisk_core::repository::testing::local::LocalGitRepository;
use std::path::Path;

use crate::analysis::types::GraphData;
use crate::graph::RelationshipType;
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;

fn init_java_references_repository() -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = Path::new(concat!(env!("FIXTURES_DIR"), "/code/java"));
    local_repo.copy_dir(fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with Java reference examples");
    local_repo
}

pub struct JavaReferenceTestSetup {
    pub _local_repo: LocalGitRepository,
    pub graph_data: GraphData,
}

#[cfg(test)]
impl JavaReferenceTestSetup {
    /// Find all callers of a method by FQN
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

    /// Get definition FQN by node ID
    fn get_definition_fqn_by_id(&self, id: u32) -> Option<String> {
        self.graph_data
            .definition_nodes
            .get(id as usize)
            .map(|node| node.fqn.to_string())
    }

    /// Find callers to an imported symbol by import path and name
    fn find_calls_to_imported_symbol(&self, import_path: &str, name: &str) -> Vec<String> {
        // First find the imported symbol node index that matches
        let matching_import_indices: Vec<usize> = self
            .graph_data
            .imported_symbol_nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.import_path == import_path
                    && node.identifier.as_ref().map(|id| id.name.as_str()) == Some(name)
            })
            .map(|(idx, _)| idx)
            .collect();

        if matching_import_indices.is_empty() {
            return vec![];
        }

        // Find relationships that call these imported symbols
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                let target_id = rel.target_id? as usize;
                // Check if target matches any of our imported symbol indices
                // Note: This assumes imported symbols are indexed separately
                // We need to check the relationship kind
                if rel.kind == crate::graph::RelationshipKind::DefinitionToImportedSymbol
                    && matching_import_indices.contains(&target_id)
                {
                    self.get_definition_fqn_by_id(rel.source_id?)
                } else {
                    None
                }
            })
            .collect()
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

    /// Get call to imported symbol with location info
    fn get_call_to_imported_symbol_with_location(
        &self,
        source_fqn: &str,
        import_path: &str,
        name: &str,
    ) -> Option<(i32, i32, i32, i32)> {
        // Find matching imported symbol indices
        let matching_import_indices: Vec<usize> = self
            .graph_data
            .imported_symbol_nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.import_path == import_path
                    && node.identifier.as_ref().map(|id| id.name.as_str()) == Some(name)
            })
            .map(|(idx, _)| idx)
            .collect();

        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter(|rel| rel.kind == crate::graph::RelationshipKind::DefinitionToImportedSymbol)
            .find_map(|rel| {
                let src_fqn = self.get_definition_fqn_by_id(rel.source_id?)?;
                let target_id = rel.target_id? as usize;
                if src_fqn == source_fqn && matching_import_indices.contains(&target_id) {
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

pub async fn setup_java_reference_pipeline() -> JavaReferenceTestSetup {
    let local_repo = init_java_references_repository();
    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "java-references-test".to_string(),
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

    // Run the indexing pipeline to get GraphData
    let indexing_result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index repository");

    // Verify we have graph data
    let graph_data = indexing_result.graph_data.expect("Should have graph data");

    JavaReferenceTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[cfg(test)]
mod integration_tests {
    use super::setup_java_reference_pipeline;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test_java_reference_resolution_main() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.app.Traceable'
        // RETURN source.fqn

        // Main.main -> Traceable
        let callers_to_traceable = setup.find_calls_to_method("com.example.app.Traceable");
        assert!(
            callers_to_traceable
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should have a Traceable annotation"
        );

        // Main.main -> new Foo()
        let callers_to_foo = setup.find_calls_to_method("com.example.app.Foo");

        assert!(
            callers_to_foo
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.Main")),
            "Main.Main should call Foo"
        );

        // Main.main -> this.myParameter.bar()
        let callers_to_foo_bar = setup.find_calls_to_method("com.example.app.Foo.bar");
        assert!(
            callers_to_foo_bar
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Foo.bar"
        );

        // Main.main -> Bar.baz (pattern variable)
        let callers_to_baz = setup.find_calls_to_method("com.example.app.Bar.baz");
        assert!(
            callers_to_baz
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Bar.baz"
        );

        // Main.main -> Executor.execute (method reference)
        let callers_to_execute = setup.find_calls_to_method("com.example.app.Executor.execute");
        assert!(
            callers_to_execute
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Executor.execute"
        );

        // Main.main -> Main.await
        let callers_to_await = setup.find_calls_to_method("com.example.app.Main.await");
        assert!(
            callers_to_await
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Main.await"
        );

        // Main.main -> Application.run (through super)
        let callers_to_application_run =
            setup.find_calls_to_method("com.example.app.Application.run");
        assert!(
            callers_to_application_run
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Application.run through super"
        );

        // Main.main -> Outer.make
        let callers_to_outer_make = setup.find_calls_to_method("com.example.util.Outer.make");
        assert!(
            callers_to_outer_make
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Outer.make via direct import resolution"
        );

        // Main.main -> Outer.outerMethod
        let callers_to_outer_outer_method =
            setup.find_calls_to_method("com.example.util.Outer.outerMethod");
        assert!(
            callers_to_outer_outer_method
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Outer.outerMethod via resolved variable type"
        );

        // Main.main -> Outer.Inner
        let callers_to_outer_inner = setup.find_calls_to_method("com.example.util.Outer.Inner");
        assert!(
            callers_to_outer_inner
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Outer.Inner"
        );

        // Main.main -> Outer.Inner.innerMethod
        let callers_to_inner_inner_method =
            setup.find_calls_to_method("com.example.util.Outer.Inner.innerMethod");
        assert!(
            callers_to_inner_inner_method
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Outer.Inner.innerMethod"
        );

        // Main.main -> Outer.Inner.innerStatic
        let callers_to_inner_inner_static =
            setup.find_calls_to_method("com.example.util.Outer.Inner.innerStatic");
        assert!(
            callers_to_inner_inner_static
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call Outer.Inner.innerStatic"
        );

        // Main.main -> EnumClass.ENUM_VALUE_1.enumMethod1
        let callers_to_enum_value_1_enum_method_1 =
            setup.find_calls_to_method("com.example.app.EnumClass.enumMethod1");
        assert!(
            callers_to_enum_value_1_enum_method_1
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call EnumClass.enumMethod1"
        );

        // Main.main -> EnumClass.ENUM_VALUE_2.enumMethod2
        let callers_to_enum_value_2_enum_method_2 =
            setup.find_calls_to_method("com.example.app.EnumClass.enumMethod2");
        assert!(
            callers_to_enum_value_2_enum_method_2
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call EnumClass.enumMethod2"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_java_reference_resolution_to_imported_symbol() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:ImportedSymbolNode)
        // WHERE target.import_path = 'java.util' AND target.name = 'ArrayList'
        // RETURN source.fqn

        // Main.main -> java.util.ArrayList
        let callers_to_array_list = setup.find_calls_to_imported_symbol("java.util", "ArrayList");
        assert!(
            callers_to_array_list
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call ArrayList"
        );

        // Main.main -> java.util.List.of
        let callers_to_array_list_of = setup.find_calls_to_imported_symbol("java.util", "List");
        assert!(
            callers_to_array_list_of
                .iter()
                .any(|c| c.ends_with("com.example.app.Main.main")),
            "Main.main should call List"
        );

        // Traceable -> java.lang.annotation.Retention
        let callers_to_retention =
            setup.find_calls_to_imported_symbol("java.lang.annotation", "Retention");

        assert!(
            callers_to_retention
                .iter()
                .any(|c| c.ends_with("com.example.app.Traceable")),
            "Traceable should have a Retention annotation"
        );

        // Traceable -> java.lang.annotation.Target
        let callers_to_target =
            setup.find_calls_to_imported_symbol("java.lang.annotation", "Target");
        assert!(
            callers_to_target
                .iter()
                .any(|c| c.ends_with("com.example.app.Traceable")),
            "Traceable should have a Retention annotation"
        );
    }

    #[traced_test]
    #[tokio::test]
    // Regression test for resolving a class in a package that contains two classes with the same name.
    async fn test_java_reference_resolution_same_class_name_in_same_package() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.filter.Filter.apply'
        // RETURN source.fqn

        // ServerFilter.Filter -> ServerFilter
        let callers_to_filter = setup.find_calls_to_method("com.example.filter.Filter.apply");
        assert!(
            callers_to_filter
                .iter()
                .any(|c| c.ends_with("com.example.filter.ServerFilter.Filter.apply")),
            "ServerFilter.Filter should call ServerFilter.apply"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_java_call_relationship_has_location() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher queries (commented out for reference):
        // let calls_id = crate::graph::RelationshipType::Calls.as_string();
        // let query = format!(
        //     "MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode) \
        //  WHERE source.fqn = 'com.example.app.Main.main' AND target.fqn = 'com.example.app.Application.run' AND r.type = '{calls_id}' \
        //  RETURN r.source_start_line, r.source_end_line"
        // );

        // 1) com.example.app.Main.main -> await(() -> super.run()) on line 22 (0-based 21)
        let location = setup.get_call_with_location(
            "com.example.app.Main.main",
            "com.example.app.Application.run",
        );
        assert!(location.is_some(), "Expected Application.run call row");
        let (start_line, end_line, _, _) = location.unwrap();
        assert_eq!(start_line, 21);
        assert_eq!(end_line, 21);

        // 2) com.example.app.Main.main -> Outer.make() on line 25 (0-based 24)
        let location = setup
            .get_call_with_location("com.example.app.Main.main", "com.example.util.Outer.make");
        assert!(location.is_some(), "Expected Outer.make call row");
        let (start_line, end_line, _, _) = location.unwrap();
        assert_eq!(start_line, 24);
        assert_eq!(end_line, 24);

        // 3) com.example.app.Main.main -> new ArrayList<String>() (imported symbol java.util.ArrayList) on line 42 (0-based 41)
        // Original Cypher query (commented out for reference):
        // let query = format!(
        //     "MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:ImportedSymbolNode) \
        //      WHERE
        //         source.fqn = 'com.example.app.Main.main'
        //         AND target.import_path = 'java.util'
        //         AND target.name = 'ArrayList'
        //         AND r.type = '{calls_id}' \
        //      RETURN r.source_start_line, r.source_end_line"
        // );
        let location = setup.get_call_to_imported_symbol_with_location(
            "com.example.app.Main.main",
            "java.util",
            "ArrayList",
        );
        assert!(location.is_some(), "Expected ArrayList call row");
        let (start_line, end_line, _, _) = location.unwrap();
        assert_eq!(start_line, 41);
        assert_eq!(end_line, 41);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_java_reference_to_deep_nested_class() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.helpers.Helpers.InnerHelpers.innerDoHelp'
        // RETURN source.fqn

        // InnerInnerHelpers.innerDoHelp -> InnerHelpers.innerDoHelp
        let callers_to_inner_do_help =
            setup.find_calls_to_method("com.example.helpers.Helpers.InnerHelpers.innerDoHelp");

        assert!(
            callers_to_inner_do_help
                .iter()
                .any(|c| c
                    .ends_with("com.example.helpers.Helpers.InnerInnerHelpers.innerInnerDoHelp")),
            "InnerInnerHelpers.innerDoHelp should call InnerHelpers.innerDoHelp"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_java_reference_resolve_type_failing_on_nested_type() {
        let setup = setup_java_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.edgecases.ResolveTypeFailingOnNestedChild.Child.GrandChild.greet'
        // RETURN source.fqn

        // ResolveTypeFailingOnNestedChild.GrandChild -> ResolveTypeFailingOnNestedChild.Child.GrandChild
        let callers_to_grand_child = setup.find_calls_to_method(
            "com.example.edgecases.ResolveTypeFailingOnNestedChild.Child.GrandChild.greet",
        );

        println!("callers_to_grand_child: {:?}", callers_to_grand_child);

        assert!(
            callers_to_grand_child.iter().any(|c| c.ends_with(
                "com.example.edgecases.ResolveTypeFailingOnNestedChild.GrandChild.greet"
            )),
            "ResolveTypeFailingOnNestedChild.Child.GrandChild.greet should call ResolveTypeFailingOnNestedChild.GrandChild.greet"
        );
    }
}
