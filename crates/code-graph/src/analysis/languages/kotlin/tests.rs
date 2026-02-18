use gitalisk_core::repository::testing::local::LocalGitRepository;
use std::path::Path;

use crate::analysis::types::GraphData;
use crate::graph::RelationshipType;
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;

fn init_kotlin_references_repository() -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures/code/kotlin");
    local_repo.copy_dir(&fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with Kotlin reference examples");
    local_repo
}

pub struct KotlinReferenceTestSetup {
    pub _local_repo: LocalGitRepository,
    pub graph_data: GraphData,
}

impl KotlinReferenceTestSetup {
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
        // First find the imported symbol node indices that match
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
}

pub async fn setup_kotlin_reference_pipeline() -> KotlinReferenceTestSetup {
    let local_repo = init_kotlin_references_repository();
    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::new(
        "kotlin-references-test".to_string(),
        repo_path_str.to_string(),
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
    let mut graph_data = indexing_result.graph_data.expect("Should have graph data");
    graph_data.assign_node_ids(1, "main");

    KotlinReferenceTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[cfg(test)]
mod integration_tests {
    use super::setup_kotlin_reference_pipeline;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_resolution_main_function_calls() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.foo.Foo'
        // RETURN source.fqn

        // Main.main -> Foo() constructor
        let callers_to_foo_constructor = setup.find_calls_to_method("com.example.foo.Foo");
        assert!(
            callers_to_foo_constructor
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main function should call Foo constructor"
        );

        // Main.main -> foo.foo() instance method
        let callers_to_foo_method = setup.find_calls_to_method("com.example.foo.Foo.foo");
        assert!(
            callers_to_foo_method
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main function should call foo.foo() instance method"
        );

        // Main.main -> foo.companionFoo() companion method
        let callers_to_companion_foo =
            setup.find_calls_to_method("com.example.foo.Foo.Companion.companionFoo");
        assert!(
            callers_to_companion_foo
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main function should call foo.companionFoo() companion method"
        );

        // Main.main -> foo.baz() interface method through inheritance
        let callers_to_baz_method = setup.find_calls_to_method("com.example.foo.Baz.baz");
        assert!(
            callers_to_baz_method
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main function should call foo.baz() interface method through inheritance"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_inheritance_and_super_calls() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.foo.Bar.bar'
        // RETURN source.fqn

        // Foo.foo -> super.bar() call to parent class method
        let callers_to_bar_method = setup.find_calls_to_method("com.example.foo.Bar.bar");
        assert!(
            callers_to_bar_method
                .iter()
                .any(|c| c.ends_with("com.example.foo.Foo.foo")),
            "Foo.foo should call super.bar() method from parent class"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_inner_class_calls() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.foo.Foo.fooInFooBody'
        // RETURN source.fqn

        // Foo.foo & Foo.InnerFoo.innerFoo -> fooInFooBody() call to inner class method
        let callers_to_inner_foo = setup.find_calls_to_method("com.example.foo.Foo.fooInFooBody");

        assert!(
            callers_to_inner_foo
                .iter()
                .any(|c| c.ends_with("com.example.foo.Foo.foo")),
            "Foo.foo should call fooInFooBody() method"
        );

        assert!(
            callers_to_inner_foo
                .iter()
                .any(|c| c.ends_with("com.example.foo.Foo.InnerFoo.innerFoo")),
            "Foo.InnerFoo.innerFoo should call fooInFooBody() method"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_type_inference_from_when_expression() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.entites.Person.getName'
        // RETURN source.fqn

        // When.whenTypeInference -> Person.getName()
        let callers_to_get_name = setup.find_calls_to_method("com.example.entites.Person.getName");

        assert!(
            callers_to_get_name
                .iter()
                .any(|c| c.ends_with("com.example.when.whenTypeInference")),
            "When.whenTypeInference should call Person.getName()"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_type_inference_from_if_expression() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.entites.Person.getName'
        // RETURN source.fqn

        // If.ifTypeInference -> Person.getName()
        let callers_to_get_name = setup.find_calls_to_method("com.example.entites.Person.getName");

        assert!(
            callers_to_get_name
                .iter()
                .any(|c| c.ends_with("com.example.if.usageOfIfTypeInference")),
            "If.ifTypeInference should call Person.getName()"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_type_inference_from_try_catch() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.entites.Person.getName'
        // RETURN source.fqn

        // Try.tryTypeInference -> Person.getName()
        let callers_to_get_name = setup.find_calls_to_method("com.example.entites.Person.getName");

        assert!(
            callers_to_get_name
                .iter()
                .any(|c| c.ends_with("com.example.try.tryTypeInference")),
            "Try.tryTypeInference should call Person.getName()"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_resolution_logger_calls() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:ImportedSymbolNode)
        // WHERE target.import_path = 'org.slf4j' AND target.name = 'Logger'
        // RETURN source.fqn

        // Main.main -> logger.info("Hello, World!")
        let callers_to_logger_info = setup.find_calls_to_imported_symbol("org.slf4j", "Logger");

        assert!(
            callers_to_logger_info
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main.main should call logger.info(\"Hello, World!\")"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_resolution_to_nested_classes() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.nestedclasses.Parent.Child.GrandChild.greet'
        // RETURN source.fqn

        // Parent.Child.GrandChild.greet()
        let callers_to_greet =
            setup.find_calls_to_method("com.example.nestedclasses.Parent.Child.GrandChild.greet");

        assert!(
            callers_to_greet
                .iter()
                .any(|c| c.ends_with("com.example.nestedclasses.Parent.GrandChild.greet")),
            "Parent.GrandChild.greet should call Parent.Child.GrandChild.greet"
        );

        assert!(
            callers_to_greet
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "main should call Parent.Child.GrandChild.greet()"
        );

        // Parent.GrandChild.greet()
        let callers_to_greet_2 =
            setup.find_calls_to_method("com.example.nestedclasses.Parent.GrandChild.greet");

        assert!(
            callers_to_greet_2
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "main should call Parent.Child.GrandChild.greet"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_resolution_inheritance_of_classs_of_same_name() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.edgecases.filter.Filter.filter'
        // RETURN source.fqn

        // ServerFilter.Filter -> Filter
        let callers_to_filter_filter =
            setup.find_calls_to_method("com.example.edgecases.filter.Filter.filter");

        assert!(
            callers_to_filter_filter
                .iter()
                .any(|c| c.ends_with("com.example.edgecases.filter.ServerFilter.filter")),
            "ServerFilter.Filter.filter should call Filter.filter"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_to_operator_functions() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.operator.AnimalList.plus'
        // RETURN source.fqn

        // AnimalList.Companion.of -> AnimalList.plus
        let callers_to_plus = setup.find_calls_to_method("com.example.operator.AnimalList.plus");

        assert!(
            callers_to_plus
                .iter()
                .any(|c| c.ends_with("com.example.operator.AnimalList.Companion.of")),
            "AnimalList.of should call AnimalList.plus"
        );

        // AnimalList.Companion.of -> AnimalList.display
        let callers_to_display =
            setup.find_calls_to_method("com.example.operator.AnimalList.display");

        assert!(
            callers_to_display
                .iter()
                .any(|c| c.ends_with("com.example.operator.AnimalList.Companion.of")),
            "AnimalList.of should call AnimalList.display"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_to_enum_constants() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.enums.Enum.enumMethod'
        // RETURN source.fqn

        // Enum.ENUM_VALUE_1.enumMethod()
        let callers_to_enum_value_1_enum_method =
            setup.find_calls_to_method("com.example.enums.Enum.enumMethod");

        assert!(
            callers_to_enum_value_1_enum_method
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main.main should call Enum.ENUM_VALUE_1.enumMethod()"
        );

        let callers_to_enum_value_2_enum_method_2 =
            setup.find_calls_to_method("com.example.enums.Enum.enumMethod2");

        assert!(
            callers_to_enum_value_2_enum_method_2
                .iter()
                .any(|c| c.ends_with("com.example.main")),
            "Main.main should call Enum.ENUM_VALUE_2.enumMethod2()"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kotlin_reference_to_extensions() {
        let setup = setup_kotlin_reference_pipeline().await;

        // Original Cypher query (commented out for reference):
        // MATCH (source:DefinitionNode)-[r:DEFINITION_RELATIONSHIPS]->(target:DefinitionNode)
        // WHERE target.fqn = 'com.example.extensions.printValue'
        // RETURN source.fqn

        // Functions

        // ExtendMe.printValue()
        let callers_to_print_value =
            setup.find_calls_to_method("com.example.extensions.printValue");

        assert!(
            callers_to_print_value
                .iter()
                .any(|c| c.ends_with("com.example.extensions.callToExtensions")),
            "callToExtensions should call ExtendMe.printValue()"
        );

        // ExtendMe.reversed()
        let callers_to_reversed =
            setup.find_calls_to_method("com.example.extensions.utils.reverse");

        assert!(
            callers_to_reversed
                .iter()
                .any(|c| c.ends_with("com.example.extensions.callToImportedExtensions")),
            "callToImportedExtensions should call ExtendMe.reversed()"
        );

        // Reference to method through extension properties

        // ExtendMeFromProperty.printValue()
        let callers_to_print_value_2 = setup.find_calls_to_method(
            "com.example.extensions.entities.ExtendMeFromProperty.printValue",
        );

        assert!(
            callers_to_print_value_2
                .iter()
                .any(|c| c.ends_with("com.example.extensions.callToExtensions")),
            "callToExtensions should call ExtendMeFromProperty.printValue()"
        );

        // ExtendMe.printValue()
        assert!(
            callers_to_print_value
                .iter()
                .any(|c| c.ends_with("com.example.extensions.callToImportedExtensions")),
            "callToExtensions should call ExtendMe.printValue()"
        );

        // ExternalType.print()
        let callers_to_print = setup.find_calls_to_method("com.example.extensions.imported.print");

        assert!(
            callers_to_print
                .iter()
                .any(|c| c.ends_with("com.example.extensions.imported.callToImported")),
            "callToImportedExtensions should call ExternalType.print()"
        );
    }
}
