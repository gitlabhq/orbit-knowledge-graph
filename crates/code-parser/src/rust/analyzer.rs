use crate::ParseResult;
use crate::analyzer::{AnalysisResult, Analyzer};
use crate::rust::definitions::extract_definitions_from_map;
use crate::rust::fqn::{build_fqn_and_node_indices, rust_fqn_to_string};
use crate::rust::types::{RustDefinitionType, RustFqn, RustImportType};

/// Type aliases for Rust-specific analyzer and analysis result
pub type RustAnalyzer = Analyzer<RustFqn, RustDefinitionType, RustImportType>;
pub type RustAnalysisResult = AnalysisResult<RustFqn, RustDefinitionType, RustImportType>;

impl RustAnalyzer {
    /// Analyze Rust code and extract definitions with FQN computation
    pub fn analyze(&self, parser_result: &ParseResult) -> crate::Result<RustAnalysisResult> {
        // Build FQN index using our iterative approach, which also collects definitions and imports
        let (_node_fqn_map, _node_index_map, definitions_map, imports) =
            build_fqn_and_node_indices(&parser_result.ast);

        // Extract definitions from the map that was populated during FQN traversal
        let definitions = extract_definitions_from_map(&definitions_map);

        Ok(RustAnalysisResult::new(definitions, imports, vec![]))
    }
}

impl RustAnalysisResult {
    /// Get FQN strings for all definitions that have them
    pub fn rust_definition_fqn_strings(&self) -> Vec<String> {
        self.definition_fqn_strings(rust_fqn_to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SupportedLanguage;
    use crate::{LanguageParser, parser::GenericParser};

    #[test]
    fn test_rust_analyzer_builds_fqn_map() -> crate::Result<()> {
        let analyzer = RustAnalyzer::new();
        let rust_code = r#"
mod network {
    pub struct Connection {
        url: String,
    }
    
    impl Connection {
        pub fn new(url: String) -> Self {
            Connection { url }
        }
        
        pub async fn connect(&self) -> Result<(), std::io::Error> {
            Ok(())
        }
    }
}

pub fn main() {
    let conn = network::Connection::new("localhost".to_string());
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs"))?;
        let result = analyzer.analyze(&parse_result)?;

        // Now the analyzer should create definitions from FQN traversal
        assert!(
            !result.definitions.is_empty(),
            "Definitions should be found during FQN traversal"
        );

        // Verify we found some expected definitions
        let definition_names: Vec<&String> = result.definitions.iter().map(|d| &d.name).collect();
        assert!(
            definition_names.contains(&&"network".to_string()),
            "Should find 'network' module"
        );
        assert!(
            definition_names.contains(&&"Connection".to_string()),
            "Should find 'Connection' struct"
        );
        assert!(
            definition_names.contains(&&"new".to_string()),
            "Should find 'new' method"
        );
        assert!(
            definition_names.contains(&&"connect".to_string()),
            "Should find 'connect' method"
        );
        assert!(
            definition_names.contains(&&"main".to_string()),
            "Should find 'main' function"
        );

        Ok(())
    }

    #[test]
    fn test_rust_analyzer_empty_result() -> crate::Result<()> {
        let analyzer = RustAnalyzer::new();
        let rust_code = r#"
fn simple_function() {
    println!("Hello, world!");
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("simple.rs"))?;
        let result = analyzer.analyze(&parse_result)?;

        // Verify the result structure - should now find definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find at least one definition"
        );
        assert_eq!(result.imports.len(), 0);

        // Find the simple_function definition
        let simple_function_def = result
            .definitions
            .iter()
            .find(|d| d.name == "simple_function")
            .expect("Should find simple_function definition");
        assert_eq!(
            simple_function_def.definition_type,
            RustDefinitionType::Function
        );

        Ok(())
    }

    #[test]
    fn test_rust_analyzer_comprehensive_definitions() -> crate::Result<()> {
        let analyzer = RustAnalyzer::new();
        let rust_code = r#"
mod utils {
    pub struct Calculator {
        value: i32,
    }
    
    impl Calculator {
        pub fn new(value: i32) -> Self {
            Calculator { value }
        }
        
        pub fn add(&mut self, other: i32) {
            self.value += other;
        }
        
        pub fn create_default() -> Self {
            Self::new(0)
        }
    }
    
    pub trait Calculable {
        fn calculate(&self) -> i32;
    }
    
    impl Calculable for Calculator {
        fn calculate(&self) -> i32 {
            self.value
        }
    }
    
    pub enum Operation {
        Add(i32),
        Subtract(i32),
    }
    
    pub const DEFAULT_VALUE: i32 = 42;
    
    pub fn helper_function() -> i32 {
        DEFAULT_VALUE
    }
}

pub fn main() {
    let calc = utils::Calculator::new(10);
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("comprehensive.rs"))?;
        let result = analyzer.analyze(&parse_result)?;

        // Verify we found comprehensive definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find many definitions"
        );

        // Check specific definition types and names
        let definition_names: Vec<&String> = result.definitions.iter().map(|d| &d.name).collect();
        let definition_types: Vec<RustDefinitionType> = result
            .definitions
            .iter()
            .map(|d| d.definition_type)
            .collect();

        // Modules
        assert!(
            definition_names.contains(&&"utils".to_string()),
            "Should find 'utils' module"
        );

        // Structs
        assert!(
            definition_names.contains(&&"Calculator".to_string()),
            "Should find 'Calculator' struct"
        );

        // Methods and associated functions
        assert!(
            definition_names.contains(&&"new".to_string()),
            "Should find 'new' method"
        );
        assert!(
            definition_names.contains(&&"add".to_string()),
            "Should find 'add' method"
        );
        assert!(
            definition_names.contains(&&"create_default".to_string()),
            "Should find 'create_default' associated function"
        );

        // Traits
        assert!(
            definition_names.contains(&&"Calculable".to_string()),
            "Should find 'Calculable' trait"
        );
        assert!(
            definition_names.contains(&&"calculate".to_string()),
            "Should find 'calculate' trait method"
        );

        // Enums and variants
        assert!(
            definition_names.contains(&&"Operation".to_string()),
            "Should find 'Operation' enum"
        );
        assert!(
            definition_names.contains(&&"Add".to_string()),
            "Should find 'Add' variant"
        );
        assert!(
            definition_names.contains(&&"Subtract".to_string()),
            "Should find 'Subtract' variant"
        );

        // Constants and functions
        assert!(
            definition_names.contains(&&"DEFAULT_VALUE".to_string()),
            "Should find 'DEFAULT_VALUE' constant"
        );
        assert!(
            definition_names.contains(&&"helper_function".to_string()),
            "Should find 'helper_function' function"
        );
        assert!(
            definition_names.contains(&&"main".to_string()),
            "Should find 'main' function"
        );

        // Verify we have different definition types
        assert!(
            definition_types.contains(&RustDefinitionType::Module),
            "Should have module definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Struct),
            "Should have struct definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Method),
            "Should have method definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::AssociatedFunction),
            "Should have associated function definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Trait),
            "Should have trait definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Enum),
            "Should have enum definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Variant),
            "Should have variant definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Constant),
            "Should have constant definition"
        );
        assert!(
            definition_types.contains(&RustDefinitionType::Function),
            "Should have function definition"
        );

        // Verify FQNs are correctly populated
        let fqn_strings = result.rust_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");
        assert!(
            fqn_strings
                .iter()
                .any(|fqn| fqn.contains("utils::Calculator")),
            "Should have utils::Calculator FQN"
        );
        assert!(
            fqn_strings
                .iter()
                .any(|fqn| fqn.contains("utils::Calculator::new")),
            "Should have utils::Calculator::new FQN"
        );

        Ok(())
    }

    #[test]
    fn test_rust_analyzer_integrates_with_imports() -> crate::Result<()> {
        let analyzer = RustAnalyzer::new();
        let rust_code = r#"
use std::collections::HashMap;
extern crate serde;
mod my_module;

fn main() {
    println!("Testing imports integration");
}
"#;

        let parser = GenericParser::new(SupportedLanguage::Rust);
        let parser_result = parser.parse(rust_code, None)?;
        let analysis = analyzer.analyze(&parser_result)?;

        // Should find both definitions and imports
        assert!(!analysis.definitions.is_empty(), "Should find definitions");
        assert!(!analysis.imports.is_empty(), "Should find imports");

        // Should find the main function
        assert!(
            analysis.definitions.iter().any(|d| d.name == "main"),
            "Should find main function"
        );

        // Should find basic imports
        assert!(
            analysis
                .imports
                .iter()
                .any(|i| i.import_type == RustImportType::Use),
            "Should find use import"
        );
        assert!(
            analysis
                .imports
                .iter()
                .any(|i| i.import_type == RustImportType::ExternCrate),
            "Should find extern crate"
        );
        assert!(
            analysis
                .imports
                .iter()
                .any(|i| i.import_type == RustImportType::ModDeclaration),
            "Should find mod declaration"
        );

        Ok(())
    }
}
