use crate::legacy::parser::ParseResult;
use crate::legacy::parser::analyzer::{AnalysisResult, Analyzer};
use crate::legacy::parser::python::fqn::{build_fqn_index, python_fqn_to_string};
use crate::legacy::parser::python::imports::find_imports;
use crate::legacy::parser::python::references::find_references;
use crate::legacy::parser::python::symbol_table::visitor::build_symbol_table;
use crate::legacy::parser::python::types::{
    PythonDefinitionType, PythonFqn, PythonImportType, PythonReferenceInfo,
};

/// Type aliases for Python-specific analyzer and analysis result
pub type PythonAnalyzer = Analyzer<PythonFqn, PythonDefinitionType, PythonImportType>;
pub type PythonAnalysisResult =
    AnalysisResult<PythonFqn, PythonDefinitionType, PythonImportType, PythonReferenceInfo>;

impl PythonAnalyzer {
    pub fn analyze(
        &self,
        parser_result: &ParseResult,
    ) -> crate::legacy::parser::Result<PythonAnalysisResult> {
        let (node_fqn_map, definitions) = build_fqn_index(&parser_result.ast);
        let imports = find_imports(&parser_result.ast, &node_fqn_map);

        let symbol_table =
            build_symbol_table(&parser_result.ast, definitions.clone(), imports.clone());
        let references = find_references(&symbol_table);

        Ok(PythonAnalysisResult::new(definitions, imports, references))
    }
}

impl PythonAnalysisResult {
    /// Get FQN strings for all definitions that have them
    pub fn python_definition_fqn_strings(&self) -> Vec<String> {
        self.definition_fqn_strings(python_fqn_to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::definitions::DefinitionLookup;
    use crate::legacy::parser::parser::SupportedLanguage;
    use crate::legacy::parser::{LanguageParser, parser::GenericParser};

    #[test]
    fn test_analyzer_with_comprehensive_definitions_fixture() -> crate::legacy::parser::Result<()> {
        let analyzer = PythonAnalyzer::new();
        let fixture_path = "src/legacy/parser/python/fixtures/definitions.py";
        let python_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read definitions.py fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let parse_result = parser.parse(&python_code, Some(fixture_path))?;
        let result = analyzer.analyze(&parse_result)?;

        // Verify we found definitions
        assert!(
            !result.definitions.is_empty(),
            "Should find definitions in definitions.py"
        );

        // Count by type
        let counts = result.count_definitions_by_type();
        println!("definitions.py analyzer counts: {counts:?}");

        // Verify all supported definition types are present
        assert!(
            counts.get(&PythonDefinitionType::Class).unwrap_or(&0) >= &4,
            "Should find at least 4 classes"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::DecoratedClass)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 decorated class"
        );
        assert!(
            counts.get(&PythonDefinitionType::Function).unwrap_or(&0) >= &5,
            "Should find at least 5 functions"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::DecoratedFunction)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 decorated function"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::AsyncFunction)
                .unwrap_or(&0)
                >= &2,
            "Should find at least 2 async functions"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::DecoratedAsyncFunction)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 decorated async function"
        );
        assert!(
            counts.get(&PythonDefinitionType::Method).unwrap_or(&0) >= &2,
            "Should find at least 2 methods"
        );
        assert!(
            counts.get(&PythonDefinitionType::AsyncMethod).unwrap_or(&0) >= &1,
            "Should find at least 1 async method"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::DecoratedMethod)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 decorated method"
        );
        assert!(
            counts
                .get(&PythonDefinitionType::DecoratedAsyncMethod)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 decorated async method"
        );
        assert!(
            counts.get(&PythonDefinitionType::Lambda).unwrap_or(&0) >= &4,
            "Should find at least 4 lambdas"
        );

        // Test specific definitions we expect
        let names = result.definition_names();
        let expected_definitions = [
            "simple_function",
            "generator_function",
            "decorated_function",
            "async_function",
            "async_generator_function",
            "decorated_async_function",
            "module_lambda",
            "outer_function",
            "inner_function",
            "inner_lambda",
            "SimpleClass",
            "DecoratedClass",
            "ClassWithMethods",
            "method",
            "self.attr_lambda",
            "async_method",
            "nested_method",
            "inner_method",
            "class_method",
            "async_class_method",
            "lambda_method",
            "OuterClass",
            "InnerClass",
        ];

        for expected_def in &expected_definitions {
            assert!(
                names.contains(expected_def),
                "Should find definition: {expected_def}"
            );
        }

        // Test FQN functionality
        let definitions = result.all_definitions();
        assert!(!definitions.is_empty(), "Should have definitions");

        let fqn_strings = result.python_definition_fqn_strings();
        assert!(!fqn_strings.is_empty(), "Should have FQN strings");

        // Verify we have good representation of each definition type
        let class_defs = result.definitions_of_type(&PythonDefinitionType::Class);
        let decorated_class_defs =
            result.definitions_of_type(&PythonDefinitionType::DecoratedClass);
        let function_defs = result.definitions_of_type(&PythonDefinitionType::Function);
        let decorated_function_defs =
            result.definitions_of_type(&PythonDefinitionType::DecoratedFunction);
        let async_function_defs = result.definitions_of_type(&PythonDefinitionType::AsyncFunction);
        let decorated_async_function_defs =
            result.definitions_of_type(&PythonDefinitionType::DecoratedAsyncFunction);
        let method_defs = result.definitions_of_type(&PythonDefinitionType::Method);
        let async_method_defs = result.definitions_of_type(&PythonDefinitionType::AsyncMethod);
        let decorated_method_defs =
            result.definitions_of_type(&PythonDefinitionType::DecoratedMethod);
        let decorated_async_method_defs =
            result.definitions_of_type(&PythonDefinitionType::DecoratedAsyncMethod);
        let lambda_defs = result.definitions_of_type(&PythonDefinitionType::Lambda);

        assert!(!class_defs.is_empty(), "Should find class definitions");
        assert!(
            !decorated_class_defs.is_empty(),
            "Should find decorated class definitions"
        );
        assert!(
            !function_defs.is_empty(),
            "Should find function definitions"
        );
        assert!(
            !decorated_function_defs.is_empty(),
            "Should find decorated function definitions"
        );
        assert!(
            !async_function_defs.is_empty(),
            "Should find async function definitions"
        );
        assert!(
            !decorated_async_function_defs.is_empty(),
            "Should find decorated async function definitions"
        );
        assert!(!method_defs.is_empty(), "Should find method definitions");
        assert!(
            !async_method_defs.is_empty(),
            "Should find async method definitions"
        );
        assert!(
            !decorated_method_defs.is_empty(),
            "Should find decorated method definitions"
        );
        assert!(
            !decorated_async_method_defs.is_empty(),
            "Should find decorated async method definitions"
        );
        assert!(!lambda_defs.is_empty(), "Should find lambda definitions");

        Ok(())
    }

    #[test]
    fn test_analyzer_with_comprehensive_imports_fixture() -> crate::legacy::parser::Result<()> {
        let analyzer = PythonAnalyzer::new();
        let fixture_path = "src/legacy/parser/python/fixtures/imports.py";
        let python_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read imports.py fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let parse_result = parser.parse(&python_code, Some(fixture_path))?;
        let result = analyzer.analyze(&parse_result)?;

        // Verify we found imports
        assert!(
            !result.imports.is_empty(),
            "Should find imports in imports.py"
        );

        // Count by type
        let counts = result.count_imports_by_type();
        println!("imports.py analyzer counts: {counts:?}");

        // Verify all supported definition types are present
        assert!(
            counts.get(&PythonImportType::Import).unwrap_or(&0) >= &3,
            "Should find at least 3 regular imports"
        );
        assert!(
            counts.get(&PythonImportType::AliasedImport).unwrap_or(&0) >= &3,
            "Should find at least 3 aliased imports"
        );
        assert!(
            counts.get(&PythonImportType::FromImport).unwrap_or(&0) >= &4,
            "Should find at least 4 from imports"
        );
        assert!(
            counts
                .get(&PythonImportType::AliasedFromImport)
                .unwrap_or(&0)
                >= &4,
            "Should find at least 4 aliased from imports"
        );
        assert!(
            counts.get(&PythonImportType::WildcardImport).unwrap_or(&0) >= &1,
            "Should find at least 1 wildcard import"
        );
        assert!(
            counts.get(&PythonImportType::RelativeImport).unwrap_or(&0) >= &3,
            "Should find at least 3 relative imports"
        );
        assert!(
            counts
                .get(&PythonImportType::AliasedRelativeImport)
                .unwrap_or(&0)
                >= &3,
            "Should find at least 3 aliased relative imports"
        );
        assert!(
            counts
                .get(&PythonImportType::RelativeWildcardImport)
                .unwrap_or(&0)
                >= &1,
            "Should find at least 1 relative wildcard import"
        );
        assert!(
            counts.get(&PythonImportType::FutureImport).unwrap_or(&0) >= &3,
            "Should find at least 3 future imports"
        );
        assert!(
            counts
                .get(&PythonImportType::AliasedFutureImport)
                .unwrap_or(&0)
                >= &3,
            "Should find at least 3 aliased future imports"
        );

        Ok(())
    }
}
