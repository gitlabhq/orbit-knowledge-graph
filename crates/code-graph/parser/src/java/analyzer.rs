use std::collections::HashMap;

use crate::{
    Analyzer, ParseResult, Result,
    java::{
        ast::{java_fqn_to_string, parse_ast},
        types::{
            JavaDefinitionInfo, JavaDefinitionType, JavaFqn, JavaImportType,
            JavaImportedSymbolInfo, JavaReferenceInfo,
        },
    },
};

pub type JavaAnalyzer = Analyzer<JavaFqn, JavaDefinitionInfo, JavaImportType>;
pub struct JavaAnalyzerResult {
    pub definitions: Vec<JavaDefinitionInfo>,
    pub imports: Vec<JavaImportedSymbolInfo>,
    pub references: Vec<JavaReferenceInfo>,
}

impl JavaAnalyzer {
    pub fn analyze(&self, parser_result: &ParseResult) -> Result<JavaAnalyzerResult> {
        Ok(parse_ast(&parser_result.ast))
    }
}

impl JavaAnalyzerResult {
    pub fn java_definition_fqn_strings(&self) -> Vec<String> {
        self.definitions
            .iter()
            .map(|def| java_fqn_to_string(&def.fqn))
            .collect()
    }

    pub fn definitions_of_type(&self, def_type: &JavaDefinitionType) -> Vec<&JavaDefinitionInfo> {
        self.definitions
            .iter()
            .filter(|def| def.definition_type == *def_type)
            .collect()
    }

    pub fn definitions_by_name(&self, name: &str) -> Vec<&JavaDefinitionInfo> {
        self.definitions
            .iter()
            .filter(|def| def.name == name)
            .collect()
    }

    pub fn imports_of_type(&self, import_type: &JavaImportType) -> Vec<&JavaImportedSymbolInfo> {
        self.imports
            .iter()
            .filter(|import| import.import_type == *import_type)
            .collect()
    }

    pub fn count_definitions_by_type(&self) -> HashMap<JavaDefinitionType, usize> {
        let mut counts = HashMap::new();
        for def in &self.definitions {
            *counts.entry(def.definition_type).or_insert(0) += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use crate::{Language, LanguageParser, parser::GenericParser};

    use super::*;

    #[test]
    fn test_definition_grouping_and_filtering() {
        let analyzer = JavaAnalyzer::new();
        let fixture_path = "src/java/fixtures/ComprehensiveJavaDefinitions.java";
        let java_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveJavaDefinitions.java fixture");

        let parser = GenericParser::default_for_language(Language::Java);
        let parse_result = parser.parse(&java_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Class)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Method)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Interface)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Enum)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::EnumConstant)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Record)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Annotation)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::AnnotationDeclaration)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&JavaDefinitionType::Lambda)
                .is_empty()
        );

        assert!(!result.imports_of_type(&JavaImportType::Import).is_empty());

        assert!(
            !result
                .imports_of_type(&JavaImportType::WildcardImport)
                .is_empty()
        );

        assert!(
            !result
                .imports_of_type(&JavaImportType::StaticImport)
                .is_empty()
        );

        assert!(!result.references.is_empty());
    }

    #[test]
    fn test_comprehensive_definitions_fixture() {
        let analyzer = JavaAnalyzer::new();
        let fixture_path = "src/java/fixtures/ComprehensiveJavaDefinitions.java";
        let java_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveJavaDefinitions.java fixture");

        let parser = GenericParser::default_for_language(Language::Java);
        let parse_result = parser.parse(&java_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        println!(
            "ComprehensiveJavaDefinitions.java analyzer counts: {:?}",
            result.count_definitions_by_type()
        );

        validate_import_exists(&result, "java.time.Clock", JavaImportType::StaticImport);
        validate_import_exists(&result, "java.net.http.HttpClient", JavaImportType::Import);
        validate_import_exists(
            &result,
            "java.util.logging.*",
            JavaImportType::WildcardImport,
        );

        validate_definition_exists(&result, "Disposable", JavaDefinitionType::Annotation);
        validate_definition_exists(&result, "value", JavaDefinitionType::AnnotationDeclaration);
        validate_definition_exists(&result, "count", JavaDefinitionType::AnnotationDeclaration);

        validate_definition_exists(&result, "Time", JavaDefinitionType::Class);

        validate_definition_exists(&result, "Project", JavaDefinitionType::Record);
        validate_definition_exists(&result, "default", JavaDefinitionType::Method);
        validate_definition_exists(&result, "display", JavaDefinitionType::Method);

        validate_definition_exists(&result, "Constants", JavaDefinitionType::Class);

        validate_definition_exists(&result, "AccessResult", JavaDefinitionType::Enum);
        validate_definition_exists(&result, "UNKNOWN_PROJECT", JavaDefinitionType::EnumConstant);
        validate_definition_exists(&result, "ACCESS_EXPIRED", JavaDefinitionType::EnumConstant);
        validate_definition_exists(&result, "ACCESS_OK", JavaDefinitionType::EnumConstant);

        validate_definition_exists(
            &result,
            "IProjectAccessService",
            JavaDefinitionType::Interface,
        );
        validate_definition_exists(&result, "ProjectAccessService", JavaDefinitionType::Class);
        validate_definition_exists(&result, "validateAccess", JavaDefinitionType::Method);
        validate_definition_exists(&result, "revokeAccess", JavaDefinitionType::Method);

        validate_definition_exists(&result, "Person", JavaDefinitionType::Record);
        validate_definition_exists(&result, "getDisplayName", JavaDefinitionType::Method);

        validate_definition_exists(&result, "Main", JavaDefinitionType::Class);
        validate_definition_exists(&result, "printServiceUrl", JavaDefinitionType::Lambda);
        validate_definition_exists(&result, "main", JavaDefinitionType::Method);
    }

    #[test]
    fn test_very_long_call_chain_fixture_does_not_stack_overflow() {
        let analyzer = JavaAnalyzer::new();
        let fixture_path = "src/java/fixtures/VeryLongCallChain.java";
        let java_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read VeryLongCallChain.java fixture");

        let parser = GenericParser::default_for_language(Language::Java);
        let parse_result = parser.parse(&java_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        assert!(!result.references.is_empty());
    }
    fn validate_definition_exists(
        result: &JavaAnalyzerResult,
        name: &str,
        expected_type: JavaDefinitionType,
    ) {
        let defs = result.definitions_by_name(name);

        assert!(!defs.is_empty(), "Should find {name} definition");
        let matching_defs: Vec<_> = defs
            .iter()
            .filter(|def| def.definition_type == expected_type)
            .collect();
        assert!(
            !matching_defs.is_empty(),
            "Definition type mismatch for {}, expected {:?}, but found types: {:?}",
            name,
            expected_type,
            defs.iter()
                .map(|def| def.definition_type)
                .collect::<Vec<_>>()
        );
    }

    fn validate_import_exists(
        result: &JavaAnalyzerResult,
        name: &str,
        expected_type: JavaImportType,
    ) {
        let imports = result.imports_of_type(&expected_type);

        assert!(!imports.is_empty(), "Should find {name} import");
        let matching_imports: Vec<_> = imports
            .iter()
            .filter(|import| import.import_type == expected_type)
            .collect();

        assert!(
            !matching_imports.is_empty(),
            "Import type mismatch for {}, expected {:?}, but found types: {:?}",
            name,
            expected_type,
            imports
                .iter()
                .map(|import| import.import_type)
                .collect::<Vec<_>>()
        );
    }
}
