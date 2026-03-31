use std::collections::HashMap;

use crate::{
    Analyzer, ParseResult, Result,
    kotlin::{
        ast::{kotlin_fqn_to_string, parse_ast},
        types::{
            KotlinDefinitionInfo, KotlinDefinitionType, KotlinFqn, KotlinImportType,
            KotlinImportedSymbolInfo, KotlinReferenceInfo,
        },
    },
};

pub type KotlinAnalyzer = Analyzer<KotlinFqn, KotlinDefinitionType, KotlinImportType>;
pub struct KotlinAnalyzerResult {
    pub definitions: Vec<KotlinDefinitionInfo>,
    pub imports: Vec<KotlinImportedSymbolInfo>,
    pub references: Vec<KotlinReferenceInfo>,
}

impl KotlinAnalyzer {
    pub fn analyze(&self, parser_result: &ParseResult) -> Result<KotlinAnalyzerResult> {
        Ok(parse_ast(&parser_result.ast))
    }
}

impl KotlinAnalyzerResult {
    pub fn kotlin_definition_fqn_strings(&self) -> Vec<String> {
        self.definitions
            .iter()
            .map(|def| kotlin_fqn_to_string(&def.fqn))
            .collect()
    }

    pub fn definitions_of_type(
        &self,
        def_type: &KotlinDefinitionType,
    ) -> Vec<&KotlinDefinitionInfo> {
        self.definitions
            .iter()
            .filter(|def| def.definition_type == *def_type)
            .collect()
    }

    pub fn definitions_by_name(&self, name: &str) -> Vec<&KotlinDefinitionInfo> {
        self.definitions
            .iter()
            .filter(|def| def.name == name)
            .collect()
    }

    pub fn imports_of_type(
        &self,
        import_type: &KotlinImportType,
    ) -> Vec<&KotlinImportedSymbolInfo> {
        self.imports
            .iter()
            .filter(|import| import.import_type == *import_type)
            .collect()
    }

    pub fn count_definitions_by_type(&self) -> HashMap<KotlinDefinitionType, usize> {
        let mut counts = HashMap::new();
        for def in &self.definitions {
            *counts.entry(def.definition_type).or_insert(0) += 1;
        }
        counts
    }

    pub fn count_imports_by_type(&self) -> HashMap<KotlinImportType, usize> {
        let mut counts = HashMap::new();
        for import in &self.imports {
            *counts.entry(import.import_type).or_insert(0) += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use crate::{LanguageParser, SupportedLanguage, parser::GenericParser};

    use super::*;

    #[test]
    fn test_analysis_result_grouping_and_filtering() {
        let analyzer = KotlinAnalyzer::new();
        let fixture_path = "src/kotlin/fixtures/ComprehensiveKotlinDefinitions.kt";
        let kotlin_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveKotlinDefinitions.kt fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::Kotlin);
        let parse_result = parser.parse(&kotlin_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Class)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::DataClass)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::ValueClass)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Function)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Property)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Constructor)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::CompanionObject)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Object)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::EnumEntry)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::CompanionObject)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Object)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::EnumEntry)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Interface)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Enum)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::AnnotationClass)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&KotlinDefinitionType::Lambda)
                .is_empty()
        );

        assert!(!result.imports_of_type(&KotlinImportType::Import).is_empty());
        assert!(
            !result
                .imports_of_type(&KotlinImportType::WildcardImport)
                .is_empty()
        );

        assert!(
            !result
                .imports_of_type(&KotlinImportType::AliasedImport)
                .is_empty()
        );
    }

    #[test]
    fn test_comprehensive_definitions_fixture() {
        let analyzer = KotlinAnalyzer::new();
        let fixture_path = "src/kotlin/fixtures/ComprehensiveKotlinDefinitions.kt";
        let kotlin_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveKotlinDefinitions.kt fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::Kotlin);
        let parse_result = parser.parse(&kotlin_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        println!(
            "ComprehensiveKotlinDefinitions.kt analyzer definition counts: {:?}",
            result.count_definitions_by_type()
        );

        println!(
            "ComprehensiveKotlinDefinitions.kt analyzer import counts: {:?}",
            result.count_imports_by_type()
        );

        validate_import_exists(
            &result,
            "java.annotation.AnnotationTarget",
            KotlinImportType::Import,
        );
        validate_import_exists(&result, "java.time", KotlinImportType::WildcardImport);
        validate_import_exists(&result, "java.log", KotlinImportType::AliasedImport);

        validate_definition_exists(&result, "Disposable", KotlinDefinitionType::AnnotationClass);

        validate_definition_exists(&result, "Time", KotlinDefinitionType::Object);
        validate_definition_exists(&result, "utcClock", KotlinDefinitionType::Property);

        validate_definition_exists(&result, "ProjectId", KotlinDefinitionType::ValueClass);
        validate_definition_exists(&result, "id", KotlinDefinitionType::Property);

        validate_definition_exists(&result, "Project", KotlinDefinitionType::DataClass);
        validate_definition_exists(&result, "absolutePath", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "name", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "default", KotlinDefinitionType::Function);
        validate_definition_exists(&result, "display", KotlinDefinitionType::Function);

        validate_definition_exists(&result, "BASE_URL", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "urlAndPort", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "httpClient", KotlinDefinitionType::Property);

        validate_definition_exists(&result, "AccessResult", KotlinDefinitionType::Enum);
        validate_definition_exists(&result, "message", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "UNKNOWN_PROJECT", KotlinDefinitionType::EnumEntry);
        validate_definition_exists(&result, "ACCESS_EXPIRED", KotlinDefinitionType::EnumEntry);
        validate_definition_exists(&result, "ACCESS_OK", KotlinDefinitionType::EnumEntry);

        validate_definition_exists(
            &result,
            "IProjectAccessService",
            KotlinDefinitionType::Interface,
        );
        validate_definition_exists(&result, "ProjectAccessService", KotlinDefinitionType::Class);
        validate_definition_exists(&result, "logger", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "clock", KotlinDefinitionType::Property);
        validate_definition_exists(&result, "validateAccess", KotlinDefinitionType::Function);
        validate_definition_exists(&result, "revokeAccess", KotlinDefinitionType::Function);

        // The fixture contains both a class property named `project` and a local variable `project` in main.
        validate_definition_exists_with_count(
            &result,
            "project",
            KotlinDefinitionType::Property,
            2,
        );

        validate_definition_exists_with_count(
            &result,
            "<init>",
            KotlinDefinitionType::Constructor,
            5,
        );
        validate_definition_exists_with_count(
            &result,
            "Companion",
            KotlinDefinitionType::CompanionObject,
            2,
        );

        validate_definition_exists(&result, "printUrlAndPort", KotlinDefinitionType::Lambda);
        validate_definition_exists(&result, "main", KotlinDefinitionType::Function);
    }

    fn validate_import_exists(
        result: &KotlinAnalyzerResult,
        name: &str,
        expected_type: KotlinImportType,
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

    fn validate_definition_exists(
        result: &KotlinAnalyzerResult,
        name: &str,
        expected_type: KotlinDefinitionType,
    ) {
        let defs = result.definitions_by_name(name);

        assert!(!defs.is_empty(), "Should find {name} definition");
        assert!(
            defs[0].definition_type == expected_type,
            "Definition type mismatch for {}, expected {:?}, got {:?}",
            name,
            expected_type,
            defs[0].definition_type
        );
        assert_eq!(defs.len(), 1, "Should find exactly 1 {name} definition");
    }

    fn validate_definition_exists_with_count(
        result: &KotlinAnalyzerResult,
        name: &str,
        expected_type: KotlinDefinitionType,
        expected_count: usize,
    ) {
        let defs = result.definitions_by_name(name);

        assert!(!defs.is_empty(), "Should find {name} definition");
        assert!(
            defs[0].definition_type == expected_type,
            "Definition type mismatch for {}, expected {:?}, got {:?}",
            name,
            expected_type,
            defs[0].definition_type
        );
        assert_eq!(
            defs.len(),
            expected_count,
            "Should find exactly {expected_count} {name} definitions"
        );
    }
}
