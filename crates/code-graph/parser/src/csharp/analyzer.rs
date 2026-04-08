use crate::ParseResult;
use crate::analyzer::{AnalysisResult, Analyzer};
use crate::csharp::parser::parse_ast;
use crate::csharp::types::{CSharpDefinitionType, CSharpFqn, CSharpImportType};

/// Type alias for CSharp-specific analyzer
pub type CSharpAnalyzer = Analyzer<CSharpFqn, CSharpDefinitionType, CSharpImportType>;

/// Type alias for CSharp-specific analysis result
pub type CSharpAnalysisResult = AnalysisResult<CSharpFqn, CSharpDefinitionType, CSharpImportType>;

impl CSharpAnalyzer {
    /// Analyze CSharp code and extract definitions with FQN computation
    pub fn analyze(&self, parser_result: &ParseResult) -> crate::Result<CSharpAnalysisResult> {
        let (definitions, imports) = parse_ast(&parser_result.ast);

        Ok(CSharpAnalysisResult::new(definitions, imports, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DefinitionLookup, LanguageParser, SupportedLanguage, csharp::analyzer::CSharpAnalyzer,
        parser::GenericParser,
    };

    use super::*;

    #[test]
    fn test_comprehensive_definitions_fixture() {
        let analyzer = CSharpAnalyzer::new();
        let fixture_path = "src/csharp/fixtures/ComprehensiveCSharp.cs";
        let csharp_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveCSharp.cs fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(&csharp_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        println!(
            "ComprehensiveCSharp.cs analyzer counts: {:?}",
            result.count_definitions_by_type()
        );

        // Test that all major definition types are found
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Class)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Interface)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Struct)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Enum)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Record)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Property)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::InstanceMethod)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Constructor)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Operator)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Indexer)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Event)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Field)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::AnonymousType)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::ExtensionMethod)
                .is_empty()
        );

        // Validate interfaces with different features
        validate_definition_exists(&result, "IBasicInterface", CSharpDefinitionType::Interface);
        validate_definition_exists(
            &result,
            "IGenericInterface",
            CSharpDefinitionType::Interface,
        );
        validate_definition_exists(&result, "IModernInterface", CSharpDefinitionType::Interface);
        validate_definition_exists(&result, "INestedInterface", CSharpDefinitionType::Interface);

        // Validate enums (simple, flags, and nested)
        validate_definition_exists(&result, "SimpleEnum", CSharpDefinitionType::Enum);
        validate_definition_exists(&result, "FlagsEnum", CSharpDefinitionType::Enum);
        validate_definition_exists(&result, "NestedEnum", CSharpDefinitionType::Enum);

        // Validate structs (simple, record, readonly, ref, and nested)
        validate_definition_exists(&result, "SimpleStruct", CSharpDefinitionType::Struct);
        validate_definition_exists(&result, "ImmutablePoint", CSharpDefinitionType::Struct);
        validate_definition_exists(&result, "SpanWrapper", CSharpDefinitionType::Struct);
        validate_definition_exists(&result, "NestedStruct", CSharpDefinitionType::Struct);

        // Validate records (basic, positional, with inheritance, and custom equality)
        validate_definition_exists(&result, "Person", CSharpDefinitionType::Record);
        validate_definition_exists(&result, "Employee", CSharpDefinitionType::Record);
        validate_definition_exists(&result, "Point", CSharpDefinitionType::Record);
        validate_definition_exists(&result, "CustomRecord", CSharpDefinitionType::Record);

        // Validate classes (attribute, abstract, sealed, generic, partial, static, nested, and specialized)
        validate_definition_exists(&result, "CustomAttribute", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "AbstractBase", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "SealedClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "GenericClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "PartialClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "FeatureDemonstration", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "NestedClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "LambdaExamples", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "ExceptionExamples", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "CustomException", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "LinqExamples", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "UnsafeExamples", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "PreprocessorExamples", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "Program", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "FileLocalClass", CSharpDefinitionType::Class);

        // Validate properties (auto, init-only, required, computed, mixed access, static, abstract)
        validate_definition_exists(&result, "AutoProperty", CSharpDefinitionType::Property);
        validate_definition_exists(&result, "InitOnlyProperty", CSharpDefinitionType::Property);
        validate_definition_exists(&result, "RequiredProperty", CSharpDefinitionType::Property);
        validate_definition_exists(
            &result,
            "PropertyWithBackingField",
            CSharpDefinitionType::Property,
        );
        validate_definition_exists(&result, "ComputedProperty", CSharpDefinitionType::Property);
        validate_definition_exists(
            &result,
            "MixedAccessProperty",
            CSharpDefinitionType::Property,
        );
        validate_definition_exists(&result, "StaticProperty", CSharpDefinitionType::Property);
        validate_definition_exists(&result, "AbstractProperty", CSharpDefinitionType::Property);

        // Validate instance methods (interface implementations, overrides, async, generic, expression-bodied)
        validate_definition_exists(
            &result,
            "AbstractMethod",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(&result, "Method", CSharpDefinitionType::InstanceMethod);
        validate_definition_exists(&result, "GetValue", CSharpDefinitionType::InstanceMethod);
        validate_definition_exists(&result, "SetValue", CSharpDefinitionType::InstanceMethod);
        validate_definition_exists(&result, "AsyncMethod", CSharpDefinitionType::InstanceMethod);
        validate_definition_exists(
            &result,
            "ExpressionBodiedMethod",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(
            &result,
            "ParameterModifiers",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(
            &result,
            "PatternMatchingExamples",
            CSharpDefinitionType::InstanceMethod,
        );

        // Validate operator and indexer definitions
        validate_definition_exists(&result, "operator+", CSharpDefinitionType::Operator);
        validate_definition_exists(&result, "indexer", CSharpDefinitionType::Indexer);

        // Validate event and field definitions
        validate_definition_exists(&result, "Event", CSharpDefinitionType::Event);
        validate_definition_exists(&result, "_backingField", CSharpDefinitionType::Field);

        // Validate anonymous type definitions
        validate_definition_exists(&result, "anonymous", CSharpDefinitionType::AnonymousType);

        // Validate static methods (entry points and utility methods)
        validate_definition_exists(&result, "Main", CSharpDefinitionType::StaticMethod);
        validate_definition_exists(&result, "MainAsync", CSharpDefinitionType::StaticMethod);
        validate_definition_exists(
            &result,
            "ExtensionMethod",
            CSharpDefinitionType::ExtensionMethod,
        );
        validate_definition_exists(
            &result,
            "StaticAbstractMethod",
            CSharpDefinitionType::StaticMethod,
        );

        // Validate constructors are found across different classes
        let constructors = result.definitions_of_type(&CSharpDefinitionType::Constructor);
        assert!(
            constructors.len() > 3,
            "Should find multiple constructors, found {}",
            constructors.len()
        );

        // Validate nested and file-local definitions
        validate_definition_exists(
            &result,
            "NestedMethod",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(
            &result,
            "NestedInterfaceMethod",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(
            &result,
            "FileLocalMethod",
            CSharpDefinitionType::InstanceMethod,
        );

        // Validate lambda definitions
        validate_definition_exists(&result, "square", CSharpDefinitionType::Lambda);
        validate_definition_exists(&result, "print", CSharpDefinitionType::Lambda);
        validate_definition_exists(&result, "add", CSharpDefinitionType::Lambda);

        // Validate that we found the finalizer/destructor
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Finalizer)
                .is_empty()
        );
    }

    #[test]
    fn test_definition_grouping_and_filtering() {
        let analyzer = CSharpAnalyzer::new();
        let fixture_path = "src/csharp/fixtures/ComprehensiveCSharp.cs";
        let csharp_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveCSharp.cs fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(&csharp_code, Some(fixture_path)).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        // Test that we can filter definitions by type and they're not empty
        for definition_type in [
            CSharpDefinitionType::Class,
            CSharpDefinitionType::Interface,
            CSharpDefinitionType::Struct,
            CSharpDefinitionType::Enum,
            CSharpDefinitionType::Record,
            CSharpDefinitionType::Property,
            CSharpDefinitionType::InstanceMethod,
            CSharpDefinitionType::Constructor,
            CSharpDefinitionType::Operator,
            CSharpDefinitionType::Indexer,
            CSharpDefinitionType::Event,
            CSharpDefinitionType::Field,
            CSharpDefinitionType::AnonymousType,
            CSharpDefinitionType::ExtensionMethod,
        ] {
            let defs = result.definitions_of_type(&definition_type);
            assert!(
                !defs.is_empty(),
                "Should find {definition_type:?} definitions"
            );
        }

        // Test count_definitions_by_type returns reasonable counts
        let counts = result.count_definitions_by_type();
        assert!(
            counts.get(&CSharpDefinitionType::Class).unwrap_or(&0) > &10,
            "Should find multiple classes"
        );
        assert!(
            counts
                .get(&CSharpDefinitionType::InstanceMethod)
                .unwrap_or(&0)
                > &20,
            "Should find multiple instance methods"
        );
        assert!(
            counts.get(&CSharpDefinitionType::Property).unwrap_or(&0) > &10,
            "Should find multiple properties"
        );
    }

    fn validate_definition_exists(
        result: &CSharpAnalysisResult,
        name: &str,
        expected_type: CSharpDefinitionType,
    ) {
        let defs = result.definitions_by_name(name);

        assert!(
            !defs.is_empty(),
            "Should find definition with name '{name}'"
        );

        let matching_defs: Vec<_> = defs
            .iter()
            .filter(|d| d.definition_type == expected_type)
            .collect();

        assert!(
            !matching_defs.is_empty(),
            "Should find '{}' with type {:?}, found types for this name: {:?}",
            name,
            expected_type,
            defs.iter().map(|d| d.definition_type).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_analyzer_with_imports() {
        let analyzer = CSharpAnalyzer::new();
        let code = r#"
using System;
using Console = System.Console;
global using System.IO;
using static System.Math;

namespace TestNamespace {
    class TestClass {
        void TestMethod() {}
    }
}
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(code, None).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        // Verify we have both definitions and imports
        assert!(!result.definitions.is_empty(), "Should have definitions");
        assert!(!result.imports.is_empty(), "Should have imports");

        // Verify we have the expected number of imports
        assert_eq!(result.imports.len(), 4, "Should have 4 imports");

        // Test import types
        let default_import = result
            .imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Default && i.import_path == "System");
        assert!(default_import.is_some(), "Should find default import");

        let alias_import = result
            .imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Alias);
        assert!(alias_import.is_some(), "Should find alias import");

        let global_import = result
            .imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Global);
        assert!(global_import.is_some(), "Should find global import");

        let static_import = result
            .imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Static);
        assert!(static_import.is_some(), "Should find static import");

        // Test that definitions still work
        let class_def = result.definitions.iter().find(|d| d.name == "TestClass");
        assert!(class_def.is_some(), "Should find TestClass definition");
    }

    #[test]
    fn test_definitions_parsing_comprehensive() {
        let analyzer = CSharpAnalyzer::new();
        let code = r#"
namespace TestNamespace {
    public interface ITestInterface {
        void InterfaceMethod();
    }

    public enum TestEnum {
        Value1,
        Value2
    }

    public struct TestStruct {
        public int Field;
        public int Property { get; set; }
    }

    public record TestRecord(string Name, int Age);

    public class TestClass : ITestInterface {
        private int _backingField;
        public event System.Action TestEvent;
        
        public int Property { get; set; }
        public int this[int index] => index;
        
        public TestClass() { }
        ~TestClass() { }
        
        public void InterfaceMethod() { }
        public static void StaticMethod() { }
        
        public static TestClass operator+(TestClass a, TestClass b) => new TestClass();
        
        public void LambdaExample() {
            System.Func<int, int> square = x => x * x;
            var anonymous = new { Name = "Test", Value = 42 };
        }
    }
    
    public static class ExtensionClass {
        public static void ExtensionMethod(this string str) { }
    }
}
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(code, None).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        // Verify all definition types are found
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Interface)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Enum)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Struct)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Record)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Class)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Property)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Constructor)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Finalizer)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::InstanceMethod)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::StaticMethod)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Operator)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Indexer)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Event)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Field)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::Lambda)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::AnonymousType)
                .is_empty()
        );
        assert!(
            !result
                .definitions_of_type(&CSharpDefinitionType::ExtensionMethod)
                .is_empty()
        );

        // Verify specific definitions exist
        validate_definition_exists(&result, "ITestInterface", CSharpDefinitionType::Interface);
        validate_definition_exists(&result, "TestEnum", CSharpDefinitionType::Enum);
        validate_definition_exists(&result, "TestStruct", CSharpDefinitionType::Struct);
        validate_definition_exists(&result, "TestRecord", CSharpDefinitionType::Record);
        validate_definition_exists(&result, "TestClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "ExtensionClass", CSharpDefinitionType::Class);
        validate_definition_exists(&result, "Property", CSharpDefinitionType::Property);
        validate_definition_exists(&result, "TestClass", CSharpDefinitionType::Constructor);
        validate_definition_exists(&result, "TestClass", CSharpDefinitionType::Finalizer);
        validate_definition_exists(
            &result,
            "InterfaceMethod",
            CSharpDefinitionType::InstanceMethod,
        );
        validate_definition_exists(&result, "StaticMethod", CSharpDefinitionType::StaticMethod);
        validate_definition_exists(&result, "operator+", CSharpDefinitionType::Operator);
        validate_definition_exists(&result, "indexer", CSharpDefinitionType::Indexer);
        validate_definition_exists(&result, "TestEvent", CSharpDefinitionType::Event);
        validate_definition_exists(&result, "_backingField", CSharpDefinitionType::Field);
        validate_definition_exists(&result, "square", CSharpDefinitionType::Lambda);
        validate_definition_exists(&result, "anonymous", CSharpDefinitionType::AnonymousType);
        validate_definition_exists(
            &result,
            "ExtensionMethod",
            CSharpDefinitionType::ExtensionMethod,
        );
    }

    #[test]
    fn test_imports_parsing_comprehensive() {
        let analyzer = CSharpAnalyzer::new();
        let code = r#"
using System;
using System.Collections.Generic;
using Console = System.Console;
using static System.Math;
global using System.IO;
global using System.Text;
global using Logger = System.Console;
global using static System.Environment;

namespace TestNamespace {
    class TestClass { }
}
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(code, None).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        assert_eq!(result.imports.len(), 8, "Should have 8 imports");

        // Test default imports
        let default_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::Default)
            .collect();
        assert_eq!(default_imports.len(), 2, "Should have 2 default imports");
        assert!(default_imports.iter().any(|i| i.import_path == "System"));
        assert!(
            default_imports
                .iter()
                .any(|i| i.import_path == "System.Collections.Generic")
        );

        // Test alias import
        let alias_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::Alias)
            .collect();
        assert_eq!(alias_imports.len(), 1, "Should have 1 alias import");
        let alias_import = &alias_imports[0];
        assert_eq!(alias_import.import_path, "System.Console");
        assert_eq!(
            alias_import
                .identifier
                .as_ref()
                .unwrap()
                .alias
                .as_ref()
                .unwrap(),
            "Console"
        );

        // Test static import
        let static_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::Static)
            .collect();
        assert_eq!(static_imports.len(), 1, "Should have 1 static import");
        assert_eq!(static_imports[0].import_path, "System.Math");

        // Test global imports
        let global_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::Global)
            .collect();
        assert_eq!(global_imports.len(), 2, "Should have 2 global imports");
        assert!(global_imports.iter().any(|i| i.import_path == "System.IO"));
        assert!(
            global_imports
                .iter()
                .any(|i| i.import_path == "System.Text")
        );

        // Test global alias import
        let global_alias_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::GlobalAlias)
            .collect();
        assert_eq!(
            global_alias_imports.len(),
            1,
            "Should have 1 global alias import"
        );
        let global_alias = &global_alias_imports[0];
        assert_eq!(global_alias.import_path, "System.Console");
        assert_eq!(
            global_alias
                .identifier
                .as_ref()
                .unwrap()
                .alias
                .as_ref()
                .unwrap(),
            "Logger"
        );

        // Test global static import
        let global_static_imports: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == CSharpImportType::GlobalStatic)
            .collect();
        assert_eq!(
            global_static_imports.len(),
            1,
            "Should have 1 global static import"
        );
        assert_eq!(global_static_imports[0].import_path, "System.Environment");
    }

    #[test]
    fn test_fqn_correctness() {
        let analyzer = CSharpAnalyzer::new();
        let code = r#"
namespace OuterNamespace.InnerNamespace {
    public class OuterClass {
        public class InnerClass {
            public void InnerMethod() { }
        }
        
        public void OuterMethod() { }
    }
}
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let parse_result = parser.parse(code, None).unwrap();

        let result = analyzer.analyze(&parse_result).unwrap();

        // Find the inner method and verify its FQN
        let inner_method = result
            .definitions
            .iter()
            .find(|d| d.name == "InnerMethod")
            .expect("Should find InnerMethod");

        let fqn_parts: Vec<_> = inner_method
            .fqn
            .iter()
            .map(|p| p.node_name.as_str())
            .collect();
        assert_eq!(
            fqn_parts,
            vec![
                "OuterNamespace.InnerNamespace",
                "OuterClass",
                "InnerClass",
                "InnerMethod"
            ]
        );

        // Find the outer method and verify its FQN
        let outer_method = result
            .definitions
            .iter()
            .find(|d| d.name == "OuterMethod")
            .expect("Should find OuterMethod");

        let fqn_parts: Vec<_> = outer_method
            .fqn
            .iter()
            .map(|p| p.node_name.as_str())
            .collect();
        assert_eq!(
            fqn_parts,
            vec!["OuterNamespace.InnerNamespace", "OuterClass", "OuterMethod"]
        );
    }
}
