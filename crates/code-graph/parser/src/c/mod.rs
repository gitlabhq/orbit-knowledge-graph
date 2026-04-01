use crate::dsl::extractors::{NameExtractor, extract_from_declarator, extract_from_field};
use crate::dsl::predicates::*;
use crate::dsl::types::{LanguageSpec, ReferenceRule, ScopeRule};

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// C language specification for the DSL engine.
///
/// ## C tree-sitter grammar — scope-creating constructs
///
/// ```text
/// function_definition
///   type: ...
///   declarator: function_declarator
///     declarator: identifier "my_func"
///     parameters: parameter_list
///   body: compound_statement
///
/// struct_specifier
///   name: type_identifier "MyStruct"
///   body: field_declaration_list
///
/// enum_specifier
///   name: type_identifier "MyEnum"
///   body: enumerator_list
///
/// union_specifier
///   name: type_identifier "MyUnion"
///   body: field_declaration_list
///
/// type_definition
///   type: ...
///   declarator: type_identifier "MyType"
/// ```
///
/// ## Reference-producing constructs
///
/// ```text
/// call_expression
///   function: identifier "printf"
///   arguments: argument_list
///
/// call_expression
///   function: field_expression
///     argument: identifier "obj"
///     field: field_identifier "method"
///   arguments: argument_list
/// ```
pub fn c_language_spec() -> LanguageSpec {
    LanguageSpec {
        name: "c",
        scope_corpus: &[
            "function_definition",
            "struct_specifier",
            "enum_specifier",
            "union_specifier",
        ],
        scope_rules: vec![
            // Functions: name is inside declarator->declarator chain
            ScopeRule::new(Box::new(kind_eq("function_definition")))
                .with_label("Function")
                .with_name_extractor(Box::new(extract_from_declarator())),
            // Structs
            ScopeRule::new(Box::new(kind_eq("struct_specifier").and(has_name_field())))
                .with_label("Struct")
                .with_name_extractor(Box::new(extract_from_field("name"))),
            // Enums
            ScopeRule::new(Box::new(kind_eq("enum_specifier").and(has_name_field())))
                .with_label("Enum")
                .with_name_extractor(Box::new(extract_from_field("name"))),
            // Unions
            ScopeRule::new(Box::new(kind_eq("union_specifier").and(has_name_field())))
                .with_label("Union")
                .with_name_extractor(Box::new(extract_from_field("name"))),
        ],
        reference_rules: vec![
            // Direct function calls: printf("hello")
            ReferenceRule::new(
                Box::new(
                    kind_eq("call_expression")
                        .and(has_field_with_kind("function", vec!["identifier"])),
                ),
                "FunctionCall",
            )
            .with_name_extractor(Box::new(extract_from_field("function"))),
            // Method-like calls via field expression: obj->method() or obj.method()
            ReferenceRule::new(
                Box::new(
                    kind_eq("call_expression")
                        .and(has_field_with_kind("function", vec!["field_expression"])),
                ),
                "FieldCall",
            )
            .with_name_extractor(Box::new(CallExpressionFieldExtractor)),
        ],
    }
}

struct CallExpressionFieldExtractor;

impl NameExtractor for CallExpressionFieldExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        let func_node = node.field("function")?;
        let field_node = func_node.field("field")?;
        Some(field_node.text().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::engine::DslAnalyzer;
    use crate::dsl::types::dsl_fqn_to_string;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    #[test]
    fn test_c_functions() {
        let spec = c_language_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
int add(int a, int b) {
    return a + b;
}

void greet(const char* name) {
    printf("Hello, %s!\n", name);
}
"#;
        let result = parser.parse(code, Some("math.c")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        let names: Vec<&str> = analysis
            .definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        assert!(names.contains(&"add"), "Expected 'add', got: {names:?}");
        assert!(names.contains(&"greet"), "Expected 'greet', got: {names:?}");
        assert_eq!(analysis.definitions.len(), 2);

        for def in &analysis.definitions {
            assert_eq!(def.definition_type.label, "Function");
        }

        // Check FQNs
        let fqns: Vec<String> = analysis
            .definitions
            .iter()
            .map(|d| dsl_fqn_to_string(&d.fqn))
            .collect();
        assert!(fqns.contains(&"add".to_string()));
        assert!(fqns.contains(&"greet".to_string()));
    }

    #[test]
    fn test_c_structs_and_enums() {
        let spec = c_language_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
struct Point {
    int x;
    int y;
};

enum Color {
    RED,
    GREEN,
    BLUE
};

union Data {
    int i;
    float f;
};
"#;
        let result = parser.parse(code, Some("types.c")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        let names: Vec<&str> = analysis
            .definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        assert!(names.contains(&"Point"), "Expected 'Point', got: {names:?}");
        assert!(names.contains(&"Color"), "Expected 'Color', got: {names:?}");
        assert!(names.contains(&"Data"), "Expected 'Data', got: {names:?}");
        assert_eq!(analysis.definitions.len(), 3);

        let point = analysis
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.definition_type.label, "Struct");

        let color = analysis
            .definitions
            .iter()
            .find(|d| d.name == "Color")
            .unwrap();
        assert_eq!(color.definition_type.label, "Enum");

        let data = analysis
            .definitions
            .iter()
            .find(|d| d.name == "Data")
            .unwrap();
        assert_eq!(data.definition_type.label, "Union");
    }

    #[test]
    fn test_c_references() {
        let spec = c_language_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
int helper(int x) {
    return x * 2;
}

int main() {
    int result = helper(42);
    printf("Result: %d\n", result);
    return 0;
}
"#;
        let result = parser.parse(code, Some("main.c")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        // Should have 2 definitions: helper, main
        assert_eq!(analysis.definitions.len(), 2);

        // Should have references: helper(42) and printf(...)
        let ref_names: Vec<&str> = analysis
            .references
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert!(
            ref_names.contains(&"helper"),
            "Expected 'helper' ref, got: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"printf"),
            "Expected 'printf' ref, got: {ref_names:?}"
        );
    }

    #[test]
    fn test_c_field_calls() {
        let spec = c_language_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
void process(struct Device* dev) {
    dev->init();
    dev->run(42);
}
"#;
        let result = parser.parse(code, Some("device.c")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        let ref_names: Vec<&str> = analysis
            .references
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert!(
            ref_names.contains(&"init"),
            "Expected 'init' ref, got: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"run"),
            "Expected 'run' ref, got: {ref_names:?}"
        );
    }

    #[test]
    fn test_c_nested_calls_in_function() {
        let spec = c_language_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
int compute(int a, int b) {
    return add(multiply(a, 2), b);
}
"#;
        let result = parser.parse(code, Some("compute.c")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        // Check references have correct scope FQN
        for r in &analysis.references {
            assert_eq!(
                dsl_fqn_to_string(r.scope_fqn.as_ref().unwrap()),
                "compute",
                "Reference '{}' should be scoped to 'compute'",
                r.name
            );
        }
    }
}
