use crate::dsl::extractors::{declarator, field, field_chain};
use crate::dsl::predicates::*;
use crate::dsl::types::{LanguageSpec, reference, scope};

pub fn c_language_spec() -> LanguageSpec {
    LanguageSpec {
        name: "c",
        scope_corpus: &[
            "function_definition",
            "struct_specifier",
            "enum_specifier",
            "union_specifier",
        ],
        scopes: vec![
            scope("function_definition", "Function").name_from(declarator()),
            scope("struct_specifier", "Struct")
                .when(has_name())
                .name_from(field("name")),
            scope("enum_specifier", "Enum")
                .when(has_name())
                .name_from(field("name")),
            scope("union_specifier", "Union")
                .when(has_name())
                .name_from(field("name")),
        ],
        refs: vec![
            reference("call_expression", "FunctionCall")
                .when(field_kind("function", &["identifier"]))
                .name_from(field("function")),
            reference("call_expression", "FieldCall")
                .when(field_kind("function", &["field_expression"]))
                .name_from(field_chain(&["function", "field"])),
        ],
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

        assert_eq!(analysis.definitions.len(), 2);

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
