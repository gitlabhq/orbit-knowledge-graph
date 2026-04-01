use crate::dsl::extractors::{declarator, field, field_chain};
use crate::dsl::predicates::*;
use crate::dsl::types::{LanguageSpec, reference, scope};

pub fn c_language_spec() -> LanguageSpec {
    LanguageSpec::new(
        "c",
        vec![
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
        vec![
            reference("call_expression")
                .when(field_kind("function", &["identifier"]))
                .name_from(field("function")),
            reference("call_expression")
                .when(field_kind("function", &["field_expression"]))
                .name_from(field_chain(&["function", "field"])),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::types::dsl_fqn_to_string;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    #[test]
    fn test_c_functions() {
        let spec = c_language_spec();

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = "int add(int a, int b) { return a + b; }\nvoid greet(const char* name) { }";
        let result = parser.parse(code, Some("math.c")).unwrap();
        let output = spec.analyze(&result).unwrap();

        assert_eq!(output.definitions.len(), 2);
        let names: Vec<&str> = output.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"add"));
        assert!(names.contains(&"greet"));
        for def in &output.definitions {
            assert_eq!(def.definition_type.label, "Function");
        }
    }

    #[test]
    fn test_c_structs_and_enums() {
        let spec = c_language_spec();

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = "struct Point { int x; int y; };\nenum Color { RED, GREEN };\nunion Data { int i; float f; };";
        let result = parser.parse(code, Some("types.c")).unwrap();
        let output = spec.analyze(&result).unwrap();

        assert_eq!(output.definitions.len(), 3);
        let point = output
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.definition_type.label, "Struct");
        let color = output
            .definitions
            .iter()
            .find(|d| d.name == "Color")
            .unwrap();
        assert_eq!(color.definition_type.label, "Enum");
        let data = output
            .definitions
            .iter()
            .find(|d| d.name == "Data")
            .unwrap();
        assert_eq!(data.definition_type.label, "Union");
    }

    #[test]
    fn test_c_references() {
        let spec = c_language_spec();

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = r#"
int helper(int x) { return x * 2; }
int main() { int r = helper(42); printf("ok"); return 0; }
"#;
        let result = parser.parse(code, Some("main.c")).unwrap();
        let output = spec.analyze(&result).unwrap();

        let ref_names: Vec<&str> = output.references.iter().map(|r| r.name.as_str()).collect();
        assert!(ref_names.contains(&"helper"));
        assert!(ref_names.contains(&"printf"));
    }

    #[test]
    fn test_c_field_calls() {
        let spec = c_language_spec();

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = "void process(struct Device* dev) { dev->init(); dev->run(42); }";
        let result = parser.parse(code, Some("device.c")).unwrap();
        let output = spec.analyze(&result).unwrap();

        let ref_names: Vec<&str> = output.references.iter().map(|r| r.name.as_str()).collect();
        assert!(ref_names.contains(&"init"));
        assert!(ref_names.contains(&"run"));
    }

    #[test]
    fn test_c_nested_calls_scoped() {
        let spec = c_language_spec();

        let parser = GenericParser::new(SupportedLanguage::C);
        let code = "int compute(int a, int b) { return add(multiply(a, 2), b); }";
        let result = parser.parse(code, Some("compute.c")).unwrap();
        let output = spec.analyze(&result).unwrap();

        assert_eq!(output.definitions.len(), 1);
        assert_eq!(dsl_fqn_to_string(&output.definitions[0].fqn), "compute");
        assert!(output.references.len() >= 2);
    }
}
