use crate::dsl::extractors::{declarator, field, field_chain};
use crate::dsl::predicates::*;
use crate::dsl::types::{LanguageSpec, import, reference, scope_fn};

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// C++ inherits all of C's constructs and adds namespaces, classes,
/// methods, templates, and lambda expressions.
pub fn cpp_language_spec() -> LanguageSpec {
    LanguageSpec::new(
        "cpp",
        vec![scope_fn("function_definition", classify_cpp_function).name_from(declarator())],
        vec![
            reference("call_expression")
                .when(field_kind("function", &["identifier"]))
                .name_from(field("function")),
            reference("call_expression")
                .when(field_kind("function", &["field_expression"]))
                .name_from(field_chain(&["function", "field"])),
            reference("call_expression")
                .when(field_kind("function", &["qualified_identifier"]))
                .name_from(field("function")),
        ],
        vec![import("preproc_include").path_from(field("path"))],
    )
    .auto(&[
        ("struct_specifier", "Struct"),
        ("enum_specifier", "Enum"),
        ("union_specifier", "Union"),
        ("namespace_definition", "Namespace"),
        ("class_specifier", "Class"),
    ])
}

fn classify_cpp_function(node: &Node<StrDoc<SupportLang>>) -> &'static str {
    let in_class = nearest_ancestor(
        &["class_specifier", "struct_specifier", "function_definition"],
        &["class_specifier", "struct_specifier"],
    )
    .test(node);

    if in_class { "Method" } else { "Function" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::engine::DslParseOutput;
    use crate::dsl::types::dsl_fqn_to_string;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    fn analyze(code: &str) -> DslParseOutput {
        let spec = cpp_language_spec();
        let parser = GenericParser::new(SupportedLanguage::Cpp);
        let result = parser.parse(code, Some("test.cpp")).unwrap();
        spec.analyze(&result).unwrap()
    }

    fn assert_def(output: &DslParseOutput, name: &str, label: &str, fqn: &str) {
        let def = output
            .definitions
            .iter()
            .find(|d| d.name == name)
            .unwrap_or_else(|| {
                let all: Vec<_> = output
                    .definitions
                    .iter()
                    .map(|d| {
                        format!(
                            "{} [{}] -> {}",
                            d.name,
                            d.definition_type.label,
                            dsl_fqn_to_string(&d.fqn)
                        )
                    })
                    .collect();
                panic!(
                    "Definition '{name}' not found. Have:\n  {}",
                    all.join("\n  ")
                )
            });
        assert_eq!(
            def.definition_type.label, label,
            "Label mismatch for '{name}': expected '{label}', got '{}'",
            def.definition_type.label
        );
        assert_eq!(
            dsl_fqn_to_string(&def.fqn),
            fqn,
            "FQN mismatch for '{name}'"
        );
    }

    #[test]
    fn test_cpp_namespace_and_class() {
        let output = analyze(
            r#"
namespace math {
    class Calculator {
    public:
        int add(int a, int b) { return a + b; }
    };
}
"#,
        );
        assert_def(&output, "math", "Namespace", "math");
        assert_def(&output, "Calculator", "Class", "math.Calculator");
        assert_def(&output, "add", "Method", "math.Calculator.add");
    }

    #[test]
    fn test_cpp_free_function() {
        let output = analyze("int main() { return 0; }");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "main", "Function", "main");
    }

    #[test]
    fn test_cpp_struct_with_methods() {
        let output = analyze(
            r#"
struct Point {
    int x, y;
    void translate(int dx, int dy) { x += dx; y += dy; }
};
"#,
        );
        assert_def(&output, "Point", "Struct", "Point");
        assert_def(&output, "translate", "Method", "Point.translate");
    }

    #[test]
    fn test_cpp_enum_class() {
        let output = analyze("enum class Color { Red, Green, Blue };");
        assert_def(&output, "Color", "Enum", "Color");
    }

    #[test]
    fn test_cpp_nested_namespaces() {
        let output = analyze(
            r#"
namespace outer {
    namespace inner {
        void helper() {}
    }
}
"#,
        );
        assert_def(&output, "outer", "Namespace", "outer");
        assert_def(&output, "inner", "Namespace", "outer.inner");
        assert_def(&output, "helper", "Function", "outer.inner.helper");
    }

    #[test]
    fn test_cpp_references() {
        let output = analyze(
            r#"
void helper() {}
int main() {
    helper();
    std::cout << "hello";
}
"#,
        );
        let ref_names: Vec<&str> = output.references.iter().map(|r| r.name.as_str()).collect();
        assert!(
            ref_names.contains(&"helper"),
            "Expected 'helper' ref, got: {ref_names:?}"
        );
    }

    #[test]
    fn test_cpp_method_calls() {
        let output = analyze(
            r#"
void process(Calculator& calc) {
    calc.add(1, 2);
    calc.subtract(3, 4);
}
"#,
        );
        let ref_names: Vec<&str> = output.references.iter().map(|r| r.name.as_str()).collect();
        assert!(
            ref_names.contains(&"add"),
            "Expected 'add' ref, got: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"subtract"),
            "Expected 'subtract' ref, got: {ref_names:?}"
        );
    }

    #[test]
    fn test_cpp_includes() {
        let output = analyze(
            r#"
#include <stdio.h>
#include "myheader.h"

int main() { return 0; }
"#,
        );
        assert_eq!(output.imports.len(), 2);
        assert_eq!(output.imports[0].path, "<stdio.h>");
        assert_eq!(output.imports[1].path, "\"myheader.h\"");
        assert!(output.imports[0].name.is_none());
        assert!(output.imports[1].name.is_none());
    }
}
