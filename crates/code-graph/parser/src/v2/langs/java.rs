use code_graph_config::Language;
use code_graph_types::DefKind;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::dsl::extractors::{Extract, ExtractList, field, metadata};
use crate::dsl::types::*;

#[derive(Default)]
pub struct JavaDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn java_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    let type_kinds = ["type_identifier", "generic_type", "scoped_type_identifier"];

    if let Some(superclass) = node.field("superclass") {
        let text = superclass.text().to_string();
        let name = text.strip_prefix("extends ").unwrap_or(&text).trim();
        if !name.is_empty() {
            result.push(name.to_string());
        }
    }
    if let Some(interfaces) = node.field("interfaces") {
        for child in interfaces.children() {
            if type_kinds.iter().any(|&k| k == child.kind().as_ref()) {
                result.push(child.text().to_string());
            }
        }
    }
    for child in node.children() {
        if child.kind() == "extends_interfaces" {
            for inner in child.children() {
                if type_kinds.iter().any(|&k| k == inner.kind().as_ref()) {
                    result.push(inner.text().to_string());
                }
            }
        }
    }
    result
}

impl DslLanguage for JavaDsl {
    fn name() -> &'static str {
        "java"
    }

    fn language() -> Language {
        Language::Java
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(ExtractList::Fn(java_super_types));

        vec![
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("interface_declaration", "Interface")
                .def_kind(DefKind::Interface)
                .metadata(class_meta()),
            scope("enum_declaration", "Enum")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("record_declaration", "Record")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("annotation_type_declaration", "AnnotationDeclaration")
                .def_kind(DefKind::Interface),
            scope("enum_constant", "EnumConstant")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            scope("constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(metadata().return_type(field("type"))),
            scope("lambda_expression", "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(field("parameters")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("method_invocation").name_from(field("name")),
            reference("object_creation_expression").name_from(field("type")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        fn java_import_classify(node: &N<'_>) -> &'static str {
            let text = node.text().to_string();
            let is_static = text.trim_start().starts_with("import static");
            let is_wildcard = node.children().any(|c| c.kind() == "asterisk");
            match (is_static, is_wildcard) {
                (true, _) => "StaticImport",
                (false, true) => "WildcardImport",
                (false, false) => "Import",
            }
        }

        vec![
            import("import_declaration")
                .classify(java_import_classify)
                .split_last("."),
        ]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        // package_declaration has a scoped_identifier or identifier child
        Some(("package_declaration", Extract::Default))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::CanonicalParser;

    fn parse(code: &str) -> code_graph_types::CanonicalResult {
        let parser = DslParser::<JavaDsl>::default();
        parser.parse_file(code.as_bytes(), "Test.java").unwrap().0
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            r#"
public class Calculator {
    public int add(int a, int b) {
        return a + b;
    }
}
"#,
        );

        assert_eq!(result.definitions.len(), 2);
        let calc = &result.definitions[0];
        assert_eq!(calc.name, "Calculator");
        assert_eq!(calc.kind, DefKind::Class);
        assert!(calc.is_top_level);

        let add = &result.definitions[1];
        assert_eq!(add.name, "add");
        assert_eq!(add.kind, DefKind::Method);
        assert_eq!(add.fqn.to_string(), "Calculator.add");
    }

    #[test]
    fn package_scoping() {
        let result = parse(
            r#"
package com.example;

public class Service {
    public void run() {}
}
"#,
        );

        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
        assert!(service.is_top_level);
    }

    #[test]
    fn super_types_extracted() {
        let result = parse(
            r#"
public class Dog extends Animal implements Serializable {
}
"#,
        );

        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("Dog should have metadata");
        assert!(
            !meta.super_types.is_empty(),
            "super_types: {:?}",
            meta.super_types
        );
    }

    #[test]
    fn method_return_type() {
        let result = parse(
            r#"
public class Service {
    public String getName() { return ""; }
}
"#,
        );

        let get_name = result
            .definitions
            .iter()
            .find(|d| d.name == "getName")
            .unwrap();
        let meta = get_name.metadata.as_ref().expect("should have metadata");
        assert_eq!(meta.return_type.as_deref(), Some("String"));
    }

    #[test]
    fn references_extracted() {
        let result = parse(
            r#"
public class App {
    public void run() {
        helper();
        new ArrayList();
    }
    private void helper() {}
}
"#,
        );

        let names: Vec<&str> = result.references.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"helper"));
        assert!(names.contains(&"ArrayList"));
    }

    #[test]
    fn imports_extracted() {
        let result = parse(
            r#"
import java.util.List;
import java.util.*;

public class Test {}
"#,
        );

        assert!(result.imports.len() >= 2);
    }
}
