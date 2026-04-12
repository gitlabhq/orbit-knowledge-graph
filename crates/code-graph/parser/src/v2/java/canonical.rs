use code_graph_config::Language;
use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, Fqn,
    Position, Range, ReferenceStatus,
};
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::CanonicalParser;

const LANG: Language = Language::Java;

#[derive(Default)]
pub struct JavaCanonicalParser;

impl CanonicalParser for JavaCanonicalParser {
    type Ast = ();

    fn parse_file(&self, source: &[u8], file_path: &str) -> crate::Result<(CanonicalResult, ())> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = LANG.parse_ast(source_str);

        let mut defs = Vec::new();
        let mut imports = Vec::new();
        let mut refs = Vec::new();
        let mut scope: Vec<Arc<str>> = Vec::new();

        walk(&ast.root(), &mut scope, &mut defs, &mut imports, &mut refs);

        let extension = file_path
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_string())
            .unwrap_or_default();

        Ok((
            CanonicalResult {
                file_path: file_path.to_string(),
                extension,
                file_size: source.len() as u64,
                language: LANG,
                definitions: defs,
                imports,
                references: refs,
            },
            (),
        ))
    }
}

fn walk(
    node: &Node<StrDoc<SupportLang>>,
    scope: &mut Vec<Arc<str>>,
    defs: &mut Vec<CanonicalDefinition>,
    imports: &mut Vec<CanonicalImport>,
    refs: &mut Vec<CanonicalReference>,
) {
    if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
        return;
    }

    let kind = node.kind();
    let mut pushed = false;

    match kind.as_ref() {
        // Package declaration — adds to scope for all subsequent siblings.
        // We push but never set `pushed = true` so it persists for the
        // entire file (package scope is file-wide, not block-scoped).
        "package_declaration" => {
            if let Some(name) = package_name(node) {
                scope.push(Arc::from(name.as_str()));
            }
        }

        // Scope-creating definitions
        "class_declaration" => {
            if let Some(d) = extract_named(node, scope, "Class", DefKind::Class) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "interface_declaration" => {
            if let Some(d) = extract_named(node, scope, "Interface", DefKind::Interface) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "enum_declaration" => {
            if let Some(d) = extract_named(node, scope, "Enum", DefKind::Class) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "record_declaration" => {
            if let Some(d) = extract_named(node, scope, "Record", DefKind::Class) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "annotation_type_declaration" => {
            if let Some(d) = extract_named(node, scope, "AnnotationDeclaration", DefKind::Interface)
            {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }

        // Non-scope definitions
        "enum_constant" => {
            if let Some(d) = extract_named(node, scope, "EnumConstant", DefKind::EnumEntry) {
                defs.push(d);
            }
        }
        "constructor_declaration" => {
            if let Some(d) = extract_named(node, scope, "Constructor", DefKind::Constructor) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "method_declaration" => {
            if let Some(d) = extract_named(node, scope, "Method", DefKind::Method) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "lambda_expression" => {
            let name = format!("lambda${}", node_range(node).byte_offset.0);
            defs.push(CanonicalDefinition {
                definition_type: "Lambda",
                kind: DefKind::Lambda,
                name: name.clone(),
                fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
                range: node_range(node),
                is_top_level: false,
                metadata: None,
            });
        }

        // Imports
        "import_declaration" => {
            extract_import(node, imports);
        }

        // References
        "method_invocation" => {
            extract_method_invocation(node, scope, refs);
        }
        "object_creation_expression" => {
            extract_object_creation(node, scope, refs);
        }

        _ => {}
    }

    for child in node.children() {
        walk(&child, scope, defs, imports, refs);
    }

    if pushed {
        scope.pop();
    }
}

fn extract_named(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    def_type: &'static str,
    kind: DefKind,
) -> Option<CanonicalDefinition> {
    let name = node.field("name")?.text().to_string();
    Some(CanonicalDefinition {
        definition_type: def_type,
        kind,
        name: name.clone(),
        fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
        range: node_range(node),
        is_top_level: scope.is_empty() || (scope.len() == 1 && is_package_scope(scope)),
        metadata: None,
    })
}

fn is_package_scope(scope: &[Arc<str>]) -> bool {
    // If the only scope entry contains a dot, it's a package
    scope.len() == 1 && scope[0].contains('.')
}

fn package_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    // package_declaration has a child that is a scoped_identifier or identifier
    for child in node.children() {
        let k = child.kind();
        if k == "scoped_identifier" || k == "identifier" {
            return Some(child.text().to_string());
        }
    }
    None
}

fn extract_import(node: &Node<StrDoc<SupportLang>>, imports: &mut Vec<CanonicalImport>) {
    let text = node.text().to_string();
    let is_static = text.trim_start().starts_with("import static");
    let is_wildcard = node.children().any(|c| c.kind() == "asterisk");

    let import_type = match (is_static, is_wildcard) {
        (true, _) => "StaticImport",
        (false, true) => "WildcardImport",
        (false, false) => "Import",
    };

    // Find the scoped_identifier or identifier child for path+name
    let (path, name) = if is_wildcard {
        // e.g. import java.util.* — scoped_identifier is "java.util"
        let path = find_child_by_kind(node, "scoped_identifier")
            .map(|n| n.text().to_string())
            .or_else(|| find_child_by_kind(node, "identifier").map(|n| n.text().to_string()))
            .unwrap_or_default();
        (path, Some("*".to_string()))
    } else if let Some(scoped) = find_child_by_kind(node, "scoped_identifier") {
        let scope_part = scoped.field("scope").map(|n| n.text().to_string());
        let name_part = scoped.field("name").map(|n| n.text().to_string());
        (scope_part.unwrap_or_default(), name_part)
    } else if let Some(ident) = find_child_by_kind(node, "identifier") {
        (String::new(), Some(ident.text().to_string()))
    } else {
        return;
    };

    imports.push(CanonicalImport {
        import_type,
        path,
        name,
        alias: None,
        scope_fqn: None,
        range: node_range(node),
    });
}

fn extract_method_invocation(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    refs: &mut Vec<CanonicalReference>,
) {
    let name = if let Some(name_node) = node.field("name") {
        name_node.text().to_string()
    } else {
        return;
    };

    refs.push(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
        expression: None,
    });
}

fn extract_object_creation(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    refs: &mut Vec<CanonicalReference>,
) {
    // `new Foo(...)` — the type is the "type" field
    let name = if let Some(type_node) = node.field("type") {
        type_node.text().to_string()
    } else {
        return;
    };

    refs.push(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
        expression: None,
    });
}

fn find_child_by_kind<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    kind_name: &str,
) -> Option<Node<'a, StrDoc<SupportLang>>> {
    node.children().find(|c| c.kind().as_ref() == kind_name)
}

fn node_range(node: &Node<StrDoc<SupportLang>>) -> Range {
    let start = node.start_pos();
    let end = node.end_pos();
    let bytes = node.range();
    Range::new(
        Position::new(start.line(), start.column(node)),
        Position::new(end.line(), end.column(node)),
        (bytes.start, bytes.end),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(code: &str) -> CanonicalResult {
        JavaCanonicalParser
            .parse_file(code.as_bytes(), "Test.java")
            .unwrap()
            .0
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            r#"
public class Calculator {
    public int add(int a, int b) {
        return a + b;
    }
    public int subtract(int a, int b) {
        return a - b;
    }
}
"#,
        );

        assert_eq!(result.definitions.len(), 3);

        let calc = &result.definitions[0];
        assert_eq!(calc.name, "Calculator");
        assert_eq!(calc.kind, DefKind::Class);
        assert_eq!(calc.fqn.to_string(), "Calculator");
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

        let run = result.definitions.iter().find(|d| d.name == "run").unwrap();
        assert_eq!(run.fqn.to_string(), "com.example.Service.run");
    }

    #[test]
    fn interfaces_and_enums() {
        let result = parse(
            r#"
public interface Runnable {
    void run();
}
public enum Color {
    RED, GREEN, BLUE
}
"#,
        );

        let runnable = result
            .definitions
            .iter()
            .find(|d| d.name == "Runnable")
            .unwrap();
        assert_eq!(runnable.kind, DefKind::Interface);

        let color = result
            .definitions
            .iter()
            .find(|d| d.name == "Color")
            .unwrap();
        assert_eq!(color.kind, DefKind::Class);

        let red = result.definitions.iter().find(|d| d.name == "RED").unwrap();
        assert_eq!(red.kind, DefKind::EnumEntry);
    }

    #[test]
    fn nested_classes() {
        let result = parse(
            r#"
public class Outer {
    public class Inner {
        public void method() {}
    }
}
"#,
        );

        assert_eq!(result.definitions.len(), 3);
        let inner = result
            .definitions
            .iter()
            .find(|d| d.name == "Inner")
            .unwrap();
        assert_eq!(inner.fqn.to_string(), "Outer.Inner");

        let method = result
            .definitions
            .iter()
            .find(|d| d.name == "method")
            .unwrap();
        assert_eq!(method.fqn.to_string(), "Outer.Inner.method");
    }

    #[test]
    fn imports() {
        let result = parse(
            r#"
import java.util.List;
import java.util.*;
import static java.lang.Math.PI;

public class Test {}
"#,
        );

        assert_eq!(result.imports.len(), 3);
        assert!(result.imports.iter().any(|i| i.import_type == "Import"));
        assert!(result
            .imports
            .iter()
            .any(|i| i.import_type == "WildcardImport"));
        assert!(result
            .imports
            .iter()
            .any(|i| i.import_type == "StaticImport"));
    }

    #[test]
    fn method_references() {
        let result = parse(
            r#"
public class App {
    public void run() {
        System.out.println("hello");
        helper();
        new ArrayList();
    }
    private void helper() {}
}
"#,
        );

        let ref_names: Vec<&str> = result.references.iter().map(|r| r.name.as_str()).collect();
        assert!(ref_names.contains(&"println"));
        assert!(ref_names.contains(&"helper"));
        assert!(ref_names.contains(&"ArrayList"));
    }

    #[test]
    fn constructor() {
        let result = parse(
            r#"
public class Foo {
    public Foo(int x) {}
}
"#,
        );

        let ctor = result
            .definitions
            .iter()
            .find(|d| d.name == "Foo" && d.kind == DefKind::Constructor);
        assert!(ctor.is_some());
        assert_eq!(ctor.unwrap().definition_type, "Constructor");
    }

    #[test]
    fn language_and_metadata() {
        let result = parse("public class X {}");
        assert_eq!(result.language, Language::Java);
        assert_eq!(result.extension, "java");
    }
}
