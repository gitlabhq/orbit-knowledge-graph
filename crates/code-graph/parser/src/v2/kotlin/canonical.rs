use code_graph_config::Language;
use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, Fqn,
    Position, Range, ReferenceStatus,
};
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::CanonicalParser;

const LANG: Language = Language::Kotlin;

#[derive(Default)]
pub struct KotlinCanonicalParser;

impl CanonicalParser for KotlinCanonicalParser {
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
        // Package — file-wide scope, never popped
        "package_header" => {
            if let Some(name) = find_identifier_text(node) {
                scope.push(Arc::from(name.as_str()));
            }
        }

        "class_declaration" => {
            let (def_type, def_kind) = classify_class(node);
            if let Some(name) = find_type_identifier(node) {
                let d = make_def(node, scope, &name, def_type, def_kind);
                scope.push(Arc::from(name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "object_declaration" => {
            if let Some(name) = find_type_identifier(node) {
                let d = make_def(node, scope, &name, "Object", DefKind::Class);
                scope.push(Arc::from(name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "companion_object" => {
            let name = find_type_identifier(node).unwrap_or_else(|| "Companion".to_string());
            let d = make_def(node, scope, &name, "CompanionObject", DefKind::Function);
            scope.push(Arc::from(name.as_str()));
            pushed = true;
            defs.push(d);
        }
        "function_declaration" => {
            if let Some(name) = find_simple_identifier(node) {
                let d = make_def(node, scope, &name, "Function", DefKind::Function);
                scope.push(Arc::from(name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "secondary_constructor" => {
            let name = "<init>".to_string();
            let d = make_def(node, scope, &name, "Constructor", DefKind::Constructor);
            scope.push(Arc::from(name.as_str()));
            pushed = true;
            defs.push(d);
        }
        "property_declaration" => {
            if let Some(name) = find_simple_identifier_child(node) {
                let d = make_def(node, scope, &name, "Property", DefKind::Property);
                defs.push(d);
            }
        }
        "enum_entry" => {
            if let Some(name) = find_simple_identifier(node) {
                let d = make_def(node, scope, &name, "EnumEntry", DefKind::EnumEntry);
                defs.push(d);
            }
        }
        "lambda_literal" | "anonymous_function" => {
            let name = format!("lambda${}", node_range(node).byte_offset.0);
            let d = make_def(node, scope, &name, "Lambda", DefKind::Lambda);
            defs.push(d);
        }

        // Imports
        "import_header" => {
            extract_import(node, imports);
        }

        // References
        "call_expression" => {
            extract_call(node, scope, refs);
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

fn classify_class(node: &Node<StrDoc<SupportLang>>) -> (&'static str, DefKind) {
    // Check for enum_class_body → Enum
    if node.children().any(|c| c.kind() == "enum_class_body") {
        return ("Enum", DefKind::Class);
    }

    // Check for interface keyword before the type_identifier
    if let Some(type_id) = node.children().find(|c| c.kind() == "type_identifier") {
        let prefix_len = type_id.range().start.saturating_sub(node.range().start);
        let prefix = &node.text()[..prefix_len];
        if prefix.contains("interface") {
            return ("Interface", DefKind::Interface);
        }
    }

    // Check modifiers
    if let Some(modifiers) = node.children().find(|c| c.kind() == "modifiers") {
        if let Some(class_mod) = modifiers.children().find(|c| c.kind() == "class_modifier") {
            let text = class_mod.text();
            match text.as_ref() {
                "data" => return ("DataClass", DefKind::Class),
                "value" => return ("ValueClass", DefKind::Class),
                "annotation" => return ("AnnotationClass", DefKind::Interface),
                _ => {}
            }
        }
    }

    ("Class", DefKind::Class)
}

fn make_def(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    name: &str,
    def_type: &'static str,
    kind: DefKind,
) -> CanonicalDefinition {
    CanonicalDefinition {
        definition_type: def_type,
        kind,
        name: name.to_string(),
        fqn: Fqn::from_scope(scope, name, LANG.fqn_separator()),
        range: node_range(node),
        is_top_level: scope.is_empty() || (scope.len() == 1 && scope[0].contains('.')),
        metadata: None,
    }
}

fn find_type_identifier(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    node.children()
        .find(|c| c.kind() == "type_identifier")
        .map(|n| n.text().to_string())
}

fn find_simple_identifier(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    node.field("name")
        .or_else(|| node.children().find(|c| c.kind() == "simple_identifier"))
        .map(|n| n.text().to_string())
}

fn find_simple_identifier_child(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    // For property_declaration, the first simple_identifier child before '=' is the name
    for child in node.children() {
        if child.kind() == "simple_identifier" {
            return Some(child.text().to_string());
        }
        // Stop at the type annotation or assignment
        if child.kind() == ":" || child.kind() == "=" {
            break;
        }
    }
    node.children()
        .find(|c| c.kind() == "simple_identifier")
        .map(|n| n.text().to_string())
}

fn find_identifier_text(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    node.children()
        .find(|c| c.kind() == "identifier" || c.kind() == "simple_identifier")
        .map(|n| n.text().to_string())
}

fn extract_import(node: &Node<StrDoc<SupportLang>>, imports: &mut Vec<CanonicalImport>) {
    let identifier = node
        .children()
        .find(|c| c.kind() == "identifier")
        .map(|n| n.text().to_string());

    let Some(full_path) = identifier else {
        return;
    };

    let is_wildcard = node
        .children()
        .any(|c| c.kind() == "MULT" || c.text() == "*");

    let alias = node
        .children()
        .find(|c| c.kind() == "import_alias")
        .and_then(|alias_node| {
            alias_node
                .children()
                .find(|c| c.kind() == "type_identifier" || c.kind() == "simple_identifier")
                .map(|n| n.text().to_string())
        });

    let import_type = if is_wildcard {
        "WildcardImport"
    } else if alias.is_some() {
        "AliasedImport"
    } else {
        "Import"
    };

    // Split into path and name: com.example.Foo → path="com.example", name="Foo"
    let (path, name) = if is_wildcard {
        (full_path, Some("*".to_string()))
    } else if let Some((p, n)) = full_path.rsplit_once('.') {
        (p.to_string(), Some(n.to_string()))
    } else {
        (String::new(), Some(full_path))
    };

    imports.push(CanonicalImport {
        import_type,
        path,
        name,
        alias,
        scope_fqn: None,
        range: node_range(node),
    });
}

fn extract_call(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    refs: &mut Vec<CanonicalReference>,
) {
    // call_expression has a first child that is the callee
    let callee = match node.child(0) {
        Some(c) => c,
        None => return,
    };

    let name = match callee.kind().as_ref() {
        "simple_identifier" => callee.text().to_string(),
        "navigation_expression" => {
            // foo.bar() — last simple_identifier in the navigation chain
            if let Some(suffix) = callee.children().last() {
                if let Some(id) = suffix.children().find(|c| c.kind() == "simple_identifier") {
                    id.text().to_string()
                } else {
                    suffix.text().to_string()
                }
            } else {
                return;
            }
        }
        _ => return,
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
        KotlinCanonicalParser
            .parse_file(code.as_bytes(), "Test.kt")
            .unwrap()
            .0
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            r#"
class Calculator {
    fun add(a: Int, b: Int): Int = a + b
    fun subtract(a: Int, b: Int): Int = a - b
}
"#,
        );

        assert_eq!(result.definitions.len(), 3);
        let calc = &result.definitions[0];
        assert_eq!(calc.name, "Calculator");
        assert_eq!(calc.kind, DefKind::Class);

        let add = result.definitions.iter().find(|d| d.name == "add").unwrap();
        assert_eq!(add.fqn.to_string(), "Calculator.add");
        assert_eq!(add.kind, DefKind::Function);
    }

    #[test]
    fn package_scoping() {
        let result = parse(
            r#"
package com.example

class Service {
    fun run() {}
}
"#,
        );

        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");

        let run = result.definitions.iter().find(|d| d.name == "run").unwrap();
        assert_eq!(run.fqn.to_string(), "com.example.Service.run");
    }

    #[test]
    fn data_class_and_enum() {
        let result = parse(
            r#"
data class Point(val x: Int, val y: Int)

enum class Color {
    RED, GREEN, BLUE
}
"#,
        );

        let point = result
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.definition_type, "DataClass");
        assert_eq!(point.kind, DefKind::Class);

        let color = result
            .definitions
            .iter()
            .find(|d| d.name == "Color")
            .unwrap();
        assert_eq!(color.definition_type, "Enum");

        let red = result.definitions.iter().find(|d| d.name == "RED").unwrap();
        assert_eq!(red.kind, DefKind::EnumEntry);
    }

    #[test]
    fn interface() {
        let result = parse(
            r#"
interface Runnable {
    fun run()
}
"#,
        );

        let runnable = result
            .definitions
            .iter()
            .find(|d| d.name == "Runnable")
            .unwrap();
        assert_eq!(runnable.kind, DefKind::Interface);
        assert_eq!(runnable.definition_type, "Interface");
    }

    #[test]
    fn object_and_companion() {
        let result = parse(
            r#"
object Singleton {
    fun instance() {}
}

class MyClass {
    companion object {
        fun create() {}
    }
}
"#,
        );

        let singleton = result
            .definitions
            .iter()
            .find(|d| d.name == "Singleton")
            .unwrap();
        assert_eq!(singleton.kind, DefKind::Class);
        assert_eq!(singleton.definition_type, "Object");

        let companion = result
            .definitions
            .iter()
            .find(|d| d.definition_type == "CompanionObject")
            .unwrap();
        assert_eq!(companion.name, "Companion");
    }

    #[test]
    fn imports() {
        let result = parse(
            r#"
import com.example.Foo
import com.example.*
import com.example.Bar as B
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
            .any(|i| i.import_type == "AliasedImport"));
    }

    #[test]
    fn call_references() {
        let result = parse(
            r#"
fun main() {
    println("hello")
    listOf(1, 2, 3).map { it * 2 }
}
"#,
        );

        let names: Vec<&str> = result.references.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"println"));
        assert!(names.contains(&"listOf"));
        assert!(names.contains(&"map"));
    }

    #[test]
    fn language_and_metadata() {
        let result = parse("class X");
        assert_eq!(result.language, Language::Kotlin);
        assert_eq!(result.extension, "kt");
    }
}
