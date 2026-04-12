use code_graph_config::Language;
use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind,
    DefinitionMetadata, ExpressionStep, Fqn, Position, Range, ReferenceStatus,
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
                let mut d = make_def(node, scope, &name, def_type, def_kind);
                d.metadata = extract_kotlin_class_metadata(node);
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
                let mut d = make_def(node, scope, &name, "Function", DefKind::Function);
                d.metadata = extract_kotlin_function_metadata(node);
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

    let expression = build_kotlin_expression_chain(node);

    refs.push(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
        expression,
    });
}

// ── Metadata extraction ─────────────────────────────────────────

fn extract_kotlin_class_metadata(
    node: &Node<StrDoc<SupportLang>>,
) -> Option<Box<DefinitionMetadata>> {
    let mut super_types = Vec::new();

    // delegation_specifiers contains super types: `: Base(), Interface`
    for child in node.children() {
        if child.kind() == "delegation_specifiers" {
            for spec in child.children() {
                let spec_kind = spec.kind();
                if spec_kind == "delegation_specifier" || spec_kind == "constructor_invocation" {
                    // Extract the type name (first user_type or type_identifier)
                    let type_text = if let Some(user_type) =
                        spec.children().find(|c| c.kind() == "user_type")
                    {
                        user_type.text().to_string()
                    } else if let Some(ctor) = spec
                        .children()
                        .find(|c| c.kind() == "constructor_invocation")
                    {
                        ctor.children()
                            .find(|c| c.kind() == "user_type")
                            .map(|n| n.text().to_string())
                            .unwrap_or_default()
                    } else {
                        spec.text().to_string()
                    };
                    if !type_text.is_empty() && type_text != "," {
                        super_types.push(type_text);
                    }
                } else if spec_kind == "user_type" {
                    super_types.push(spec.text().to_string());
                }
            }
        }
    }

    if super_types.is_empty() {
        return None;
    }

    Some(Box::new(DefinitionMetadata {
        super_types,
        ..Default::default()
    }))
}

fn extract_kotlin_function_metadata(
    node: &Node<StrDoc<SupportLang>>,
) -> Option<Box<DefinitionMetadata>> {
    let mut return_type = None;
    let mut receiver_type = None;

    // Return type: `: Type` after the parameters
    for child in node.children() {
        if child.kind() == "user_type" || child.kind() == "nullable_type" {
            // This could be either receiver type or return type
            // Receiver type comes before the name, return type comes after parameters
            if return_type.is_none() {
                // Check position relative to function_value_parameters
                let params_pos = node
                    .children()
                    .find(|c| c.kind() == "function_value_parameters")
                    .map(|c| c.range().end);

                if params_pos.is_some_and(|p| child.range().start > p) {
                    return_type = Some(child.text().to_string());
                }
            }
        }
    }

    // Receiver type: `Type.functionName` — look for user_type before the simple_identifier
    let name_pos = node
        .children()
        .find(|c| c.kind() == "simple_identifier")
        .map(|c| c.range().start);
    if let Some(name_start) = name_pos {
        for child in node.children() {
            if child.range().start < name_start
                && (child.kind() == "user_type" || child.kind() == "nullable_type")
            {
                receiver_type = Some(child.text().to_string());
                break;
            }
        }
    }

    if return_type.is_none() && receiver_type.is_none() {
        return None;
    }

    Some(Box::new(DefinitionMetadata {
        return_type,
        receiver_type,
        ..Default::default()
    }))
}

fn build_kotlin_expression_chain(node: &Node<StrDoc<SupportLang>>) -> Option<Vec<ExpressionStep>> {
    let callee = node.child(0)?;
    if callee.kind() != "navigation_expression" {
        return None; // bare call, no chain
    }

    let mut steps = Vec::new();
    flatten_kotlin_expression(&callee, &mut steps);

    if steps.len() <= 1 {
        return None;
    }

    // The last step becomes a Call (since this is a call_expression)
    if let Some(last) = steps.last_mut() {
        match last {
            ExpressionStep::Ident(n) | ExpressionStep::Field(n) => {
                *last = ExpressionStep::Call(n.clone());
            }
            _ => {}
        }
    }

    Some(steps)
}

fn flatten_kotlin_expression(node: &Node<StrDoc<SupportLang>>, steps: &mut Vec<ExpressionStep>) {
    match node.kind().as_ref() {
        "simple_identifier" => {
            if steps.is_empty() {
                steps.push(ExpressionStep::Ident(node.text().to_string()));
            } else {
                steps.push(ExpressionStep::Field(node.text().to_string()));
            }
        }
        "this_expression" => {
            steps.push(ExpressionStep::This);
        }
        "super_expression" => {
            steps.push(ExpressionStep::Super);
        }
        "navigation_expression" => {
            // Left side
            if let Some(left) = node.child(0) {
                flatten_kotlin_expression(&left, steps);
            }
            // Right side (after the `.`)
            if let Some(right) = node.children().find(|c| c.kind() == "navigation_suffix") {
                if let Some(id) = right.children().find(|c| c.kind() == "simple_identifier") {
                    steps.push(ExpressionStep::Field(id.text().to_string()));
                }
            }
        }
        "call_expression" => {
            if let Some(callee) = node.child(0) {
                flatten_kotlin_expression(&callee, steps);
                if let Some(last) = steps.last_mut() {
                    match last {
                        ExpressionStep::Ident(n) | ExpressionStep::Field(n) => {
                            *last = ExpressionStep::Call(n.clone());
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {
            steps.push(ExpressionStep::Ident(node.text().to_string()));
        }
    }
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

    // ── Metadata tests ──────────────────────────────────────────

    #[test]
    fn class_super_types() {
        let result = parse(
            r#"
open class Animal
interface Serializable

class Dog : Animal(), Serializable {
}
"#,
        );

        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        if let Some(meta) = &dog.metadata {
            assert!(
                !meta.super_types.is_empty(),
                "Dog should have super_types: {:?}",
                meta.super_types
            );
        }
    }

    #[test]
    fn function_return_type() {
        let result = parse(
            r#"
class Service {
    fun getName(): String = ""
    fun doWork() {}
}
"#,
        );

        let get_name = result
            .definitions
            .iter()
            .find(|d| d.name == "getName")
            .unwrap();
        if let Some(meta) = &get_name.metadata {
            assert_eq!(
                meta.return_type.as_deref(),
                Some("String"),
                "getName return type should be String"
            );
        }
    }

    #[test]
    fn no_metadata_for_simple_class() {
        let result = parse("class Empty");
        let empty = result
            .definitions
            .iter()
            .find(|d| d.name == "Empty")
            .unwrap();
        assert!(
            empty.metadata.is_none(),
            "Empty class should have no metadata"
        );
    }

    // ── Expression chain tests ──────────────────────────────────

    #[test]
    fn simple_call_no_chain() {
        let result = parse(
            r#"
fun main() {
    println("hello")
}
"#,
        );

        let println_ref = result
            .references
            .iter()
            .find(|r| r.name == "println")
            .unwrap();
        assert!(
            println_ref.expression.is_none(),
            "Bare call should have no expression chain"
        );
    }

    #[test]
    fn chained_method_call() {
        let result = parse(
            r#"
fun main() {
    listOf(1, 2, 3).map { it * 2 }
}
"#,
        );

        let map_ref = result.references.iter().find(|r| r.name == "map").unwrap();
        if let Some(chain) = &map_ref.expression {
            assert!(
                chain.len() >= 2,
                "Chain should have multiple steps: {chain:?}"
            );
        }
    }
}
