use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, Fqn,
    Language, Position, Range, ReferenceStatus,
};
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::CanonicalParser;

const LANG: Language = Language::CSharp;

pub struct CSharpCanonicalParser;

impl CanonicalParser for CSharpCanonicalParser {
    fn parse_file(&self, source: &[u8], file_path: &str) -> crate::Result<CanonicalResult> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = LANG.parse_ast(source_str);

        let mut defs = Vec::new();
        let mut imports = Vec::new();
        let mut refs = Vec::new();
        let mut scope: Vec<Arc<str>> = Vec::new();

        // Handle file-scoped namespace (C# 10+): `namespace Foo.Bar;`
        if let Some(ns_node) = ast
            .root()
            .children()
            .find(|c| c.kind() == "file_scoped_namespace_declaration")
        {
            if let Some(name_node) = ns_node.field("name") {
                scope.push(Arc::from(name_node.text().to_string().as_str()));
            }
        }

        walk(&ast.root(), &mut scope, &mut defs, &mut imports, &mut refs);

        let extension = file_path
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_string())
            .unwrap_or_default();

        Ok(CanonicalResult {
            file_path: file_path.to_string(),
            extension,
            file_size: source.len() as u64,
            language: LANG,
            definitions: defs,
            imports,
            references: refs,
        })
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
        "namespace_declaration" => {
            if let Some(name) = node.field("name").map(|n| n.text().to_string()) {
                scope.push(Arc::from(name.as_str()));
                pushed = true;
                // Namespace is not a definition we index, just a scope
            }
        }

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
        "struct_declaration" => {
            if let Some(d) = extract_named(node, scope, "Struct", DefKind::Class) {
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
        "delegate_declaration" => {
            if let Some(d) = extract_named(node, scope, "Delegate", DefKind::Class) {
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
        "destructor_declaration" => {
            if let Some(d) = extract_named(node, scope, "Finalizer", DefKind::Constructor) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "method_declaration" => {
            if let Some(d) = extract_method(node, scope) {
                scope.push(Arc::from(d.name.as_str()));
                pushed = true;
                defs.push(d);
            }
        }
        "property_declaration" => {
            if let Some(d) = extract_named(node, scope, "Property", DefKind::Property) {
                defs.push(d);
            }
        }
        "field_declaration" => {
            if let Some(d) = extract_field(node, scope, "Field") {
                defs.push(d);
            }
        }
        "event_field_declaration" => {
            if let Some(d) = extract_field(node, scope, "Event") {
                defs.push(d);
            }
        }
        "operator_declaration" => {
            if let Some(d) = extract_operator(node, scope) {
                defs.push(d);
            }
        }
        "indexer_declaration" => {
            defs.push(CanonicalDefinition {
                definition_type: "Indexer",
                kind: DefKind::Property,
                name: "indexer".to_string(),
                fqn: Fqn::from_scope(scope, "indexer", LANG.fqn_separator()),
                range: node_range(node),
                is_top_level: false,
            });
        }
        "variable_declarator" => {
            // Lambda assignments: `var f = () => ...`
            if node.children().any(|c| c.kind() == "lambda_expression") {
                if let Some(name_node) = node.field("name") {
                    let name = name_node.text().to_string();
                    defs.push(CanonicalDefinition {
                        definition_type: "Lambda",
                        kind: DefKind::Lambda,
                        name: name.clone(),
                        fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
                        range: node_range(node),
                        is_top_level: false,
                    });
                }
            }
        }

        // Imports
        "using_directive" => {
            extract_using(node, scope, imports);
        }

        // References
        "invocation_expression" => {
            extract_invocation(node, scope, refs);
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
        is_top_level: scope.is_empty() || scope.iter().all(|s| s.contains('.')),
    })
}

fn extract_method(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
) -> Option<CanonicalDefinition> {
    let name = node.field("name")?.text().to_string();

    let is_static = node
        .children()
        .any(|c| c.kind() == "modifier" && c.text().as_ref() == "static");

    let def_type = if is_static {
        let is_extension = node
            .field("parameters")
            .and_then(|params| params.children().find(|c| c.kind() == "parameter"))
            .is_some_and(|first| {
                first
                    .children()
                    .any(|c| c.kind() == "modifier" && c.text().as_ref() == "this")
            });
        if is_extension {
            "ExtensionMethod"
        } else {
            "StaticMethod"
        }
    } else {
        "InstanceMethod"
    };

    let kind = DefKind::Method;

    Some(CanonicalDefinition {
        definition_type: def_type,
        kind,
        name: name.clone(),
        fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
        range: node_range(node),
        is_top_level: false,
    })
}

fn extract_field(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    def_type: &'static str,
) -> Option<CanonicalDefinition> {
    let var_decl = node
        .children()
        .find(|c| c.kind() == "variable_declaration")?;
    let var_declarator = var_decl
        .children()
        .find(|c| c.kind() == "variable_declarator")?;
    let name = var_declarator.field("name")?.text().to_string();

    Some(CanonicalDefinition {
        definition_type: def_type,
        kind: DefKind::Property,
        name: name.clone(),
        fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
        range: node_range(node),
        is_top_level: false,
    })
}

fn extract_operator(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
) -> Option<CanonicalDefinition> {
    let mut children = node.children();
    children.find(|c| c.kind() == "operator")?;
    let op_node = children.next()?;
    let name = format!("operator{}", op_node.text());

    Some(CanonicalDefinition {
        definition_type: "Operator",
        kind: DefKind::Property,
        name: name.clone(),
        fqn: Fqn::from_scope(scope, &name, LANG.fqn_separator()),
        range: node_range(node),
        is_top_level: false,
    })
}

fn extract_using(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    imports: &mut Vec<CanonicalImport>,
) {
    let text = node.text().to_string();
    let is_global = text.starts_with("global ");
    let is_static = text.contains(" static ");

    let children: Vec<_> = node.children().collect();
    let identifier_node = children.iter().find(|c| c.kind() == "identifier");
    let qualified_name_node = children.iter().find(|c| c.kind() == "qualified_name");

    let (path, alias) = if let (Some(ident), Some(qname)) = (identifier_node, qualified_name_node) {
        // Alias: `using Foo = Some.Namespace;`
        (qname.text().to_string(), Some(ident.text().to_string()))
    } else if let Some(qname) = qualified_name_node {
        (qname.text().to_string(), None)
    } else if let Some(ident) = identifier_node {
        (ident.text().to_string(), None)
    } else {
        return;
    };

    let import_type = match (is_global, is_static, &alias) {
        (true, _, _) => "Global",
        (_, true, _) => "Static",
        (_, _, Some(_)) => "Alias",
        _ => "Default",
    };

    // Split into module path and symbol name
    let (module_path, symbol) = if let Some((p, s)) = path.rsplit_once('.') {
        (p.to_string(), Some(s.to_string()))
    } else {
        (path, None)
    };

    imports.push(CanonicalImport {
        import_type,
        path: module_path,
        name: symbol,
        alias,
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        range: node_range(node),
    });
}

fn extract_invocation(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    refs: &mut Vec<CanonicalReference>,
) {
    let func_node = match node.child(0) {
        Some(c) => c,
        None => return,
    };

    let name = match func_node.kind().as_ref() {
        "identifier" => func_node.text().to_string(),
        "member_access_expression" => func_node
            .field("name")
            .map(|n| n.text().to_string())
            .unwrap_or_default(),
        "generic_name" => func_node
            .field("name")
            .or_else(|| func_node.children().find(|c| c.kind() == "identifier"))
            .map(|n| n.text().to_string())
            .unwrap_or_default(),
        _ => return,
    };

    if name.is_empty() {
        return;
    }

    refs.push(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
    });
}

fn extract_object_creation(
    node: &Node<StrDoc<SupportLang>>,
    scope: &[Arc<str>],
    refs: &mut Vec<CanonicalReference>,
) {
    let name = node
        .field("type")
        .map(|n| n.text().to_string())
        .unwrap_or_default();

    if name.is_empty() {
        return;
    }

    refs.push(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: Fqn::from_scope_only(scope, LANG.fqn_separator()),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
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
        CSharpCanonicalParser
            .parse_file(code.as_bytes(), "Test.cs")
            .unwrap()
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            r#"
public class Calculator {
    public int Add(int a, int b) { return a + b; }
    public static int Multiply(int a, int b) { return a * b; }
}
"#,
        );

        assert_eq!(result.definitions.len(), 3);
        let calc = &result.definitions[0];
        assert_eq!(calc.name, "Calculator");
        assert_eq!(calc.kind, DefKind::Class);

        let add = result.definitions.iter().find(|d| d.name == "Add").unwrap();
        assert_eq!(add.definition_type, "InstanceMethod");
        assert_eq!(add.fqn.to_string(), "Calculator.Add");

        let mul = result
            .definitions
            .iter()
            .find(|d| d.name == "Multiply")
            .unwrap();
        assert_eq!(mul.definition_type, "StaticMethod");
    }

    #[test]
    fn namespace_scoping() {
        let result = parse(
            r#"
namespace MyApp.Services {
    public class UserService {
        public void GetUser() {}
    }
}
"#,
        );

        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "UserService")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "MyApp.Services.UserService");

        let method = result
            .definitions
            .iter()
            .find(|d| d.name == "GetUser")
            .unwrap();
        assert_eq!(method.fqn.to_string(), "MyApp.Services.UserService.GetUser");
    }

    #[test]
    fn interfaces_enums_structs() {
        let result = parse(
            r#"
public interface IRunnable { void Run(); }
public enum Color { Red, Green, Blue }
public struct Point { public int X; public int Y; }
"#,
        );

        let iface = result
            .definitions
            .iter()
            .find(|d| d.name == "IRunnable")
            .unwrap();
        assert_eq!(iface.kind, DefKind::Interface);

        let color = result
            .definitions
            .iter()
            .find(|d| d.name == "Color")
            .unwrap();
        assert_eq!(color.kind, DefKind::Class);
        assert_eq!(color.definition_type, "Enum");

        let point = result
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.definition_type, "Struct");
    }

    #[test]
    fn using_directives() {
        let result = parse(
            r#"
using System;
using System.Collections.Generic;
using static System.Math;

public class Test {}
"#,
        );

        assert!(result.imports.len() >= 3);
        assert!(result.imports.iter().any(|i| i.import_type == "Default"));
        assert!(result.imports.iter().any(|i| i.import_type == "Static"));
    }

    #[test]
    fn constructor_and_property() {
        let result = parse(
            r#"
public class Foo {
    public string Name { get; set; }
    public Foo(string name) { Name = name; }
}
"#,
        );

        let prop = result
            .definitions
            .iter()
            .find(|d| d.name == "Name")
            .unwrap();
        assert_eq!(prop.kind, DefKind::Property);

        let ctor = result
            .definitions
            .iter()
            .find(|d| d.definition_type == "Constructor")
            .unwrap();
        assert_eq!(ctor.kind, DefKind::Constructor);
    }

    #[test]
    fn invocation_references() {
        let result = parse(
            r#"
public class App {
    public void Run() {
        Console.WriteLine("hello");
        Helper();
        var list = new List<int>();
    }
    private void Helper() {}
}
"#,
        );

        let names: Vec<&str> = result.references.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"WriteLine"));
        assert!(names.contains(&"Helper"));
    }

    #[test]
    fn language_metadata() {
        let result = parse("public class X {}");
        assert_eq!(result.language, Language::CSharp);
        assert_eq!(result.extension, "cs");
    }
}
