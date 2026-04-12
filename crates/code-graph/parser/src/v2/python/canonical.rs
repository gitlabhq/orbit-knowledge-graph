use code_graph_config::Language;
use code_graph_types::{
    CanonicalDefinition, CanonicalImport, CanonicalReference, CanonicalResult, DefKind, Fqn,
    Position, Range, ReferenceStatus,
};
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::CanonicalParser;

const LANG: Language = Language::Python;

#[derive(Default)]
pub struct PythonCanonicalParser;

impl CanonicalParser for PythonCanonicalParser {
    type Ast = ();

    fn parse_file(&self, source: &[u8], file_path: &str) -> crate::Result<(CanonicalResult, ())> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = LANG.parse_ast(source_str);

        let mut definitions = Vec::new();
        let mut imports = Vec::new();
        let mut references = Vec::new();
        let mut scope_stack: Vec<Arc<str>> = Vec::new();

        walk_node(
            &ast.root(),
            &mut scope_stack,
            &mut definitions,
            &mut imports,
            &mut references,
        );

        let extension = file_path
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_string())
            .unwrap_or_default();

        Ok((CanonicalResult {
            file_path: file_path.to_string(),
            extension,
            file_size: source.len() as u64,
            language: Language::Python,
            definitions,
            imports,
            references,
        }, ()))
    }
}

fn walk_node(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &mut Vec<Arc<str>>,
    definitions: &mut Vec<CanonicalDefinition>,
    imports: &mut Vec<CanonicalImport>,
    references: &mut Vec<CanonicalReference>,
) {
    if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
        return;
    }

    let kind = node.kind();
    let mut pushed_scope = false;

    match kind.as_ref() {
        "class_definition" => {
            if let Some(def) = extract_class(node, scope_stack) {
                scope_stack.push(Arc::from(def.name.as_str()));
                pushed_scope = true;
                definitions.push(def);
            }
        }
        "function_definition" => {
            if let Some(def) = extract_function(node, scope_stack) {
                scope_stack.push(Arc::from(def.name.as_str()));
                pushed_scope = true;
                definitions.push(def);
            }
        }
        "assignment" => {
            if let Some(def) = extract_lambda_assignment(node, scope_stack) {
                definitions.push(def);
            }
        }
        "import_statement" => {
            extract_import_statement(node, imports);
        }
        "import_from_statement" => {
            extract_import_from_statement(node, imports);
        }
        "call" => {
            if let Some(reference) = extract_call_reference(node, scope_stack) {
                references.push(reference);
            }
        }
        _ => {}
    }

    for child in node.children() {
        walk_node(&child, scope_stack, definitions, imports, references);
    }

    if pushed_scope {
        scope_stack.pop();
    }
}

fn build_fqn(scope_stack: &[Arc<str>], name: &str) -> Fqn {
    Fqn::from_scope(scope_stack, name, LANG.fqn_separator())
}

fn scope_fqn(scope_stack: &[Arc<str>]) -> Option<Fqn> {
    Fqn::from_scope_only(scope_stack, LANG.fqn_separator())
}

fn node_range(node: &Node<StrDoc<SupportLang>>) -> Range {
    let start = node.start_pos();
    let end = node.end_pos();
    let byte_range = node.range();
    Range::new(
        Position::new(start.line(), start.column(node)),
        Position::new(end.line(), end.column(node)),
        (byte_range.start, byte_range.end),
    )
}

fn has_decorators(node: &Node<StrDoc<SupportLang>>) -> bool {
    node.parent()
        .is_some_and(|p| p.kind() == "decorated_definition")
}

fn is_in_class_scope(scope_stack: &[Arc<str>]) -> bool {
    !scope_stack.is_empty()
}

fn classify_function(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &[Arc<str>],
) -> (&'static str, DefKind) {
    let is_async = node.children().any(|c| c.kind() == "async");
    let has_decorator = has_decorators(node);
    let is_method = is_in_class_scope(scope_stack);

    let def_type = match (is_method, is_async, has_decorator) {
        (true, true, true) => "DecoratedAsyncMethod",
        (true, true, false) => "AsyncMethod",
        (true, false, true) => "DecoratedMethod",
        (true, false, false) => "Method",
        (false, true, true) => "DecoratedAsyncFunction",
        (false, true, false) => "AsyncFunction",
        (false, false, true) => "DecoratedFunction",
        (false, false, false) => "Function",
    };

    let kind = if is_method {
        DefKind::Method
    } else {
        DefKind::Function
    };

    (def_type, kind)
}

fn extract_class(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &[Arc<str>],
) -> Option<CanonicalDefinition> {
    let name_node = node.field("name")?;
    let name = name_node.text().to_string();
    let def_type = if has_decorators(node) {
        "DecoratedClass"
    } else {
        "Class"
    };

    Some(CanonicalDefinition {
        definition_type: def_type,
        kind: DefKind::Class,
        name: name.clone(),
        fqn: build_fqn(scope_stack, &name),
        range: node_range(node),
        is_top_level: scope_stack.is_empty(),
    })
}

fn extract_function(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &[Arc<str>],
) -> Option<CanonicalDefinition> {
    let name_node = node.field("name")?;
    let name = name_node.text().to_string();
    let (def_type, kind) = classify_function(node, scope_stack);

    let range = if let Some(parent) = node.parent()
        && parent.kind() == "decorated_definition"
    {
        node_range(&parent)
    } else {
        node_range(node)
    };

    Some(CanonicalDefinition {
        definition_type: def_type,
        kind,
        name: name.clone(),
        fqn: build_fqn(scope_stack, &name),
        range,
        is_top_level: scope_stack.is_empty(),
    })
}

fn extract_lambda_assignment(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &[Arc<str>],
) -> Option<CanonicalDefinition> {
    let left = node.field("left")?;
    let right = node.field("right")?;

    if right.kind() != "lambda" {
        return None;
    }

    let left_kind = left.kind();
    if left_kind != "identifier" && left_kind != "attribute" {
        return None;
    }

    let name = left.text().to_string();

    Some(CanonicalDefinition {
        definition_type: "Lambda",
        kind: DefKind::Lambda,
        name: name.clone(),
        fqn: build_fqn(scope_stack, &name),
        range: node_range(node),
        is_top_level: scope_stack.is_empty(),
    })
}

fn extract_import_statement(
    node: &Node<StrDoc<SupportLang>>,
    imports: &mut Vec<CanonicalImport>,
) {
    for child in node.children() {
        match child.kind().as_ref() {
            "dotted_name" => {
                imports.push(CanonicalImport {
                    import_type: "Import",
                    path: child.text().to_string(),
                    name: None,
                    alias: None,
                    scope_fqn: None,
                    range: node_range(node),
                });
            }
            "aliased_import" => {
                if let Some(name_node) = child.field("name") {
                    let alias_node = child.field("alias");
                    imports.push(CanonicalImport {
                        import_type: "AliasedImport",
                        path: name_node.text().to_string(),
                        name: None,
                        alias: alias_node.map(|a| a.text().to_string()),
                        scope_fqn: None,
                        range: node_range(node),
                    });
                }
            }
            _ => {}
        }
    }
}

fn extract_import_from_statement(
    node: &Node<StrDoc<SupportLang>>,
    imports: &mut Vec<CanonicalImport>,
) {
    let module_name = node.field("module_name").map(|n| n.text().to_string());
    let path = module_name.unwrap_or_default();

    let is_relative = node.children().any(|c| c.kind() == "relative_import");
    let is_future = path == "__future__";

    for child in node.children() {
        match child.kind().as_ref() {
            "dotted_name" | "identifier" if child.field("alias").is_none() => {
                // Skip the module_name itself (already extracted as path)
            }
            "wildcard_import" => {
                let import_type = if is_relative {
                    "RelativeWildcardImport"
                } else {
                    "WildcardImport"
                };
                imports.push(CanonicalImport {
                    import_type,
                    path: path.clone(),
                    name: Some("*".to_string()),
                    alias: None,
                    scope_fqn: None,
                    range: node_range(node),
                });
            }
            _ => {}
        }
    }

    // Handle `from X import a, b, c`
    if let Some(import_list) = node
        .children()
        .find(|c| c.kind() == "import_prefix" || c.kind() == "import_from_specifier")
    {
        // This path handles individual specifiers
        let _ = import_list; // handled below
    }

    // Walk children for import specifiers
    for child in node.children() {
        let child_kind = child.kind();
        if child_kind == "dotted_name" || child_kind == "identifier" {
            // Could be the imported symbol name
            // Check if this is not the module_name by comparing text
            let text = child.text().to_string();
            let module_text = node.field("module_name").map(|n| n.text().to_string());
            if module_text.as_deref() != Some(&text) {
                let import_type = if is_future {
                    "FutureImport"
                } else if is_relative {
                    "RelativeImport"
                } else {
                    "FromImport"
                };
                imports.push(CanonicalImport {
                    import_type,
                    path: path.clone(),
                    name: Some(text),
                    alias: None,
                    scope_fqn: None,
                    range: node_range(node),
                });
            }
        } else if child_kind == "aliased_import" {
            if let Some(name_node) = child.field("name") {
                let alias_node = child.field("alias");
                let import_type = if is_future {
                    "AliasedFutureImport"
                } else if is_relative {
                    "AliasedRelativeImport"
                } else {
                    "AliasedFromImport"
                };
                imports.push(CanonicalImport {
                    import_type,
                    path: path.clone(),
                    name: Some(name_node.text().to_string()),
                    alias: alias_node.map(|a| a.text().to_string()),
                    scope_fqn: None,
                    range: node_range(node),
                });
            }
        }
    }
}

fn extract_call_reference(
    node: &Node<StrDoc<SupportLang>>,
    scope_stack: &[Arc<str>],
) -> Option<CanonicalReference> {
    let function_node = node.field("function")?;
    let name = match function_node.kind().as_ref() {
        "identifier" => function_node.text().to_string(),
        "attribute" => {
            // obj.method() — extract "method"
            function_node
                .field("attribute")
                .map(|a| a.text().to_string())?
        }
        _ => return None,
    };

    Some(CanonicalReference {
        reference_type: "Call",
        name,
        range: node_range(node),
        scope_fqn: scope_fqn(scope_stack),
        status: ReferenceStatus::Unresolved,
        target_fqn: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(code: &str) -> CanonicalResult {
        PythonCanonicalParser
            .parse_file(code.as_bytes(), "test.py")
            .unwrap()
    }

    #[test]
    fn classes_and_methods() {
        let result = parse(
            r#"
class Calculator:
    def add(self, a, b):
        return a + b

    def subtract(self, a, b):
        return a - b
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
        assert!(!add.is_top_level);
    }

    #[test]
    fn top_level_functions() {
        let result = parse("def greet(name):\n    print(name)\n");
        assert_eq!(result.definitions.len(), 1);
        assert_eq!(result.definitions[0].kind, DefKind::Function);
        assert_eq!(result.definitions[0].definition_type, "Function");
        assert!(result.definitions[0].is_top_level);
    }

    #[test]
    fn nested_classes() {
        let result = parse(
            r#"
class Outer:
    class Inner:
        def method(self):
            pass
"#,
        );

        assert_eq!(result.definitions.len(), 3);
        assert_eq!(result.definitions[0].fqn.to_string(), "Outer");
        assert_eq!(result.definitions[1].fqn.to_string(), "Outer.Inner");
        assert_eq!(
            result.definitions[2].fqn.to_string(),
            "Outer.Inner.method"
        );
    }

    #[test]
    fn lambda_assignment() {
        let result = parse("square = lambda x: x * x\n");
        assert_eq!(result.definitions.len(), 1);
        assert_eq!(result.definitions[0].kind, DefKind::Lambda);
        assert_eq!(result.definitions[0].fqn.to_string(), "square");
    }

    #[test]
    fn imports() {
        let result = parse(
            r#"
import os
import sys as system
from pathlib import Path
from collections import OrderedDict as OD
"#,
        );

        assert!(result.imports.len() >= 3);
        assert!(result.imports.iter().any(|i| i.path == "os"));
    }

    #[test]
    fn call_references() {
        let result = parse(
            r#"
def foo():
    bar()
    obj.method()
"#,
        );

        assert_eq!(result.definitions.len(), 1);
        assert!(result.references.len() >= 2);

        let names: Vec<&str> = result.references.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"bar"));
        assert!(names.contains(&"method"));
    }

    #[test]
    fn language_and_metadata() {
        let result = parse("x = 1\n");
        assert_eq!(result.language, Language::Python);
        assert_eq!(result.extension, "py");
        assert_eq!(result.file_path, "test.py");
    }
}
