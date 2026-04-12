use code_graph_types::{CanonicalDefinition, CanonicalImport, Position, Range};
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::types::{Connector, MethodType, Symbol, SymbolChain};
use super::visitor::SymbolTableBuilder;
use super::types::Binding;

const MINIMUM_STACK_REMAINING: usize = 128 * 1024; // 128 KB

fn has_sufficient_stack(node: &Node<'_, StrDoc<SupportLang>>) -> bool {
    stacker::remaining_stack().unwrap_or(usize::MAX) >= MINIMUM_STACK_REMAINING
}

pub fn node_range(node: &Node<StrDoc<SupportLang>>) -> Range {
    let start = node.start_pos();
    let end = node.end_pos();
    let byte_range = node.range();
    Range::new(
        Position::new(start.line(), start.column(node)),
        Position::new(end.line(), end.column(node)),
        (byte_range.start, byte_range.end),
    )
}

/// Build a lookup table from definition name-node range → definition index.
///
/// In v2, the parser produces `CanonicalDefinition` with ranges covering the
/// entire definition. We key by range to match definitions found during AST
/// walking to their canonical index.
pub fn build_definition_lookup_table(
    definitions: &[CanonicalDefinition],
) -> FxHashMap<Range, usize> {
    definitions
        .iter()
        .enumerate()
        .map(|(idx, def)| (def.range, idx))
        .collect()
}

/// Build a lookup table from import range → import index.
pub fn build_import_lookup_table(
    imports: &[CanonicalImport],
) -> FxHashMap<Range, usize> {
    imports
        .iter()
        .enumerate()
        .map(|(idx, imp)| (imp.range, idx))
        .collect()
}

/// Parse a tree-sitter expression node into a SymbolChain.
///
/// Returns `None` for nodes that aren't expression chains (literals, binary ops, etc.).
pub fn parse_expression(node: &Node<StrDoc<SupportLang>>) -> Option<SymbolChain> {
    let mut symbols = Vec::new();

    fn parse_recursive(
        node: &Node<StrDoc<SupportLang>>,
        symbols: &mut Vec<Symbol>,
    ) -> Result<(), ()> {
        if !has_sufficient_stack(node) {
            return Err(());
        }

        match node.kind().as_ref() {
            "identifier" => {
                symbols.push(Symbol::Identifier(node.text().to_string()));
                Ok(())
            }
            "attribute" => {
                if let Some(object) = node.field("object") {
                    parse_recursive(&object, symbols)?;
                    if let Some(attribute) = node.field("attribute") {
                        symbols.push(Symbol::Connector(Connector::Attribute));
                        parse_recursive(&attribute, symbols)?;
                    }
                }
                Ok(())
            }
            "call" => {
                if let Some(function) = node.field("function") {
                    parse_recursive(&function, symbols)?;
                    symbols.push(Symbol::Connector(Connector::Call));
                }
                Ok(())
            }
            "parenthesized_expression" => {
                for (index, child) in node.children().enumerate() {
                    if index >= 1 {
                        return Err(());
                    }
                    parse_recursive(&child, symbols)?;
                }
                Ok(())
            }
            "named_expression" => {
                if let Some(target) = node.field("name") {
                    parse_recursive(&target, symbols)?;
                }
                Ok(())
            }
            _ => Err(()),
        }
    }

    match parse_recursive(node, &mut symbols) {
        Ok(()) => Some(SymbolChain::new(symbols)),
        Err(()) => None,
    }
}

/// Get parameter name from a tree-sitter parameter node.
pub fn get_parameter_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    match node.kind().as_ref() {
        "identifier" => Some(node.text().to_string()),
        "default_parameter" | "list_splat_pattern" | "dictionary_splat_pattern" => {
            node.field("identifier")
                .map(|n| n.text().to_string())
        }
        "typed_parameter" | "typed_default_parameter" => {
            node.child(0).and_then(|child| get_parameter_name(&child))
        }
        _ => None,
    }
}

/// Get the method type from decorator analysis.
pub fn get_method_type(node: &Node<StrDoc<SupportLang>>) -> Option<MethodType> {
    match node.kind().as_ref() {
        "function_definition" => {
            if let Some(parent_node) = node.parent()
                && parent_node.kind() == "decorated_definition"
            {
                for child in parent_node.children() {
                    if child.kind() == "decorator" {
                        let decorator_name = child.text().trim_start_matches("@").to_string();
                        match decorator_name.as_str() {
                            "classmethod" => return Some(MethodType::Class),
                            "property" => return Some(MethodType::Property),
                            "staticmethod" => return Some(MethodType::Static),
                            _ => continue,
                        }
                    }
                }
                return Some(MethodType::Instance);
            }
            Some(MethodType::Instance)
        }
        "lambda" => {
            if let Some(parent) = node.parent()
                && let Some(grandparent) = parent.parent()
                && grandparent.kind() == "call"
            {
                if let Some(function) = grandparent.field("function") {
                    if function.kind() == "identifier" {
                        match function.text().to_string().as_str() {
                            "classmethod" => return Some(MethodType::Class),
                            "staticmethod" => return Some(MethodType::Static),
                            "property" => return Some(MethodType::Property),
                            _ => return None,
                        }
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            Some(MethodType::Instance)
        }
        _ => None,
    }
}

/// Walk up the tree to find the enclosing class name.
pub fn get_containing_class_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    let mut current = Some(node.clone());
    while let Some(n) = current {
        if n.kind() == "class_definition" {
            if let Some(name_node) = n.field("name") {
                return Some(name_node.text().to_string());
            }
        }
        current = n.parent();
    }
    None
}

/// Check if a node is a lambda (possibly wrapped in parentheses).
pub fn is_named_lambda(node: &Node<StrDoc<SupportLang>>) -> bool {
    if !has_sufficient_stack(node) {
        return false;
    }
    match node.kind().as_ref() {
        "parenthesized_expression" => {
            node.child(0).is_some_and(|child| is_named_lambda(&child))
        }
        "lambda" => true,
        _ => false,
    }
}

/// Check if a lambda is assigned to a name as a class member.
pub fn is_lambda_method(node: &Node<StrDoc<SupportLang>>) -> bool {
    if node.kind() == "lambda"
        && let Some(parent) = node.parent()
        && parent.kind() == "assignment"
        && let Some(left) = parent.field("left")
    {
        return left.kind() == "identifier";
    }
    false
}

/// Parse for-loop iteration targets into symbol chains.
pub fn parse_for_loop_targets(node: &Node<StrDoc<SupportLang>>) -> Vec<SymbolChain> {
    fn parse_recursive(
        node: &Node<StrDoc<SupportLang>>,
        chains: &mut Vec<SymbolChain>,
    ) {
        if !has_sufficient_stack(node) {
            return;
        }
        match node.kind().as_ref() {
            "identifier" => {
                if let Some(chain) = parse_expression(node) {
                    chains.push(chain);
                }
            }
            "pattern_list" | "list_pattern" | "list_splat_pattern" | "tuple_pattern" => {
                for child in node.children() {
                    parse_recursive(&child, chains);
                }
            }
            _ => {}
        }
    }

    let mut chains = Vec::new();
    parse_recursive(node, &mut chains);
    chains
}

/// Parse assignment LHS targets into symbol chains.
/// Returns `None` entries for destructuring patterns.
pub fn parse_assignment_targets(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
) -> Vec<Option<SymbolChain>> {
    fn parse_recursive(
        node: &Node<StrDoc<SupportLang>>,
        root_node: &Node<StrDoc<SupportLang>>,
        chains: &mut Vec<Option<SymbolChain>>,
        builder: &mut SymbolTableBuilder,
        should_append: bool,
    ) {
        if !has_sufficient_stack(node) {
            return;
        }
        match node.kind().as_ref() {
            "pattern_list" => {
                for child in node.children() {
                    parse_recursive(&child, root_node, chains, builder, true);
                }
            }
            "list_splat_pattern" | "tuple_pattern" | "list_pattern" => {
                for child in node.children() {
                    parse_recursive(&child, root_node, chains, builder, false);
                }
                chains.push(None);
            }
            _ => {
                let chain = parse_expression(node);
                if should_append {
                    chains.push(chain);
                } else if let Some(chain) = chain {
                    let binding = Binding::dead_end(node_range(root_node));
                    builder.add_binding(chain, binding);
                }
            }
        }
    }

    let mut chains = Vec::new();
    parse_recursive(node, node, &mut chains, builder, true);
    chains
}
