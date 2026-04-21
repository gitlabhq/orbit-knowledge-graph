use rustc_hash::FxHashMap as HashMap;
use tracing::error;

use crate::legacy::parser::python::symbol_table::visitor::SymbolTableBuilder;
use crate::legacy::parser::python::types::{
    Binding, Connector, MethodType, PythonDefinitionInfo, PythonImportedSymbolInfo, Symbol,
    SymbolChain,
};
use crate::utils::{Range, node_to_range};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

pub fn build_definition_lookup_table(
    definitions: Vec<PythonDefinitionInfo>,
) -> HashMap<Range, PythonDefinitionInfo> {
    definitions
        .into_iter()
        .map(|definition| {
            let range = definition.fqn.parts.last().unwrap().range;
            (range, definition)
        })
        .collect()
}

pub fn build_imported_symbol_lookup_table(
    imported_symbols: Vec<PythonImportedSymbolInfo>,
) -> HashMap<Range, PythonImportedSymbolInfo> {
    imported_symbols
        .into_iter()
        .map(|symbol| (symbol.range, symbol))
        .collect()
}

pub fn get_parameter_name<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> Option<String> {
    let node_kind = node.kind();
    let kind_str = node_kind.as_ref();

    match kind_str {
        // def foo(x)
        "identifier" => Some(node.text().to_string()),
        // def foo(x=10), def foo(*args), def foo(**kwargs)
        "default_parameter" | "list_splat_pattern" | "dictionary_splat_pattern" => node
            .field("identifier")
            .map(|identifier_node| identifier_node.text().to_string()),
        // def foo(x: int), def foo(x: int = 10), def foo(*args: int), def foo(**kwargs: Any)
        "typed_parameter" | "typed_default_parameter" => {
            if let Some(child) = node.child(0) {
                get_parameter_name(&child)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn get_method_type<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> Option<MethodType> {
    // Only use this for nodes confirmed to be methods

    // TODO: We need to check for shadowing of a built-in decorator (e.g. re-assigning `classmethod = foo`)
    // - Will require resolution of the decorator reference, which isn't the job of the symbol table builder
    // - Very unlikely that shadowing would happen in the real world, so this is low priority
    let node_kind = node.kind();
    let kind_str = node_kind.as_ref();

    match kind_str {
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
                        let function_name = function.text().to_string();
                        match function_name.as_str() {
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

pub fn get_containing_class_name<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> Option<String> {
    let mut current = Some(node.clone());

    while let Some(node) = current {
        if node.kind() == "class_definition"
            && let Some(name_node) = node.field("name")
        {
            return Some(name_node.text().to_string());
        }
        current = node.parent();
    }

    None
}

fn has_sufficient_stack(node: &Node<'_, StrDoc<SupportLang>>, context: &'static str) -> bool {
    let remaining_stack = stacker::remaining_stack().unwrap_or(0);
    if remaining_stack < crate::legacy::parser::MINIMUM_STACK_REMAINING {
        error!(
            remaining_stack,
            node_kind = node.kind().as_ref(),
            context,
            "stack limit reached, aborting Python recursive traversal"
        );
        return false;
    }

    true
}

// TODO: Update to return ParsedExpression type
pub fn parse_expression<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> Option<SymbolChain> {
    let mut symbol_chain = Vec::new();

    fn parse_recursive<'a>(
        node: &Node<'a, StrDoc<SupportLang>>,
        symbol_chain: &mut Vec<Symbol>,
    ) -> Result<(), ()> {
        if !has_sufficient_stack(node, "python expression parsing") {
            return Err(());
        }

        // We are ignoring nodes that can be represented as function calls. For example, binary
        // operations like x + y are really dunder method calls, i.e. x.__add__(y). We will handle
        // this in the future.
        let node_kind = node.kind();
        let kind_str = node_kind.as_ref();

        match kind_str {
            "identifier" => {
                // x
                symbol_chain.push(Symbol::Identifier(node.text().to_string()));
                Ok(())
            }
            "attribute" => {
                // x.y
                if let Some(object) = node.field("object") {
                    parse_recursive(&object, symbol_chain)?;
                    if let Some(attribute) = node.field("attribute") {
                        symbol_chain.push(Symbol::Connector(Connector::Attribute));
                        parse_recursive(&attribute, symbol_chain)?;
                    }
                }
                Ok(())
            }
            "call" => {
                // x()
                if let Some(function) = node.field("function") {
                    parse_recursive(&function, symbol_chain)?;
                    symbol_chain.push(Symbol::Connector(Connector::Call));
                }
                Ok(())
            }
            // "subscript" => {
            //     // x[y]
            //     if let Some(value) = node.field("value") {
            //         parse_recursive(&value, symbol_chain)?;
            //         symbol_chain.push(Symbol::Connector(Connector::Index));
            //     }
            //     Ok(())
            // }
            "parenthesized_expression" => {
                // (x).y
                for (index, child) in node.children().enumerate() {
                    if index >= 1 {
                        return Err(());
                    }

                    parse_recursive(&child, symbol_chain)?;
                }
                Ok(())
            }
            "named_expression" => {
                // (x := foo()).y
                if let Some(target) = node.field("name") {
                    parse_recursive(&target, symbol_chain)?;
                }
                Ok(())
            }
            _ => Err(()),
        }
    }

    match parse_recursive(node, &mut symbol_chain) {
        Ok(()) => Some(SymbolChain::new(symbol_chain)),
        Err(()) => None,
    }
}

pub fn is_lambda_method<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> bool {
    // TODO: This handles the `x = lambda` case, which is the most common. It does *not* handle
    // the following cases:
    // - `x = (lambda)`
    // - `x, y = lambda, lambda`
    // - `x, y = (lambda), (lambda)`
    // - `x, y = (lambda, lambda)`
    // - `x = staticmethod(lambda)`

    if node.kind() == "lambda"
        && let Some(parent) = node.parent()
        && parent.kind() == "assignment"
        && let Some(left) = parent.field("left")
    {
        return left.kind() == "identifier";
    }

    false
}

pub fn is_named_lambda(node: &Node<StrDoc<SupportLang>>) -> bool {
    if !has_sufficient_stack(node, "python named lambda detection") {
        return false;
    }

    let node_kind = node.kind();
    let kind_str = node_kind.as_ref();

    match kind_str {
        "parenthesized_expression" => {
            if let Some(child) = node.child(0) {
                is_named_lambda(&child)
            } else {
                false
            }
        }
        "lambda" => true,
        _ => false,
    }
}

/// Parses iteration targets in for loops. Returns a symbol chain for each target.
pub fn parse_for_loop_targets<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> Vec<SymbolChain> {
    fn parse_recursive<'a>(
        node: &Node<'a, StrDoc<SupportLang>>,
        symbol_chains: &mut Vec<SymbolChain>,
    ) {
        if !has_sufficient_stack(node, "python for-loop target parsing") {
            return;
        }

        let node_kind = node.kind();
        let kind_str = node_kind.as_ref();

        match kind_str {
            "identifier" => {
                if let Some(symbol_chain) = parse_expression(node) {
                    symbol_chains.push(symbol_chain);
                }
            }
            "pattern_list" | "list_pattern" | "list_splat_pattern" | "tuple_pattern" => {
                for child in node.children() {
                    parse_recursive(&child, symbol_chains);
                }
            }
            _ => {}
        }
    }

    let mut symbol_chains = Vec::new();
    parse_recursive(node, &mut symbol_chains);

    symbol_chains
}

pub fn parse_assignment_targets<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
) -> Vec<Option<SymbolChain>> {
    fn parse_recursive<'a>(
        node: &Node<'a, StrDoc<SupportLang>>,
        root_node: &Node<'a, StrDoc<SupportLang>>,
        symbol_chains: &mut Vec<Option<SymbolChain>>,
        builder: &mut SymbolTableBuilder,
        should_append: bool,
    ) {
        if !has_sufficient_stack(node, "python assignment target parsing") {
            return;
        }

        let node_kind = node.kind();
        let kind_str = node_kind.as_ref();

        match kind_str {
            // x, y = ...
            "pattern_list" => {
                for child in node.children() {
                    parse_recursive(&child, root_node, symbol_chains, builder, true);
                }
            }
            // *rest = ..., (x, y) = ..., [x, y] = ...
            "list_splat_pattern" | "tuple_pattern" | "list_pattern" => {
                // TODO: We ignore these assignments, for now, by treating them as dead-end bindings

                for child in node.children() {
                    parse_recursive(&child, root_node, symbol_chains, builder, false);
                }

                symbol_chains.push(None);
            }
            // Identifiers, attributes, indexes, etc.
            _ => {
                let symbol_chain = parse_expression(node);

                if should_append {
                    symbol_chains.push(symbol_chain);
                } else if let Some(symbol_chain) = symbol_chain {
                    let binding = Binding::dead_end(node_to_range(root_node));
                    builder.add_binding(symbol_chain, binding);
                }
            }
        }
    }

    let mut symbol_chains = Vec::new();
    parse_recursive(node, node, &mut symbol_chains, builder, true);

    symbol_chains
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::python::symbol_table::test_utils::{
        find_first_node_by_kind, parse_python, run_on_small_stack,
    };
    use crate::legacy::parser::python::types::ScopeType;
    use crate::utils::node_to_range;

    #[test]
    fn test_parse_expression_bails_out_on_low_stack() {
        let result = run_on_small_stack(|| {
            let ast = parse_python("foo.bar()");
            let call = find_first_node_by_kind(&ast, "call").expect("call node should exist");
            parse_expression(&call)
        });

        assert!(result.is_none());
    }

    #[test]
    fn test_parse_for_loop_targets_bails_out_on_low_stack() {
        let result = run_on_small_stack(|| {
            let ast = parse_python("for first, second in items:\n    pass\n");
            let for_statement =
                find_first_node_by_kind(&ast, "for_statement").expect("for_statement should exist");
            let left = for_statement
                .field("left")
                .expect("for loop left side should exist");
            parse_for_loop_targets(&left)
        });

        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_assignment_targets_bails_out_on_low_stack() {
        let result = run_on_small_stack(|| {
            let ast = parse_python("first, second = value\n");
            let assignment =
                find_first_node_by_kind(&ast, "assignment").expect("assignment node should exist");
            let left = assignment
                .field("left")
                .expect("assignment left side should exist");
            let mut builder =
                SymbolTableBuilder::new(node_to_range(&ast.root()), ScopeType::Module, None);

            parse_assignment_targets(&left, &mut builder)
        });

        assert!(result.is_empty());
    }
}
