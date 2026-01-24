use crate::ruby::utils::LineOffsetCache;
use crate::utils::Range;
use ruby_prism::Node;
use std::sync::Arc;

/// Represents the type of a symbol within a Ruby expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RubySymbolType {
    // A local variable or method call with no explicit receiver (e.g., `user` or `puts 'foo'`)
    Identifier,
    // A constant (e.g., `User` in `User.new`)
    Constant,
    // An instance variable (e.g., `@user`)
    InstanceVariable,
    // A class variable (e.g., `@@count`)
    ClassVariable,
    // A global variable (e.g., `$stdout`)
    GlobalVariable,
    // A method call on a receiver (e.g., `.save` in `user.save`)
    MethodCall,
    // A safe navigation method call (e.g., `&.save` in `user&.save`)
    SafeMethodCall,
}
/// Represents a single, identifiable part of a Ruby expression.
#[derive(Debug, Clone)]
pub struct RubyExpressionSymbol {
    pub symbol_type: RubySymbolType, // Place enum first for better packing
    pub name: Arc<str>,              // Shared string to reduce
    pub range: Range,
}

impl RubyExpressionSymbol {
    pub fn new(
        symbol_type: RubySymbolType,
        name: Arc<str>,
        start_offset: usize,
        end_offset: usize,
        cache: &LineOffsetCache,
    ) -> Self {
        Self {
            symbol_type,
            name,
            range: Range::new(
                cache.offset_to_position(start_offset),
                cache.offset_to_position(end_offset),
                (start_offset, end_offset),
            ),
        }
    }
}

/// Truly stack-based symbol chain extraction using immediate data extraction
pub fn extract_symbol_chain_from_node_with_cache(
    node: &Node<'_>,
    symbols: &mut Vec<RubyExpressionSymbol>,
    cache: &LineOffsetCache,
) {
    // Use a work queue that stores extracted data rather than Node references
    // This completely avoids lifetime issues by extracting all needed data immediately
    let mut work_queue = std::collections::VecDeque::with_capacity(8);

    // Extract data from the root node and add to work queue
    extract_and_queue_node_data(node, &mut work_queue, cache);

    // Process all collected data in FIFO order (maintaining left-to-right order)
    while let Some(symbol) = work_queue.pop_front() {
        symbols.push(symbol);
    }
}

/// Extract data from a node and add to work queue, handling call chains iteratively
fn extract_and_queue_node_data(
    node: &Node<'_>,
    work_queue: &mut std::collections::VecDeque<RubyExpressionSymbol>,
    cache: &LineOffsetCache,
) {
    match node {
        Node::CallNode { .. } => {
            if let Some(call_node) = node.as_call_node() {
                // Process receiver first (left-to-right order)
                if let Some(receiver) = call_node.receiver() {
                    extract_and_queue_node_data(&receiver, work_queue, cache);
                }

                // Then add this method call
                let name = call_node.name().as_slice();
                let location = call_node.location();

                let symbol = RubyExpressionSymbol::new(
                    if call_node.is_safe_navigation() {
                        RubySymbolType::SafeMethodCall
                    } else {
                        RubySymbolType::MethodCall
                    },
                    String::from_utf8_lossy(name).into(),
                    location.start_offset(),
                    location.end_offset(),
                    cache,
                );

                work_queue.push_back(symbol);
            }
        }
        Node::ConstantPathNode { .. } => {
            // ConstantPathNode represents A::B::C syntax
            // For now, let the terminal handler process it as a single symbol
            if let Some(terminal_symbol) = extract_terminal_node_symbol(node, cache) {
                work_queue.push_back(terminal_symbol);
            }
        }
        _ => {
            // Handle terminal node
            if let Some(terminal_symbol) = extract_terminal_node_symbol(node, cache) {
                work_queue.push_back(terminal_symbol);
            }
        }
    }
}

/// Extract symbol data from terminal nodes (non-call nodes)
fn extract_terminal_node_symbol(
    node: &Node<'_>,
    cache: &LineOffsetCache,
) -> Option<RubyExpressionSymbol> {
    match node {
        Node::ConstantReadNode { .. } => node.as_constant_read_node().map(|const_node| {
            let name = const_node.name().as_slice();
            let location = const_node.location();
            RubyExpressionSymbol::new(
                RubySymbolType::Constant,
                String::from_utf8_lossy(name).into(),
                location.start_offset(),
                location.end_offset(),
                cache,
            )
        }),
        Node::LocalVariableReadNode { .. } => node.as_local_variable_read_node().map(|var_node| {
            let name = var_node.name().as_slice();
            let location = var_node.location();
            RubyExpressionSymbol::new(
                RubySymbolType::Identifier,
                String::from_utf8_lossy(name).into(),
                location.start_offset(),
                location.end_offset(),
                cache,
            )
        }),
        Node::InstanceVariableReadNode { .. } => {
            node.as_instance_variable_read_node().map(|var_node| {
                let name = var_node.name().as_slice();
                let location = var_node.location();
                RubyExpressionSymbol::new(
                    RubySymbolType::InstanceVariable,
                    String::from_utf8_lossy(name).into(),
                    location.start_offset(),
                    location.end_offset(),
                    cache,
                )
            })
        }
        Node::ClassVariableReadNode { .. } => node.as_class_variable_read_node().map(|var_node| {
            let name = var_node.name().as_slice();
            let location = var_node.location();
            RubyExpressionSymbol::new(
                RubySymbolType::ClassVariable,
                String::from_utf8_lossy(name).into(),
                location.start_offset(),
                location.end_offset(),
                cache,
            )
        }),
        Node::GlobalVariableReadNode { .. } => {
            node.as_global_variable_read_node().map(|var_node| {
                let name = var_node.name().as_slice();
                let location = var_node.location();
                RubyExpressionSymbol::new(
                    RubySymbolType::GlobalVariable,
                    String::from_utf8_lossy(name).into(),
                    location.start_offset(),
                    location.end_offset(),
                    cache,
                )
            })
        }
        Node::ConstantPathNode { .. } => {
            // For now, extract just the full path as a single constant
            // TODO: Later, properly handle the path structure
            node.as_constant_path_node().map(|path_node| {
                let location = path_node.location();
                // Get the full source text for this constant path
                let start_offset = location.start_offset();
                let end_offset = location.end_offset();

                // Create a placeholder name - TODO: improve this later
                let name = format!("ConstantPath@{start_offset}-{end_offset}");

                RubyExpressionSymbol::new(
                    RubySymbolType::Constant,
                    name.into(),
                    start_offset,
                    end_offset,
                    cache,
                )
            })
        }
        _ => {
            // Unknown node type - might need to add support for additional node types
            None
        }
    }
}

/// Stack-based traversal of a prism node to build a symbol chain.
pub fn extract_symbol_chain_from_node(
    node: &Node<'_>,
    symbols: &mut Vec<RubyExpressionSymbol>,
    source: &str,
) {
    let cache = LineOffsetCache::new(source);
    extract_symbol_chain_from_node_with_cache(node, symbols, &cache);
}
