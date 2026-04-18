//! Ruby visitor implementation using ruby-prism for extracting definitions and references

use crate::legacy::parser::imports::{ImportIdentifier, ImportedSymbolInfo};
use crate::legacy::parser::references::{ReferenceInfo, ReferenceTarget};
use crate::legacy::parser::ruby::definitions::{RubyDefinitionInfo, create_definition_from_fqn};
use crate::legacy::parser::ruby::imports::RubyImportedSymbolInfo;
use crate::legacy::parser::ruby::references::expressions::{
    RubyExpressionSymbol, RubySymbolType, extract_symbol_chain_from_node_with_cache,
};
use crate::legacy::parser::ruby::references::types::{
    RubyExpressionMetadata, RubyReferenceInfo, RubyReferenceType, RubyTargetResolution,
};
use crate::legacy::parser::ruby::types::{
    RubyFqn, RubyFqnPart, RubyFqnPartType, RubyImportType, constants,
};
use crate::legacy::parser::ruby::utils::LineOffsetCache;
use crate::utils::Range;
use ruby_prism::{ParseResult, Visit};
use smallvec::SmallVec;
use std::sync::Arc;

/// Type alias for the extraction result tuple
type ExtractionResult = (
    Vec<RubyDefinitionInfo>,
    Vec<RubyImportedSymbolInfo>,
    Vec<RubyReferenceInfo>,
);

const RUBY_VISITOR_STACK_SIZE: usize = 1024 * 1024;

/// Extract definitions, references, and imports from Ruby Prism AST using the Visit trait
pub fn extract_definitions_and_references_from_prism(
    source_code: &str,
    parse_result: &ParseResult<'_>,
) -> ExtractionResult {
    // Fast capacity estimation based on source code size
    let (estimated_definitions, estimated_references) = estimate_capacity_from_source(source_code);

    let mut extractor =
        RubyAstVisitor::with_capacity(source_code, estimated_definitions, estimated_references);

    // Visit the program node to extract all definitions, expressions, and imports
    stacker::maybe_grow(
        crate::legacy::parser::MINIMUM_STACK_REMAINING,
        RUBY_VISITOR_STACK_SIZE,
        || extractor.visit(&parse_result.node()),
    );

    // Shrink vectors to fit their current data size to reduce memory usage
    extractor.shrink_to_fit();

    (
        extractor.definitions,
        extractor.imports,
        extractor.references,
    )
}

fn estimate_capacity_from_source(source_code: &str) -> (usize, usize) {
    // Simple heuristic based on file size - much faster than AST traversal
    let file_size = source_code.len();

    // Rough estimates based on typical Ruby code patterns
    let estimated_definitions = (file_size / 500).max(8); // ~1 definition per 500 chars
    let estimated_references = (file_size / 100).max(16); // ~1 reference per 100 chars

    (estimated_definitions, estimated_references)
}

/// Context for tracking the type of call currently being visited
#[derive(Debug, Clone, Copy, PartialEq)]
enum CallContext {
    /// Currently visiting within a lambda call (like `lambda { ... }`)
    LambdaCall,
    /// Currently visiting within an assignment context
    Assignment,
}

/// Visitor implementation for extracting definitions and references from ruby-prism AST
pub struct RubyAstVisitor<'a> {
    definitions: Vec<RubyDefinitionInfo>,
    references: Vec<
        ReferenceInfo<RubyTargetResolution, RubyReferenceType, RubyExpressionMetadata, RubyFqn>,
    >,
    imports: Vec<RubyImportedSymbolInfo>,
    current_scope: SmallVec<[RubyFqnPart; 8]>,
    source_code: &'a str,
    line_cache: LineOffsetCache,
    /// Stack to track call context (lambda calls, assignments, etc.)
    call_context_stack: SmallVec<[CallContext; 4]>,
}

impl<'a> RubyAstVisitor<'a> {
    pub fn with_capacity(
        source_code: &'a str,
        estimated_definitions: usize,
        estimated_references: usize,
    ) -> Self {
        Self {
            definitions: Vec::with_capacity(estimated_definitions),
            references: Vec::with_capacity(estimated_references),
            imports: Vec::new(),
            current_scope: SmallVec::new(),
            source_code,
            line_cache: LineOffsetCache::new(source_code),
            call_context_stack: SmallVec::new(),
        }
    }

    pub fn new(source_code: &'a str) -> Self {
        Self {
            definitions: Vec::new(),
            references: Vec::new(),
            imports: Vec::new(),
            current_scope: SmallVec::new(),
            source_code,
            line_cache: LineOffsetCache::new(source_code),
            call_context_stack: SmallVec::new(),
        }
    }

    fn create_fqn_part(
        &self,
        node_type: RubyFqnPartType,
        name: &[u8],
        start_offset: usize,
        end_offset: usize,
    ) -> RubyFqnPart {
        let range = self.offset_to_range(start_offset, end_offset);
        RubyFqnPart::new(node_type, String::from_utf8_lossy(name).to_string(), range)
    }

    fn offset_to_range(&self, start_offset: usize, end_offset: usize) -> Range {
        let start_pos = self.line_cache.offset_to_position(start_offset);
        let end_pos = self.line_cache.offset_to_position(end_offset);
        Range::new(start_pos, end_pos, (start_offset, end_offset))
    }

    fn create_definition(
        &mut self,
        node_type: RubyFqnPartType,
        name: &[u8],
        start_offset: usize,
        end_offset: usize,
    ) {
        let mut fqn_parts = self.current_scope.clone();
        let fqn_part = self.create_fqn_part(node_type, name, start_offset, end_offset);
        fqn_parts.push(fqn_part.clone());

        let fqn = RubyFqn::new(fqn_parts);
        let range = self.offset_to_range(start_offset, end_offset);

        let name_string = String::from_utf8_lossy(name).to_string();
        if let Some(definition) = create_definition_from_fqn(node_type, &name_string, fqn, range) {
            self.definitions.push(definition);
        }
    }

    fn push_scope(
        &mut self,
        node_type: RubyFqnPartType,
        name: &[u8],
        start_offset: usize,
        end_offset: usize,
    ) {
        let fqn_part = self.create_fqn_part(node_type, name, start_offset, end_offset);
        self.current_scope.push(fqn_part);
    }

    fn pop_scope(&mut self) {
        self.current_scope.pop();
    }

    fn create_scope_reference(&self) -> RubyFqn {
        RubyFqn::new(self.current_scope.clone())
    }

    /// Push a call context onto the stack
    fn push_call_context(&mut self, context: CallContext) {
        self.call_context_stack.push(context);
    }

    /// Pop the most recent call context from the stack
    fn pop_call_context(&mut self) {
        self.call_context_stack.pop();
    }

    /// Check if we're currently in an assignment context
    fn in_assignment_context(&self) -> bool {
        self.call_context_stack.contains(&CallContext::Assignment)
    }

    /// Check if we're currently in a lambda call context  
    fn in_lambda_call_context(&self) -> bool {
        self.call_context_stack.contains(&CallContext::LambdaCall)
    }

    /// Create a reference directly from symbol chain, avoiding intermediate expression allocation
    fn create_reference_from_symbols(
        &self,
        symbols: Vec<RubyExpressionSymbol>,
        assignment_target: Option<RubyExpressionSymbol>,
        range: Range,
    ) -> ReferenceInfo<RubyTargetResolution, RubyReferenceType, RubyExpressionMetadata, RubyFqn>
    {
        // Determine the reference type based on assignment
        let reference_type = if assignment_target.is_some() {
            RubyReferenceType::Assignment
        } else {
            RubyReferenceType::Call
        };

        // Name is never used for Ruby references
        let name = "".to_string();

        // Create metadata with the symbols for later resolution
        let metadata = Some(RubyExpressionMetadata {
            assignment_target,
            symbols,
        });

        // Create unresolved reference target - will be resolved later in the indexer
        let target = ReferenceTarget::Unresolved();

        ReferenceInfo::new(
            name,
            range,
            target,
            reference_type,
            metadata,
            Some(self.create_scope_reference()),
        )
    }

    /// Check if a node value may contain nested definitions that need traversal
    /// This helps avoid unnecessary double-walking of simple expressions
    fn value_may_contain_definitions(&self, node: &ruby_prism::Node) -> bool {
        match node {
            // These nodes can contain nested definitions
            ruby_prism::Node::LambdaNode { .. }
            | ruby_prism::Node::BlockNode { .. }
            | ruby_prism::Node::CallNode { .. } => true,
            // Simple values don't need traversal after symbol extraction
            ruby_prism::Node::ConstantReadNode { .. }
            | ruby_prism::Node::LocalVariableReadNode { .. }
            | ruby_prism::Node::InstanceVariableReadNode { .. }
            | ruby_prism::Node::ClassVariableReadNode { .. }
            | ruby_prism::Node::GlobalVariableReadNode { .. }
            | ruby_prism::Node::StringNode { .. }
            | ruby_prism::Node::IntegerNode { .. }
            | ruby_prism::Node::FloatNode { .. }
            | ruby_prism::Node::TrueNode { .. }
            | ruby_prism::Node::FalseNode { .. }
            | ruby_prism::Node::NilNode { .. } => false,
            // For other nodes, be conservative and traverse
            _ => true,
        }
    }

    fn is_lambda_assignment(&self, node: &ruby_prism::Node) -> bool {
        match node {
            ruby_prism::Node::LambdaNode { .. } => true,
            ruby_prism::Node::CallNode { .. } => {
                if let Some(call_node) = node.as_call_node() {
                    let name_bytes = call_node.name().as_slice();
                    constants::matches_lambda(name_bytes) && call_node.receiver().is_none()
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn is_proc_assignment(&self, node: &ruby_prism::Node) -> bool {
        match node {
            ruby_prism::Node::CallNode { .. } => {
                if let Some(call_node) = node.as_call_node() {
                    let name_bytes = call_node.name().as_slice();

                    // Handle Proc.new
                    if constants::matches_new(name_bytes)
                        && let Some(receiver) = call_node.receiver()
                        && let Some(const_node) = receiver.as_constant_read_node()
                    {
                        let const_name_bytes = const_node.name().as_slice();
                        return constants::matches_proc_const(const_name_bytes);
                    }

                    // Handle proc (Kernel method)
                    if constants::matches_proc(name_bytes) && call_node.receiver().is_none() {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn is_standalone_proc_call(&self) -> bool {
        // A proc call is standalone if we're not currently in an assignment context
        // Assignment contexts are tracked when visiting *_write_node methods
        !self.in_assignment_context()
    }

    fn visit_with_stack_growth<'pr>(&mut self, node: &ruby_prism::Node<'pr>) {
        stacker::maybe_grow(
            crate::legacy::parser::MINIMUM_STACK_REMAINING,
            RUBY_VISITOR_STACK_SIZE,
            || self.visit(node),
        );
    }

    fn is_lambda_block(&self) -> bool {
        // A block is a lambda block if we're currently in a lambda call context
        self.in_lambda_call_context()
    }

    fn extract_receiver_name(&self, receiver: &ruby_prism::Node) -> Option<Vec<u8>> {
        match receiver {
            ruby_prism::Node::SelfNode { .. } => Some(constants::SELF_NAME.to_vec()),
            ruby_prism::Node::ConstantReadNode { .. } => receiver
                .as_constant_read_node()
                .map(|const_node| const_node.name().as_slice().to_vec()),
            ruby_prism::Node::LocalVariableReadNode { .. } => receiver
                .as_local_variable_read_node()
                .map(|var_node| var_node.name().as_slice().to_vec()),
            ruby_prism::Node::CallNode { .. } => {
                // Handle method calls as receivers (e.g., def receiver_obj.method_name)
                receiver
                    .as_call_node()
                    .map(|call_node| call_node.name().as_slice().to_vec())
            }
            _ => {
                // For more complex receivers, we might need more sophisticated extraction
                None
            }
        }
    }

    fn check_for_import_call(&mut self, node: &ruby_prism::CallNode) {
        let method_name_bytes = node.name().as_slice();

        // Check for import method calls using zero-allocation byte comparisons
        let import_type = if constants::matches_require(method_name_bytes) {
            Some(RubyImportType::Require)
        } else if constants::matches_require_relative(method_name_bytes) {
            Some(RubyImportType::RequireRelative)
        } else if constants::matches_load(method_name_bytes) {
            Some(RubyImportType::Load)
        } else if constants::matches_autoload(method_name_bytes) {
            Some(RubyImportType::Autoload)
        } else {
            None
        };

        if let Some(import_type) = import_type
            && let Some(import_info) = self.extract_import_info(node, import_type)
        {
            self.imports.push(import_info);
        }
    }

    fn extract_import_info(
        &self,
        node: &ruby_prism::CallNode,
        import_type: RubyImportType,
    ) -> Option<RubyImportedSymbolInfo> {
        let arguments = node.arguments()?;
        let args_list = arguments.arguments();
        let mut args_iter = args_list.iter();

        let range =
            self.offset_to_range(node.location().start_offset(), node.location().end_offset());

        // For top-level imports, scope should be None
        let scope = if self.current_scope.is_empty() {
            None
        } else {
            Some(RubyFqn::new(self.current_scope.clone()))
        };

        // Handle autoload specially (has two arguments: symbol and path)
        if import_type == RubyImportType::Autoload {
            let symbol_arg = args_iter.next()?;
            let path_arg = args_iter.next()?;

            // Extract symbol name (e.g., :MyClass)
            let symbol_name = if let Some(symbol_node) = symbol_arg.as_symbol_node() {
                let location = symbol_node.location();
                let start_offset = location.start_offset();
                let end_offset = location.end_offset();
                self.source_code[start_offset..end_offset].to_string()
            } else {
                return None;
            };

            // Extract path (e.g., 'my_class')
            let import_path = if let Some(string_node) = path_arg.as_string_node() {
                let location = string_node.location();
                let start_offset = location.start_offset();
                let end_offset = location.end_offset();
                self.source_code[start_offset..end_offset].to_string()
            } else {
                return None;
            };

            return Some(ImportedSymbolInfo::new(
                import_type,
                import_path, // path
                Some(ImportIdentifier {
                    name: symbol_name,
                    alias: None,
                }),
                range,
                scope,
            ));
        }

        // Handle other import types (require, require_relative, load)
        let first_arg = args_iter.next()?;

        // Extract the import path from string literals
        let import_name = if let Some(string_node) = first_arg.as_string_node() {
            // Get the string content including quotes
            let location = string_node.location();
            let start_offset = location.start_offset();
            let end_offset = location.end_offset();
            self.source_code[start_offset..end_offset].to_string()
        } else {
            // For non-string arguments, use the raw text
            let location = first_arg.location();
            let start_offset = location.start_offset();
            let end_offset = location.end_offset();
            self.source_code[start_offset..end_offset].to_string()
        };

        Some(ImportedSymbolInfo::new(
            import_type,
            import_name.clone(), // path
            Some(ImportIdentifier {
                name: import_name,
                alias: None, // Ruby imports don't typically use aliases like Python
            }),
            range,
            scope,
        ))
    }

    /// Shrink vectors to fit their current data size to reduce memory usage
    fn shrink_to_fit(&mut self) {
        self.definitions.shrink_to_fit();
        self.references.shrink_to_fit();
        self.imports.shrink_to_fit();
    }
}

impl<'a, 'pr> Visit<'pr> for RubyAstVisitor<'a> {
    fn visit_class_node(&mut self, node: &ruby_prism::ClassNode) {
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        self.create_definition(
            RubyFqnPartType::Class,
            node.name().as_slice(),
            start_offset,
            end_offset,
        );
        self.push_scope(
            RubyFqnPartType::Class,
            node.name().as_slice(),
            start_offset,
            end_offset,
        );

        // Use stack-based approach for children
        let mut stack = Vec::with_capacity(4);

        if let Some(superclass) = node.superclass() {
            stack.push(superclass);
        }
        if let Some(body) = node.body() {
            stack.push(body);
        }

        while let Some(child_node) = stack.pop() {
            self.visit_with_stack_growth(&child_node);
        }

        self.pop_scope();
    }

    fn visit_module_node(&mut self, node: &ruby_prism::ModuleNode) {
        let name = node.name().as_slice();
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        self.create_definition(RubyFqnPartType::Module, name, start_offset, end_offset);
        self.push_scope(RubyFqnPartType::Module, name, start_offset, end_offset);

        // Use stack-based approach for children
        if let Some(body) = node.body() {
            self.visit_with_stack_growth(&body);
        }

        self.pop_scope();
    }

    fn visit_def_node(&mut self, node: &ruby_prism::DefNode) {
        let name = node.name().as_slice();
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        // Check if this is a singleton method
        if let Some(receiver) = node.receiver() {
            // Handle singleton methods by extracting receiver name
            let receiver_name = self.extract_receiver_name(&receiver);

            // Special handling for class methods (def self.method inside class/module)
            let is_class_method = receiver_name.as_deref() == Some(constants::SELF_NAME)
                && self.current_scope.iter().any(|part| {
                    part.node_type == RubyFqnPartType::Class
                        || part.node_type == RubyFqnPartType::Module
                });

            if !is_class_method {
                // For singleton methods on objects (def obj.method), add the receiver to scope
                self.push_scope(
                    RubyFqnPartType::Receiver,
                    &receiver_name.unwrap_or_else(|| constants::UNKNOWN.to_vec()),
                    start_offset,
                    end_offset,
                );
            }

            self.create_definition(
                RubyFqnPartType::SingletonMethod,
                name,
                start_offset,
                end_offset,
            );

            self.push_scope(
                RubyFqnPartType::SingletonMethod,
                name,
                start_offset,
                end_offset,
            );
        } else {
            self.create_definition(RubyFqnPartType::Method, name, start_offset, end_offset);

            self.push_scope(RubyFqnPartType::Method, name, start_offset, end_offset);
        }

        // Use stack-based approach for children
        let mut stack = Vec::with_capacity(4);

        if let Some(parameters) = node.parameters() {
            stack.push(parameters.as_node());
        }
        if let Some(body) = node.body() {
            stack.push(body);
        }

        while let Some(child_node) = stack.pop() {
            self.visit_with_stack_growth(&child_node);
        }

        // Pop the method scope
        self.pop_scope();

        // Pop the receiver scope if it was added (for non-class singleton methods)
        if let Some(receiver) = node.receiver() {
            let receiver_name = self.extract_receiver_name(&receiver);
            let is_class_method = receiver_name.as_deref() == Some(constants::SELF_NAME)
                && self.current_scope.iter().any(|part| {
                    part.node_type == RubyFqnPartType::Class
                        || part.node_type == RubyFqnPartType::Module
                });

            if !is_class_method {
                self.pop_scope();
            }
        }
    }

    fn visit_lambda_node(&mut self, node: &ruby_prism::LambdaNode) {
        // Lambda definitions are created by assignment nodes (e.g., var = lambda { ... })
        // This method only handles traversal and context tracking

        // Use stack-based approach for children
        let mut stack = Vec::with_capacity(4);

        if let Some(parameters) = node.parameters() {
            stack.push(parameters);
        }
        if let Some(body) = node.body() {
            stack.push(body);
        }

        // Track lambda context when visiting children
        self.push_call_context(CallContext::LambdaCall);

        while let Some(child_node) = stack.pop() {
            self.visit_with_stack_growth(&child_node);
        }

        // Pop lambda context
        self.pop_call_context();
    }

    fn visit_constant_write_node(&mut self, node: &ruby_prism::ConstantWriteNode) {
        let name = node.name().as_slice();
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();
        let range = self.offset_to_range(start_offset, end_offset);

        // Check if this is a lambda or proc assignment
        let value = node.value();
        if self.is_lambda_assignment(&value) {
            self.create_definition(RubyFqnPartType::Lambda, name, start_offset, end_offset);
        } else if self.is_proc_assignment(&value) {
            self.create_definition(RubyFqnPartType::Proc, name, start_offset, end_offset);
        }

        let name_arc = Arc::from(String::from_utf8_lossy(name).as_ref());
        let assignment_target = Some(RubyExpressionSymbol::new(
            RubySymbolType::Constant,
            name_arc,
            node.name_loc().start_offset(),
            node.name_loc().end_offset(),
            &self.line_cache,
        ));

        // Extract symbols and create reference directly
        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.value(), &mut symbols, &self.line_cache);

        if !symbols.is_empty() {
            let reference = self.create_reference_from_symbols(symbols, assignment_target, range);
            self.references.push(reference);
        }

        // Only visit the value if it can contain nested definitions (lambdas, blocks, etc.)
        // This avoids double-walking simple RHS expressions
        if self.value_may_contain_definitions(&value) {
            // Track that we're entering an assignment context
            self.push_call_context(CallContext::Assignment);
            self.visit_with_stack_growth(&value);
            self.pop_call_context();
        }
    }

    fn visit_local_variable_write_node(&mut self, node: &ruby_prism::LocalVariableWriteNode) {
        let name = node.name().as_slice();
        let name_arc = String::from_utf8_lossy(name).into();
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();
        let range = self.offset_to_range(start_offset, end_offset);

        // Check if this is a lambda or proc assignment
        let value = node.value();
        if self.is_lambda_assignment(&value) {
            self.create_definition(RubyFqnPartType::Lambda, name, start_offset, end_offset);
        } else if self.is_proc_assignment(&value) {
            self.create_definition(RubyFqnPartType::Proc, name, start_offset, end_offset);
        }

        let assignment_target = Some(RubyExpressionSymbol::new(
            RubySymbolType::Identifier,
            name_arc,
            node.name_loc().start_offset(),
            node.name_loc().end_offset(),
            &self.line_cache,
        ));

        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.value(), &mut symbols, &self.line_cache);

        if !symbols.is_empty() {
            let reference = self.create_reference_from_symbols(symbols, assignment_target, range);
            self.references.push(reference);
        }

        // Only visit the value if it can contain nested definitions (lambdas, blocks, etc.)
        // This avoids double-walking simple RHS expressions
        if self.value_may_contain_definitions(&value) {
            // Track that we're entering an assignment context
            self.push_call_context(CallContext::Assignment);
            self.visit_with_stack_growth(&value);
            self.pop_call_context();
        }
    }

    fn visit_instance_variable_write_node(&mut self, node: &ruby_prism::InstanceVariableWriteNode) {
        let name = node.name().as_slice();
        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        // Check if this is a lambda or proc assignment
        let value = node.value();
        if self.is_lambda_assignment(&value) {
            self.create_definition(RubyFqnPartType::Lambda, name, start_offset, end_offset);
        } else if self.is_proc_assignment(&value) {
            self.create_definition(RubyFqnPartType::Proc, name, start_offset, end_offset);
        }

        // Always create expression info for tracking
        let name_arc = String::from_utf8_lossy(name).into();
        let assignment_target = Some(RubyExpressionSymbol::new(
            RubySymbolType::InstanceVariable,
            name_arc,
            node.name_loc().start_offset(),
            node.name_loc().end_offset(),
            &self.line_cache,
        ));

        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.value(), &mut symbols, &self.line_cache);

        if !symbols.is_empty() {
            let range = self.offset_to_range(start_offset, end_offset);
            let reference = self.create_reference_from_symbols(symbols, assignment_target, range);
            self.references.push(reference);
        }

        // Only visit the value if it can contain nested definitions (lambdas, blocks, etc.)
        // This avoids double-walking simple RHS expressions
        if self.value_may_contain_definitions(&value) {
            // Track that we're entering an assignment context
            self.push_call_context(CallContext::Assignment);
            self.visit_with_stack_growth(&value);
            self.pop_call_context();
        }
    }

    fn visit_class_variable_write_node(&mut self, node: &ruby_prism::ClassVariableWriteNode) {
        let name = node.name().as_slice();

        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        // Check if this is a lambda or proc assignment
        let value = node.value();
        if self.is_lambda_assignment(&value) {
            self.create_definition(RubyFqnPartType::Lambda, name, start_offset, end_offset);
        } else if self.is_proc_assignment(&value) {
            self.create_definition(RubyFqnPartType::Proc, name, start_offset, end_offset);
        }

        // Always create expression info for tracking
        let name_arc = String::from_utf8_lossy(name).into();
        let assignment_target = Some(RubyExpressionSymbol::new(
            RubySymbolType::ClassVariable,
            name_arc,
            node.name_loc().start_offset(),
            node.name_loc().end_offset(),
            &self.line_cache,
        ));

        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.value(), &mut symbols, &self.line_cache);

        // Always create a reference for assignments, even if RHS has no symbols (e.g., literals)
        let range = self.offset_to_range(start_offset, end_offset);
        if !symbols.is_empty() {
            let reference = self.create_reference_from_symbols(symbols, assignment_target, range);
            self.references.push(reference);
        } else {
            // Create reference for literal assignments (e.g., @@count = 0)
            let metadata = Some(RubyExpressionMetadata {
                symbols: Vec::new(),
                assignment_target: assignment_target.clone(),
            });
            let target = ReferenceTarget::Unresolved();
            let reference = ReferenceInfo::new(
                "".into(), // Empty name for literal assignments
                range,
                target,
                RubyReferenceType::Assignment,
                metadata,
                Some(self.create_scope_reference()),
            );
            self.references.push(reference);
        }

        // Only visit the value if it can contain nested definitions (lambdas, blocks, etc.)
        // This avoids double-walking simple RHS expressions
        if self.value_may_contain_definitions(&value) {
            // Track that we're entering an assignment context
            self.push_call_context(CallContext::Assignment);
            self.visit_with_stack_growth(&value);
            self.pop_call_context();
        }
    }

    fn visit_global_variable_write_node(&mut self, node: &ruby_prism::GlobalVariableWriteNode) {
        let name = node.name().as_slice();

        let location = node.location();
        let start_offset = location.start_offset();
        let end_offset = location.end_offset();

        // Check if this is a lambda or proc assignment
        let value = node.value();
        if self.is_lambda_assignment(&value) {
            self.create_definition(RubyFqnPartType::Lambda, name, start_offset, end_offset);
        } else if self.is_proc_assignment(&value) {
            self.create_definition(RubyFqnPartType::Proc, name, start_offset, end_offset);
        }

        // Always create expression info for tracking
        let name_arc = String::from_utf8_lossy(name).into();
        let assignment_target = Some(RubyExpressionSymbol::new(
            RubySymbolType::GlobalVariable,
            name_arc,
            node.name_loc().start_offset(),
            node.name_loc().end_offset(),
            &self.line_cache,
        ));

        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.value(), &mut symbols, &self.line_cache);

        // Always create a reference for assignments, even if RHS has no symbols (e.g., literals)
        let range = self.offset_to_range(start_offset, end_offset);
        if !symbols.is_empty() {
            let reference = self.create_reference_from_symbols(symbols, assignment_target, range);
            self.references.push(reference);
        } else {
            // Create reference for literal assignments (e.g., $var = 0)
            let metadata = Some(RubyExpressionMetadata {
                symbols: Vec::new(),
                assignment_target: assignment_target.clone(),
            });
            let target = ReferenceTarget::Unresolved();
            let reference = ReferenceInfo::new(
                "".into(), // Empty name for literal assignments
                range,
                target,
                RubyReferenceType::Assignment,
                metadata,
                Some(self.create_scope_reference()),
            );
            self.references.push(reference);
        }

        // Only visit the value if it can contain nested definitions (lambdas, blocks, etc.)
        // This avoids double-walking simple RHS expressions
        if self.value_may_contain_definitions(&value) {
            // Track that we're entering an assignment context
            self.push_call_context(CallContext::Assignment);
            self.visit_with_stack_growth(&value);
            self.pop_call_context();
        }
    }

    fn visit_call_node(&mut self, node: &ruby_prism::CallNode) {
        let mut symbols = Vec::new();
        extract_symbol_chain_from_node_with_cache(&node.as_node(), &mut symbols, &self.line_cache);

        if !symbols.is_empty() {
            let location = node.location();
            let range = self.offset_to_range(location.start_offset(), location.end_offset());
            let reference = self.create_reference_from_symbols(symbols, None, range);
            self.references.push(reference);
        }

        // Handle standalone Proc.new calls
        let name_bytes = node.name().as_slice();
        if constants::matches_new(name_bytes) {
            if let Some(receiver) = node.receiver()
                && let Some(const_node) = receiver.as_constant_read_node()
            {
                let const_name_bytes = const_node.name().as_slice();
                if constants::matches_proc_const(const_name_bytes) {
                    let location = node.location();
                    let start_offset = location.start_offset();
                    let end_offset = location.end_offset();

                    // Check if this is a standalone Proc.new (not assigned to anything)
                    if self.is_standalone_proc_call() {
                        self.create_definition(
                            RubyFqnPartType::Proc,
                            "Proc.new".as_bytes(),
                            start_offset,
                            end_offset,
                        );
                    }
                }
            }
        } else if constants::matches_proc(name_bytes) && node.receiver().is_none() {
            // Handle standalone proc call (Kernel#proc)
            let location = node.location();
            let start_offset = location.start_offset();
            let end_offset = location.end_offset();

            if self.is_standalone_proc_call() {
                self.create_definition(
                    RubyFqnPartType::Proc,
                    "proc".as_bytes(),
                    start_offset,
                    end_offset,
                );
            }
        }

        // Check for import/require calls
        self.check_for_import_call(node);

        // Determine if this is a lambda call
        let is_lambda_call = constants::matches_lambda(name_bytes) && node.receiver().is_none();

        // Use stack-based approach for children
        let mut stack = Vec::with_capacity(4);

        if let Some(receiver) = node.receiver() {
            stack.push(receiver);
        }
        if let Some(arguments) = node.arguments() {
            stack.push(arguments.as_node());
        }
        if let Some(block) = node.block() {
            stack.push(block);
        }

        // Track lambda context when visiting children
        if is_lambda_call {
            self.push_call_context(CallContext::LambdaCall);
        }

        while let Some(child_node) = stack.pop() {
            self.visit_with_stack_growth(&child_node);
        }

        // Pop lambda context
        if is_lambda_call {
            self.pop_call_context();
        }
    }

    fn visit_block_node(&mut self, node: &ruby_prism::BlockNode) {
        // Only handle blocks with parameters as definitions
        if node.parameters().is_some() {
            let location = node.location();
            let start_offset = location.start_offset();
            let end_offset = location.end_offset();

            // Check if this is a lambda block (parent is lambda call)
            if !self.is_lambda_block() {
                self.create_definition(
                    RubyFqnPartType::Block,
                    constants::BLOCK,
                    start_offset,
                    end_offset,
                );
            }
        }

        // Use stack-based approach for children
        let mut stack = Vec::with_capacity(4);

        if let Some(parameters) = node.parameters() {
            stack.push(parameters);
        }
        if let Some(body) = node.body() {
            stack.push(body);
        }

        while let Some(child_node) = stack.pop() {
            self.visit_with_stack_growth(&child_node);
        }
    }
}

#[cfg(test)]
mod tests {
    use ruby_prism::parse;
    use std::thread;

    use super::*;

    #[test]
    fn test_definition_extraction_basic() -> crate::legacy::parser::Result<()> {
        let code = r#"
class User
  def initialize(name)
    @name = name
  end
  
  def self.find_by_name(name)
    # Implementation
  end
end

module Authentication
  def self.authenticate(user)
    # Implementation
  end
end
"#;

        let result = parse(code.as_bytes());
        let (definitions, _references, _imports) =
            extract_definitions_and_references_from_prism(code, &result);

        assert!(
            !definitions.is_empty(),
            "Should find definitions with prism parser"
        );

        println!("Found {} definitions:", definitions.len());
        for def in &definitions {
            println!("  {:?}: {}", def.definition_type, def.name);
        }

        Ok(())
    }

    #[test]
    fn test_context_tracking() -> crate::legacy::parser::Result<()> {
        let code = r#"
# Assigned lambdas and procs (should NOT generate standalone definitions)
LAMBDA_CONSTANT = lambda { |x| x * 2 }
proc_variable = Proc.new { |y| y + 1 }

# Standalone lambda and proc calls (should generate standalone definitions)
lambda { puts "standalone lambda" }
Proc.new { puts "standalone proc" }
proc { puts "standalone proc (Kernel)" }

# Lambda with blocks - blocks inside lambda should be detected as lambda blocks
lambda_with_block = lambda do |items|
  items.each do |item|  # This block should be detected as a lambda block
    puts item
  end
end

# Regular method call with blocks - blocks should NOT be lambda blocks
[1, 2, 3].each do |num|  # This block should NOT be a lambda block
  puts num
end
"#;

        let result = parse(code.as_bytes());
        let (definitions, _references, _imports) =
            extract_definitions_and_references_from_prism(code, &result);

        let lambda_defs: Vec<_> = definitions
            .iter()
            .filter(|d| {
                d.definition_type == crate::legacy::parser::ruby::types::RubyDefinitionType::Lambda
            })
            .collect();
        let proc_defs: Vec<_> = definitions
            .iter()
            .filter(|d| {
                d.definition_type == crate::legacy::parser::ruby::types::RubyDefinitionType::Proc
            })
            .collect();

        assert_eq!(
            lambda_defs.len(),
            2,
            "Should find exactly 2 lambda definitions"
        );

        let lambda_names: Vec<&str> = lambda_defs.iter().map(|d| d.name.as_str()).collect();
        assert!(
            lambda_names.contains(&"LAMBDA_CONSTANT"),
            "Should find LAMBDA_CONSTANT lambda"
        );
        assert!(
            lambda_names.contains(&"lambda_with_block"),
            "Should find lambda_with_block lambda"
        );

        assert_eq!(proc_defs.len(), 3, "Should find exactly 3 proc definitions");

        let proc_names: Vec<&str> = proc_defs.iter().map(|d| d.name.as_str()).collect();
        assert!(
            proc_names.contains(&"proc_variable"),
            "Should find proc_variable proc (assigned)"
        );
        assert!(
            proc_names.contains(&"Proc.new"),
            "Should find Proc.new proc (standalone)"
        );
        assert!(
            proc_names.contains(&"proc"),
            "Should find proc proc (standalone)"
        );

        Ok(())
    }

    #[test]
    fn test_prism_visitor_grows_stack_on_low_stack() {
        let code = r#"
class Outer
  class Inner
    def call
      value = lambda { |x| x + 1 }
      value.call(1)
    end
  end
end
"#;

        let (definitions, references) = thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(move || {
                let result = parse(code.as_bytes());
                let (definitions, _imports, references) =
                    extract_definitions_and_references_from_prism(code, &result);
                (definitions, references)
            })
            .expect("small-stack thread should start")
            .join()
            .expect("small-stack thread should complete");

        assert!(!definitions.is_empty());
        assert!(!references.is_empty());
    }
}
