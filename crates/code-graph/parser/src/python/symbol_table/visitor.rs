use rustc_hash::FxHashMap as HashMap;
use tracing::error;

use crate::python::symbol_table::utils::*;
use crate::python::types::{
    Binding, BindingValue, Connector, MethodType, ParsedExpression, PythonDefinitionInfo,
    PythonFqn, PythonImportedSymbolInfo, ScopeGroup, ScopeGroupType, ScopeType, Symbol,
    SymbolChain, SymbolTableId, SymbolTableTree,
};
use crate::utils::{Position, Range, node_to_range};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

// Structures

pub struct SymbolTableBuilder {
    pub tree: SymbolTableTree,
    pub current_scope: SymbolTableId,
}

impl SymbolTableBuilder {
    pub fn new(
        root_location: Range,
        root_scope_type: ScopeType,
        root_fqn: Option<PythonFqn>,
    ) -> Self {
        let tree = SymbolTableTree::new(root_location, root_scope_type, root_fqn);
        Self {
            tree,
            current_scope: SymbolTableId(0),
        }
    }

    pub fn current_scope(&self) -> SymbolTableId {
        self.current_scope
    }

    pub fn parent_scope(&self) -> Option<SymbolTableId> {
        self.tree.get(self.current_scope)?.parent
    }

    /// Creates a new scope (creates a child table and sets it as current)
    pub fn create_scope(
        &mut self,
        location: Range,
        scope_type: ScopeType,
        fqn: Option<PythonFqn>,
    ) -> SymbolTableId {
        let new_scope = self
            .tree
            .add_child(self.current_scope, location, scope_type, fqn);
        self.current_scope = new_scope;
        new_scope
    }

    /// Changes the current scope
    pub fn enter_scope(&mut self, id: SymbolTableId) {
        self.current_scope = id;
    }

    /// Exit the current scope (moves to parent)
    pub fn exit_scope(&mut self) {
        if let Some(table) = self.tree.get(self.current_scope)
            && let Some(parent_id) = table.parent
        {
            self.current_scope = parent_id;
        }
    }

    /// Add a binding to the current scope
    pub fn add_binding(&mut self, key: SymbolChain, binding: Binding) {
        self.tree.add_binding(self.current_scope, key, binding);
    }

    /// Add a binding to a specific scope
    pub fn add_binding_to(&mut self, scope_id: SymbolTableId, key: SymbolChain, value: Binding) {
        self.tree.add_binding(scope_id, key, value);
    }

    pub fn add_reference(&mut self, symbol_chain: SymbolChain, range: Range) {
        self.tree
            .add_reference(self.current_scope, symbol_chain, range);
    }

    pub fn add_scope_group(&mut self, scope_group: ScopeGroup) {
        self.tree.add_conditional(self.current_scope, scope_group);
    }

    pub fn add_definition(&mut self, definition: PythonDefinitionInfo) {
        self.tree.add_definition(self.current_scope, definition);
    }

    /// Consume the builder and return the completed tree
    pub fn build(self) -> SymbolTableTree {
        self.tree
    }
}

// Main

pub fn build_symbol_table(
    ast: &Root<StrDoc<SupportLang>>,
    definitions: Vec<PythonDefinitionInfo>,
    imported_symbols: Vec<PythonImportedSymbolInfo>,
) -> SymbolTableTree {
    let defs_table = build_definition_lookup_table(definitions);
    let imports_table = build_imported_symbol_lookup_table(imported_symbols);

    let root = ast.root();
    let mut builder = SymbolTableBuilder::new(node_to_range(&root), ScopeType::Module, None);
    visit_children(&root, &mut builder, &defs_table, &imports_table);

    builder.build()
}

// Processors

fn visit_children<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    for child in node.children() {
        parent_bindings.extend(visit_node(&child, builder, defs_table, imports_table));
    }

    parent_bindings
}

fn visit_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let remaining_stack = stacker::remaining_stack().unwrap_or(0);
    if remaining_stack < crate::MINIMUM_STACK_REMAINING {
        error!(
            remaining_stack,
            node_kind = node.kind().as_ref(),
            "stack limit reached, aborting Python symbol table visitor"
        );
        return vec![];
    }

    let node_kind = node.kind();
    let kind_str = node_kind.as_ref();

    let parent_bindings = match kind_str {
        // Import statements
        "import_statement" | "import_from_statement" | "future_import_statement" => {
            visit_import_statement(node, builder, imports_table)
        }

        // Isolated scopes
        // TODO: Handle global and nonlocal keywords
        "class_definition" => visit_class_definition(node, builder, defs_table, imports_table),
        "function_definition" => {
            visit_function_definition(node, builder, defs_table, imports_table)
        }
        "lambda" => visit_lambda_definition(node, builder, defs_table, imports_table),

        // Semi-isolated scopes
        "list_comprehension"
        | "set_comprehension"
        | "dictionary_comprehension"
        | "generator_expression" => visit_comprehension(node, builder, defs_table, imports_table),

        // Non-isolated scopes
        "if_statement" => visit_if_statement(node, builder, defs_table, imports_table),
        "for_statement" => visit_for_loop(node, builder, defs_table, imports_table),
        "while_statement" => visit_while_loop(node, builder, defs_table, imports_table),
        "try_statement" => visit_try_statement(node, builder, defs_table, imports_table),
        "match_statement" => visit_match_statement(node, builder, defs_table, imports_table),
        "conditional_expression" => {
            visit_conditional_expression(node, builder, defs_table, imports_table)
        } // x if condition else y

        // Assignments
        "assignment" => visit_assignment(node, builder, defs_table, imports_table), // x = y
        "augmented_assignment" => {
            visit_augmented_assignment(node, builder, defs_table, imports_table)
        } // x += y
        "type_alias_statement" => {
            visit_type_alias_statement(node, builder, defs_table, imports_table)
        } // type x = tuple[float, float]
        "named_expression" => visit_named_expression(node, builder, defs_table, imports_table), // x := y
        "with_item" => visit_with_item(node, builder, defs_table, imports_table), // with x as y

        // Deletions
        "delete_statement" => visit_deletion(node, builder, defs_table, imports_table), // del x

        // Calls
        "call" => visit_call(node, builder, defs_table, imports_table),

        _ => visit_children(node, builder, defs_table, imports_table),
    };

    // If we're in a class, we propagate bindings up the tree
    if let Some(symbol_table) = builder.tree.get(builder.current_scope)
        && symbol_table.scope_type == ScopeType::Class
    {
        return parent_bindings;
    }

    Vec::new()
}

/// Processes a class definition (class MyClass: ...).
/// Binds the class name to its definition, and binds the 'self' receiver to the definition.
fn visit_class_definition<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    let parent_scope_id = builder.current_scope();

    let mut class_name = String::new();
    if let Some(name_node) = node.field("name") {
        class_name = name_node.text().to_string();
        let name_range = node_to_range(&name_node);
        let definition_info = defs_table.get(&name_range);
        let definition_binding = Binding::new(
            definition_info
                .map(|def_info| BindingValue::Definition(def_info.clone()))
                .unwrap_or(BindingValue::DeadEnd()),
            name_range,
        );

        // Create symbol table for the class scope
        let fqn = definition_info.map(|def_info| def_info.fqn.clone());
        builder.create_scope(node_to_range(node), ScopeType::Class, fqn);

        // Add mapping from scope to definition info
        if let Some(def_info) = definition_info {
            builder.add_definition(def_info.clone());
        }

        // Bind the class name to the class definition, in the parent scope
        let key = SymbolChain::new(vec![Symbol::Identifier(class_name.clone())]);
        builder.add_binding_to(parent_scope_id, key, definition_binding.clone());

        // Bind the class instantiation (MyClass()) to the class definition in the parent scope
        let key = SymbolChain::new(vec![
            Symbol::Identifier(class_name.clone()),
            Symbol::Connector(Connector::Call),
        ]);
        builder.add_binding_to(parent_scope_id, key, definition_binding.clone());

        // Bind the receiver symbol ('self') to the class definition, in the class scope
        let key = SymbolChain::new(vec![Symbol::Receiver()]);
        builder.add_binding(key, definition_binding.clone());
    }

    // Process the class scope
    let child_bindings = visit_children(node, builder, defs_table, imports_table);

    // Add child bindings to class scope and parent scope
    for (symbol_chain, binding) in child_bindings {
        // Add 'self.{symbol_chain}' binding to the class scope
        let self_symbol_chain = SymbolChain::new(
            vec![Symbol::Receiver(), Symbol::Connector(Connector::Attribute)]
                .into_iter()
                .chain(symbol_chain.symbols.clone())
                .collect::<Vec<_>>(),
        );
        builder.add_binding(self_symbol_chain, binding.clone());

        // Add '{ClassName}.{method_name}' binding to the parent scope
        let class_symbol_chain = SymbolChain::new(
            vec![
                Symbol::Identifier(class_name.clone()),
                Symbol::Connector(Connector::Attribute),
            ]
            .into_iter()
            .chain(symbol_chain.symbols.clone())
            .collect::<Vec<_>>(),
        );
        builder.add_binding_to(parent_scope_id, class_symbol_chain.clone(), binding.clone());
        parent_bindings.push((class_symbol_chain.clone(), binding.clone())); // We want to propagate these up the tree
    }

    // Exit the class scope
    builder.exit_scope();

    parent_bindings
}

/// Processes a function definition (def foo(): ...).
/// - Binds the function name to its definition in the parent scope.
/// - Special handling for functions defined inside classes (i.e. methods).
/// - Binds parameter names to dead-ends within the function scope.
/// - TODO: Special handling for property methods (@property, @property.setter). These
///   aren't called like normal methods (e.g. `obj.property` instead of `obj.property()`).
fn visit_function_definition<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    // Bindings to be made in the parent scope
    let mut parent_bindings = Vec::new();

    // Default values are technically declared outside the scope of the function body, so
    // we run the parameters through the normal processor to deal with them
    let mut fqn = None;
    let mut definition_info = None;
    if let Some(parameters_node) = node.field("parameters") {
        visit_node(&parameters_node, builder, defs_table, imports_table);
    }

    if let Some(name_node) = node.field("name") {
        let name = name_node.text().to_string();
        let name_range = node_to_range(&name_node);
        definition_info = defs_table.get(&name_range);
        let definition_binding = Binding::new(
            definition_info
                .map(|def_info| BindingValue::Definition(def_info.clone()))
                .unwrap_or(BindingValue::DeadEnd()),
            name_range,
        );
        fqn = definition_info.map(|def_info| def_info.fqn.clone());

        // Add function definition binding to the symbol table
        let symbol_chain = SymbolChain::new(vec![Symbol::Identifier(name.clone())]);
        builder.add_binding(symbol_chain.clone(), definition_binding.clone());
        parent_bindings.push((symbol_chain.clone(), definition_binding.clone()));
    }

    if let Some(body_node) = node.field("body") {
        builder.create_scope(node_to_range(&body_node), ScopeType::Function, fqn);
        if let Some(definition_info) = definition_info {
            builder.add_definition(definition_info.clone());
        }

        // Process the function parameters
        visit_parameters(node, builder);

        // Process the function body
        if let Some(body_node) = node.field("body") {
            visit_children(&body_node, builder, defs_table, imports_table);
        }

        builder.exit_scope();
    }

    parent_bindings
}

/// Processes lambda definitions (lambda x: ...).
/// - Binds parameter names to dead-ends within the lambda scope.
/// - Special handling for method lambdas happens in visit_assignment.
fn visit_lambda_definition<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let parent_bindings = Vec::new();

    // Default values are technically declared outside the scope of the function body, so
    // we run the parameters through the normal processor to deal with them
    if let Some(parameters_node) = node.field("parameters") {
        visit_node(&parameters_node, builder, defs_table, imports_table);
    }

    // Create symbol table for the lambda scope
    builder.create_scope(node_to_range(node), ScopeType::Lambda, None);

    // Process the lambda parameters
    visit_parameters(node, builder);

    // Process the lambda body
    if let Some(body_node) = node.field("body") {
        visit_children(&body_node, builder, defs_table, imports_table);
    }

    builder.exit_scope();

    parent_bindings
}

/// Processes function parameters.
/// - Binds parameter names to dead-ends because their values are ambiguous until runtime.
/// - The exception is implicit parameters in instance and class methods, like 'self' and 'cls'.
///   We bind these to the class definition or receiver.
/// - TODO: Handle the tuple pattern parameters (def foo((x, y)): ...)
///   - Low priority since these were deprecated in Python 3
fn visit_parameters<'a>(node: &Node<'a, StrDoc<SupportLang>>, builder: &mut SymbolTableBuilder) {
    let parent_scope_id = builder.parent_scope();

    if let Some(parameters_node) = node.field("parameters") {
        let range = node_to_range(&parameters_node);

        let parameters = parameters_node
            .children()
            .filter_map(|n| get_parameter_name(&n));
        for (index, parameter_name) in parameters.enumerate() {
            // Handle implicit parameters in class methods
            if index == 0
                && let Some(parent_scope_id) = parent_scope_id
                && let Some(parent_scope) = builder.tree.get(parent_scope_id)
                && parent_scope.scope_type == ScopeType::Class
            {
                // A method is a function definition or a named lambda inside a class
                if !(is_lambda_method(node) || node.kind() == "function_definition") {
                    continue;
                }

                match get_method_type(node) {
                    Some(MethodType::Instance) | Some(MethodType::Property) => {
                        // Add 'self' binding to the function symbol table
                        let key =
                            SymbolChain::new(vec![Symbol::Identifier(parameter_name.clone())]);
                        let value = Binding::new(
                            BindingValue::SymbolChain(SymbolChain::new(vec![Symbol::Receiver()])),
                            range,
                        );
                        builder.add_binding(key, value);
                    }
                    Some(MethodType::Class) => {
                        // Add 'cls' binding to the function symbol table
                        if let Some(class_name) = get_containing_class_name(node) {
                            let key =
                                SymbolChain::new(vec![Symbol::Identifier(parameter_name.clone())]);
                            let value = Binding::new(
                                BindingValue::SymbolChain(SymbolChain::new(vec![
                                    Symbol::Identifier(class_name.clone()),
                                ])),
                                range,
                            );
                            builder.add_binding(key, value);
                        }
                    }
                    _ => {}
                }

                continue;
            }

            // We bind all other parameters to "dead ends" because we cannot
            // resolve them further without inspecting specific function calls
            // and/or making assumptions about runtime behavior
            let key = SymbolChain::new(vec![Symbol::Identifier(parameter_name)]);
            let value = Binding::dead_end(range);
            builder.add_binding(key, value);
        }
    }
}

/// Processes a list/set/dictionary comprehension or generator expression.
/// - Binds the iteration target to a dead end.
/// - TODO: Iteration target bindings should be marked as "isolated" or "local" to indicate that
///   they cannot be accessed outside the comprehension scope.
fn visit_comprehension<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    // TODO: What about nested for loops?
    // TODO: What about nested comprehensions? Will these be handled properly?

    let parent_bindings = Vec::new();
    builder.create_scope(node_to_range(node), ScopeType::Comprehension, None);

    for for_in_clause in node.children().filter(|n| n.kind() == "for_in_clause") {
        // We process these the same way we process for loops, and with the same limitations (see
        // visit_for_loop for more information)

        // left_node == `x` in `for x in stuff`
        if let Some(left_node) = for_in_clause.field("left") {
            let targets = parse_for_loop_targets(&left_node);
            visit_node(&left_node, builder, defs_table, imports_table);

            // right_node == `stuff` in `for x in stuff`
            if let Some(right_node) = for_in_clause.field("right") {
                // Bind each iteration target to a dead-end (holdover until we handle dunder methods, like `__iter__` and `__next__`)
                for target in targets {
                    let location = node_to_range(&right_node);
                    let binding = Binding::dead_end(location);
                    builder.add_binding(target, binding);
                }

                visit_node(&right_node, builder, defs_table, imports_table);
            }
        }
    }

    // No need to create a separate scope, since if the condition is false, the comprehension never executes,
    // and if it's true at least once, the comprehension will execute –– therefore this is on the same branch
    // as the comprehension scope
    for if_clause in node.children().filter(|n| n.kind() == "if_clause") {
        visit_node(&if_clause, builder, defs_table, imports_table);
    }

    if let Some(body) = node.field("body") {
        visit_node(&body, builder, defs_table, imports_table);
    }

    builder.exit_scope();

    parent_bindings
}

/// Processes an if block.
/// - Creates a new symbol table for the if statement body (block).
fn visit_if_statement<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    // Will always be empty b/c we cannot propagate conditional bindings up the tree
    let mut parent_bindings = Vec::new();
    let mut scope_ids = Vec::new();

    // Process the condition outside the scope of the if block
    if let Some(condition) = node.field("condition") {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    // Process if body
    if let Some(block) = node.field("consequence") {
        let if_scope_id = builder.create_scope(node_to_range(&block), ScopeType::If, None);
        visit_node(&block, builder, defs_table, imports_table);
        builder.exit_scope();
        scope_ids.push(if_scope_id);
    }

    // Handles elif and else clauses
    for alt_node in node.field_children("alternative") {
        let alt_kind = alt_node.kind();
        let kind_str = alt_kind.as_ref();

        match kind_str {
            "elif_clause" => {
                let elif_scope_id =
                    builder.create_scope(node_to_range(&alt_node), ScopeType::Elif, None);

                // Process the condition inside the scope of the elif block
                if let Some(condition) = alt_node.field("condition") {
                    visit_node(&condition, builder, defs_table, imports_table);
                }

                // Process elif body
                if let Some(block) = alt_node.field("consequence") {
                    visit_node(&block, builder, defs_table, imports_table);
                }

                builder.exit_scope();
                scope_ids.push(elif_scope_id);
            }
            "else_clause" => {
                // Process else body
                if let Some(block) = alt_node.field("body") {
                    let else_scope_id =
                        builder.create_scope(node_to_range(&alt_node), ScopeType::Else, None);
                    visit_node(&block, builder, defs_table, imports_table);
                    builder.exit_scope();
                    scope_ids.push(else_scope_id);
                }
            }
            _ => {
                visit_node(&alt_node, builder, defs_table, imports_table);
            }
        }
    }

    // Group together branches into one control flow group
    builder.add_scope_group(ScopeGroup::new(
        node_to_range(node),
        scope_ids,
        ScopeGroupType::If,
    ));

    parent_bindings
}

/// Processes a for loop.
/// - Notably, if the iterable is empty, the loop won't execute and none of the bindings will take effect.
///   Therefore, we treat this as a non-isolated (conditional) scope.
/// - We bind iteration targets to dead ends (until dunder methods, like `__iter__`, are handled).
/// - TODO: Find `break` statements in the loop body and treat all the code underneath as conditional
fn visit_for_loop<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(body_node) = node.field("body") {
        let for_scope_id = builder.create_scope(node_to_range(&body_node), ScopeType::For, None);

        if let Some(left_node) = node.field("left") {
            // `x` in `for x in stuff`
            let targets = parse_for_loop_targets(&left_node);
            if let Some(right_node) = node.field("right") {
                // `stuff` in `for x in stuff`
                for target in targets {
                    // TODO: Ensure binding location is *inside* the for loop body
                    let location = node_to_range(&right_node);
                    let binding = Binding::dead_end(location);
                    builder.add_binding(target, binding);

                    // Iteration targets are marked as dead ends as a holdover until dunder methods are
                    // handled. Technically, `for target in stuff` -> `target = stuff.__iter__().__next__()`.
                }

                // Iteration target processed in the loop scope, since if the iterable is empty, then
                // the target never resolves
                visit_node(&left_node, builder, defs_table, imports_table);

                // Iterable processed in the parent scope since it's not conditional
                builder.exit_scope();
                parent_bindings.extend(visit_node(&right_node, builder, defs_table, imports_table));
                builder.enter_scope(for_scope_id);
            }
        }

        // Process the loop body
        visit_node(&body_node, builder, defs_table, imports_table);

        builder.exit_scope();
        builder.add_scope_group(ScopeGroup::new(
            node_to_range(&body_node),
            vec![for_scope_id],
            ScopeGroupType::Loop,
        ));
    }

    // Else blocks execute if the loop doesn't break (or if the loop never executes at all)
    if let Some(else_node) = node.field("alternative") {
        parent_bindings.extend(visit_children(
            &else_node,
            builder,
            defs_table,
            imports_table,
        ));
    }

    parent_bindings
}

/// Processes a while loop.
fn visit_while_loop<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    // Process the condition in the parent scope because it's not conditional
    if let Some(condition) = node.field("condition") {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    // Process the while loop body (this is conditional)
    if let Some(block) = node.field("body") {
        let while_scope_id = builder.create_scope(node_to_range(&block), ScopeType::While, None);
        visit_node(&block, builder, defs_table, imports_table);
        builder.exit_scope();
        builder.add_scope_group(ScopeGroup::new(
            node_to_range(&block),
            vec![while_scope_id],
            ScopeGroupType::Loop,
        ));
    }

    // Else blocks execute if the loop doesn't break (or if the loop never executes at all)
    if let Some(else_node) = node.field("alternative") {
        parent_bindings.extend(visit_children(
            &else_node,
            builder,
            defs_table,
            imports_table,
        ));
    }

    parent_bindings
}

/// Processes a try block.
/// - Creates a new symbol table for the `try` block (`except`, `except*`, and `finally`
///   blocks are processed separately)
/// - Also processes the else block, if it exists, in the same scope as the try block
/// - TODO: try and except blocks are not always separate scopes. Technically, *some* of the try
///   block coulud execute before an exception is raised, in which case *some* bindings in the try
///   block would take effect in the same scope as the bindings made in the except block. We should
///   treat each line in the try block as a control flow branch.
fn visit_try_statement<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let parent_bindings = Vec::new();
    let mut scope_ids = Vec::new();

    // IMPORTANT: The range of the try scope only covers the try body, but we also store bindings from
    // the else clause in the same scope. These bindings occur outside the range of the try body.

    // Handle try and else blocks
    if let Some(body) = node.field("body") {
        let try_scope_id = builder.create_scope(node_to_range(&body), ScopeType::Try, None);
        visit_node(&body, builder, defs_table, imports_table);

        // else block executes if the try block does not raise an exception, so we include it in the try
        // block scope
        for else_clause in node.children().filter(|n| n.kind() == "else_clause") {
            visit_node(&else_clause, builder, defs_table, imports_table);
        }
        builder.exit_scope();
        scope_ids.push(try_scope_id);
    }

    // Process except and finally blocks
    let mut last_conditional_node = None;
    for child in node.children() {
        let child_kind = child.kind();
        let kind_str = child_kind.as_ref();

        match kind_str {
            "block" | "else_clause" => {
                last_conditional_node = Some(child);
            }
            "except_clause" | "except_group_clause" => {
                let except_scope_id =
                    builder.create_scope(node_to_range(&child), ScopeType::Except, None);

                // Handle as-pattern aliasing
                for as_pattern in child.children().filter(|n| n.kind() == "as_pattern") {
                    if let Some(alias_node) = as_pattern.field("alias")
                        && let Some(alias_node) = alias_node.child(0)
                        && let Some(alias) = parse_expression(&alias_node)
                    {
                        let child_kind = child.kind();
                        let kind_str = child_kind.as_ref();

                        match kind_str {
                            "except_clause" => {
                                if let Some(value_node) = as_pattern.child(0)
                                    && value_node.kind() != "as_pattern_target"
                                {
                                    let value = if let Some(mut value_chain) =
                                        parse_expression(&value_node)
                                    {
                                        value_chain
                                            .symbols
                                            .push(Symbol::Connector(Connector::Call)); // `except ExceptionType as e` -> `e = ExceptionType()`
                                        BindingValue::SymbolChain(value_chain)
                                    } else {
                                        BindingValue::DeadEnd()
                                    };

                                    let binding = Binding::new(value, node_to_range(&as_pattern));
                                    builder.add_binding(alias, binding);
                                }
                            }
                            "except_group_clause" => {
                                // Caught exception is always an `ExceptionGroup` instance, which is a built-in, so we
                                // just mark the alias as a dead-end
                                let binding = Binding::dead_end(node_to_range(&as_pattern));
                                builder.add_binding(alias, binding);
                            }
                            _ => {}
                        }
                    }
                }

                visit_children(&child, builder, defs_table, imports_table);
                builder.exit_scope();
                scope_ids.push(except_scope_id);
                last_conditional_node = Some(child);
            }
            "finally_clause" => {
                visit_node(&child, builder, defs_table, imports_table);
            }
            _ => {}
        }
    }

    // Add scope group
    let group_location = if let Some(last_node) = last_conditional_node {
        // We exclude the `finally` clause from the group range, since it's not conditional
        let start_pos = node.start_pos();
        let end_pos = last_node.end_pos();

        Range::new(
            Position::new(start_pos.line(), start_pos.column(node)),
            Position::new(end_pos.line(), end_pos.column(&last_node)),
            (node.range().start, last_node.range().end),
        )
    } else {
        node_to_range(node)
    };
    builder.add_scope_group(ScopeGroup::new(
        group_location,
        scope_ids,
        ScopeGroupType::Try,
    ));

    parent_bindings
}

/// Processes the cases inside a match statement.
/// - Creates a symbol table for each case block.
/// - Creates a scope group for the case scopes.
/// - TODO: Handle bindings for all the case patterns, e.g. for `match expr(): ...`:
///   - `case x` -> `x = expr()`
///   - `case [x, y]` -> `x = None, y = None`
///   - etc.
fn visit_match_statement<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let parent_bindings = Vec::new();

    // Process subject (`expr()` in `match expr(): ...`) in parent scope since it's not conditional
    if let Some(subject_node) = node.field("subject") {
        visit_node(&subject_node, builder, defs_table, imports_table);
    }

    // Process body of the match statement
    if let Some(body_node) = node.field("body") {
        let mut scope_ids = Vec::new();
        for child in body_node.children() {
            if child.kind() == "case_clause" {
                // Check if we have exactly one pattern that is the wildcard "_"
                let mut case_patterns = child.children().filter(|n| n.kind() == "case_pattern");
                let scope_type = match (case_patterns.next(), case_patterns.next()) {
                    (Some(first), None) if first.text() == "_" => ScopeType::DefaultCase,
                    _ => ScopeType::Case,
                };

                // Process body of the case statement as its own scope since it's conditional
                let case_scope_id = builder.create_scope(node_to_range(&child), scope_type, None);
                for case_child in child.children().filter(|n| n.kind() == "block") {
                    visit_node(&case_child, builder, defs_table, imports_table);
                }

                // Process non-body nodes (none should exist) in parent scope
                builder.exit_scope();
                for case_child in node.children().filter(|n| n.kind() != "block") {
                    visit_node(&case_child, builder, defs_table, imports_table);
                }

                scope_ids.push(case_scope_id);
            }
        }

        builder.add_scope_group(ScopeGroup::new(
            node_to_range(&body_node),
            scope_ids,
            ScopeGroupType::Match,
        ));
    }

    parent_bindings
}

/// Processes a conditional expression (x if condition else y).
/// - Creates scopes (symbol tables) for the if and else expressions if either contains an assignment
///   (via a walrus operator). Otherwise, no new scopes are created.
fn visit_conditional_expression<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    let has_walrus_operator = |node: &Node<'a, StrDoc<SupportLang>>| {
        node.dfs().any(|child| child.kind() == "named_expression")
    };
    let left_branch = node.child(0);
    let right_branch = node.child(4);

    // We create separate scopes for branches if either has an assignment (via a walrus operator)
    let needs_branch_scopes = left_branch.as_ref().is_some_and(has_walrus_operator)
        || right_branch.as_ref().is_some_and(has_walrus_operator);

    if needs_branch_scopes {
        let mut scope_ids = Vec::new();

        // Process left branch with its own scope
        if let Some(left) = left_branch {
            let if_scope_id = builder.create_scope(node_to_range(&left), ScopeType::If, None);
            visit_node(&left, builder, defs_table, imports_table);
            builder.exit_scope();
            scope_ids.push(if_scope_id);
        }

        // Process right branch with its own scope
        if let Some(right) = right_branch {
            let else_scope_id = builder.create_scope(node_to_range(&right), ScopeType::Else, None);
            visit_node(&right, builder, defs_table, imports_table);
            builder.exit_scope();
            scope_ids.push(else_scope_id);
        }

        builder.add_scope_group(ScopeGroup::new(
            node_to_range(node),
            scope_ids,
            ScopeGroupType::If,
        ));
    } else {
        // Process branches without creating new scopes
        if let Some(left) = left_branch {
            visit_node(&left, builder, defs_table, imports_table);
        }
        if let Some(right) = right_branch {
            visit_node(&right, builder, defs_table, imports_table);
        }
    }

    // Always process the condition in the current scope (even if it contains a walrus operator)
    if let Some(condition) = node.child(2) {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    parent_bindings
}

/// Processes an import statement.
/// - Binds the imported symbol name to its `ImportedSymbolInfo`.
fn visit_import_statement<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    // Parent bindings
    let mut parent_bindings = Vec::new();

    for child in node.children() {
        let child_kind = child.kind();
        let kind_str = child_kind.as_ref();

        let range = match kind_str {
            "dotted_name" => Some(node_to_range(&child)),
            "aliased_import" => child
                .field("name")
                .map(|name_node| node_to_range(&name_node)),
            _ => None,
        };

        if let Some(range) = range
            && let Some(imported_symbol_info) = imports_table.get(&range)
            && let Some(ref identifier) = imported_symbol_info.identifier
        {
            let name = identifier.alias.clone().unwrap_or(identifier.name.clone());
            let key = SymbolChain::new(vec![Symbol::Identifier(name.clone())]);
            let binding = Binding::new(
                BindingValue::ImportedSymbol(imported_symbol_info.clone()),
                range,
            );
            builder.add_binding(key.clone(), binding.clone());
            parent_bindings.push((key.clone(), binding.clone()));
        }
    }

    parent_bindings
}

/// Processes an assignment statement (`x = ...`, `x, y = ...`, etc.).
/// - Binds targets to their corresponding values in the symbol table.
/// - Handles many special cases:
///   - Chained assignments `x = y = z = 10`
///   - Lambda assignments `x = lambda: pass`
///   - Class-level assignments `class MyClass: ... var = 0` -> `MyClass.var = 0, self.var = 0`
/// - TODO: Handle pattern-matching against data structures (e.g. `[x, y] = [1, 2], (x, y) = (1, 2)`)
/// - TODO: Handle conditional values (e.g. `x = y if condition else z`)
fn visit_assignment<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    // Process the RHS of the assignment
    let mut values: Vec<ParsedExpression> = Vec::new();
    if let Some(right_node) = node.field("right") {
        let right_node_kind = right_node.kind();
        let kind_str = right_node_kind.as_ref();

        match kind_str {
            "assignment" => {
                // x = y = z = 10
                // We treat the target of the nested assignment as the value of this assignment
                if let Some(right_node_target) = right_node.field("left") {
                    let symbol_chains = parse_assignment_targets(&right_node_target, builder);
                    for symbol_chain in symbol_chains {
                        if let Some(symbol_chain) = symbol_chain {
                            values.push(ParsedExpression::SymbolChain(symbol_chain));
                        } else {
                            values.push(ParsedExpression::Ignored());
                        }
                    }
                }
            }
            "expression_list" => {
                // x, y = 10, 20
                for child in right_node.children() {
                    let symbol_chain = if let Some(parse_result) = parse_expression(&child) {
                        ParsedExpression::SymbolChain(parse_result)
                    } else {
                        ParsedExpression::Ignored()
                    };
                    values.push(symbol_chain);
                }
            }
            "list" | "tuple" => {
                // [x, y] = [10, 20], (x, y) = (10, 20)
                values.push(ParsedExpression::Ignored());
            }
            _ => {
                if is_named_lambda(&right_node) {
                    values.push(ParsedExpression::Lambda());
                } else {
                    let symbol_chain = if let Some(parse_result) = parse_expression(&right_node) {
                        ParsedExpression::SymbolChain(parse_result)
                    } else {
                        ParsedExpression::Ignored()
                    };
                    values.push(symbol_chain);
                }
            }
        }
        parent_bindings.extend(visit_node(&right_node, builder, defs_table, imports_table));
    }

    // Process the LHS of the assignment
    if let Some(left_node) = node.field("left") {
        let targets = parse_assignment_targets(&left_node, builder);

        if targets.len() != values.len() {
            for target in targets.into_iter().flatten() {
                let binding = Binding::dead_end(node_to_range(node));
                builder.add_binding(target.clone(), binding.clone());

                // Attribute, index, etc. assignments don't register as class variables
                if target.is_identifier() {
                    parent_bindings.push((target, binding.clone()));
                }
            }
        } else {
            for (target, value) in targets.into_iter().zip(values) {
                if let Some(target) = target {
                    let binding = match value {
                        ParsedExpression::SymbolChain(symbol_chain) => Binding::new(
                            BindingValue::SymbolChain(symbol_chain),
                            node_to_range(node),
                        ),
                        ParsedExpression::Lambda() => {
                            let name_range = node_to_range(&left_node);
                            if let Some(definition_info) = defs_table.get(&name_range) {
                                Binding::new(
                                    BindingValue::Definition(definition_info.clone()),
                                    node_to_range(node),
                                )
                            } else {
                                Binding::dead_end(node_to_range(node))
                            }
                        }
                        ParsedExpression::Ignored() => Binding::dead_end(node_to_range(node)),
                    };
                    builder.add_binding(target.clone(), binding.clone());

                    if target.is_identifier() {
                        parent_bindings.push((target, binding.clone()));
                    }
                }
            }
        }

        visit_node(&left_node, builder, defs_table, imports_table);
    }

    parent_bindings
}

/// Processes an augmented assignment (x += y).
/// - Binds the target symbol chain to a dead end.
/// - TODO: Every augmented assignment is really shorthand for a dunder method call:
///   - `x += y` -> `x.__iadd__(y)`
///   - `x *= y` -> `x.__imul__(y)`
///   - etc.
fn visit_augmented_assignment<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(left_node) = node.field("left")
        && let Some(left) = parse_expression(&left_node)
    {
        let binding = Binding::dead_end(node_to_range(node));
        builder.add_binding(left, binding);
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));

    parent_bindings
}

/// Processes a type alias statement (type Point = [float, float]).
/// - We bind the type alias to a dead end, since it'll never be assigned to a callable.
fn visit_type_alias_statement<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    // Extract and process the identifier from the type alias
    if let Some(left_node) = node.child(1)
        && let Some(target_node) = left_node.child(0)
    {
        let target_node_kind = target_node.kind();
        let kind_str = target_node_kind.as_ref();

        let identifier = match kind_str {
            "identifier" => parse_expression(&target_node),
            "generic_type" => target_node
                .child(0)
                .filter(|n| n.kind() == "identifier")
                .and_then(|n| parse_expression(&n)),
            _ => None,
        };

        if let Some(identifier) = identifier {
            let location = node_to_range(node);
            let binding = Binding::dead_end(location);
            builder.add_binding(identifier.clone(), binding.clone());
            parent_bindings.push((identifier.clone(), binding.clone()));
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));

    parent_bindings
}

/// Processes a named expression (walrus operator, x := y).
/// - TODO: Handle bindings to lambdas.
fn visit_named_expression<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(name_node) = node.field("name")
        && let Some(name) = parse_expression(&name_node)
    {
        let location = node_to_range(node);
        let binding = match node.field("value").and_then(|n| parse_expression(&n)) {
            Some(value) => Binding::new(BindingValue::SymbolChain(value), location),
            None => Binding::dead_end(location),
        };

        builder.add_binding(name.clone(), binding.clone());
        parent_bindings.push((name.clone(), binding.clone()));
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));

    parent_bindings
}

/// Processes a with item (with x as y: ...).
/// - Binds targets to expressions in the `as` statements.
fn visit_with_item<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(as_pattern) = node.field("value")
        && as_pattern.kind() == "as_pattern"
        && let Some(alias_node) = as_pattern.field("alias")
        && let Some(alias) = alias_node.child(0)
    {
        let targets = parse_assignment_targets(&alias, builder);
        if targets.len() > 1 {
            // Invalid (e.g. `with expr as x, y`), therefore mark targets as dead ends
            for target in targets.into_iter().flatten() {
                let binding = Binding::dead_end(node_to_range(&as_pattern));
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target.clone(), binding.clone()));
            }
        } else if let Some(Some(target)) = targets.first() {
            if let Some(value_node) = as_pattern.child(0)
                && value_node.kind() != "as_pattern_target"
            {
                let binding = if let Some(value) = parse_expression(&value_node) {
                    // Bind target to value
                    Binding::new(BindingValue::SymbolChain(value), node_to_range(&as_pattern))
                } else {
                    // Value couldn't be parsed, target is a dead-end
                    Binding::dead_end(node_to_range(&as_pattern))
                };
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target.clone(), binding.clone()));
            } else {
                // No value, target is a dead-end
                let binding = Binding::dead_end(node_to_range(&as_pattern));
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target.clone(), binding.clone()));
            }
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));

    parent_bindings
}

/// Processes a delete statement (del x).
/// - Binds symbol chains to dead ends.
fn visit_deletion<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    // Unrolls deletion targets into symbol chains
    fn visit_recursive<'a>(node: &Node<'a, StrDoc<SupportLang>>, targets: &mut Vec<SymbolChain>) {
        let node_kind = node.kind();
        let kind_str = node_kind.as_ref();

        match kind_str {
            "tuple" | "list" | "expression_list" => {
                for child in node.children() {
                    visit_recursive(&child, targets);
                }
            }
            _ => {
                if let Some(target) = parse_expression(node) {
                    targets.push(target);
                }
            }
        }
    }

    let mut parent_bindings = Vec::new();

    if let Some(child) = node.child(1) {
        let mut targets = Vec::new();
        visit_recursive(&child, &mut targets);

        for target in targets {
            let binding = Binding::dead_end(node_to_range(node));
            builder.add_binding(target.clone(), binding.clone());
            parent_bindings.push((target.clone(), binding.clone()));
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));

    parent_bindings
}

/// Processes a function call.
/// - Parses it and adds to the symbol table.
fn visit_call<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &HashMap<Range, PythonDefinitionInfo>,
    imports_table: &HashMap<Range, PythonImportedSymbolInfo>,
) -> Vec<(SymbolChain, Binding)> {
    if let Some(symbol_chain) = parse_expression(node) {
        let range = node_to_range(node);
        builder.add_reference(symbol_chain, range);
    }

    visit_children(node, builder, defs_table, imports_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::tree_sitter::StrDoc;
    use treesitter_visit::{Root, SupportLang};

    // Helper function to create an AST from Python code
    fn parse_python(code: &str) -> Root<StrDoc<SupportLang>> {
        Root::new(code, SupportLang::Python)
    }

    // Helper function to count total bindings in a tree (including all children)
    fn count_bindings(tree: &SymbolTableTree) -> usize {
        let mut count = 0;
        for (_, table) in tree.iter() {
            for bindings in table.symbols.values() {
                count += bindings.len();
            }
        }
        count
    }

    #[test]
    fn test_empty_module() {
        println!("\n=== Testing Empty Module ===");
        let code = "";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 0);
        assert_eq!(root_table.children.len(), 0);
        println!("✓ Empty module test passed");
    }

    #[test]
    fn test_simple_assignment() {
        println!("\n=== Testing Simple Assignment ===");
        let code = "x = 10\ny = 20";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 2);
        assert_eq!(root_table.children.len(), 0);

        // Check that both x and y are in the symbol table
        let x_key = SymbolChain::new(vec![Symbol::Identifier("x".to_string())]);
        let y_key = SymbolChain::new(vec![Symbol::Identifier("y".to_string())]);

        println!(
            "Checking for 'x' in symbols: {}",
            root_table.symbols.contains_key(&x_key)
        );
        println!(
            "Checking for 'y' in symbols: {}",
            root_table.symbols.contains_key(&y_key)
        );

        assert!(root_table.symbols.contains_key(&x_key));
        assert!(root_table.symbols.contains_key(&y_key));
        println!("✓ Simple assignment test passed");
    }

    #[test]
    fn test_function_definition() {
        println!("\n=== Testing Function Definition ===");
        let code = r#"
def foo(x, y):
    z = x + y
    return z
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // foo
        assert_eq!(root_table.children.len(), 1); // function scope

        // Check function scope
        let func_scope_id = root_table.children[0];
        let func_scope = tree
            .get(func_scope_id)
            .expect("Function scope should exist");

        println!(
            "Function scope - type: {:?}, symbols: {}",
            func_scope.scope_type,
            func_scope.symbols.len()
        );
        assert_eq!(func_scope.scope_type, ScopeType::Function);
        assert_eq!(func_scope.symbols.len(), 3); // x, y, z parameters/locals
        println!("✓ Function definition test passed");
    }

    #[test]
    fn test_class_definition() {
        println!("\n=== Testing Class Definition ===");
        let code = r#"
class MyClass:
    x = 20

    def __init__(self):
        self.value = 10
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 4); // MyClass, MyClass.x, MyClass.__init__, MyClass()
        assert_eq!(root_table.children.len(), 1); // class scope

        // Check class scope
        let class_scope_id = root_table.children[0];
        let class_scope = tree.get(class_scope_id).expect("Class scope should exist");

        println!(
            "Class scope - type: {:?}, children: {}",
            class_scope.scope_type,
            class_scope.children.len()
        );
        assert_eq!(class_scope.scope_type, ScopeType::Class);
        assert_eq!(class_scope.symbols.len(), 5); // x, __init__, self, self.x, self.__init__
        assert_eq!(class_scope.children.len(), 1); // __init__ method scope

        // Check method scope
        let method_scope_id = class_scope.children[0];
        let method_scope = tree
            .get(method_scope_id)
            .expect("Method scope should exist");

        println!("Method scope - type: {:?}", method_scope.scope_type);
        assert_eq!(method_scope.scope_type, ScopeType::Function);
        println!("✓ Class definition test passed");
    }

    #[test]
    fn test_if_statement() {
        println!("\n=== Testing If Statement ===");
        let code = r#"
x = 10
if x > 5:
    y = 20
    z = 30
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // x
        assert_eq!(root_table.children.len(), 1); // if scope

        // Check if scope
        let if_scope_id = root_table.children[0];
        let if_scope = tree.get(if_scope_id).expect("If scope should exist");

        println!(
            "If scope - type: {:?}, symbols: {}",
            if_scope.scope_type,
            if_scope.symbols.len()
        );
        assert_eq!(if_scope.scope_type, ScopeType::If);
        assert_eq!(if_scope.symbols.len(), 2); // y, z
        println!("✓ If statement test passed");
    }

    #[test]
    fn test_if_elif_else() {
        println!("\n=== Testing If-Elif-Else ===");
        let code = r#"
x = 10
if x > 10:
    a = 1
elif x > 5:
    b = 2
else:
    c = 3
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // x
        assert_eq!(root_table.children.len(), 3); // if, elif, else scopes

        // Check scope types
        for (i, &child_id) in root_table.children.iter().enumerate() {
            let child = tree.get(child_id).expect("Child scope should exist");
            println!("Child {} scope type: {:?}", i, child.scope_type);
        }

        let if_scope = tree.get(root_table.children[0]).unwrap();
        let elif_scope = tree.get(root_table.children[1]).unwrap();
        let else_scope = tree.get(root_table.children[2]).unwrap();

        assert_eq!(if_scope.scope_type, ScopeType::If);
        assert_eq!(elif_scope.scope_type, ScopeType::Elif);
        assert_eq!(else_scope.scope_type, ScopeType::Else);

        // Check symbols in each scope
        println!("If scope symbols: {}", if_scope.symbols.len());
        println!("Elif scope symbols: {}", elif_scope.symbols.len());
        println!("Else scope symbols: {}", else_scope.symbols.len());

        assert_eq!(if_scope.symbols.len(), 1); // a
        assert_eq!(elif_scope.symbols.len(), 1); // b
        assert_eq!(else_scope.symbols.len(), 1); // c
        println!("✓ If-elif-else test passed");
    }

    #[test]
    fn test_for_loop() {
        println!("\n=== Testing For Loop ===");
        let code = r#"
items = [1, 2, 3]
for item in items:
    x = item * 2
    y = x + 1
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // items
        assert_eq!(root_table.children.len(), 1); // for scope

        // Check for scope
        let for_scope_id = root_table.children[0];
        let for_scope = tree.get(for_scope_id).expect("For scope should exist");

        println!(
            "For scope - type: {:?}, symbols: {}",
            for_scope.scope_type,
            for_scope.symbols.len()
        );
        assert_eq!(for_scope.scope_type, ScopeType::For);
        assert_eq!(for_scope.symbols.len(), 3); // item, x, y
        println!("✓ For loop test passed");
    }

    #[test]
    fn test_while_loop() {
        println!("\n=== Testing While Loop ===");
        let code = r#"
i = 0
while i < 10:
    x = i * 2
    i += 1
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // i
        assert_eq!(root_table.children.len(), 1); // while scope

        // Check while scope
        let while_scope_id = root_table.children[0];
        let while_scope = tree.get(while_scope_id).expect("While scope should exist");

        println!(
            "While scope - type: {:?}, symbols: {}",
            while_scope.scope_type,
            while_scope.symbols.len()
        );
        assert_eq!(while_scope.scope_type, ScopeType::While);
        assert_eq!(while_scope.symbols.len(), 2); // x, i (from augmented assignment)
        println!("✓ While loop test passed");
    }

    #[test]
    fn test_try_except() {
        println!("\n=== Testing Try-Except ===");
        let code = r#"
try:
    x = 10
    y = x / 0
except ZeroDivisionError as e:
    z = str(e)
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 0);
        assert_eq!(root_table.children.len(), 2); // try and except scopes

        // Check try scope
        let try_scope_id = root_table.children[0];
        let try_scope = tree.get(try_scope_id).expect("Try scope should exist");

        println!(
            "Try scope - type: {:?}, symbols: {}",
            try_scope.scope_type,
            try_scope.symbols.len()
        );
        assert_eq!(try_scope.scope_type, ScopeType::Try);
        assert_eq!(try_scope.symbols.len(), 2); // x, y

        // Check except scope
        let except_scope_id = root_table.children[1];
        let except_scope = tree
            .get(except_scope_id)
            .expect("Except scope should exist");

        println!(
            "Except scope - type: {:?}, symbols: {}",
            except_scope.scope_type,
            except_scope.symbols.len()
        );
        assert_eq!(except_scope.scope_type, ScopeType::Except);
        assert_eq!(except_scope.symbols.len(), 2); // e, z
        println!("✓ Try-except test passed");
    }

    #[test]
    fn test_lambda() {
        println!("\n=== Testing Lambda ===");
        let code = r#"
f = lambda x, y: x + y
result = f(10, 20)
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 2); // f, result
        assert_eq!(root_table.children.len(), 1); // lambda scope

        // Check lambda scope
        let lambda_scope_id = root_table.children[0];
        let lambda_scope = tree
            .get(lambda_scope_id)
            .expect("Lambda scope should exist");

        println!(
            "Lambda scope - type: {:?}, symbols: {}",
            lambda_scope.scope_type,
            lambda_scope.symbols.len()
        );
        assert_eq!(lambda_scope.scope_type, ScopeType::Lambda);
        assert_eq!(lambda_scope.symbols.len(), 2); // x, y parameters
        println!("✓ Lambda test passed");
    }

    #[test]
    fn test_list_comprehension() {
        println!("\n=== Testing List Comprehension ===");
        let code = "[x * 2 for x in range(10) if x > 5]";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 0);
        assert_eq!(root_table.children.len(), 1); // comprehension scope

        // Check comprehension scope
        let comp_scope_id = root_table.children[0];
        let comp_scope = tree
            .get(comp_scope_id)
            .expect("Comprehension scope should exist");

        println!(
            "Comprehension scope - type: {:?}, symbols: {}",
            comp_scope.scope_type,
            comp_scope.symbols.len()
        );
        assert_eq!(comp_scope.scope_type, ScopeType::Comprehension);
        assert_eq!(comp_scope.symbols.len(), 1); // x
        println!("✓ List comprehension test passed");
    }

    #[test]
    fn test_nested_functions() {
        println!("\n=== Testing Nested Functions ===");
        let code = r#"
def outer(x):
    y = x * 2
    def inner(z):
        return y + z
    return inner
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // outer
        assert_eq!(root_table.children.len(), 1); // outer function scope

        // Check outer function scope
        let outer_scope_id = root_table.children[0];
        let outer_scope = tree.get(outer_scope_id).expect("Outer scope should exist");

        println!(
            "Outer function scope - type: {:?}, symbols: {}, children: {}",
            outer_scope.scope_type,
            outer_scope.symbols.len(),
            outer_scope.children.len()
        );
        assert_eq!(outer_scope.scope_type, ScopeType::Function);
        assert_eq!(outer_scope.symbols.len(), 3); // x, y, inner
        assert_eq!(outer_scope.children.len(), 1); // inner function scope

        // Check inner function scope
        let inner_scope_id = outer_scope.children[0];
        let inner_scope = tree.get(inner_scope_id).expect("Inner scope should exist");

        println!(
            "Inner function scope - type: {:?}, symbols: {}",
            inner_scope.scope_type,
            inner_scope.symbols.len()
        );
        assert_eq!(inner_scope.scope_type, ScopeType::Function);
        assert_eq!(inner_scope.symbols.len(), 1); // z
        println!("✓ Nested functions test passed");
    }

    #[test]
    fn test_method_bindings() {
        println!("\n=== Testing Method Bindings ===");
        let code = r#"
class MyClass:
    def method1(self):
        pass

    @classmethod
    def method2(cls):
        pass

    @staticmethod
    def method3():
        pass
    "#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        // Should have MyClass and the three methods as MyClass.method1, etc.
        assert_eq!(root_table.symbols.len(), 5); // MyClass, MyClass.method1, MyClass.method2, MyClass.method3, MyClass()
        assert_eq!(root_table.children.len(), 1); // class scope

        let class_scope_id = root_table.children[0];
        let class_scope = tree.get(class_scope_id).expect("Class scope should exist");

        println!(
            "Class scope - type: {:?}, symbols: {}, children: {}",
            class_scope.scope_type,
            class_scope.symbols.len(),
            class_scope.children.len()
        );
        assert_eq!(class_scope.scope_type, ScopeType::Class);
        assert_eq!(class_scope.symbols.len(), 7); // self, self.method1, self.method2, self.method3, method1, method2, method3
        assert_eq!(class_scope.children.len(), 3); // three method scopes
        println!("✓ Method bindings test passed");
    }

    #[test]
    fn test_deletion() {
        println!("\n=== Testing Deletion ===");
        let code = r#"
x = 10
y = 20
del x
z = 30
del y, z
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");
        let total_bindings = count_bindings(&tree);

        println!(
            "Module scope - symbols: {}, total bindings: {}",
            root_table.symbols.len(),
            total_bindings
        );

        assert_eq!(root_table.scope_type, ScopeType::Module);
        // x, y, z should each have two bindings (assignment and deletion)
        assert_eq!(root_table.symbols.len(), 3);
        assert_eq!(total_bindings, 6); // 3 assignments + 3 deletions
        println!("✓ Deletion test passed");
    }

    #[test]
    fn test_walrus_operator() {
        println!("\n=== Testing Walrus Operator ===");
        let code = r#"
if (x := 10) > 5:
    y = x * 2
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // x (from walrus)
        assert_eq!(root_table.children.len(), 1); // if scope

        let if_scope_id = root_table.children[0];
        let if_scope = tree.get(if_scope_id).expect("If scope should exist");

        println!(
            "If scope - type: {:?}, symbols: {}",
            if_scope.scope_type,
            if_scope.symbols.len()
        );
        assert_eq!(if_scope.scope_type, ScopeType::If);
        assert_eq!(if_scope.symbols.len(), 1); // y
        println!("✓ Walrus operator test passed");
    }

    #[test]
    fn test_with_statement() {
        println!("\n=== Testing With Statement ===");
        let code = r#"
with open('file.txt') as f:
    content = f.read()
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 2); // f, content
        assert_eq!(root_table.children.len(), 0); // with doesn't create a new scope
        println!("✓ With statement test passed");
    }

    #[test]
    fn test_match_statement() {
        println!("\n=== Testing Match Statement ===");
        let code = r#"
value = 10
match value:
    case 0:
        x = "zero"
    case 10:
        y = "ten"
    case _:
        z = "other"
"#;
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // value
        assert_eq!(root_table.children.len(), 3); // three case scopes

        // Check case scopes
        for (i, &case_id) in root_table.children.iter().enumerate() {
            let case_scope = tree.get(case_id).expect("Case scope should exist");
            println!(
                "Case {} scope - type: {:?}, symbols: {}",
                i,
                case_scope.scope_type,
                case_scope.symbols.len()
            );
        }

        let case0 = tree.get(root_table.children[0]).unwrap();
        let case1 = tree.get(root_table.children[1]).unwrap();
        let case2 = tree.get(root_table.children[2]).unwrap();

        assert_eq!(case0.scope_type, ScopeType::Case);
        assert_eq!(case0.symbols.len(), 1); // x

        assert_eq!(case1.scope_type, ScopeType::Case);
        assert_eq!(case1.symbols.len(), 1); // y

        assert_eq!(case2.scope_type, ScopeType::DefaultCase);
        assert_eq!(case2.symbols.len(), 1); // z
        println!("✓ Match statement test passed");
    }

    #[test]
    fn test_chained_assignment() {
        println!("\n=== Testing Chained Assignment ===");
        let code = "x = y = z = 10";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");
        let total_bindings = count_bindings(&tree);

        println!(
            "Module scope - symbols: {}, total bindings: {}",
            root_table.symbols.len(),
            total_bindings
        );

        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 3); // x, y, z
        assert_eq!(total_bindings, 3); // one binding for each
        println!("✓ Chained assignment test passed");
    }

    #[test]
    fn test_multiple_assignment() {
        println!("\n=== Testing Multiple Assignment ===");
        let code = "x, y = 1, 2";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");
        let total_bindings = count_bindings(&tree);

        println!(
            "Module scope - symbols: {}, total bindings: {}",
            root_table.symbols.len(),
            total_bindings
        );

        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 2); // x, y
        assert_eq!(total_bindings, 2);
        println!("✓ Multiple assignment test passed");
    }

    #[test]
    fn test_conditional_expression_with_walrus() {
        println!("\n=== Testing Conditional Expression with Walrus ===");
        let code = "result = (x := 10) if condition else (y := 20)";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // result
        assert_eq!(root_table.children.len(), 2); // if and else branches

        // Check if branch
        let if_branch_id = root_table.children[0];
        let if_branch = tree.get(if_branch_id).expect("If branch should exist");

        println!(
            "If branch scope - type: {:?}, symbols: {}",
            if_branch.scope_type,
            if_branch.symbols.len()
        );
        assert_eq!(if_branch.scope_type, ScopeType::If);
        assert_eq!(if_branch.symbols.len(), 1); // x

        // Check else branch
        let else_branch_id = root_table.children[1];
        let else_branch = tree.get(else_branch_id).expect("Else branch should exist");

        println!(
            "Else branch scope - type: {:?}, symbols: {}",
            else_branch.scope_type,
            else_branch.symbols.len()
        );
        assert_eq!(else_branch.scope_type, ScopeType::Else);
        assert_eq!(else_branch.symbols.len(), 1); // y
        println!("✓ Conditional expression with walrus test passed");
    }

    #[test]
    fn test_generator_expression() {
        println!("\n=== Testing Generator Expression ===");
        let code = "(x * 2 for x in range(10))";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");

        println!(
            "Module scope - symbols: {}, children: {}",
            root_table.symbols.len(),
            root_table.children.len()
        );
        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 0);
        assert_eq!(root_table.children.len(), 1); // generator scope

        let gen_scope_id = root_table.children[0];
        let gen_scope = tree
            .get(gen_scope_id)
            .expect("Generator scope should exist");

        println!(
            "Generator scope - type: {:?}, symbols: {}",
            gen_scope.scope_type,
            gen_scope.symbols.len()
        );
        assert_eq!(gen_scope.scope_type, ScopeType::Comprehension);
        assert_eq!(gen_scope.symbols.len(), 1); // x
        println!("✓ Generator expression test passed");
    }

    #[test]
    fn test_type_alias() {
        println!("\n=== Testing Type Alias ===");
        let code = "type Point = tuple[float, float]";
        println!("Code:\n{code}");

        let ast = parse_python(code);
        let tree = build_symbol_table(&ast, vec![], vec![]);

        let root = tree.root();
        let root_table = tree.get(root).expect("Root table should exist");
        let total_bindings = count_bindings(&tree);

        println!(
            "Module scope - symbols: {}, total bindings: {}",
            root_table.symbols.len(),
            total_bindings
        );

        assert_eq!(root_table.scope_type, ScopeType::Module);
        assert_eq!(root_table.symbols.len(), 1); // Point
        assert_eq!(total_bindings, 1);
        println!("✓ Type alias test passed");
    }
}
