use code_graph_types::{CanonicalDefinition, CanonicalImport, Range};
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use super::types::*;
use super::utils::*;

const MINIMUM_STACK_REMAINING: usize = 128 * 1024;

/// Builder wrapping a SymbolTableTree with a cursor tracking the current scope.
pub struct SymbolTableBuilder {
    pub tree: SymbolTableTree,
    pub current_scope: ScopeId,
}

impl SymbolTableBuilder {
    pub fn new(location: Range, scope_type: ScopeType, fqn: Option<String>) -> Self {
        let tree = SymbolTableTree::new(location, scope_type, fqn);
        Self {
            tree,
            current_scope: ScopeId(0),
        }
    }

    pub fn current_scope(&self) -> ScopeId {
        self.current_scope
    }

    pub fn parent_scope(&self) -> Option<ScopeId> {
        self.tree.get(self.current_scope)?.parent
    }

    pub fn create_scope(
        &mut self,
        location: Range,
        scope_type: ScopeType,
        fqn: Option<String>,
    ) -> ScopeId {
        let new_scope = self.tree.add_child(self.current_scope, location, scope_type, fqn);
        self.current_scope = new_scope;
        new_scope
    }

    pub fn enter_scope(&mut self, id: ScopeId) {
        self.current_scope = id;
    }

    pub fn exit_scope(&mut self) {
        if let Some(table) = self.tree.get(self.current_scope)
            && let Some(parent_id) = table.parent
        {
            self.current_scope = parent_id;
        }
    }

    pub fn add_binding(&mut self, key: SymbolChain, binding: Binding) {
        self.tree.add_binding(self.current_scope, key, binding);
    }

    pub fn add_binding_to(&mut self, scope_id: ScopeId, key: SymbolChain, binding: Binding) {
        self.tree.add_binding(scope_id, key, binding);
    }

    pub fn add_reference(&mut self, chain: SymbolChain, range: Range) {
        self.tree.add_reference(self.current_scope, chain, range);
    }

    pub fn add_scope_group(&mut self, group: ScopeGroup) {
        self.tree.add_conditional(self.current_scope, group);
    }

    pub fn add_definition(&mut self, def_idx: usize) {
        self.tree.add_definition(self.current_scope, def_idx);
    }

    pub fn build(self) -> SymbolTableTree {
        self.tree
    }
}

// ── Main entry point ────────────────────────────────────────────

/// Build a symbol table tree from a parsed AST and its canonical results.
///
/// `definitions` and `imports` are the slices from the file's `CanonicalResult`.
/// The lookup tables map ranges to indices into these slices.
pub fn build_symbol_table(
    ast: &Root<StrDoc<SupportLang>>,
    definitions: &[CanonicalDefinition],
    imports: &[CanonicalImport],
) -> SymbolTableTree {
    let defs_table = build_definition_lookup_table(definitions);
    let imports_table = build_import_lookup_table(imports);

    let root = ast.root();
    let mut builder = SymbolTableBuilder::new(node_range(&root), ScopeType::Module, None);
    visit_children(&root, &mut builder, &defs_table, &imports_table);

    builder.build()
}

// ── Visitor dispatch ────────────────────────────────────────────

fn visit_children(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    for child in node.children() {
        parent_bindings.extend(visit_node(&child, builder, defs_table, imports_table));
    }
    parent_bindings
}

fn visit_node(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    if stacker::remaining_stack().unwrap_or(usize::MAX) < MINIMUM_STACK_REMAINING {
        return vec![];
    }

    let parent_bindings = match node.kind().as_ref() {
        // Import statements
        "import_statement" | "import_from_statement" | "future_import_statement" => {
            visit_import_statement(node, builder, imports_table)
        }

        // Isolated scopes
        "class_definition" => visit_class_definition(node, builder, defs_table, imports_table),
        "function_definition" => visit_function_definition(node, builder, defs_table, imports_table),
        "lambda" => visit_lambda_definition(node, builder, defs_table, imports_table),

        // Semi-isolated scopes
        "list_comprehension" | "set_comprehension" | "dictionary_comprehension"
        | "generator_expression" => visit_comprehension(node, builder, defs_table, imports_table),

        // Non-isolated (conditional) scopes
        "if_statement" => visit_if_statement(node, builder, defs_table, imports_table),
        "for_statement" => visit_for_loop(node, builder, defs_table, imports_table),
        "while_statement" => visit_while_loop(node, builder, defs_table, imports_table),
        "try_statement" => visit_try_statement(node, builder, defs_table, imports_table),
        "match_statement" => visit_match_statement(node, builder, defs_table, imports_table),
        "conditional_expression" => {
            visit_conditional_expression(node, builder, defs_table, imports_table)
        }

        // Assignments
        "assignment" => visit_assignment(node, builder, defs_table, imports_table),
        "augmented_assignment" => visit_augmented_assignment(node, builder, defs_table, imports_table),
        "type_alias_statement" => visit_type_alias_statement(node, builder, defs_table, imports_table),
        "named_expression" => visit_named_expression(node, builder, defs_table, imports_table),
        "with_item" => visit_with_item(node, builder, defs_table, imports_table),

        // Deletions
        "delete_statement" => visit_deletion(node, builder, defs_table, imports_table),

        // Calls
        "call" => visit_call(node, builder, defs_table, imports_table),

        _ => visit_children(node, builder, defs_table, imports_table),
    };

    // If we're in a class scope, propagate bindings up the tree
    if let Some(table) = builder.tree.get(builder.current_scope)
        && table.scope_type == ScopeType::Class
    {
        return parent_bindings;
    }

    Vec::new()
}

// ── Class definition ────────────────────────────────────────────

fn visit_class_definition(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    let parent_scope_id = builder.current_scope();

    let mut class_name = String::new();
    if let Some(name_node) = node.field("name") {
        class_name = name_node.text().to_string();
        let range = node_range(node);

        // Look up the definition by the entire definition range
        let def_idx = defs_table.get(&range).copied();
        let definition_binding = match def_idx {
            Some(idx) => Binding::definition(idx, range),
            None => Binding::dead_end(range),
        };

        // Create class scope
        let fqn_str = def_idx.map(|_| class_name.clone()); // simplified: will get proper FQN from context
        builder.create_scope(node_range(node), ScopeType::Class, fqn_str);

        // Add definition mapping
        if let Some(idx) = def_idx {
            builder.add_definition(idx);
        }

        // Bind class name in parent scope
        let key = SymbolChain::single(class_name.clone());
        builder.add_binding_to(parent_scope_id, key, definition_binding.clone());

        // Bind class instantiation (MyClass()) in parent scope
        let key = SymbolChain::new(vec![
            Symbol::Identifier(class_name.clone()),
            Symbol::Connector(Connector::Call),
        ]);
        builder.add_binding_to(parent_scope_id, key, definition_binding.clone());

        // Bind Receiver (self) in class scope
        let key = SymbolChain::new(vec![Symbol::Receiver]);
        builder.add_binding(key, definition_binding.clone());
    }

    // Process class body
    let child_bindings = visit_children(node, builder, defs_table, imports_table);

    // Propagate child bindings as self.{name} and ClassName.{name}
    for (chain, binding) in child_bindings {
        // self.{chain}
        let self_chain = SymbolChain::new(
            std::iter::once(Symbol::Receiver)
                .chain(std::iter::once(Symbol::Connector(Connector::Attribute)))
                .chain(chain.symbols.iter().cloned())
                .collect(),
        );
        builder.add_binding(self_chain, binding.clone());

        // ClassName.{chain} in parent scope
        let class_chain = SymbolChain::new(
            std::iter::once(Symbol::Identifier(class_name.clone()))
                .chain(std::iter::once(Symbol::Connector(Connector::Attribute)))
                .chain(chain.symbols.iter().cloned())
                .collect(),
        );
        builder.add_binding_to(parent_scope_id, class_chain.clone(), binding.clone());
        parent_bindings.push((class_chain, binding));
    }

    builder.exit_scope();
    parent_bindings
}

// ── Function definition ─────────────────────────────────────────

fn visit_function_definition(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    // Default values are evaluated in the enclosing scope
    if let Some(parameters_node) = node.field("parameters") {
        visit_node(&parameters_node, builder, defs_table, imports_table);
    }

    let mut fqn = None;
    let mut def_idx = None;
    if let Some(name_node) = node.field("name") {
        let name = name_node.text().to_string();

        // Find the definition by checking both the function range and the decorated range
        let range = if let Some(parent) = node.parent()
            && parent.kind() == "decorated_definition"
        {
            node_range(&parent)
        } else {
            node_range(node)
        };

        def_idx = defs_table.get(&range).copied();
        let definition_binding = match def_idx {
            Some(idx) => Binding::definition(idx, range),
            None => Binding::dead_end(range),
        };
        fqn = def_idx.map(|_| name.clone());

        // Bind function name in current scope
        let key = SymbolChain::single(name);
        builder.add_binding(key.clone(), definition_binding.clone());
        parent_bindings.push((key, definition_binding));
    }

    // Create function body scope
    if let Some(body_node) = node.field("body") {
        builder.create_scope(node_range(&body_node), ScopeType::Function, fqn);
        if let Some(idx) = def_idx {
            builder.add_definition(idx);
        }

        // Process parameters
        visit_parameters(node, builder);

        // Process body
        if let Some(body_node) = node.field("body") {
            visit_children(&body_node, builder, defs_table, imports_table);
        }

        builder.exit_scope();
    }

    parent_bindings
}

// ── Lambda definition ───────────────────────────────────────────

fn visit_lambda_definition(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    // Default values evaluated in enclosing scope
    if let Some(parameters_node) = node.field("parameters") {
        visit_node(&parameters_node, builder, defs_table, imports_table);
    }

    builder.create_scope(node_range(node), ScopeType::Lambda, None);

    // Process parameters
    visit_parameters(node, builder);

    // Process body
    if let Some(body_node) = node.field("body") {
        visit_children(&body_node, builder, defs_table, imports_table);
    }

    builder.exit_scope();
    Vec::new()
}

// ── Parameters ──────────────────────────────────────────────────

fn visit_parameters(node: &Node<StrDoc<SupportLang>>, builder: &mut SymbolTableBuilder) {
    let parent_scope_id = builder.parent_scope();

    if let Some(parameters_node) = node.field("parameters") {
        let range = node_range(&parameters_node);

        let parameters = parameters_node
            .children()
            .filter_map(|n| get_parameter_name(&n));

        for (index, param_name) in parameters.enumerate() {
            // Handle implicit parameters in class methods (self, cls)
            if index == 0
                && let Some(parent_scope_id) = parent_scope_id
                && let Some(parent_scope) = builder.tree.get(parent_scope_id)
                && parent_scope.scope_type == ScopeType::Class
            {
                if !(is_lambda_method(node) || node.kind() == "function_definition") {
                    continue;
                }

                match get_method_type(node) {
                    Some(MethodType::Instance) | Some(MethodType::Property) => {
                        // self → Receiver
                        let key = SymbolChain::single(param_name);
                        let value = Binding::alias(
                            SymbolChain::new(vec![Symbol::Receiver]),
                            range,
                        );
                        builder.add_binding(key, value);
                    }
                    Some(MethodType::Class) => {
                        // cls → ClassName
                        if let Some(class_name) = get_containing_class_name(node) {
                            let key = SymbolChain::single(param_name);
                            let value = Binding::alias(
                                SymbolChain::single(class_name),
                                range,
                            );
                            builder.add_binding(key, value);
                        }
                    }
                    _ => {}
                }
                continue;
            }

            // All other parameters are dead ends
            let key = SymbolChain::single(param_name);
            builder.add_binding(key, Binding::dead_end(range));
        }
    }
}

// ── Comprehensions ──────────────────────────────────────────────

fn visit_comprehension(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    builder.create_scope(node_range(node), ScopeType::Comprehension, None);

    for for_in_clause in node.children().filter(|n| n.kind() == "for_in_clause") {
        if let Some(left_node) = for_in_clause.field("left") {
            let targets = parse_for_loop_targets(&left_node);
            visit_node(&left_node, builder, defs_table, imports_table);

            if let Some(right_node) = for_in_clause.field("right") {
                for target in targets {
                    builder.add_binding(target, Binding::dead_end(node_range(&right_node)));
                }
                visit_node(&right_node, builder, defs_table, imports_table);
            }
        }
    }

    for if_clause in node.children().filter(|n| n.kind() == "if_clause") {
        visit_node(&if_clause, builder, defs_table, imports_table);
    }

    if let Some(body) = node.field("body") {
        visit_node(&body, builder, defs_table, imports_table);
    }

    builder.exit_scope();
    Vec::new()
}

// ── If statement ────────────────────────────────────────────────

fn visit_if_statement(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    let mut scope_ids = Vec::new();

    // Condition evaluated in parent scope
    if let Some(condition) = node.field("condition") {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    // If body
    if let Some(block) = node.field("consequence") {
        let scope_id = builder.create_scope(node_range(&block), ScopeType::If, None);
        visit_node(&block, builder, defs_table, imports_table);
        builder.exit_scope();
        scope_ids.push(scope_id);
    }

    // Elif and else
    for alt_node in node.field_children("alternative") {
        match alt_node.kind().as_ref() {
            "elif_clause" => {
                let scope_id = builder.create_scope(node_range(&alt_node), ScopeType::Elif, None);
                if let Some(condition) = alt_node.field("condition") {
                    visit_node(&condition, builder, defs_table, imports_table);
                }
                if let Some(block) = alt_node.field("consequence") {
                    visit_node(&block, builder, defs_table, imports_table);
                }
                builder.exit_scope();
                scope_ids.push(scope_id);
            }
            "else_clause" => {
                if let Some(block) = alt_node.field("body") {
                    let scope_id = builder.create_scope(node_range(&alt_node), ScopeType::Else, None);
                    visit_node(&block, builder, defs_table, imports_table);
                    builder.exit_scope();
                    scope_ids.push(scope_id);
                }
            }
            _ => {
                visit_node(&alt_node, builder, defs_table, imports_table);
            }
        }
    }

    builder.add_scope_group(ScopeGroup::new(node_range(node), scope_ids, ScopeGroupType::If));
    parent_bindings
}

// ── For loop ────────────────────────────────────────────────────

fn visit_for_loop(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(body_node) = node.field("body") {
        let for_scope_id = builder.create_scope(node_range(&body_node), ScopeType::For, None);

        if let Some(left_node) = node.field("left") {
            let targets = parse_for_loop_targets(&left_node);
            if let Some(right_node) = node.field("right") {
                for target in targets {
                    builder.add_binding(target, Binding::dead_end(node_range(&right_node)));
                }
                visit_node(&left_node, builder, defs_table, imports_table);

                // Iterable evaluated in parent scope
                builder.exit_scope();
                parent_bindings.extend(visit_node(&right_node, builder, defs_table, imports_table));
                builder.enter_scope(for_scope_id);
            }
        }

        visit_node(&body_node, builder, defs_table, imports_table);
        builder.exit_scope();
        builder.add_scope_group(ScopeGroup::new(
            node_range(&body_node),
            vec![for_scope_id],
            ScopeGroupType::Loop,
        ));
    }

    // Else block
    if let Some(else_node) = node.field("alternative") {
        parent_bindings.extend(visit_children(&else_node, builder, defs_table, imports_table));
    }

    parent_bindings
}

// ── While loop ──────────────────────────────────────────────────

fn visit_while_loop(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(condition) = node.field("condition") {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    if let Some(block) = node.field("body") {
        let scope_id = builder.create_scope(node_range(&block), ScopeType::While, None);
        visit_node(&block, builder, defs_table, imports_table);
        builder.exit_scope();
        builder.add_scope_group(ScopeGroup::new(
            node_range(&block),
            vec![scope_id],
            ScopeGroupType::Loop,
        ));
    }

    if let Some(else_node) = node.field("alternative") {
        parent_bindings.extend(visit_children(&else_node, builder, defs_table, imports_table));
    }

    parent_bindings
}

// ── Try statement ───────────────────────────────────────────────

fn visit_try_statement(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut scope_ids = Vec::new();

    // Try body + else clause
    if let Some(body) = node.field("body") {
        let try_scope_id = builder.create_scope(node_range(&body), ScopeType::Try, None);
        visit_node(&body, builder, defs_table, imports_table);

        for else_clause in node.children().filter(|n| n.kind() == "else_clause") {
            visit_node(&else_clause, builder, defs_table, imports_table);
        }
        builder.exit_scope();
        scope_ids.push(try_scope_id);
    }

    // Except clauses
    let mut last_conditional_node = None;
    for child in node.children() {
        match child.kind().as_ref() {
            "block" | "else_clause" => {
                last_conditional_node = Some(child);
            }
            "except_clause" | "except_group_clause" => {
                let scope_id = builder.create_scope(node_range(&child), ScopeType::Except, None);

                // Handle as-pattern aliasing
                for as_pattern in child.children().filter(|n| n.kind() == "as_pattern") {
                    if let Some(alias_node) = as_pattern.field("alias")
                        && let Some(alias_node) = alias_node.child(0)
                        && let Some(alias) = parse_expression(&alias_node)
                    {
                        match child.kind().as_ref() {
                            "except_clause" => {
                                if let Some(value_node) = as_pattern.child(0)
                                    && value_node.kind() != "as_pattern_target"
                                {
                                    let value = if let Some(mut value_chain) = parse_expression(&value_node) {
                                        value_chain.symbols.push(Symbol::Connector(Connector::Call));
                                        BindingValue::Alias(value_chain)
                                    } else {
                                        BindingValue::DeadEnd
                                    };
                                    let binding = Binding::new(value, node_range(&as_pattern));
                                    builder.add_binding(alias, binding);
                                }
                            }
                            "except_group_clause" => {
                                builder.add_binding(alias, Binding::dead_end(node_range(&as_pattern)));
                            }
                            _ => {}
                        }
                    }
                }

                visit_children(&child, builder, defs_table, imports_table);
                builder.exit_scope();
                scope_ids.push(scope_id);
                last_conditional_node = Some(child);
            }
            "finally_clause" => {
                visit_node(&child, builder, defs_table, imports_table);
            }
            _ => {}
        }
    }

    let group_location = if let Some(last_node) = last_conditional_node {
        let start = node.start_pos();
        let end = last_node.end_pos();
        Range::new(
            code_graph_types::Position::new(start.line(), start.column(node)),
            code_graph_types::Position::new(end.line(), end.column(&last_node)),
            (node.range().start, last_node.range().end),
        )
    } else {
        node_range(node)
    };
    builder.add_scope_group(ScopeGroup::new(group_location, scope_ids, ScopeGroupType::Try));

    Vec::new()
}

// ── Match statement ─────────────────────────────────────────────

fn visit_match_statement(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    // Subject evaluated in parent scope
    if let Some(subject_node) = node.field("subject") {
        visit_node(&subject_node, builder, defs_table, imports_table);
    }

    if let Some(body_node) = node.field("body") {
        let mut scope_ids = Vec::new();
        for child in body_node.children() {
            if child.kind() == "case_clause" {
                let mut case_patterns = child.children().filter(|n| n.kind() == "case_pattern");
                let scope_type = match (case_patterns.next(), case_patterns.next()) {
                    (Some(first), None) if first.text() == "_" => ScopeType::DefaultCase,
                    _ => ScopeType::Case,
                };

                let scope_id = builder.create_scope(node_range(&child), scope_type, None);
                for case_child in child.children().filter(|n| n.kind() == "block") {
                    visit_node(&case_child, builder, defs_table, imports_table);
                }

                builder.exit_scope();
                for case_child in node.children().filter(|n| n.kind() != "block") {
                    visit_node(&case_child, builder, defs_table, imports_table);
                }

                scope_ids.push(scope_id);
            }
        }

        builder.add_scope_group(ScopeGroup::new(
            node_range(&body_node),
            scope_ids,
            ScopeGroupType::Match,
        ));
    }

    Vec::new()
}

// ── Conditional expression ──────────────────────────────────────

fn visit_conditional_expression(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    let has_walrus = |node: &Node<StrDoc<SupportLang>>| {
        node.dfs().any(|child| child.kind() == "named_expression")
    };
    let left_branch = node.child(0);
    let right_branch = node.child(4);

    let needs_scopes = left_branch.as_ref().is_some_and(has_walrus)
        || right_branch.as_ref().is_some_and(has_walrus);

    if needs_scopes {
        let mut scope_ids = Vec::new();

        if let Some(left) = left_branch {
            let id = builder.create_scope(node_range(&left), ScopeType::If, None);
            visit_node(&left, builder, defs_table, imports_table);
            builder.exit_scope();
            scope_ids.push(id);
        }
        if let Some(right) = right_branch {
            let id = builder.create_scope(node_range(&right), ScopeType::Else, None);
            visit_node(&right, builder, defs_table, imports_table);
            builder.exit_scope();
            scope_ids.push(id);
        }

        builder.add_scope_group(ScopeGroup::new(node_range(node), scope_ids, ScopeGroupType::If));
    } else {
        if let Some(left) = left_branch {
            visit_node(&left, builder, defs_table, imports_table);
        }
        if let Some(right) = right_branch {
            visit_node(&right, builder, defs_table, imports_table);
        }
    }

    // Condition always in current scope
    if let Some(condition) = node.child(2) {
        parent_bindings.extend(visit_node(&condition, builder, defs_table, imports_table));
    }

    parent_bindings
}

// ── Import statement ────────────────────────────────────────────

fn visit_import_statement(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();
    let import_range = node_range(node);

    // Walk children to find individual import names
    for child in node.children() {
        match child.kind().as_ref() {
            "dotted_name" => {
                // `import foo` or `import foo.bar`
                let name = child.text().to_string();
                // For `import foo.bar`, the local name is the first component `foo`
                let local_name = name.split('.').next().unwrap_or(&name).to_string();
                if let Some(&import_idx) = imports_table.get(&import_range) {
                    let key = SymbolChain::single(local_name);
                    let binding = Binding::import(import_idx, import_range);
                    builder.add_binding(key.clone(), binding.clone());
                    parent_bindings.push((key, binding));
                }
            }
            "aliased_import" => {
                // `import foo as bar` or `from X import foo as bar`
                if let Some(alias_node) = child.field("alias") {
                    let alias = alias_node.text().to_string();
                    if let Some(&import_idx) = imports_table.get(&import_range) {
                        let key = SymbolChain::single(alias);
                        let binding = Binding::import(import_idx, import_range);
                        builder.add_binding(key.clone(), binding.clone());
                        parent_bindings.push((key, binding));
                    }
                } else if let Some(name_node) = child.field("name") {
                    let name = name_node.text().to_string();
                    if let Some(&import_idx) = imports_table.get(&import_range) {
                        let key = SymbolChain::single(name);
                        let binding = Binding::import(import_idx, import_range);
                        builder.add_binding(key.clone(), binding.clone());
                        parent_bindings.push((key, binding));
                    }
                }
            }
            _ => {}
        }
    }

    // For import_from_statement, also handle the imported symbols
    if node.kind() == "import_from_statement" || node.kind() == "future_import_statement" {
        for child in node.children() {
            match child.kind().as_ref() {
                "dotted_name" | "identifier" => {
                    // Could be the module name or an imported symbol
                    let text = child.text().to_string();
                    let module_text = node.field("module_name").map(|n| n.text().to_string());
                    if module_text.as_deref() != Some(&text) {
                        // This is an imported symbol, not the module name
                        if let Some(&import_idx) = imports_table.get(&import_range) {
                            let key = SymbolChain::single(text);
                            let binding = Binding::import(import_idx, import_range);
                            builder.add_binding(key.clone(), binding.clone());
                            parent_bindings.push((key, binding));
                        }
                    }
                }
                "aliased_import" => {
                    if let Some(alias_node) = child.field("alias") {
                        let alias = alias_node.text().to_string();
                        if let Some(&import_idx) = imports_table.get(&import_range) {
                            let key = SymbolChain::single(alias);
                            let binding = Binding::import(import_idx, import_range);
                            builder.add_binding(key.clone(), binding.clone());
                            parent_bindings.push((key, binding));
                        }
                    } else if let Some(name_node) = child.field("name") {
                        let name = name_node.text().to_string();
                        if let Some(&import_idx) = imports_table.get(&import_range) {
                            let key = SymbolChain::single(name);
                            let binding = Binding::import(import_idx, import_range);
                            builder.add_binding(key.clone(), binding.clone());
                            parent_bindings.push((key, binding));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    parent_bindings
}

// ── Assignment ──────────────────────────────────────────────────

fn visit_assignment(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    // Process RHS
    let mut values: Vec<ParsedExpression> = Vec::new();
    if let Some(right_node) = node.field("right") {
        match right_node.kind().as_ref() {
            "assignment" => {
                // Chained: x = y = z = 10
                if let Some(right_target) = right_node.field("left") {
                    let chains = parse_assignment_targets(&right_target, builder);
                    for chain in chains {
                        if let Some(chain) = chain {
                            values.push(ParsedExpression::SymbolChain(chain));
                        } else {
                            values.push(ParsedExpression::Ignored);
                        }
                    }
                }
            }
            "expression_list" => {
                // x, y = 10, 20
                for child in right_node.children() {
                    values.push(match parse_expression(&child) {
                        Some(chain) => ParsedExpression::SymbolChain(chain),
                        None => ParsedExpression::Ignored,
                    });
                }
            }
            "list" | "tuple" => {
                values.push(ParsedExpression::Ignored);
            }
            _ => {
                if is_named_lambda(&right_node) {
                    values.push(ParsedExpression::Lambda);
                } else {
                    values.push(match parse_expression(&right_node) {
                        Some(chain) => ParsedExpression::SymbolChain(chain),
                        None => ParsedExpression::Ignored,
                    });
                }
            }
        }
        parent_bindings.extend(visit_node(&right_node, builder, defs_table, imports_table));
    }

    // Process LHS
    if let Some(left_node) = node.field("left") {
        let targets = parse_assignment_targets(&left_node, builder);

        if targets.len() != values.len() {
            for target in targets.into_iter().flatten() {
                let binding = Binding::dead_end(node_range(node));
                builder.add_binding(target.clone(), binding.clone());
                if target.is_identifier() {
                    parent_bindings.push((target, binding));
                }
            }
        } else {
            for (target, value) in targets.into_iter().zip(values) {
                if let Some(target) = target {
                    let binding = match value {
                        ParsedExpression::SymbolChain(chain) => {
                            Binding::alias(chain, node_range(node))
                        }
                        ParsedExpression::Lambda => {
                            // Lambda assignment: look up definition
                            let def_range = node_range(node);
                            match defs_table.get(&def_range) {
                                Some(&idx) => Binding::definition(idx, def_range),
                                None => Binding::dead_end(def_range),
                            }
                        }
                        ParsedExpression::Ignored => Binding::dead_end(node_range(node)),
                    };
                    builder.add_binding(target.clone(), binding.clone());
                    if target.is_identifier() {
                        parent_bindings.push((target, binding));
                    }
                }
            }
        }

        visit_node(&left_node, builder, defs_table, imports_table);
    }

    parent_bindings
}

// ── Augmented assignment ────────────────────────────────────────

fn visit_augmented_assignment(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(left_node) = node.field("left")
        && let Some(left) = parse_expression(&left_node)
    {
        builder.add_binding(left, Binding::dead_end(node_range(node)));
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));
    parent_bindings
}

// ── Type alias ──────────────────────────────────────────────────

fn visit_type_alias_statement(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(left_node) = node.child(1)
        && let Some(target_node) = left_node.child(0)
    {
        let identifier = match target_node.kind().as_ref() {
            "identifier" => parse_expression(&target_node),
            "generic_type" => target_node
                .child(0)
                .filter(|n| n.kind() == "identifier")
                .and_then(|n| parse_expression(&n)),
            _ => None,
        };

        if let Some(id) = identifier {
            let binding = Binding::dead_end(node_range(node));
            builder.add_binding(id.clone(), binding.clone());
            parent_bindings.push((id, binding));
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));
    parent_bindings
}

// ── Named expression (walrus) ───────────────────────────────────

fn visit_named_expression(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(name_node) = node.field("name")
        && let Some(name) = parse_expression(&name_node)
    {
        let location = node_range(node);
        let binding = match node.field("value").and_then(|n| parse_expression(&n)) {
            Some(value) => Binding::alias(value, location),
            None => Binding::dead_end(location),
        };
        builder.add_binding(name.clone(), binding.clone());
        parent_bindings.push((name, binding));
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));
    parent_bindings
}

// ── With item ───────────────────────────────────────────────────

fn visit_with_item(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    let mut parent_bindings = Vec::new();

    if let Some(as_pattern) = node.field("value")
        && as_pattern.kind() == "as_pattern"
        && let Some(alias_node) = as_pattern.field("alias")
        && let Some(alias) = alias_node.child(0)
    {
        let targets = parse_assignment_targets(&alias, builder);
        if targets.len() > 1 {
            for target in targets.into_iter().flatten() {
                let binding = Binding::dead_end(node_range(&as_pattern));
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target, binding));
            }
        } else if let Some(Some(target)) = targets.first() {
            if let Some(value_node) = as_pattern.child(0)
                && value_node.kind() != "as_pattern_target"
            {
                let binding = match parse_expression(&value_node) {
                    Some(value) => Binding::alias(value, node_range(&as_pattern)),
                    None => Binding::dead_end(node_range(&as_pattern)),
                };
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target.clone(), binding));
            } else {
                let binding = Binding::dead_end(node_range(&as_pattern));
                builder.add_binding(target.clone(), binding.clone());
                parent_bindings.push((target.clone(), binding));
            }
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));
    parent_bindings
}

// ── Deletion ────────────────────────────────────────────────────

fn visit_deletion(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    fn collect_targets(node: &Node<StrDoc<SupportLang>>, targets: &mut Vec<SymbolChain>) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < MINIMUM_STACK_REMAINING {
            return;
        }
        match node.kind().as_ref() {
            "tuple" | "list" | "expression_list" => {
                for child in node.children() {
                    collect_targets(&child, targets);
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
        collect_targets(&child, &mut targets);

        for target in targets {
            let binding = Binding::dead_end(node_range(node));
            builder.add_binding(target.clone(), binding.clone());
            parent_bindings.push((target, binding));
        }
    }

    parent_bindings.extend(visit_children(node, builder, defs_table, imports_table));
    parent_bindings
}

// ── Call ─────────────────────────────────────────────────────────

fn visit_call(
    node: &Node<StrDoc<SupportLang>>,
    builder: &mut SymbolTableBuilder,
    defs_table: &FxHashMap<Range, usize>,
    imports_table: &FxHashMap<Range, usize>,
) -> Vec<(SymbolChain, Binding)> {
    if let Some(chain) = parse_expression(node) {
        builder.add_reference(chain, node_range(node));
    }
    visit_children(node, builder, defs_table, imports_table)
}
