use crate::typescript::swc::references::types::{
    TypeScriptExpression, TypeScriptSymbol, TypeScriptSymbolType,
};
use crate::utils::{HasRange, IntervalTree};
use std::collections::HashSet;

pub fn fold_expressions<T: HasRange + Clone + Eq + Send + Sync>(
    expressions: &Vec<TypeScriptExpression>,
) -> Vec<TypeScriptExpression> {
    let spatial_index = expression_index(expressions);

    // Find all expressions that are not contained within any other expression
    let mut top_level_expressions = vec![];
    for e in expressions {
        let (start_byte, end_byte) = (e.range.byte_offset.0, e.range.byte_offset.1);
        let containing = spatial_index.find_containing(start_byte as u64, end_byte as u64);
        if containing.is_none() {
            top_level_expressions.push(e.clone());
        }
    }

    // For each top-level expression, find all contained expressions and merge their symbols
    let mut folded_expressions = vec![];
    for top_expr in top_level_expressions {
        let mut folded_expr = top_expr.clone();
        let (start_byte, end_byte) = (top_expr.range.byte_offset.0, top_expr.range.byte_offset.1);
        let contained = spatial_index.find_contained(start_byte as u64, end_byte as u64);

        // Collect all symbols from contained expressions (including the top-level one)
        let mut all_symbols = vec![];
        let mut all_assignment_target_symbols = vec![];

        all_symbols.extend(top_expr.symbols.clone());
        all_assignment_target_symbols.extend(top_expr.assigment_target_symbols.clone());

        // Add symbols from all contained expressions
        for contained_expr in contained {
            if contained_expr.range != top_expr.range {
                all_symbols.extend(contained_expr.symbols.clone());
                all_assignment_target_symbols
                    .extend(contained_expr.assigment_target_symbols.clone());
            }
        }

        // Sort symbols by their byte position to maintain order
        all_symbols.sort_by_key(|s| s.range.byte_offset);
        all_assignment_target_symbols.sort_by_key(|s| s.range.byte_offset);

        folded_expr.symbols = all_symbols;
        folded_expr.assigment_target_symbols = all_assignment_target_symbols;
        folded_expressions.push(folded_expr);
    }

    folded_expressions
}

pub fn expression_index(
    expressions: &[TypeScriptExpression],
) -> IntervalTree<TypeScriptExpression> {
    IntervalTree::new(expressions.iter().map(|e| (e.range, e.clone())).collect())
}

/// Get all expressions that are in the same containing scope as the given expression
pub fn get_expressions_in_containing_scope(
    expression: &TypeScriptExpression,
    expression_index: &IntervalTree<TypeScriptExpression>,
    symbol_index: &IntervalTree<TypeScriptSymbol>,
) -> Vec<TypeScriptExpression> {
    let expr_start = expression.range.byte_offset.0 as u64;
    let expr_end = expression.range.byte_offset.1 as u64;

    // Find the containing scope for this expression
    if let Some(containing_scope) = symbol_index.find_containing(expr_start, expr_end) {
        let scope_start = containing_scope.range().byte_offset.0 as u64;
        let scope_end = containing_scope.range().byte_offset.1 as u64;
        // Find all expressions within this scope
        expression_index
            .find_contained(scope_start, scope_end)
            .into_iter()
            .cloned()
            .collect()
    } else {
        vec![]
    }
}

/// Find expression chains within the same scope where variables are assigned and then used
/// Returns a vector of expression chains, where each chain starts with an assignment
/// and is followed by all uses of that variable in the same scope
/// For example: x = new DatabaseConnection(); x.connect(); x.disconnect()
/// Would yield: [x = new DatabaseConnection(), x.connect()] and [x = new DatabaseConnection(), x.disconnect()]
pub fn find_shared_scope_expressions(
    expressions: &[TypeScriptExpression],
    symbol_index: &IntervalTree<TypeScriptSymbol>,
    _expression_index: &IntervalTree<TypeScriptExpression>,
) -> Vec<Vec<TypeScriptExpression>> {
    let mut scope_groups = std::collections::HashMap::<
        String,
        (
            TypeScriptSymbol,
            Vec<TypeScriptExpression>,
            Vec<TypeScriptExpression>,
        ),
    >::new();

    // First pass: group expressions by scope and categorize as assignments or uses
    for expression in expressions {
        // Find the containing scope for this expression
        let expr_start = expression.range.byte_offset.0 as u64;
        let expr_end = expression.range.byte_offset.1 as u64;

        if let Some(containing_scope) = symbol_index.find_containing(expr_start, expr_end) {
            // Create a unique key for this scope based on its range and type
            let scope_key = format!(
                "{}:{}-{}",
                containing_scope.symbol_type() as u8,
                containing_scope.range().byte_offset.0,
                containing_scope.range().byte_offset.1
            );

            // Get or create the scope group
            let scope_group = scope_groups
                .entry(scope_key)
                .or_insert_with(|| (containing_scope.clone(), Vec::new(), Vec::new()));

            // Check if this expression is an assignment (creates new variables)
            let is_assignment = !expression.assigment_target_symbols.is_empty();

            if is_assignment {
                scope_group.1.push(expression.clone()); // assignments
            }

            // Check if this expression contains identifier references (uses existing variables)
            // Note: An expression can be both an assignment AND a use (e.g., y = x.connect())
            let has_identifier_use = expression.symbols.iter().any(|sym| {
                matches!(
                    sym.symbol_type,
                    TypeScriptSymbolType::Identifier
                        | TypeScriptSymbolType::Call
                        | TypeScriptSymbolType::MethodCall
                )
            });

            if has_identifier_use {
                scope_group.2.push(expression.clone()); // uses
            }
        }
    }

    let mut expression_chains = Vec::new();

    // Second pass: build expression chains by linking assignments to their uses
    for (_, assignments, uses) in scope_groups.into_values() {
        if assignments.is_empty() || uses.is_empty() {
            continue;
        }

        // For each assignment, find all uses that reference the same variable
        for assignment in &assignments {
            let assignment_target_names: std::collections::HashSet<String> = assignment
                .assigment_target_symbols
                .iter()
                .map(|sym| sym.name.clone())
                .collect();

            // Find all uses that reference any of the assignment target names
            for use_expr in &uses {
                let use_names: std::collections::HashSet<String> = use_expr
                    .symbols
                    .iter()
                    .map(|sym| sym.name.clone())
                    .collect();

                // If this use references any of the assignment targets, create a chain
                if !assignment_target_names.is_disjoint(&use_names) {
                    expression_chains.push(vec![assignment.clone(), use_expr.clone()]);
                }
            }
        }
    }

    expression_chains
}

/// Enhanced scope context for tracking hierarchical relationships
#[derive(Debug, Clone)]
pub struct ScopeContext {
    /// Current scope chain from innermost to outermost
    pub scope_chain: Vec<TypeScriptSymbol>,
    /// Mapping of symbols to their direct children
    pub scope_children: std::collections::HashMap<TypeScriptSymbol, Vec<TypeScriptSymbol>>,
    /// Mapping of symbols to their direct parent
    pub scope_parents: std::collections::HashMap<TypeScriptSymbol, TypeScriptSymbol>,
}

impl Default for ScopeContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeContext {
    pub fn new() -> Self {
        Self {
            scope_chain: Vec::new(),
            scope_children: std::collections::HashMap::new(),
            scope_parents: std::collections::HashMap::new(),
        }
    }

    /// Get the containing scope for a symbol
    pub fn get_containing_scope(&self, symbol: &TypeScriptSymbol) -> Option<&TypeScriptSymbol> {
        self.scope_parents.get(symbol)
    }

    /// Get the children of a scope
    pub fn get_scope_children(&self, scope: &TypeScriptSymbol) -> Option<&Vec<TypeScriptSymbol>> {
        self.scope_children.get(scope)
    }

    /// Get the full scope chain from current symbol to root
    pub fn get_scope_chain_for(&self, symbol: &TypeScriptSymbol) -> Vec<TypeScriptSymbol> {
        let mut chain = vec![symbol.clone()];
        let mut current = symbol;

        while let Some(parent) = self.scope_parents.get(current) {
            chain.push(parent.clone());
            current = parent;
        }

        chain
    }
}

pub type ScopeCrawlCallback = fn(
    &TypeScriptSymbol,
    &Option<TypeScriptSymbol>,
    &TypeScriptExpression,
    &ScopeContext,
    &Vec<TypeScriptSymbol>,
) -> bool;

/// Enhanced scope crawling function with better context tracking
pub fn crawl_scope<F>(
    expression: &TypeScriptExpression,
    symbol_index: &IntervalTree<TypeScriptSymbol>,
    callback: F,
) -> Option<TypeScriptSymbol>
where
    F: Fn(
        &TypeScriptSymbol,
        &Option<TypeScriptSymbol>,
        &TypeScriptExpression,
        &ScopeContext,
        &Vec<TypeScriptSymbol>,
    ) -> bool,
{
    let mut visited = HashSet::new();
    let mut scope_context = ScopeContext::new();

    // Build scope hierarchy first
    build_scope_hierarchy(expression, symbol_index, &mut scope_context);

    // Extract symbols from the expression for analysis
    let expression_symbols = extract_expression_symbols(expression);

    // Start traversal from the most specific (innermost) scope
    let expr_start = expression.range.byte_offset.0 as u64;
    let expr_end = expression.range.byte_offset.1 as u64;

    // Find all containing scopes, ordered from innermost to outermost
    let containing_scopes = symbol_index.find_all_containing(expr_start, expr_end);

    // Process each scope level
    for scope_symbol in containing_scopes {
        if visited.contains(scope_symbol) {
            continue;
        }
        visited.insert(scope_symbol.clone());

        let parent_scope = scope_context.get_containing_scope(scope_symbol);

        // Call the visitor callback with enhanced context
        let should_continue = callback(
            scope_symbol,
            &parent_scope.cloned(),
            expression,
            &scope_context,
            &expression_symbols,
        );

        if !should_continue {
            return Some(scope_symbol.clone());
        }
    }

    None
}

/// Build a comprehensive scope hierarchy for the given expression
fn build_scope_hierarchy(
    expression: &TypeScriptExpression,
    symbol_index: &IntervalTree<TypeScriptSymbol>,
    scope_context: &mut ScopeContext,
) {
    let expr_start = expression.range.byte_offset.0 as u64;
    let expr_end = expression.range.byte_offset.1 as u64;

    // Get all symbols that could be relevant to this expression
    let all_containing = symbol_index.find_all_containing(expr_start, expr_end);

    // Build parent-child relationships
    for symbol in &all_containing {
        let symbol_start = symbol.range().byte_offset.0 as u64;
        let symbol_end = symbol.range().byte_offset.1 as u64;

        // Find immediate children
        let children = symbol_index.find_immediate_children(symbol_start, symbol_end);
        scope_context
            .scope_children
            .insert((*symbol).clone(), children.into_iter().cloned().collect());

        // Find immediate parent
        if let Some(parent) = symbol_index.find_immediate_parent(symbol_start, symbol_end) {
            scope_context
                .scope_parents
                .insert((*symbol).clone(), parent.clone());
        }
    }

    // Build scope chain from innermost to outermost
    if let Some(innermost) = all_containing.first() {
        scope_context.scope_chain = scope_context.get_scope_chain_for(innermost);
    }
}

/// Extract relevant symbols from an expression for analysis
fn extract_expression_symbols(expression: &TypeScriptExpression) -> Vec<TypeScriptSymbol> {
    // For now, we'll convert expression symbols to TypeScriptSymbol::Unknown
    // This could be enhanced to create proper TypeScriptSymbol::Reference instances
    expression
        .symbols
        .iter()
        .map(|_| TypeScriptSymbol::Unknown)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::create_typescript_parser;
    use crate::typescript::swc::definitions::extract_swc_definitions;
    use crate::typescript::swc::expressions::extract_swc_expressions;
    use crate::typescript::swc::imports::extract_swc_imports;
    use crate::typescript::swc::references::types::TypeScriptReferenceMetadata;
    use crate::typescript::swc::references::types::*;

    fn get_expressions(code: &str) -> Vec<TypeScriptExpression> {
        let parser = create_typescript_parser();
        let parse_result = parser.parse(code, Some("test.ts")).unwrap();

        extract_swc_expressions(&parse_result.ast)
    }

    fn print_expressions(expressions: &Vec<TypeScriptExpression>) {
        for expression in expressions {
            println!("Expression: {:?}", expression.string);
        }
    }

    #[test]
    fn test_fold_expressions() {
        let code: &'static str = r#"
          this.processNotification(notification)
                    .then(() => this.emit('notification:success', notification))
                    .catch(error => this.emit('notification:failed', notification, error.message));
          
        "#;

        let expressions = get_expressions(code);
        println!("Original expressions:");
        print_expressions(&expressions);
        println!("--------------------------------");
        println!("Folded expressions:");
        let folded_expressions = fold_expressions::<TypeScriptExpression>(&expressions);
        print_expressions(&folded_expressions);

        // Should have exactly one folded expression (the entire chain)
        assert_eq!(
            folded_expressions.len(),
            1,
            "Should have exactly one folded expression"
        );

        let main_expr = &folded_expressions[0];

        // Verify expression range
        assert_eq!(main_expr.range.start.line, 1);
        assert_eq!(main_expr.range.start.column, 10);
        assert_eq!(main_expr.range.end.line, 3);
        assert_eq!(main_expr.range.end.column, 98);

        // Verify we have the expected number of symbols
        assert!(
            main_expr.symbols.len() >= 7,
            "Should have at least 7 symbols in the folded expression"
        );

        // Verify specific symbols are present with correct types and metadata
        let symbol_names: Vec<&str> = main_expr.symbols.iter().map(|s| s.name.as_str()).collect();
        println!("Symbol names: {symbol_names:?}");

        // Check for key symbols
        assert!(
            symbol_names.contains(&"this"),
            "Should contain 'this' symbol"
        );
        assert!(
            symbol_names.contains(&"processNotification"),
            "Should contain 'processNotification' symbol"
        );
        assert!(
            symbol_names.contains(&"then"),
            "Should contain 'then' symbol"
        );
        assert!(
            symbol_names.contains(&"catch"),
            "Should contain 'catch' symbol"
        );
        assert!(
            symbol_names.contains(&"emit"),
            "Should contain 'emit' symbol"
        );

        // Verify MethodCallSource symbols
        let this_symbols: Vec<_> = main_expr
            .symbols
            .iter()
            .filter(|s| s.name == "this" && s.symbol_type == TypeScriptSymbolType::MethodCallSource)
            .collect();
        assert_eq!(
            this_symbols.len(),
            3,
            "Should have 3 'this' MethodCallSource symbols"
        );

        // Verify MethodCall symbols
        let method_calls: Vec<_> = main_expr
            .symbols
            .iter()
            .filter(|s| s.symbol_type == TypeScriptSymbolType::MethodCall)
            .collect();
        let method_call_names: Vec<&str> = method_calls.iter().map(|s| s.name.as_str()).collect();
        println!("Method call names: {method_call_names:?}");
        assert_eq!(
            method_calls.len(),
            5,
            "Should have 5 MethodCall symbols (processNotification, then, catch, emit x2)"
        );
    }

    #[test]
    fn test_fold_expressions_validates_exact_terminal_output() {
        let code: &'static str = r#"
          this.processNotification(notification)
                    .then(() => this.emit('notification:success', notification))
                    .catch(error => this.emit('notification:failed', notification, error.message));
          
        "#;
        let expressions = get_expressions(code);
        let folded_expressions = fold_expressions::<TypeScriptExpression>(&expressions);

        assert_eq!(
            folded_expressions.len(),
            1,
            "Should have exactly one folded expression"
        );
        let main_expr = &folded_expressions[0];

        // Validate the exact symbol names from terminal output
        let symbol_names: Vec<&str> = main_expr.symbols.iter().map(|s| s.name.as_str()).collect();
        let expected_names = vec![
            "this",
            "processNotification",
            "then",
            "this",
            "emit",
            "catch",
            "this",
            "emit",
        ];
        assert_eq!(
            symbol_names, expected_names,
            "Symbol names should match terminal output exactly"
        );

        // Validate method call names
        let method_calls: Vec<_> = main_expr
            .symbols
            .iter()
            .filter(|s| s.symbol_type == TypeScriptSymbolType::MethodCall)
            .collect();
        let method_call_names: Vec<&str> = method_calls.iter().map(|s| s.name.as_str()).collect();
        let expected_method_names = vec!["processNotification", "then", "emit", "catch", "emit"];
        assert_eq!(
            method_call_names, expected_method_names,
            "Method call names should match terminal output exactly"
        );

        // Validate Call metadata structure matches terminal output
        let call_symbols: Vec<_> = main_expr
            .symbols
            .iter()
            .filter(|s| matches!(s.metadata, Some(TypeScriptReferenceMetadata::Call { .. })))
            .collect();

        // Should have 8 call symbols based on terminal output (3 'this' sources + 5 method calls)
        assert_eq!(
            call_symbols.len(),
            8,
            "Should have 8 symbols with Call metadata"
        );

        // Verify specific calls match the terminal output structure:

        // 1. First 'this' call (MethodCallSource)
        let first_this = &call_symbols[0];
        assert_eq!(first_this.name, "this");
        if let Some(TypeScriptReferenceMetadata::Call {
            args,
            is_this,
            is_async,
            is_super,
        }) = &first_this.metadata
        {
            assert!(!is_async && !is_super && *is_this);
            assert_eq!(args.len(), 0);
        }

        // 2. processNotification call
        let process_call = call_symbols
            .iter()
            .find(|s| s.name == "processNotification")
            .expect("Should find processNotification call");
        if let Some(TypeScriptReferenceMetadata::Call { args, is_this, .. }) =
            &process_call.metadata
        {
            assert!(*is_this);
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].symbol, "notification");
            assert_eq!(args[0].symbol_type, TypeScriptSymbolType::Identifier);
        }

        // 3. then call with arrow function callback
        let then_call = call_symbols
            .iter()
            .find(|s| s.name == "then")
            .expect("Should find then call");
        if let Some(TypeScriptReferenceMetadata::Call { args, is_this, .. }) = &then_call.metadata {
            assert!(*is_this);
            assert_eq!(args.len(), 1);
            let callback_arg = &args[0];
            assert_eq!(
                callback_arg.symbol,
                "() => this.emit('notification:success', notification)"
            );
            assert_eq!(
                callback_arg.symbol_type,
                TypeScriptSymbolType::ArrowFunctionCallback
            );

            // Verify callback metadata
            if let Some(TypeScriptReferenceMetadata::Callback {
                parameters,
                body,
                is_async,
            }) = &callback_arg.metadata
            {
                assert!(!is_async);
                assert_eq!(parameters.len(), 0);
                assert_eq!(
                    body.as_ref().unwrap(),
                    "this.emit('notification:success', notification)"
                );
            } else {
                panic!("Callback argument should have Callback metadata");
            }
        }

        // 4. catch call with arrow function callback
        let catch_call = call_symbols
            .iter()
            .find(|s| s.name == "catch")
            .expect("Should find catch call");
        if let Some(TypeScriptReferenceMetadata::Call { args, is_this, .. }) = &catch_call.metadata
        {
            assert!(*is_this);
            assert_eq!(args.len(), 1);
            let callback_arg = &args[0];
            assert_eq!(
                callback_arg.symbol,
                "error => this.emit('notification:failed', notification, error.message)"
            );
            assert_eq!(
                callback_arg.symbol_type,
                TypeScriptSymbolType::ArrowFunctionCallback
            );

            // Verify callback metadata
            if let Some(TypeScriptReferenceMetadata::Callback {
                parameters,
                body,
                is_async,
            }) = &callback_arg.metadata
            {
                assert!(!is_async);
                assert_eq!(parameters.len(), 1);
                assert_eq!(parameters[0], "error");
                assert_eq!(
                    body.as_ref().unwrap(),
                    "this.emit('notification:failed', notification, error.message)"
                );
            }
        }

        // 5. First emit call
        let emit_calls: Vec<_> = call_symbols.iter().filter(|s| s.name == "emit").collect();
        assert_eq!(emit_calls.len(), 2, "Should have 2 emit calls");

        let first_emit = emit_calls[0];
        if let Some(TypeScriptReferenceMetadata::Call { args, is_this, .. }) = &first_emit.metadata
        {
            assert!(*is_this);
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].symbol, "'notification:success'");
            assert_eq!(args[1].symbol, "notification");
        }

        // 6. Second emit call
        let second_emit = emit_calls[1];
        if let Some(TypeScriptReferenceMetadata::Call { args, is_this, .. }) = &second_emit.metadata
        {
            assert!(*is_this);
            assert_eq!(args.len(), 3);
            assert_eq!(args[0].symbol, "'notification:failed'");
            assert_eq!(args[1].symbol, "notification");
            assert_eq!(args[2].symbol, "error.message");
        }
    }

    #[test]
    fn test_fold_expressions_with_aliases() {
        let code: &'static str = r#"
          const {id: userId, name: userName} = this.service.getData(userId)
                    .then(data => this.transform(data, userName));
          const {id: userId, name: userName} = this.service.getData(userId).transform();
        "#;
        let expressions = get_expressions(code);
        println!("Alias test - Original expressions:");
        print_expressions(&expressions);
        println!("--------------------------------");
        println!("Alias test - Folded expressions:");
        let folded_expressions = fold_expressions::<TypeScriptExpression>(&expressions);
        print_expressions(&folded_expressions);

        // Verify that alias information is preserved
        let has_alias = folded_expressions.iter().any(|expr| {
            expr.assigment_target_symbols.iter().any(|symbol| {
                if let Some(TypeScriptReferenceMetadata::Assignment { aliased_from, .. }) =
                    &symbol.metadata
                {
                    aliased_from.is_some()
                } else {
                    false
                }
            })
        });
        assert!(has_alias);
        if !has_alias {
            println!("✗ Alias information was not found in folded expressions");
        }
    }

    #[test]
    fn test_complex_expressions_with_folding() {
        let code = include_str!("../../fixtures/typescript/references/complex.ts");
        let expressions = get_expressions(code);
        let folded_expressions = fold_expressions::<TypeScriptExpression>(&expressions);
        print_expressions(&folded_expressions);
        println!("Total # of Expressions: {:?}", expressions.len());
        println!(
            "Total # of Folded Expressions: {:?}",
            folded_expressions.len()
        );
        assert_eq!(folded_expressions.len(), 95);
    }

    #[test]
    fn test_complex_expressions_with_folding_and_references() {
        let code = r#"
        // Arrow function callbacks
        setTimeout(() => console.log('done'), 1000);
        promise.then(result => result.data);
        numbers.map(num => num * 2);
        
        // Function expression callbacks
        array.forEach(function(item) {
            console.log(item);
        });
        
        // Named function expression callbacks
        button.addEventListener('click', function onClick(event) {
            event.preventDefault();
        });
        
        // Async arrow function callbacks
        asyncOperation(async (data) => {
            await processData(data);
        });
        
        // Promise constructor with callback
        new Promise(resolve => setTimeout(resolve, 100));
        new Promise((resolve, reject) => {
            if (Math.random() > 0.5) {
                resolve('success');
            } else {
                reject('error');
            }
        });
        
        // Mixed arguments (callbacks and regular)
        higherOrder('regularArg', (x) => x + 1, 42);
        "#;
        let expressions = get_expressions(code);
        let folded_expressions = fold_expressions::<TypeScriptExpression>(&expressions);
        print_expressions(&folded_expressions);
        assert_eq!(folded_expressions.len(), 9);
    }

    #[test]
    fn test_enhanced_crawl_scope_with_complex_expressions() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let code = r#"
        class NotificationProcessor {
            private handlerFactory: NotificationHandlerFactory;
            
            constructor() {
                this.setupEventListeners();
            }
            
            processNotification(notification) {
                return this.handlerFactory.createHandler(notification.type);
            }
            
            setupEventListeners() {
                // Event listener setup
            }
        }
        
        const processor = new NotificationProcessor();
        processor.processNotification({type: 'email'});
        "#;

        let parser = create_typescript_parser();
        let parse_result = parser.parse(code, Some("test.ts")).unwrap();
        let definitions = extract_swc_definitions(&parse_result.ast);
        let imports = extract_swc_imports(&parse_result.ast);
        let expressions = extract_swc_expressions(&parse_result.ast);

        // Build symbol index
        let mut symbols = vec![];
        for d in &definitions {
            symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
        }
        for i in &imports {
            symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
        }
        let symbol_index = IntervalTree::new(symbols);

        let visited_scopes = Rc::new(RefCell::new(Vec::new()));
        let found_relationships = Rc::new(RefCell::new(Vec::new()));

        // Test the enhanced crawl_scope with complex member access expressions
        for expression in &expressions {
            // Look for expressions with method calls (like this.handlerFactory.createHandler)
            let has_method_call = expression
                .symbols
                .iter()
                .any(|s| s.symbol_type == TypeScriptSymbolType::MethodCall);

            if has_method_call {
                println!(
                    "Testing enhanced crawl_scope for expression: {}",
                    expression.string
                );

                let visited_scopes_clone = visited_scopes.clone();
                let found_relationships_clone = found_relationships.clone();

                let result = crawl_scope(
                    expression,
                    &symbol_index,
                    |symbol, parent, _expr, context, _expr_symbols| {
                        visited_scopes_clone.borrow_mut().push(symbol.clone());

                        let symbol_name = symbol.name().unwrap_or_else(|| "unnamed".to_string());
                        let parent_name = parent
                            .as_ref()
                            .map(|p| p.name().unwrap_or_else(|| "unnamed".to_string()))
                            .unwrap_or_else(|| "none".to_string());

                        println!("  Visiting symbol: {symbol_name} (parent: {parent_name})");

                        // Check scope chain
                        let scope_chain = context.get_scope_chain_for(symbol);
                        let chain_names: Vec<String> = scope_chain
                            .iter()
                            .map(|s| s.name().unwrap_or_else(|| "unnamed".to_string()))
                            .collect();
                        println!("    Scope chain: {chain_names:?}");

                        // Look for method definitions in class scopes
                        if let TypeScriptSymbol::Definition(def) = symbol
                            && def.definition_type
                                == crate::typescript::types::TypeScriptDefinitionType::Class
                            && let Some(children) = context.get_scope_children(symbol)
                        {
                            let method_names: Vec<String> = children.iter()
                                    .filter_map(|child| {
                                        if let TypeScriptSymbol::Definition(child_def) = child {
                                            if child_def.definition_type == crate::typescript::types::TypeScriptDefinitionType::Method {
                                                Some(child_def.name.clone())
                                            } else { None }
                                        } else { None }
                                    })
                                    .collect();

                            if !method_names.is_empty() {
                                found_relationships_clone
                                    .borrow_mut()
                                    .push((symbol_name.clone(), method_names.clone()));
                                println!(
                                    "    Class {symbol_name} contains methods: {method_names:?}"
                                );
                            }
                        }

                        true // Continue traversal
                    },
                );

                if let Some(resolved) = result {
                    println!(
                        "  Resolved to: {}",
                        resolved.name().unwrap_or_else(|| "unnamed".to_string())
                    );
                } else {
                    println!("  No resolution found");
                }
                println!("--------------------------------");
            }
        }

        // Verify that we found some meaningful relationships
        let relationships = found_relationships.borrow();
        println!("Found relationships: {:?}", *relationships);

        // Should have found at least one class with methods
        assert!(
            !relationships.is_empty(),
            "Should have found at least one class-method relationship"
        );

        // Should have visited some scopes
        assert!(
            !visited_scopes.borrow().is_empty(),
            "Should have visited some scopes"
        );
    }
}
