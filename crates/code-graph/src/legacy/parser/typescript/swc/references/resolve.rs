use crate::legacy::parser::references;
use crate::legacy::parser::typescript::swc::references::types::{
    TypeScriptExpression, TypeScriptReferenceInfo, TypeScriptReferenceType, TypeScriptSymbol,
    TypeScriptSymbolType, TypeScriptTargetResolution,
};
use crate::legacy::parser::typescript::types::{
    TypeScriptDefinitionInfo, TypeScriptDefinitionType, TypeScriptFqn, TypeScriptImportedSymbolInfo,
};
use crate::utils::{IntervalTree, Range};
use std::collections::HashMap;

pub fn find_containing_fqn(
    symbol_index: &IntervalTree<TypeScriptSymbol>,
    range: Range,
) -> Option<TypeScriptFqn> {
    let expr_start = range.byte_offset.0 as u64;
    let expr_end = range.byte_offset.1 as u64;
    let containing_scope = symbol_index.find_immediate_parent(expr_start, expr_end);
    if let Some(TypeScriptSymbol::Definition(containing_scope)) = containing_scope {
        Some(containing_scope.fqn.clone())
    } else {
        None
    }
}

pub fn find_references(
    definitions: &[TypeScriptDefinitionInfo],
    _imports: &[TypeScriptImportedSymbolInfo],
    expression: &TypeScriptExpression,
    symbol_index: &IntervalTree<TypeScriptSymbol>,
) -> Vec<TypeScriptReferenceInfo> {
    let mut references = vec![];

    // resolve method calls
    let head = expression
        .symbols
        .iter()
        .find(|s| s.symbol_type == TypeScriptSymbolType::MethodCall);

    if let Some(head) = head {
        let symbol = head;
        let definition = definitions.iter().find(|d| d.name == symbol.name);
        if let Some(definition) = definition {
            if definition.definition_type == TypeScriptDefinitionType::Method {
                references.push(TypeScriptReferenceInfo::new(
                    symbol.name.clone(),
                    symbol.range,
                    references::ReferenceTarget::Resolved(Box::new(
                        TypeScriptTargetResolution::Definition(definition.clone()),
                    )),
                    TypeScriptReferenceType::MethodCall,
                    None,
                    find_containing_fqn(symbol_index, expression.range),
                ));
            }
        } else {
            // Either partially resolved or unresolved
            references.push(TypeScriptReferenceInfo::new(
                symbol.name.clone(),
                symbol.range,
                references::ReferenceTarget::Unresolved(),
                TypeScriptReferenceType::MethodCall,
                None,
                None,
            ));
        }
    }

    // resolve constructor calls
    let head = expression
        .symbols
        .iter()
        .find(|s| s.symbol_type == TypeScriptSymbolType::ConstructorCall);
    if let Some(head) = head {
        let symbol = head;
        let definition = definitions.iter().find(|d| d.name == symbol.name);
        if let Some(definition) = definition {
            if definition.definition_type == TypeScriptDefinitionType::Class {
                references.push(TypeScriptReferenceInfo::new(
                    symbol.name.clone(),
                    symbol.range,
                    references::ReferenceTarget::Resolved(Box::new(
                        TypeScriptTargetResolution::Definition(definition.clone()),
                    )),
                    TypeScriptReferenceType::ConstructorCall,
                    None,
                    find_containing_fqn(symbol_index, expression.range),
                ));
            }
        } else {
            // Either partially resolved or unresolved
            references.push(TypeScriptReferenceInfo::new(
                symbol.name.clone(),
                symbol.range,
                references::ReferenceTarget::Unresolved(),
                TypeScriptReferenceType::ConstructorCall,
                None,
                None,
            ));
        }
    }

    references
}

/// Main entry point for resolving function call references in a TypeScript/JavaScript file
pub fn resolve_references<'a>(
    definitions: &'a [TypeScriptDefinitionInfo],
    imports: &'a [TypeScriptImportedSymbolInfo],
    expressions: &'a [TypeScriptExpression],
) -> Vec<TypeScriptReferenceInfo> {
    let mut symbols = vec![];

    for d in definitions {
        symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
    }

    for i in imports {
        symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
    }

    let symbol_lookup_table = build_lookup_table(&symbols);
    let symbol_index = IntervalTree::new(symbols);

    let mut references = vec![];

    let mut exprs_to_remove = vec![];
    for expr in expressions {
        if let Some(reference) = resolve_simple_call(expr, &symbol_lookup_table, &symbol_index) {
            references.push(reference);
            exprs_to_remove.push(expr.range);
        }
    }

    // Remove expressions from the search space that we've already resolved
    let expressions: Vec<TypeScriptExpression> = expressions
        .iter()
        .filter(|expr| !exprs_to_remove.contains(&expr.range))
        .cloned()
        .collect();

    for expr in expressions {
        references.extend(find_references(definitions, imports, &expr, &symbol_index));
    }

    references
}

fn build_lookup_table(
    symbols: &Vec<(Range, TypeScriptSymbol)>,
) -> HashMap<String, Vec<TypeScriptSymbol>> {
    let mut symbol_table: HashMap<String, Vec<TypeScriptSymbol>> = HashMap::new();
    for (_range, symbol) in symbols {
        if symbol.symbol_type() == TypeScriptSymbolType::Reference {
            continue;
        }
        if let Some(name) = symbol.name() {
            symbol_table.entry(name).or_default().push(symbol.clone());
        }
    }
    symbol_table
}

// NOTE: This does not handle shadowing
fn resolve_simple_call(
    expression: &TypeScriptExpression,
    symbol_table: &HashMap<String, Vec<TypeScriptSymbol>>,
    symbol_index: &IntervalTree<TypeScriptSymbol>,
) -> Option<TypeScriptReferenceInfo> {
    let head = expression.symbols.first();
    if let Some(head) = head
        && head.symbol_type == TypeScriptSymbolType::Call
    {
        // println!("expression with head Call: {:?}", expression.string);
        let symbols = symbol_table.get(&head.name);
        if let Some(symbols) = symbols
            && symbols.len() == 1
        {
            let symbol = &symbols[0];
            if symbol.symbol_type() == TypeScriptSymbolType::Definition {
                let definition = symbol.definition().unwrap();
                return Some(TypeScriptReferenceInfo::new(
                    head.name.clone(),
                    head.range,
                    references::ReferenceTarget::Resolved(Box::new(
                        TypeScriptTargetResolution::Definition(definition.clone()),
                    )),
                    TypeScriptReferenceType::FunctionCall,
                    None,
                    find_containing_fqn(symbol_index, expression.range),
                ));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::parser::create_typescript_parser;
    use crate::legacy::parser::references::ReferenceTarget;
    use crate::legacy::parser::typescript::swc::definitions::extract_swc_definitions;
    use crate::legacy::parser::typescript::swc::expressions::extract_swc_expressions;
    use crate::legacy::parser::typescript::swc::imports::extract_swc_imports;
    use crate::legacy::parser::typescript::swc::references::utils::{
        crawl_scope, expression_index, find_shared_scope_expressions, fold_expressions,
    };
    use crate::utils::IntervalTree;

    struct TypescriptParseResult {
        definitions: Vec<TypeScriptDefinitionInfo>,
        imports: Vec<TypeScriptImportedSymbolInfo>,
        expressions: Vec<TypeScriptExpression>,
    }

    fn parse_typescript_code(code: &str) -> TypescriptParseResult {
        let parser = create_typescript_parser();
        let parse_result = parser.parse(code, Some("test.ts")).unwrap();
        let definitions = extract_swc_definitions(&parse_result.ast);
        let imports = extract_swc_imports(&parse_result.ast);
        let expressions = extract_swc_expressions(&parse_result.ast);
        TypescriptParseResult {
            definitions,
            imports,
            expressions,
        }
    }

    fn print_references(references: &Vec<TypeScriptReferenceInfo>) -> (usize, usize, usize) {
        let mut unresolved_refs = vec![];
        let mut ambiguous_refs = vec![];
        let mut resolved_refs = vec![];
        println!("--------------------------------");
        for r in references {
            match &r.target {
                ReferenceTarget::Resolved(_) => {
                    resolved_refs.push(r);
                }
                ReferenceTarget::Ambiguous(_) => {
                    ambiguous_refs.push(r);
                }
                ReferenceTarget::Unresolved() => {
                    unresolved_refs.push(r);
                }
            }
        }

        for r in &unresolved_refs {
            println!(
                "Unresolved reference: NAME({:?})[sl={}, el={}] (type={:?})",
                r.name, r.range.start.line, r.range.end.line, r.reference_type
            );
        }
        for r in &ambiguous_refs {
            println!(
                "Ambiguous reference: NAME({:?})[sl={}, el={}] (type={:?})",
                r.name, r.range.start.line, r.range.end.line, r.reference_type
            );
        }
        for r in &resolved_refs {
            println!(
                "Resolved reference: NAME({:?})[sl={}, el={}] (type={:?})",
                r.name, r.range.start.line, r.range.end.line, r.reference_type
            );
            println!("Resolved reference TARGET {:?}", r.target);
            println!("Resolved reference SCOPE {:?}", r.scope);
        }

        (
            unresolved_refs.len(),
            ambiguous_refs.len(),
            resolved_refs.len(),
        )
    }

    #[test]
    fn test_basic_global_function_resolution() {
        let code = r#"
        const { getRequest } = require("express");

        function globalFunction() {
            return "hello";
        }
        
        function anotherFunction() {
            return "world";
        }
        
        // This should resolve to the globalFunction above
        globalFunction();
        
        // This should resolve to anotherFunction above  
        anotherFunction();
        
        // This should not resolve (no definition)
        unknownFunction();
        "#;

        let parse_result = parse_typescript_code(code);
        let references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );
        println!("references: {:?}", references.len());
    }

    #[test]
    fn test_scope_shadowing() {
        let code = r#"
        const { getRequest } = require("express");

        function globalFunction() {
            return "global";
        }
        
        function testShadowing() {
            function globalFunction() {
                return "local";
            }
            // This should resolve to the local function, not the global one
            globalFunction();
            unknownFunction();
        }

        testShadowing();
        "#;

        let parse_result = parse_typescript_code(code);
        let references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );
        println!("references: {:?}", references.len());
    }

    #[test]
    fn test_complex_references() {
        let code = include_str!("../../fixtures/typescript/references/complex.ts");
        let parse_result = parse_typescript_code(code);
        let references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );
        print_references(&references);
    }

    #[test]
    fn test_gfm_auto_complete_references() {
        let code = include_str!("../../fixtures/javascript/references/gfm_auto_complete.js");
        let parse_result = parse_typescript_code(code);
        let references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );
        let (unresolved_refs, ambiguous_refs, resolved_refs) = print_references(&references);
        println!("references count: {:?}", references.len());
        println!("unresolved_refs: {unresolved_refs:?}");
        println!("ambiguous_refs: {ambiguous_refs:?}");
        println!("resolved_refs: {resolved_refs:?}");
    }

    #[test]
    fn test_require_import_filtering() {
        let code = r#"
            // These should be filtered out
            const fs = require('fs');
            const path = await import('path');
            const { readFile, writeFile } = require('fs/promises');
            
            // // These should be included
            console.log('test');
            someFunction();
            obj.method();
        "#;

        let parse_result = parse_typescript_code(code);
        let references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );
        println!("references: {:?}", references.len());
    }

    #[test]
    fn test_shared_scope_expressions() {
        let code = r#"
        function testScope() {
            x = foo();    // assignment (call expression)
            x();          // use (call expression)
            
            y = bar();    // assignment (call expression)
            console.log(y); // use (call expression)
            
            // Different scope
            function innerScope() {
                z = baz();  // assignment in different scope
                z();        // use in different scope
            }
        }
        "#;

        let parse_result = parse_typescript_code(code);
        let _references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );

        // Debug: print what expressions we found
        println!("Raw expressions found:");
        for expr in &parse_result.expressions {
            println!("  Expression: {}", expr.string);
            println!(
                "    Assignment targets: {:?}",
                expr.assigment_target_symbols
                    .iter()
                    .map(|s| &s.name)
                    .collect::<Vec<_>>()
            );
            println!(
                "    Symbols: {:?}",
                expr.symbols.iter().map(|s| &s.name).collect::<Vec<_>>()
            );
        }

        let mut symbols = vec![];
        for d in &parse_result.definitions {
            symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
        }
        for i in &parse_result.imports {
            symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
        }

        // Debug: print what symbols we found
        println!("Symbols found:");
        for (range, symbol) in &symbols {
            println!("  Symbol: {:?} at {:?}", symbol.name(), range);
        }

        let expressions = fold_expressions::<TypeScriptExpression>(&parse_result.expressions);
        let expression_index = expression_index(&expressions);
        let symbol_index = IntervalTree::new(symbols);

        let expression_chains =
            find_shared_scope_expressions(&expressions, &symbol_index, &expression_index);

        println!("Found {} expression chains", expression_chains.len());
        for (i, chain) in expression_chains.iter().enumerate() {
            println!("Chain {i}:");
            for (j, expr) in chain.iter().enumerate() {
                let head = expr.symbols.first().unwrap();
                println!("  [{}]: {}", j, head.name);
            }
        }

        // Should find at least one expression chain
        assert!(
            !expression_chains.is_empty(),
            "Should find at least one expression chain"
        );

        // Verify that we found meaningful chains (each chain should have at least 2 expressions)
        let first_chain = &expression_chains[0];
        assert!(
            first_chain.len() >= 2,
            "Each chain should have at least 2 expressions (assignment + use)"
        );

        // Check that the first expression in each chain is an assignment
        assert!(
            !first_chain[0].assigment_target_symbols.is_empty(),
            "First expression in chain should be an assignment"
        );

        // Check that subsequent expressions use the assigned variable
        let assignment_target_names: std::collections::HashSet<String> = first_chain[0]
            .assigment_target_symbols
            .iter()
            .map(|sym| sym.name.clone())
            .collect();

        let use_names: std::collections::HashSet<String> = first_chain[1]
            .symbols
            .iter()
            .map(|sym| sym.name.clone())
            .collect();

        let shared_names: Vec<_> = assignment_target_names.intersection(&use_names).collect();
        println!("Shared variable names: {shared_names:?}");
        assert!(
            !shared_names.is_empty(),
            "Should have shared variable names between assignment and use in chain"
        );
    }

    #[test]
    fn test_shared_scope_class_and_property_access() {
        let code = r#"
        function processData() {
            // Class instantiation and method calls
            x = new DatabaseConnection();
            y = x.connect();        // x is used here
            x.disconnect();         // x is used again (no assignment)
            z = x;
            z.connect();
            
            // Property access and method chaining
            config = getConfig();
            result = config.database.host;  // config is used here
            config.validate();      // config is used again (no assignment)
            
            // Array/object indexing and method calls
            z = obj[idx];
            zz = z.bar();          // z is used here
            z.cleanup();           // z is used again (no assignment)
            
            // Method chaining
            service = createService();
            data = service.getData().transform();  // service is used here
            service.reset();       // service is used again (no assignment)
            
            // Different scope to test isolation
            function innerProcess() {
                inner = new LocalService();
                output = inner.process();  // inner is used here
                inner.close();     // inner is used again (no assignment)
            }
        }
        "#;

        let parse_result = parse_typescript_code(code);
        let _references = resolve_references(
            &parse_result.definitions,
            &parse_result.imports,
            &parse_result.expressions,
        );

        // Debug: print what expressions we found
        println!("Raw expressions found:");
        for expr in &parse_result.expressions {
            println!("  Expression: {}", expr.string);
            println!(
                "    Assignment targets: {:?}",
                expr.assigment_target_symbols
                    .iter()
                    .map(|s| &s.name)
                    .collect::<Vec<_>>()
            );
            println!(
                "    Symbols: {:?}",
                expr.symbols.iter().map(|s| &s.name).collect::<Vec<_>>()
            );
        }

        let mut symbols = vec![];
        for d in &parse_result.definitions {
            symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
        }
        for i in &parse_result.imports {
            symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
        }

        let expressions = fold_expressions::<TypeScriptExpression>(&parse_result.expressions);
        let expression_index = expression_index(&expressions);
        let symbol_index = IntervalTree::new(symbols);

        let expression_chains =
            find_shared_scope_expressions(&expressions, &symbol_index, &expression_index);

        println!("Found {} expression chains", expression_chains.len());
        for (i, chain) in expression_chains.iter().enumerate() {
            println!("Chain {i}:");
            for (j, expr) in chain.iter().enumerate() {
                if j == 0 {
                    println!(
                        "  [{}] Assignment: {} (targets: {:?})",
                        j,
                        expr.string,
                        expr.assigment_target_symbols
                            .iter()
                            .map(|s| &s.name)
                            .collect::<Vec<_>>()
                    );
                } else {
                    println!(
                        "  [{}] Use: {} (symbols: {:?})",
                        j,
                        expr.string,
                        expr.symbols.iter().map(|s| &s.name).collect::<Vec<_>>()
                    );
                }
            }
        }

        // Should find at least one expression chain
        assert!(
            !expression_chains.is_empty(),
            "Should find at least one expression chain"
        );

        // Verify that we found meaningful chains (each chain should have at least 2 expressions)
        assert!(
            expression_chains.iter().all(|chain| chain.len() >= 2),
            "Each chain should have at least 2 expressions (assignment + use)"
        );

        // Collect all chains that contain database-related expressions
        let db_chains: Vec<_> = expression_chains
            .iter()
            .filter(|chain| {
                chain.iter().any(|expr| {
                    expr.string.contains("DatabaseConnection") || expr.string.contains("x.")
                })
            })
            .collect();

        println!("Found {} database-related chains", db_chains.len());
        assert!(
            !db_chains.is_empty(),
            "Should find database-related expression chains"
        );

        // Check for specific expected patterns in the database chains
        let has_class_instantiation = db_chains
            .iter()
            .any(|chain| chain[0].string.contains("new DatabaseConnection"));

        let has_method_calls = db_chains.iter().any(|chain| {
            chain.iter().skip(1).any(|expr| {
                expr.string.contains("x.connect") || expr.string.contains("x.disconnect")
            })
        });

        println!("Has database class instantiation: {has_class_instantiation}");
        println!("Has database method calls: {has_method_calls}");

        // Verify we have the expected patterns
        assert!(
            has_class_instantiation,
            "Should have database class instantiation"
        );
        assert!(has_method_calls, "Should have database method calls");

        // At minimum, we should detect some form of assignment and use patterns
        assert!(
            !expression_chains.is_empty(),
            "Should have expression chains"
        );
    }

    #[test]
    fn test_crawl_scope_with_this_method_calls() {
        let code = r#"
        import { EventEmitter } from 'events';

        class NotificationProcessor extends EventEmitter {
            private handlerFactory: NotificationHandlerFactory;
            
            constructor(handlerFactory) {
                super();
                this.handlerFactory = handlerFactory;
                this.setupEventListeners();
            }
            
            processNotification(notification) {
                // Expression like this.getData() - we want to resolve getData to the containing class
                return this.handlerFactory.createHandler(notification.type);
            }
            
            setupEventListeners() {
                // Complex closure with this references
                this.on('notification:queue', (notification) => {
                    this.processNotification(notification)
                        .then(() => this.emit('notification:success', notification))
                        .catch(error => this.emit('notification:failed', notification, error.message));
                });
            }
            
            getData() {
                return this.handlerFactory.getAll();
            }
        }
        "#;

        let parse_result = parse_typescript_code(code);

        // Build symbol index
        let mut symbols = vec![];
        for d in &parse_result.definitions {
            symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
        }
        for i in &parse_result.imports {
            symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
        }
        let symbol_index = IntervalTree::new(symbols);
        let expressions = fold_expressions::<TypeScriptExpression>(&parse_result.expressions);
        println!("Testing enhanced crawl_scope with this.method() patterns:");

        // Find expressions that contain "this.processNotification" or similar patterns
        for expression in &expressions {
            if expression.string.contains("this.")
                && expression
                    .symbols
                    .iter()
                    .any(|s| s.symbol_type == TypeScriptSymbolType::MethodCall)
            {
                println!("\n=== Analyzing expression: {} ===", expression.string);

                // Use enhanced crawl_scope to understand the context
                let result = crawl_scope(
                    expression,
                    &symbol_index,
                    |symbol, parent, _expr, context, _expr_symbols| {
                        let symbol_name = symbol.name().unwrap_or_else(|| "unnamed".to_string());
                        let parent_name = parent
                            .as_ref()
                            .map(|p| p.name().unwrap_or_else(|| "unnamed".to_string()))
                            .unwrap_or_else(|| "none".to_string());

                        println!("  Scope: {symbol_name} (parent: {parent_name})");

                        // Show the scope chain to understand containment
                        let scope_chain = context.get_scope_chain_for(symbol);
                        let chain_names: Vec<String> = scope_chain
                            .iter()
                            .map(|s| s.name().unwrap_or_else(|| "unnamed".to_string()))
                            .collect();
                        println!("    Chain: {chain_names:?}");

                        // If this is a class, show its methods
                        if let TypeScriptSymbol::Definition(def) = symbol
                            && def.definition_type
                                == crate::legacy::parser::typescript::types::TypeScriptDefinitionType::Class
                            && let Some(children) = context.get_scope_children(symbol)
                        {
                            let method_names: Vec<String> = children.iter()
                                    .filter_map(|child| {
                                        if let TypeScriptSymbol::Definition(child_def) = child {
                                            if child_def.definition_type == crate::legacy::parser::typescript::types::TypeScriptDefinitionType::Method {
                                                Some(child_def.name.clone())
                                            } else { None }
                                        } else { None }
                                    })
                                    .collect();

                            if !method_names.is_empty() {
                                println!("    Available methods: {method_names:?}");

                                // Check if any method in the expression matches available methods
                                for expr_symbol in &_expr.symbols {
                                    if expr_symbol.symbol_type == TypeScriptSymbolType::MethodCall
                                        && method_names.contains(&expr_symbol.name)
                                    {
                                        println!(
                                            "    ✓ Method '{}' resolved to class '{}'",
                                            expr_symbol.name, symbol_name
                                        );
                                    }
                                }
                            }
                        }

                        true // Continue traversal
                    },
                );

                if let Some(resolved) = result {
                    println!(
                        "  Final resolution: {}",
                        resolved.name().unwrap_or_else(|| "unnamed".to_string())
                    );
                }
            }
        }
    }

    #[test]
    fn test_shared_scope_class_and_property_access_complex() {
        let code = include_str!("../../fixtures/typescript/references/complex.ts");

        let parse_result = parse_typescript_code(code);

        // Debug: print what expressions we found
        println!("Raw expressions found:");
        for expr in &parse_result.expressions {
            println!("  Expression: {}", expr.string);
            println!(
                "    Assignment targets: {:?}",
                expr.assigment_target_symbols
                    .iter()
                    .map(|s| &s.name)
                    .collect::<Vec<_>>()
            );
            println!(
                "    Symbols: {:?}",
                expr.symbols.iter().map(|s| &s.name).collect::<Vec<_>>()
            );
        }

        let mut symbols = vec![];
        for d in &parse_result.definitions {
            symbols.push((d.range, TypeScriptSymbol::Definition(d.clone())));
        }
        for i in &parse_result.imports {
            symbols.push((i.range, TypeScriptSymbol::Import(i.clone())));
        }

        let expressions = fold_expressions::<TypeScriptExpression>(&parse_result.expressions);
        let expression_index = expression_index(&expressions);
        let symbol_index = IntervalTree::new(symbols);

        let expression_chains =
            find_shared_scope_expressions(&expressions, &symbol_index, &expression_index);

        println!("Found {} expression chains", expression_chains.len());
        for (i, chain) in expression_chains.iter().enumerate() {
            println!("Chain {i}:");
            for (j, expr) in chain.iter().enumerate() {
                if j == 0 {
                    println!(
                        "  [{}] Assignment: {} (targets: {:?})",
                        j,
                        expr.string,
                        expr.assigment_target_symbols
                            .iter()
                            .map(|s| &s.name)
                            .collect::<Vec<_>>()
                    );
                } else {
                    println!(
                        "  [{}] Use: {} (symbols: {:?})",
                        j,
                        expr.string,
                        expr.symbols.iter().map(|s| &s.name).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
