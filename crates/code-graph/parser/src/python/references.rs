use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;

use crate::python::types::{
    PythonDefinitionInfo, PythonDefinitionType, ScopeGroup, ScopeGroupType, ScopeType,
};
use crate::utils::Range;
use crate::{
    python::types::{
        Binding, BindingValue, Connector, PartialResolution, PythonReferenceInfo,
        PythonReferenceType, PythonTargetResolution, Symbol, SymbolChain, SymbolTable,
        SymbolTableId, SymbolTableTree,
    },
    references::ReferenceTarget,
};

// Types

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct BindingWithScope {
    pub binding: Binding,
    pub scope_id: SymbolTableId,
}

type BindingCache = HashMap<(SymbolChain, Range, SymbolTableId), HashSet<BindingWithScope>>;

// Main

/// Resolves references
pub fn find_references(scope_tree: &SymbolTableTree) -> Vec<PythonReferenceInfo> {
    let mut binding_cache = BindingCache::default();
    let mut references = Vec::new();

    for (scope_id, scope) in scope_tree.iter() {
        for (reference_chain, reference_location) in &scope.references {
            let targets = resolve_reference(
                reference_chain,
                reference_location,
                &scope_id,
                scope_tree,
                &mut binding_cache,
            );
            let reference = PythonReferenceInfo {
                name: reference_chain.as_str(),
                range: *reference_location,
                target: if targets.len() > 1 {
                    ReferenceTarget::Ambiguous(targets)
                } else if targets.len() == 1 {
                    // TODO: Technically, even a single target can be ambiguous (if the alternative is a dead-end)
                    ReferenceTarget::Resolved(Box::new(targets[0].clone()))
                } else {
                    ReferenceTarget::Unresolved()
                },
                reference_type: PythonReferenceType::Call,
                metadata: None,
                scope: scope.fqn.clone(),
            };
            references.push(reference);
        }
    }

    references
}

// Helpers

fn resolve_reference(
    symbol_chain: &SymbolChain,
    location: &Range,
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> Vec<PythonTargetResolution> {
    // Pop the last `Call` symbol
    let function_chain = get_function(symbol_chain.clone());

    // Try to resolve full chain first
    let targets = resolve_symbol_chain(
        &function_chain,
        location,
        scope_id,
        scope_tree,
        binding_cache,
    );

    // If no resolution found, try partial resolution
    if targets.is_empty() && function_chain.symbols.len() > 1 {
        let mut partial_chain = function_chain.clone();

        while partial_chain.symbols.len() > 1 {
            // Drop last symbol from chain and try resolution again
            pop_last_symbol(&mut partial_chain);

            let partial_targets = resolve_symbol_chain(
                &partial_chain,
                location,
                scope_id,
                scope_tree,
                binding_cache,
            );

            if !partial_targets.is_empty() {
                let last_index = partial_chain.symbols.len() - 1;
                let mut final_resolutions = Vec::new();

                for partial_target in partial_targets {
                    // Check if partially resolved to a class definition
                    if let PythonTargetResolution::Definition(def_info) = &partial_target
                        && matches!(
                            def_info.definition_type,
                            PythonDefinitionType::Class | PythonDefinitionType::DecoratedClass
                        )
                    {
                        // Try to continue reoslution within the class scope
                        if let Some(class_resolutions) = resolve_within_class(
                            def_info,
                            &function_chain,
                            partial_chain.symbols.len(),
                            scope_tree,
                            binding_cache,
                        ) {
                            final_resolutions.extend(class_resolutions);
                            continue; // Skip adding as a partial resolution
                        }
                    }

                    // If not a class or couldn't resolve within class, add as partial resolution
                    let partial_resolution =
                        PythonTargetResolution::PartialResolution(PartialResolution {
                            symbol_chain: symbol_chain.clone(),
                            index: last_index,
                            target: Box::new(partial_target),
                        });
                    final_resolutions.push(partial_resolution);
                }

                return final_resolutions;
            }
        }
    }

    targets
}

/// Returns all possible targets for a symbol chain.
fn resolve_symbol_chain(
    symbol_chain: &SymbolChain,
    location: &Range,
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> Vec<PythonTargetResolution> {
    let mut visited = HashSet::default();
    resolve_chain_recursive(
        symbol_chain,
        location,
        scope_id,
        scope_tree,
        binding_cache,
        &mut visited,
    )
}

fn resolve_chain_recursive(
    symbol_chain: &SymbolChain,
    location: &Range,
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
    visited: &mut HashSet<(String, Range, SymbolTableId)>,
) -> Vec<PythonTargetResolution> {
    // Check for circular references
    let key = (symbol_chain.as_str(), *location, *scope_id);
    if visited.contains(&key) {
        return Vec::new(); // Break circular reference
    }
    visited.insert(key);

    // Get all possible bindings for this chain
    let possible_bindings =
        find_bindings(symbol_chain, location, scope_id, scope_tree, binding_cache);

    // Process each binding based on its type
    let mut targets = Vec::new();
    for result in possible_bindings {
        match &result.binding.value {
            BindingValue::Definition(def_info) => {
                targets.push(PythonTargetResolution::Definition(def_info.clone()));
            }
            BindingValue::ImportedSymbol(import_info) => {
                targets.push(PythonTargetResolution::ImportedSymbol(import_info.clone()));
            }
            BindingValue::SymbolChain(chain) => {
                // Follow the chain recursively
                let nested_targets = resolve_chain_recursive(
                    chain,
                    &result.binding.location,
                    &result.scope_id,
                    scope_tree,
                    binding_cache,
                    visited,
                );
                targets.extend(nested_targets);
            }
            _ => {} // Ignore dead ends
        }
    }

    targets
}

/// Get all possible bindings (values) of the symbol chain.
fn find_bindings(
    symbol_chain: &SymbolChain,
    cutoff: &Range,
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> HashSet<BindingWithScope> {
    let mut current_cutoff = *cutoff;
    let mut current_scope_id = Some(*scope_id);

    while let Some(scope_id) = current_scope_id {
        let bindings = find_bindings_in_scope(
            symbol_chain,
            &current_cutoff,
            &scope_id,
            scope_tree,
            binding_cache,
        );

        if !bindings.is_empty() {
            return bindings;
        }

        // Move up the scope tree, following Python's LEGB rule
        if let Some(scope) = scope_tree.get(scope_id) {
            // Class bodies aren't enclosing scopes, so unless the base of the chain is a receiver ('self'),
            // we recurse up the tree until we hit a non-class scope
            if is_isolated_scope(&scope.scope_type)
                && !symbol_chain.is_single()
                && !is_receiver_base(
                    symbol_chain,
                    &current_cutoff,
                    &scope_id,
                    scope_tree,
                    binding_cache,
                )
            {
                current_scope_id = skip_class_scopes(scope.parent.as_ref(), scope_tree);
            } else {
                current_scope_id = scope.parent;
            }

            if current_scope_id.is_some() {
                current_cutoff = scope.location;
            }
        } else {
            break;
        }
    }

    // Loop never broke, no bindings found
    HashSet::default()
}

/// Looks for matching bindings in a single scope (including conditional sub-scopes).
fn find_bindings_in_scope(
    symbol_chain: &SymbolChain,
    cutoff: &Range, // We can only search *above* this point, never below
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> HashSet<BindingWithScope> {
    let cache_key = (symbol_chain.clone(), *cutoff, *scope_id);
    if let Some(cached_result) = binding_cache.get(&cache_key) {
        return cached_result.clone();
    }

    let mut bindings = HashSet::default();
    let scope = match scope_tree.get(*scope_id) {
        Some(s) => s,
        None => return bindings,
    };

    // Get the *unconditional* binding in the scope
    let unconditional_binding = get_unconditional_binding(symbol_chain, cutoff, scope_id, scope);

    // Get all control flow structures sorted by proximity to cutoff (closest first)
    let conditionals = get_sorted_conditionals(cutoff, scope);

    // Check each scope group in order of proximity to the cutoff
    for conditional in conditionals {
        // Check if group is *closer* to the cutoff than the unconditional binding
        let is_closer_than_unconditional =
            if let Some(ref unconditional_binding) = unconditional_binding {
                unconditional_binding.binding.location.byte_offset.1
                    < conditional.location.byte_offset.1
            } else {
                true // If no unconditional binding, any conditional is relevant
            };

        if is_closer_than_unconditional {
            let conditional_bindings = get_conditional_bindings(
                symbol_chain,
                cutoff,
                conditional,
                scope_tree,
                binding_cache,
            );

            if !conditional_bindings.is_empty() {
                // These bindings are closer to the cutoff than the unconditional binding
                bindings.extend(conditional_bindings);
                break;
            }
        } else {
            // This conditional (and all subsequent ones) are further from cutoff than the unconditional binding
            break;
        }
    }

    // If no *closer* conditional bindings are found, we return the unconditional binding
    if let Some(ref unconditional_binding) = unconditional_binding {
        bindings.insert(unconditional_binding.clone());
    }

    binding_cache.insert(cache_key, bindings.clone());
    bindings
}

/// Gets the unconditional (i.e. not within a conditional scope) value for a chain
/// that is closest to the cutoff
fn get_unconditional_binding(
    symbol_chain: &SymbolChain,
    cutoff: &Range,
    scope_id: &SymbolTableId,
    scope: &SymbolTable,
) -> Option<BindingWithScope> {
    if let Some(bindings) = scope.symbols.get(symbol_chain) {
        let nearest_binding = bindings
            .iter()
            .filter(|b| b.location.byte_offset.1 <= cutoff.byte_offset.0)
            .min_by_key(|b| cutoff.byte_offset.0 - b.location.byte_offset.1);

        if let Some(nearest_binding) = nearest_binding {
            return Some(BindingWithScope {
                binding: nearest_binding.clone(),
                scope_id: *scope_id,
            });
        }
    }

    None
}

/// Gets the value of a chain *in each branch* of a control flow structure (e.g. if/elif/else) that is
/// closest to the cutoff
/// TODO: We have a bindings cache, which is good for now, but we can eliminate unnecessary traversals in the first place.
/// - For each scope in the group, get all the *in-scope* bindings.
/// - If there are none, then we run find_bindings after each scope is processed, with the cutoff being the top of the scope group.
fn get_conditional_bindings(
    symbol_chain: &SymbolChain,
    cutoff: &Range,
    scope_group: ScopeGroup,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> HashSet<BindingWithScope> {
    let mut bindings = HashSet::default();
    let should_search_alternative = match scope_group.group_type {
        ScopeGroupType::If => {
            let mut should_handle_alternative = true;
            for scope_id in &scope_group.scope_ids {
                if let Some(scope) = scope_tree.get(*scope_id) {
                    let if_bindings =
                        find_bindings(symbol_chain, cutoff, scope_id, scope_tree, binding_cache);
                    bindings.extend(if_bindings);

                    if scope.scope_type == ScopeType::Else {
                        should_handle_alternative = false;
                    }
                }
            }

            should_handle_alternative // Handle case where no else clause exists
        }
        ScopeGroupType::Loop | ScopeGroupType::Comprehension => {
            let scope_id = *scope_group
                .scope_ids
                .first()
                .expect("Loop scope group should have one scope");
            let loop_bindings =
                find_bindings(symbol_chain, cutoff, &scope_id, scope_tree, binding_cache);
            bindings.extend(loop_bindings);

            true // Handle case where loop never executes
        }
        ScopeGroupType::Match | ScopeGroupType::Try => {
            let mut should_handle_alternative = true;
            for scope_id in &scope_group.scope_ids {
                if let Some(scope) = scope_tree.get(*scope_id) {
                    let match_bindings =
                        find_bindings(symbol_chain, cutoff, scope_id, scope_tree, binding_cache);
                    bindings.extend(match_bindings);

                    if scope.scope_type == ScopeType::DefaultCase
                        || scope.scope_type == ScopeType::Except
                    {
                        should_handle_alternative = false;
                    }
                }
            }

            should_handle_alternative // Handle case where no pattern matches (or no except clause exists)
        }
    };

    // Handle conditional scopes with no alternative (e.g. an if statement without an else clause, or
    // a match statement without a default case)
    if let Some(scope_id) = scope_group.scope_ids.first() {
        let found_ancestor_binding = bindings
            .iter()
            .any(|b| is_ancestor_scope(&b.scope_id, scope_id, scope_tree));
        if !found_ancestor_binding && should_search_alternative {
            let alt_bindings = find_bindings(
                symbol_chain,
                &scope_group.location.clone(),
                scope_id,
                scope_tree,
                binding_cache,
            );
            bindings.extend(alt_bindings);
        }
    }

    bindings
}

/// Gets the control flow structures preceding the cutoff, in order of proximity (closest first).
fn get_sorted_conditionals(cutoff: &Range, scope: &SymbolTable) -> Vec<ScopeGroup> {
    let mut valid_groups: Vec<ScopeGroup> = scope
        .conditionals
        .iter()
        .filter(|group| group.location.byte_offset.1 <= cutoff.byte_offset.0)
        .cloned()
        .collect();
    valid_groups.sort_by_key(|group| cutoff.byte_offset.0 - group.location.byte_offset.1);

    // TODO: Pre-sort conditionals

    valid_groups
}

fn resolve_within_class(
    class_def: &PythonDefinitionInfo,
    original_chain: &SymbolChain,
    resolved_up_to_index: usize,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> Option<Vec<PythonTargetResolution>> {
    // Get the class scope within the tree
    let scope_id = scope_tree.get_definition_scope(class_def)?;

    // Create a fake chain for resolution within the class
    // We convert the resolved part to a receiver ('self') and append the unresolved part
    let mut class_chain_symbols = vec![Symbol::Receiver()];
    for (i, symbol) in original_chain.symbols.iter().enumerate() {
        if i < resolved_up_to_index {
            continue; // Skip already resolved parts
        }

        class_chain_symbols.push(symbol.clone());
    }
    let class_chain = SymbolChain::new(class_chain_symbols);

    // Create a fake location (bottom of the class definition)
    let class_scope = scope_tree.get(scope_id)?;
    let chain_location = Range::new(
        class_scope.location.end,
        class_scope.location.end,
        (
            class_scope.location.byte_offset.1,
            class_scope.location.byte_offset.1,
        ),
    );

    let resolutions = resolve_symbol_chain(
        &class_chain,
        &chain_location,
        &scope_id,
        scope_tree,
        binding_cache,
    );

    if !resolutions.is_empty() {
        Some(resolutions)
    } else {
        None
    }
}

/// Removes the last `Call` symbol in a chain.
fn get_function(symbol_chain: SymbolChain) -> SymbolChain {
    if let Some(last_symbol) = symbol_chain.symbols.last()
        && matches!(last_symbol, Symbol::Connector(Connector::Call))
    {
        return SymbolChain::new(symbol_chain.symbols[0..symbol_chain.symbols.len() - 1].to_vec());
    }

    symbol_chain
}

fn is_isolated_scope(scope_type: &ScopeType) -> bool {
    matches!(
        scope_type,
        ScopeType::Function | ScopeType::Class | ScopeType::Lambda
    )
}

/// Checks if the base of a symbol chain (first symbol) resolves to a class receiver.
fn is_receiver_base(
    symbol_chain: &SymbolChain,
    cutoff: &Range,
    scope_id: &SymbolTableId,
    scope_tree: &SymbolTableTree,
    binding_cache: &mut BindingCache,
) -> bool {
    if matches!(symbol_chain.symbols[0], Symbol::Receiver()) {
        true
    } else {
        let base_symbol_chain = SymbolChain::new(vec![symbol_chain.symbols[0].clone()]);
        let bindings = find_bindings(
            &base_symbol_chain,
            cutoff,
            scope_id,
            scope_tree,
            binding_cache,
        );
        for result in bindings {
            if let BindingValue::Definition(def_info) = result.binding.value
                && matches!(
                    def_info.definition_type,
                    PythonDefinitionType::Class | PythonDefinitionType::DecoratedClass
                )
            {
                return true;
            }
        }

        false
    }
}

fn skip_class_scopes(
    scope_id: Option<&SymbolTableId>,
    scope_tree: &SymbolTableTree,
) -> Option<SymbolTableId> {
    let mut current_scope_id = scope_id.copied();

    while let Some(id) = current_scope_id {
        if let Some(scope) = scope_tree.get(id) {
            // If this scope is not a class, return it
            if scope.scope_type != ScopeType::Class {
                return Some(id);
            }

            // Otherwise, continue climbing up to the parent
            current_scope_id = scope.parent;
        } else {
            // Scope not found in tree
            return None;
        }
    }

    // Reached the top of the tree without finding a non-class scope (should be impossible)
    None
}

/// Checks if a given scope is an ancestor of another scope
fn is_ancestor_scope(
    potential_ancestor: &SymbolTableId,
    descendant: &SymbolTableId,
    scope_tree: &SymbolTableTree,
) -> bool {
    let mut current_scope_id = Some(*descendant);

    while let Some(scope_id) = current_scope_id {
        // Check if we've reached the potential ancestor
        if scope_id == *potential_ancestor {
            return true;
        }

        // Move up to parent scope
        if let Some(scope) = scope_tree.get(scope_id) {
            current_scope_id = scope.parent;
        } else {
            break;
        }
    }

    false
}

fn pop_last_symbol(symbol_chain: &mut SymbolChain) {
    if symbol_chain.symbols.len() > 1 {
        symbol_chain.symbols.pop();
    }

    match symbol_chain.symbols.last() {
        Some(Symbol::Identifier(_)) | Some(Symbol::Receiver()) => {}
        Some(Symbol::Connector(Connector::Call)) => {}
        _ => {
            symbol_chain.symbols.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SupportedLanguage;
    use crate::python::analyzer::{PythonAnalysisResult, PythonAnalyzer};
    use crate::references::ReferenceTarget;
    use crate::{LanguageParser, parser::GenericParser};

    fn analyze_python_code(code: &str) -> crate::Result<PythonAnalysisResult> {
        let analyzer = PythonAnalyzer::new();
        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let parse_result = parser.parse(code, Some("test.py"))?;
        analyzer.analyze(&parse_result)
    }

    fn get_reference_by_name<'a>(
        result: &'a PythonAnalysisResult,
        name: &'a str,
    ) -> Option<&'a PythonReferenceInfo> {
        result.references.iter().find(|r| r.name == name)
    }

    #[test]
    fn test_simple_function_reference() -> crate::Result<()> {
        let code = r#"
def foo():
    pass

foo()
"#;
        let result = analyze_python_code(code)?;

        // Should find the function call
        let reference =
            get_reference_by_name(&result, "foo()").expect("Should find foo() reference");

        // Should resolve to the function definition
        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "foo".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_method_call_on_instance() -> crate::Result<()> {
        let code = r#"
class MyClass:
    def method(self):
        pass

obj = MyClass()
obj.method()
"#;
        let result = analyze_python_code(code)?;

        // Should find the method call
        let reference = get_reference_by_name(&result, "obj.method()")
            .expect("Should find obj.method() reference");

        // Should resolve to the method definition
        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "method".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_unresolved_reference() -> crate::Result<()> {
        let code = r#"
undefined_function()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "undefined_function()")
            .expect("Should find undefined_function() reference");

        // Should be unresolved
        match &reference.target {
            ReferenceTarget::Unresolved() => {}
            _ => panic!("Expected unresolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_imported_function_reference() -> crate::Result<()> {
        let code = r#"
from math import sqrt
sqrt()
"#;
        let result = analyze_python_code(code)?;

        let reference =
            get_reference_by_name(&result, "sqrt()").expect("Should find sqrt() reference");

        // Should resolve to imported symbol
        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::ImportedSymbol(import_info) => {
                    assert!(import_info.identifier.as_ref().unwrap().name == "sqrt");
                }
                _ => panic!("Expected ImportedSymbol target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_aliased_import_reference() -> crate::Result<()> {
        let code = r#"
import numpy as np
np.array()
"#;
        let result = analyze_python_code(code)?;

        // This should result in a partial resolution since we can't resolve beyond the module
        let reference =
            get_reference_by_name(&result, "np.array()").expect("Should find np.array() reference");

        match &reference.target {
            ReferenceTarget::Resolved(target) => {
                match &**target {
                    crate::python::types::PythonTargetResolution::PartialResolution(partial) => {
                        assert_eq!(partial.symbol_chain.as_str(), "np.array()");
                        // The partial resolution should point to the import
                        match *partial.target {
                            crate::python::types::PythonTargetResolution::ImportedSymbol(_) => {}
                            _ => panic!("Expected ImportedSymbol in partial resolution"),
                        }
                    }
                    _ => panic!("Expected PartialResolution"),
                }
            }
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_nested_function_scope() -> crate::Result<()> {
        let code = r#"
def outer():
    def inner():
        pass
    inner()

outer()
"#;
        let result = analyze_python_code(code)?;

        // Both function calls should be resolved
        let outer_ref =
            get_reference_by_name(&result, "outer()").expect("Should find outer() reference");
        let inner_ref =
            get_reference_by_name(&result, "inner()").expect("Should find inner() reference");

        match &outer_ref.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("outer() should be resolved"),
        }

        match &inner_ref.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("inner() should be resolved"),
        }

        Ok(())
    }

    #[test]
    fn test_class_method_types() -> crate::Result<()> {
        let code = r#"
class MyClass:
    def instance_method(self):
        pass
    
    @classmethod
    def class_method(cls):
        pass
    
    @staticmethod
    def static_method():
        pass

obj = MyClass()
obj.instance_method()
MyClass.class_method()
MyClass.static_method()
"#;
        let result = analyze_python_code(code)?;

        // All method calls should be resolved
        let instance_ref = get_reference_by_name(&result, "obj.instance_method()")
            .expect("Should find instance method reference");
        let class_ref = get_reference_by_name(&result, "MyClass.class_method()")
            .expect("Should find class method reference");
        let static_ref = get_reference_by_name(&result, "MyClass.static_method()")
            .expect("Should find static method reference");

        for reference in [instance_ref, class_ref, static_ref] {
            match &reference.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("Method reference should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_lambda_reference() -> crate::Result<()> {
        let code = r#"
my_lambda = lambda x: x * 2
my_lambda()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "my_lambda()")
            .expect("Should find my_lambda() reference");

        match &reference.target {
            ReferenceTarget::Resolved(target) => {
                match &**target {
                    crate::python::types::PythonTargetResolution::Definition(def_info) => {
                        // Lambda functions get special names
                        assert_eq!(def_info.name, "my_lambda".to_string());
                    }
                    _ => panic!("Expected Definition target"),
                }
            }
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_conditional_binding() -> crate::Result<()> {
        let code = r#"
def foo():
    pass

def bar():
    pass

if True:
    x = foo
else:
    x = bar

x()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "x()").expect("Should find x() reference");

        // Should be ambiguous since x could be either foo or bar
        match &reference.target {
            ReferenceTarget::Ambiguous(targets) => {
                assert_eq!(targets.len(), 2, "Should have 2 possible targets");
            }
            _ => panic!("Expected ambiguous reference"),
        }

        Ok(())
    }

    #[test]
    fn test_chained_attribute_access() -> crate::Result<()> {
        let code = r#"
class A:
    def method_a(self):
        return self

class B:
    def get_a(self):
        return A()

b = B()
a = b.get_a()
a.method_a()
"#;
        let result = analyze_python_code(code)?;

        // Both method calls should be found
        let get_a_ref =
            get_reference_by_name(&result, "b.get_a()").expect("Should find b.get_a() reference");
        let method_a_ref = get_reference_by_name(&result, "a.method_a()")
            .expect("Should find a.method_a() reference");

        match &get_a_ref.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("b.get_a() should be resolved"),
        }

        // a.method_a() might not resolve fully since we don't track return types
        // but it should at least be found as a reference
        assert!(method_a_ref.name == "a.method_a()");

        Ok(())
    }

    #[test]
    fn test_self_method_call() -> crate::Result<()> {
        let code = r#"
class MyClass:
    def method1(self):
        self.method2()
    
    def method2(self):
        pass
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "self.method2()")
            .expect("Should find self.method2() reference");
        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "method2".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_for_loop_variable() -> crate::Result<()> {
        let code = r#"
def process():
    pass

items = [1, 2, 3]
for item in items:
    process()
"#;
        let result = analyze_python_code(code)?;

        // The function call inside the loop should still resolve
        let reference =
            get_reference_by_name(&result, "process()").expect("Should find process() reference");

        match &reference.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("process() should be resolved"),
        }

        Ok(())
    }

    #[test]
    fn test_reassignment() -> crate::Result<()> {
        let code = r#"
def foo():
    pass

def bar():
    pass

x = foo
x = bar
x()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "x()").expect("Should find x() reference");

        // Should resolve to bar (the latest assignment)
        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "bar".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_global_vs_local_scope() -> crate::Result<()> {
        let code = r#"
def global_func():
    pass

def outer():
    def global_func():  # Shadows the global
        pass
    global_func()

global_func()
"#;
        let result = analyze_python_code(code)?;

        // Both calls should be resolved (to different targets)
        let references: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "global_func()")
            .collect();

        assert_eq!(references.len(), 2, "Should find 2 global_func() calls");

        for reference in references {
            match &reference.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("global_func() should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_try_except_bindings() -> crate::Result<()> {
        let code = r#"
def foo():
    pass

def bar():
    pass

try:
    x = foo
except:
    x = bar

x()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "x()").expect("Should find x() reference");

        // Should be ambiguous since x could be either foo or bar
        match &reference.target {
            ReferenceTarget::Ambiguous(targets) => {
                assert_eq!(targets.len(), 2, "Should have 2 possible targets");
            }
            _ => panic!("Expected ambiguous reference"),
        }

        Ok(())
    }

    #[test]
    fn test_comprehension_scope() -> crate::Result<()> {
        let code = r#"
def process(x):
    pass

result = [process(i) for i in range(10)]
"#;
        let result = analyze_python_code(code)?;

        let reference =
            get_reference_by_name(&result, "process()").expect("Should find process(i) reference");
        match &reference.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("process(i) should be resolved"),
        }

        Ok(())
    }

    #[test]
    fn test_nested_class_reference() -> crate::Result<()> {
        let code = r#"
class Outer:
    class Inner:
        def inner_method(self):
            pass

obj = Outer.Inner()
obj.inner_method()
"#;
        let result = analyze_python_code(code)?;

        // The Outer.Inner() call should be found
        let constructor_ref = get_reference_by_name(&result, "Outer.Inner()");
        assert!(
            constructor_ref.is_some(),
            "Should find Outer.Inner() reference"
        );

        // The method call should be found
        let method_ref = get_reference_by_name(&result, "obj.inner_method()");
        assert!(
            method_ref.is_some(),
            "Should find obj.inner_method() reference"
        );

        Ok(())
    }

    #[test]
    fn test_decorator_reference() -> crate::Result<()> {
        let code = r#"
def my_decorator(func):
    return func

@my_decorator
def decorated():
    pass

decorated()
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "decorated()")
            .expect("Should find decorated() reference");

        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "decorated".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_multiple_imports_same_name() -> crate::Result<()> {
        let code = r#"
from module1 import func
from module2 import func  # Shadows the first import

func()
"#;
        let result = analyze_python_code(code)?;

        let reference =
            get_reference_by_name(&result, "func()").expect("Should find func() reference");

        // Should resolve to the second import (latest binding)
        match &reference.target {
            ReferenceTarget::Resolved(target) => {
                match &**target {
                    crate::python::types::PythonTargetResolution::ImportedSymbol(import_info) => {
                        // Should resolve to module2.func
                        assert!(import_info.import_path.contains("module2"));
                    }
                    _ => panic!("Expected ImportedSymbol target"),
                }
            }
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_walrus_operator_reference() -> crate::Result<()> {
        let code = r#"
def get_value():
    return 42

if (x := get_value()):
    pass

get_value()
"#;
        let result = analyze_python_code(code)?;

        // Both calls to get_value() should be found and resolved
        let references: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "get_value()")
            .collect();

        assert_eq!(references.len(), 2, "Should find 2 get_value() calls");

        for reference in references {
            match &reference.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("get_value() should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_reference_count() -> crate::Result<()> {
        let code = r#"
def func1():
    pass

def func2():
    func1()
    func1()

func1()
func2()
"#;
        let result = analyze_python_code(code)?;

        // Should find 3 calls to func1() and 1 call to func2()
        let func1_refs: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "func1()")
            .collect();
        let func2_refs: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "func2()")
            .collect();

        assert_eq!(func1_refs.len(), 3, "Should find 3 func1() calls");
        assert_eq!(func2_refs.len(), 1, "Should find 1 func2() call");

        Ok(())
    }

    #[test]
    fn test_binding_cache_for_repeated_calls() -> crate::Result<()> {
        // Tests that cache is used when the same function is called multiple times
        // The cache should speed up resolution of repeated calls
        let code = r#"
def frequently_called():
    pass

def caller1():
    frequently_called()
    frequently_called()
    frequently_called()

def caller2():
    frequently_called()
    frequently_called()

frequently_called()
frequently_called()
"#;
        let result = analyze_python_code(code)?;

        let references: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "frequently_called()")
            .collect();

        assert_eq!(
            references.len(),
            7,
            "Should find 7 frequently_called() calls"
        );

        // All should resolve to the same definition (cache should help here)
        for reference in references {
            match &reference.target {
                ReferenceTarget::Resolved(target) => match &**target {
                    crate::python::types::PythonTargetResolution::Definition(def_info) => {
                        assert_eq!(def_info.name, "frequently_called".to_string());
                    }
                    _ => panic!("Expected Definition target"),
                },
                _ => panic!("Expected resolved reference"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_cache_with_shadowed_function_calls() -> crate::Result<()> {
        // Tests that cache correctly handles function shadowing at different scopes
        let code = r#"
def target():
    return "global"

def outer():
    def target():  # Shadows global
        return "outer"
    
    def inner():
        def target():  # Shadows outer
            return "inner"
        target()  # Calls inner's target
    
    target()  # Calls outer's target
    inner()

target()  # Calls global target
outer()
"#;
        let result = analyze_python_code(code)?;

        // Should find 3 target() calls at different scope levels
        let target_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "target()")
            .collect();

        assert_eq!(target_calls.len(), 3, "Should find 3 target() calls");

        // Each should be resolved (not unresolved)
        for call in target_calls {
            match &call.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("All target() calls should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_conditional_function_assignment_then_call() -> crate::Result<()> {
        // Tests calling a function that was conditionally assigned
        let code = r#"
def option_a():
    pass

def option_b():
    pass

if condition:
    chosen_func = option_a
else:
    chosen_func = option_b

chosen_func()  # Ambiguous call
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "chosen_func()")
            .expect("Should find chosen_func() call");

        match &reference.target {
            ReferenceTarget::Ambiguous(targets) => {
                assert_eq!(targets.len(), 2, "Should have 2 possible call targets");
            }
            _ => panic!("Expected ambiguous call"),
        }

        Ok(())
    }

    #[test]
    fn test_chained_method_calls() -> crate::Result<()> {
        // Tests resolution of chained method calls
        let code = r#"
class Builder:
    def step1(self):
        return self
    
    def step2(self):
        return self
    
    def build(self):
        return "result"

builder = Builder()
builder.step1().step2().build()
"#;
        let result = analyze_python_code(code)?;

        // Should find calls to step1, step2, and build
        let step1_ref = result
            .references
            .iter()
            .find(|r| r.name == "builder.step1()" || r.name.contains("step1()"));
        let build_ref = result
            .references
            .iter()
            .find(|r| r.name.contains("build()"));

        assert!(step1_ref.is_some(), "Should find step1() call");
        assert!(build_ref.is_some(), "Should find build() call");

        Ok(())
    }

    #[test]
    fn test_class_instantiation_vs_function_call() -> crate::Result<()> {
        // Tests that both class instantiation and function calls are tracked
        let code = r#"
class MyClass:
    def __init__(self):
        pass
    
    def method(self):
        pass

def my_function():
    pass

# Class instantiation (also a call)
obj = MyClass()

# Method call
obj.method()

# Function call
my_function()
"#;
        let result = analyze_python_code(code)?;

        // Should find all three types of calls
        let class_call = get_reference_by_name(&result, "MyClass()");
        let method_call = get_reference_by_name(&result, "obj.method()");
        let func_call = get_reference_by_name(&result, "my_function()");

        assert!(class_call.is_some(), "Should find MyClass() instantiation");
        assert!(method_call.is_some(), "Should find obj.method() call");
        assert!(func_call.is_some(), "Should find my_function() call");

        Ok(())
    }

    #[test]
    fn test_decorated_function_call_resolution() -> crate::Result<()> {
        // Tests that decorated functions are still resolved correctly when called
        let code = r#"
def decorator(func):
    def wrapper():
        return func()
    return wrapper

@decorator
def decorated_func():
    pass

decorated_func()  # Should resolve to the decorated function
"#;
        let result = analyze_python_code(code)?;

        let reference = get_reference_by_name(&result, "decorated_func()")
            .expect("Should find decorated_func() call");

        match &reference.target {
            ReferenceTarget::Resolved(target) => match &**target {
                crate::python::types::PythonTargetResolution::Definition(def_info) => {
                    assert_eq!(def_info.name, "decorated_func".to_string());
                }
                _ => panic!("Expected Definition target"),
            },
            _ => panic!("Expected resolved reference"),
        }

        Ok(())
    }

    #[test]
    fn test_callable_object_pattern() -> crate::Result<()> {
        // Tests calling objects with __call__ method
        let code = r#"
class Callable:
    def __call__(self):
        pass

obj = Callable()
obj()  # Calling the object itself
"#;
        let result = analyze_python_code(code)?;

        // Should find both the instantiation and the call
        let instantiation = get_reference_by_name(&result, "Callable()");
        let object_call = get_reference_by_name(&result, "obj()");

        assert!(
            instantiation.is_some(),
            "Should find Callable() instantiation"
        );
        assert!(object_call.is_some(), "Should find obj() call");

        Ok(())
    }

    #[test]
    fn test_higher_order_function_calls() -> crate::Result<()> {
        // Tests functions that return functions and their subsequent calls
        let code = r#"
def outer():
    def inner():
        pass
    return inner

func = outer()  # Call outer
func()  # Call the returned inner function
"#;
        let result = analyze_python_code(code)?;

        let outer_call =
            get_reference_by_name(&result, "outer()").expect("Should find outer() call");
        let func_call = get_reference_by_name(&result, "func()").expect("Should find func() call");

        // outer() should resolve
        match &outer_call.target {
            ReferenceTarget::Resolved(_) => {}
            _ => panic!("outer() should be resolved"),
        }

        // func() might be unresolved or partially resolved since we don't track return types
        assert!(func_call.name == "func()");

        Ok(())
    }

    #[test]
    fn test_recursive_function_calls() -> crate::Result<()> {
        // Tests that recursive calls are properly resolved
        let code = r#"
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)  # Recursive call

factorial(5)  # External call
"#;
        let result = analyze_python_code(code)?;

        let calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "factorial()")
            .collect();

        assert_eq!(
            calls.len(),
            2,
            "Should find 2 factorial() calls (1 recursive, 1 external)"
        );

        for call in calls {
            match &call.target {
                ReferenceTarget::Resolved(target) => match &**target {
                    crate::python::types::PythonTargetResolution::Definition(def_info) => {
                        assert_eq!(def_info.name, "factorial".to_string());
                    }
                    _ => panic!("Expected Definition target"),
                },
                _ => panic!("Expected resolved reference"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_partial_resolution_of_module_function_calls() -> crate::Result<()> {
        // Tests partial resolution when calling functions on imported modules
        let code = r#"
import os
import sys

os.path.exists()  # Should be partial resolution
sys.exit()  # Should be partial resolution
"#;
        let result = analyze_python_code(code)?;

        let os_call = get_reference_by_name(&result, "os.path.exists()");
        let sys_call = get_reference_by_name(&result, "sys.exit()");

        assert!(os_call.is_some(), "Should find os.path.exists() call");
        assert!(sys_call.is_some(), "Should find sys.exit() call");

        // Both should result in partial resolution
        for reference in [os_call, sys_call].iter().filter_map(|r| *r) {
            if let ReferenceTarget::Resolved(target) = &reference.target {
                match &**target {
                    crate::python::types::PythonTargetResolution::PartialResolution(_) => {}
                    _ => panic!("Expected PartialResolution for module function calls"),
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_match_statement_with_function_calls() -> crate::Result<()> {
        // Tests function calls within match statement patterns and bodies
        let code = r#"
def get_value():
    return 42

def handle_one():
    pass

def handle_two():
    pass

def handle_default():
    pass

match get_value():  # Call in match expression
    case 1:
        handle_one()  # Call in case body
    case 2:
        handle_two()
    case _:
        handle_default()
"#;
        let result = analyze_python_code(code)?;

        // Should find all function calls
        let get_value_call = get_reference_by_name(&result, "get_value()");
        let handle_one_call = get_reference_by_name(&result, "handle_one()");
        let handle_two_call = get_reference_by_name(&result, "handle_two()");
        let handle_default_call = get_reference_by_name(&result, "handle_default()");

        assert!(get_value_call.is_some(), "Should find get_value() call");
        assert!(handle_one_call.is_some(), "Should find handle_one() call");
        assert!(handle_two_call.is_some(), "Should find handle_two() call");
        assert!(
            handle_default_call.is_some(),
            "Should find handle_default() call"
        );

        Ok(())
    }

    #[test]
    fn test_comprehension_with_function_calls() -> crate::Result<()> {
        // Tests function calls within various comprehension types
        let code = r#"
def transform(x):
    return x * 2

def condition(x):
    return x > 0

# List comprehension with function calls
list_result = [transform(x) for x in range(10) if condition(x)]

# Set comprehension
set_result = {transform(x) for x in range(10)}

# Dict comprehension
dict_result = {x: transform(x) for x in range(10)}

# Generator expression
gen_result = (transform(x) for x in range(10))
"#;
        let result = analyze_python_code(code)?;

        // Should find multiple transform() and condition() calls
        let transform_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "transform()")
            .collect();

        let condition_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name == "condition()")
            .collect();

        assert!(
            transform_calls.len() >= 4,
            "Should find at least 4 transform() calls"
        );
        assert!(
            !condition_calls.is_empty(),
            "Should find at least 1 condition() call"
        );

        // All should be resolved
        for call in transform_calls.iter().chain(condition_calls.iter()) {
            match &call.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("Comprehension function calls should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_exception_handler_function_calls() -> crate::Result<()> {
        // Tests function calls in exception handlers
        let code = r#"
def risky_operation():
    pass

def handle_error():
    pass

def cleanup():
    pass

try:
    risky_operation()
except Exception:
    handle_error()
finally:
    cleanup()
"#;
        let result = analyze_python_code(code)?;

        let risky_call = get_reference_by_name(&result, "risky_operation()");
        let handle_call = get_reference_by_name(&result, "handle_error()");
        let cleanup_call = get_reference_by_name(&result, "cleanup()");

        assert!(risky_call.is_some(), "Should find risky_operation() call");
        assert!(handle_call.is_some(), "Should find handle_error() call");
        assert!(cleanup_call.is_some(), "Should find cleanup() call");

        // All should be resolved
        for reference in [risky_call, handle_call, cleanup_call]
            .iter()
            .filter_map(|r| *r)
        {
            match &reference.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("Exception handler calls should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_async_function_calls() -> crate::Result<()> {
        // Tests async function definition and await calls
        let code = r#"
async def async_func():
    pass

async def caller():
    await async_func()  # Async call with await

# Regular call to async function (without await)
async_func()
"#;
        let result = analyze_python_code(code)?;

        // Should find both the await call and regular call
        let calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name.contains("async_func()"))
            .collect();

        assert!(
            !calls.is_empty(),
            "Should find at least 1 async_func() call"
        );

        for call in calls {
            match &call.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("Async function calls should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_property_getter_calls() -> crate::Result<()> {
        // Tests that property decorators don't interfere with method call resolution
        let code = r#"
class MyClass:
    @property
    def my_property(self):
        return self._value
    
    def regular_method(self):
        pass

obj = MyClass()
# Property access (not a call in the traditional sense)
value = obj.my_property
# Regular method call
obj.regular_method()
"#;
        let result = analyze_python_code(code)?;

        // Should find the regular method call
        let method_call = get_reference_by_name(&result, "obj.regular_method()");
        assert!(
            method_call.is_some(),
            "Should find obj.regular_method() call"
        );

        // Should find MyClass instantiation
        let class_call = get_reference_by_name(&result, "MyClass()");
        assert!(class_call.is_some(), "Should find MyClass() instantiation");

        Ok(())
    }

    #[test]
    fn test_staticmethod_and_classmethod_calls() -> crate::Result<()> {
        // Tests different types of method calls
        let code = r#"
class MyClass:
    @staticmethod
    def static_method():
        pass
    
    @classmethod
    def class_method(cls):
        cls.static_method()  # Call static from classmethod
    
    def instance_method(self):
        self.static_method()  # Call static from instance
        self.class_method()   # Call classmethod from instance

# Various ways to call these methods
MyClass.static_method()
MyClass.class_method()
obj = MyClass()
obj.instance_method()
"#;
        let result = analyze_python_code(code)?;

        // Count total method calls
        let static_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name.contains("static_method()"))
            .collect();
        let class_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name.contains("class_method()"))
            .collect();
        let instance_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name.contains("instance_method()"))
            .collect();

        assert!(
            !static_calls.is_empty(),
            "Should find static_method() calls"
        );
        assert!(!class_calls.is_empty(), "Should find class_method() calls");
        assert!(
            !instance_calls.is_empty(),
            "Should find instance_method() calls"
        );

        Ok(())
    }

    #[test]
    fn test_nested_function_call_in_arguments() -> crate::Result<()> {
        // Tests function calls used as arguments to other function calls
        let code = r#"
def inner1():
    return 1

def inner2():
    return 2

def outer(a, b):
    return a + b

# Nested function calls as arguments
result = outer(inner1(), inner2())
"#;
        let result = analyze_python_code(code)?;

        // Should find all three function calls
        let outer_call = get_reference_by_name(&result, "outer()");
        let inner1_call = get_reference_by_name(&result, "inner1()");
        let inner2_call = get_reference_by_name(&result, "inner2()");

        assert!(outer_call.is_some(), "Should find outer() call");
        assert!(inner1_call.is_some(), "Should find inner1() call");
        assert!(inner2_call.is_some(), "Should find inner2() call");

        // All should be resolved
        for reference in [outer_call, inner1_call, inner2_call]
            .iter()
            .filter_map(|r| *r)
        {
            match &reference.target {
                ReferenceTarget::Resolved(_) => {}
                _ => panic!("Nested function calls should be resolved"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_walrus_operator_function_call() -> crate::Result<()> {
        // Tests function calls with walrus operator assignment
        let code = r#"
def get_value():
    return 42

def process(x):
    return x * 2

# Walrus operator with function call
if (value := get_value()) > 0:
    result = process(value)
"#;
        let result = analyze_python_code(code)?;

        let get_value_call = get_reference_by_name(&result, "get_value()");
        let process_call = get_reference_by_name(&result, "process()");

        assert!(get_value_call.is_some(), "Should find get_value() call");
        assert!(process_call.is_some(), "Should find process() call");

        Ok(())
    }

    #[test]
    fn test_function_call_in_f_string() -> crate::Result<()> {
        // Tests function calls within f-strings
        let code = r#"
def get_name():
    return "Alice"

def get_age():
    return 30

# Function calls inside f-string
message = f"Name: {get_name()}, Age: {get_age()}"
"#;
        let result = analyze_python_code(code)?;

        let name_call = get_reference_by_name(&result, "get_name()");
        let age_call = get_reference_by_name(&result, "get_age()");

        assert!(
            name_call.is_some(),
            "Should find get_name() call in f-string"
        );
        assert!(age_call.is_some(), "Should find get_age() call in f-string");

        Ok(())
    }

    #[test]
    fn test_multiple_inheritance_method_calls() -> crate::Result<()> {
        // Tests method resolution with multiple inheritance
        let code = r#"
class A:
    def method_a(self):
        pass

class B:
    def method_b(self):
        pass

class C(A, B):
    def method_c(self):
        self.method_a()  # From A
        self.method_b()  # From B

obj = C()
obj.method_a()
obj.method_b()
obj.method_c()
"#;
        let result = analyze_python_code(code)?;

        // Should find all method calls
        let method_calls: Vec<_> = result
            .references
            .iter()
            .filter(|r| r.name.contains("method_"))
            .collect();

        assert!(
            method_calls.len() >= 5,
            "Should find at least 5 method calls"
        );

        Ok(())
    }
}
