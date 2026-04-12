use code_graph_types::Range;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

use super::types::*;

/// A binding with the scope it was found in.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingWithScope {
    binding: Binding,
    scope_id: ScopeId,
}

/// Memoization cache for binding lookups.
type BindingCache = FxHashMap<(SymbolChain, Range, ScopeId), HashSet<BindingWithScope>>;

/// Resolve all references in a symbol table tree.
///
/// Walks every scope, resolves each recorded reference against the scope
/// tree using Python's LEGB-like scoping rules, and returns resolved references.
pub fn find_references(tree: &SymbolTableTree) -> Vec<ResolvedReference> {
    let mut cache = BindingCache::default();
    let mut results = Vec::new();

    for (scope_id, table) in tree.iter() {
        for (chain, range) in &table.references {
            let targets = resolve_reference(tree, scope_id, chain, *range, &mut cache);
            results.push(ResolvedReference {
                chain: chain.clone(),
                range: *range,
                targets,
            });
        }
    }

    results
}

/// Resolve a single reference to its targets.
fn resolve_reference(
    tree: &SymbolTableTree,
    scope_id: ScopeId,
    chain: &SymbolChain,
    range: Range,
    cache: &mut BindingCache,
) -> Vec<ResolvedTarget> {
    // Strip trailing Call connector (foo.bar() → foo.bar)
    let lookup_chain = strip_trailing_call(chain);

    // Try full chain resolution first
    let mut targets = resolve_symbol_chain(
        tree,
        scope_id,
        &lookup_chain,
        range,
        cache,
        &mut HashSet::new(),
    );

    // If no result and chain has >1 symbol, try partial resolution
    if targets.is_empty() && lookup_chain.symbols.len() > 1 {
        targets = try_partial_resolution(tree, scope_id, &lookup_chain, range, cache);
    }

    targets
}

/// Strip a trailing Call connector from a chain.
/// `foo.bar()` → `foo.bar`, `foo()` → `foo`.
fn strip_trailing_call(chain: &SymbolChain) -> SymbolChain {
    if chain.symbols.last() == Some(&Symbol::Connector(Connector::Call)) {
        SymbolChain::new(chain.symbols[..chain.symbols.len() - 1].to_vec())
    } else {
        chain.clone()
    }
}

/// Resolve a symbol chain by walking up the scope tree.
fn resolve_symbol_chain(
    tree: &SymbolTableTree,
    scope_id: ScopeId,
    chain: &SymbolChain,
    range: Range,
    cache: &mut BindingCache,
    visited: &mut HashSet<(String, Range, ScopeId)>,
) -> Vec<ResolvedTarget> {
    let visit_key = (chain.to_string(), range, scope_id);
    if visited.contains(&visit_key) {
        return vec![];
    }
    visited.insert(visit_key);

    let bindings = find_bindings(tree, scope_id, chain, range, cache);

    let mut targets = Vec::new();
    for bws in bindings {
        match &bws.binding.value {
            BindingValue::Definition(def_binding) => {
                targets.push(ResolvedTarget::Definition(def_binding.def_idx));
            }
            BindingValue::Import(import_binding) => {
                targets.push(ResolvedTarget::Import(import_binding.import_idx));
            }
            BindingValue::Alias(alias_chain) => {
                // Follow the alias recursively
                let alias_targets = resolve_symbol_chain(
                    tree,
                    bws.scope_id,
                    alias_chain,
                    bws.binding.location,
                    cache,
                    visited,
                );
                targets.extend(alias_targets);
            }
            BindingValue::DeadEnd => {}
        }
    }

    targets
}

/// Try partial resolution: iteratively shorten the chain and resolve prefixes.
fn try_partial_resolution(
    tree: &SymbolTableTree,
    scope_id: ScopeId,
    chain: &SymbolChain,
    range: Range,
    cache: &mut BindingCache,
) -> Vec<ResolvedTarget> {
    // Try progressively shorter prefixes
    let mut end = chain.symbols.len();
    while end > 0 {
        // Find the last identifier position
        if end >= 2 && matches!(chain.symbols[end - 1], Symbol::Identifier(_)) {
            // Check if there's a connector before it
            if matches!(chain.symbols.get(end - 2), Some(Symbol::Connector(_))) {
                end -= 2; // skip the connector + identifier
            } else {
                end -= 1;
            }
        } else {
            end -= 1;
        }

        if end == 0 {
            break;
        }

        let prefix = SymbolChain::new(chain.symbols[..end].to_vec());
        let prefix_targets = resolve_symbol_chain(
            tree,
            scope_id,
            &prefix,
            range,
            cache,
            &mut HashSet::new(),
        );

        if !prefix_targets.is_empty() {
            // Check if resolved to a class — if so, try resolve_within_class
            for target in &prefix_targets {
                if let ResolvedTarget::Definition(def_idx) = target {
                    if let Some(class_scope_id) = tree.get_definition_scope(*def_idx) {
                        let class_targets = resolve_within_class(
                            tree,
                            class_scope_id,
                            chain,
                            end,
                            cache,
                        );
                        if !class_targets.is_empty() {
                            return class_targets;
                        }
                    }
                }
            }

            // Wrap as partial resolution
            return prefix_targets
                .into_iter()
                .map(|t| {
                    ResolvedTarget::Partial(PartialResolution {
                        chain: chain.clone(),
                        resolved_index: end,
                        target: Box::new(t),
                    })
                })
                .collect();
        }
    }

    vec![]
}

/// Resolve the unresolved suffix within a class scope.
///
/// When partial resolution hits a class definition, this converts
/// the unresolved suffix to `self.{suffix}` and resolves within
/// the class scope.
fn resolve_within_class(
    tree: &SymbolTableTree,
    class_scope_id: ScopeId,
    chain: &SymbolChain,
    resolved_index: usize,
    cache: &mut BindingCache,
) -> Vec<ResolvedTarget> {
    // Build self.{unresolved_suffix} chain
    let suffix = &chain.symbols[resolved_index..];
    // The suffix starts with a Connector::Attribute, then identifiers
    let mut self_chain_symbols = vec![Symbol::Receiver];
    self_chain_symbols.extend_from_slice(suffix);

    let self_chain = SymbolChain::new(self_chain_symbols);

    // Use a fake location at the bottom of the class scope to see all bindings
    let fake_range = tree.get(class_scope_id)
        .map(|scope| scope.location)
        .unwrap_or(Range::empty());

    resolve_symbol_chain(
        tree,
        class_scope_id,
        &self_chain,
        fake_range,
        cache,
        &mut HashSet::new(),
    )
}

// ── Binding lookup (LEGB-like scope walking) ────────────────────

/// Find all bindings for a chain at a given scope, following Python's
/// LEGB-like scoping rules.
fn find_bindings(
    tree: &SymbolTableTree,
    scope_id: ScopeId,
    chain: &SymbolChain,
    cutoff: Range,
    cache: &mut BindingCache,
) -> Vec<BindingWithScope> {
    // Try current scope
    let bindings = find_bindings_in_scope(tree, scope_id, chain, cutoff, cache);
    if !bindings.is_empty() {
        return bindings;
    }

    // Walk up to parent
    let scope = match tree.get(scope_id) {
        Some(s) => s,
        None => return vec![],
    };

    if let Some(parent_id) = scope.parent {
        // Determine cutoff for parent: use this scope's location
        let parent_cutoff = scope.location;

        // Skip class scopes when crossing isolated boundaries
        // (Python rule: class body names aren't in enclosing scope for nested functions)
        if scope.scope_type.is_isolated() && !chain.is_single() && !chain.starts_with_receiver() {
            // Skip any class scopes in the parent chain
            let mut current_parent = Some(parent_id);
            while let Some(pid) = current_parent {
                if let Some(parent_scope) = tree.get(pid) {
                    if parent_scope.scope_type.is_class() {
                        current_parent = parent_scope.parent;
                        continue;
                    }
                    return find_bindings(tree, pid, chain, parent_cutoff, cache);
                }
                break;
            }
            return vec![];
        }

        return find_bindings(tree, parent_id, chain, parent_cutoff, cache);
    }

    vec![]
}

/// Find bindings for a chain within a single scope.
fn find_bindings_in_scope(
    tree: &SymbolTableTree,
    scope_id: ScopeId,
    chain: &SymbolChain,
    cutoff: Range,
    cache: &mut BindingCache,
) -> Vec<BindingWithScope> {
    let cache_key = (chain.clone(), cutoff, scope_id);
    if let Some(cached) = cache.get(&cache_key) {
        return cached.iter().cloned().collect();
    }

    let scope = match tree.get(scope_id) {
        Some(s) => s,
        None => return vec![],
    };

    // Find the unconditional binding: the closest binding before the cutoff
    let unconditional = scope
        .symbols
        .get(chain)
        .and_then(|bindings| {
            bindings
                .iter()
                .filter(|b| b.location.byte_offset.0 <= cutoff.byte_offset.0)
                .max_by_key(|b| b.location.byte_offset.0)
        })
        .map(|b| BindingWithScope {
            binding: b.clone(),
            scope_id,
        });

    // Check conditional scope groups closer than the unconditional binding
    let unconditional_offset = unconditional
        .as_ref()
        .map(|b| b.binding.location.byte_offset.0)
        .unwrap_or(0);

    let mut conditional_bindings = Vec::new();
    let mut relevant_groups: Vec<_> = scope
        .conditionals
        .iter()
        .filter(|group| {
            group.location.byte_offset.0 > unconditional_offset
                && group.location.byte_offset.0 <= cutoff.byte_offset.0
        })
        .collect();

    // Sort by proximity (closest first)
    relevant_groups.sort_by_key(|g| std::cmp::Reverse(g.location.byte_offset.0));

    for group in relevant_groups {
        let group_bindings = get_conditional_bindings(tree, group, chain, cutoff, cache);
        if !group_bindings.is_empty() {
            conditional_bindings.extend(group_bindings);
        }
    }

    let result = if !conditional_bindings.is_empty() {
        conditional_bindings
    } else if let Some(ub) = unconditional {
        vec![ub]
    } else {
        vec![]
    };

    let result_set: HashSet<BindingWithScope> = result.iter().cloned().collect();
    cache.insert(cache_key, result_set);
    result
}

/// Search conditional scope groups for bindings.
fn get_conditional_bindings(
    tree: &SymbolTableTree,
    group: &ScopeGroup,
    chain: &SymbolChain,
    cutoff: Range,
    cache: &mut BindingCache,
) -> Vec<BindingWithScope> {
    let mut bindings = Vec::new();

    for &branch_scope_id in &group.scope_ids {
        let branch_bindings = find_bindings(tree, branch_scope_id, chain, cutoff, cache);
        bindings.extend(branch_bindings);
    }

    // If the group doesn't have a catch-all branch, also search for an
    // "alternative" binding (what value the name would have if none of
    // the conditional branches executed)
    if !group.has_catch_all(tree) && !bindings.is_empty() {
        // Look for the binding at the location before this group
        let alt_cutoff = Range::new(
            group.location.start,
            group.location.start,
            (group.location.byte_offset.0, group.location.byte_offset.0),
        );

        if let Some(scope) = tree.get(group.scope_ids.first().copied().unwrap_or(ScopeId(0)))
            && let Some(parent_id) = scope.parent
        {
            let alt_bindings = find_bindings_in_scope(tree, parent_id, chain, alt_cutoff, cache);
            bindings.extend(alt_bindings);
        }
    }

    bindings
}
