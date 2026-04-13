//! `ReachingResolver` — generic resolver that uses the SSA graph + declarative
//! rules to produce call edges.
//!
//! This is the integration layer: it runs the walker to build the SSA graph,
//! then for each reference, resolves the reaching definitions to concrete
//! definitions via import strategies and chain resolution.

use code_graph_types::{CanonicalImport, CanonicalResult, EdgeKind, NodeKind, Relationship};

use super::context::{DefRef, ResolutionContext};
use super::edges::ResolvedEdge;
use super::resolver::ReferenceResolver;
use super::rules::{ImportStrategy, ResolutionRules};
use super::ssa::{ReachingDefs, Value};
use super::walker::{AsAst, walk_files};

/// Trait to get rules from the type parameter.
/// Each language implements this on a zero-sized struct.
pub trait HasRules {
    fn default_rules() -> ResolutionRules;
}

/// Generic resolver parameterized by a `HasRules` type.
///
/// Usage in register_v2_pipelines!:
/// ```ignore
/// Python => GenericPipeline<PythonParser, RulesResolver<PythonRules>>
/// Java   => GenericPipeline<JavaParser,   RulesResolver<JavaRules>>
/// ```
pub struct RulesResolver<R: HasRules>(std::marker::PhantomData<R>);

impl<A, R> ReferenceResolver<A> for RulesResolver<R>
where
    A: AsAst + Send + Sync,
    R: HasRules + Send + Sync,
{
    fn resolve(ctx: &ResolutionContext<A>) -> Vec<ResolvedEdge> {
        let rules = R::default_rules();
        resolve_with_rules(&rules, ctx)
    }
}

/// Core resolution logic, shared by all resolver wrappers.
fn resolve_with_rules<A: AsAst>(
    rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
) -> Vec<ResolvedEdge> {
    let mut walk_result = walk_files(rules, &ctx.results, &ctx.asts);
    let mut edges = Vec::new();
    let reads = std::mem::take(&mut walk_result.reads);

    for read in &reads {
        let reaching = walk_result
            .ssa
            .read_variable_stateless(&read.name, read.block);

        let resolved_defs = resolve_reaching_defs(rules, ctx, read.file_idx, &read.name, &reaching);

        let result = &ctx.results[read.file_idx];
        let reference = &result.references[read.ref_idx];

        let source_enclosing = ctx.scopes.enclosing_scope(
            &result.file_path,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        let source_def_kind = source_enclosing.map(|s| {
            let (def, _) = ctx.resolve_def(DefRef {
                file_idx: s.file_idx,
                def_idx: s.def_idx,
            });
            def.kind
        });

        let source = source_enclosing
            .map(|s| DefRef {
                file_idx: s.file_idx,
                def_idx: s.def_idx,
            })
            .unwrap_or(DefRef {
                file_idx: read.file_idx,
                def_idx: 0,
            });

        for target in resolved_defs {
            let (target_def, _) = ctx.resolve_def(target);

            edges.push(ResolvedEdge {
                relationship: Relationship {
                    edge_kind: EdgeKind::Calls,
                    source_node: NodeKind::Definition,
                    target_node: NodeKind::Definition,
                    source_def_kind,
                    target_def_kind: Some(target_def.kind),
                },
                source,
                target,
                reference_range: reference.range,
            });
        }
    }

    edges
}

/// Resolve reaching definitions to concrete DefRefs.
///
/// For SSA values that are Def, return directly.
/// For Import values, chase through import strategies.
/// For Type values, look up by FQN.
fn resolve_reaching_defs<A>(
    rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
    file_idx: usize,
    name: &str,
    reaching: &ReachingDefs,
) -> Vec<DefRef> {
    let mut result = Vec::new();

    for value in &reaching.values {
        match value {
            Value::Def(f, d) => {
                result.push(DefRef {
                    file_idx: *f,
                    def_idx: *d,
                });
            }
            Value::Import(f, i) => {
                // Chase the import to find terminal definitions
                let import = &ctx.results[*f].imports[*i];
                let import_defs = resolve_import(rules, ctx, import, &ctx.results[*f].file_path);
                result.extend(import_defs);
            }
            Value::Type(type_name) => {
                // Look up the type by FQN
                for def_ref in ctx.definitions.lookup_fqn(type_name) {
                    result.push(*def_ref);
                }
            }
            _ => {}
        }
    }

    // If SSA didn't find anything, fall back to import strategies
    if result.is_empty() {
        result = apply_import_strategies(rules, ctx, file_idx, name);
    }

    // Deduplicate
    let mut seen = rustc_hash::FxHashSet::default();
    result.retain(|r| seen.insert((r.file_idx, r.def_idx)));

    result
}

/// Apply the language's import resolution strategies in order.
fn apply_import_strategies<A>(
    rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
    file_idx: usize,
    name: &str,
) -> Vec<DefRef> {
    let result = &ctx.results[file_idx];

    for strategy in &rules.import_strategies {
        let candidates = match strategy {
            ImportStrategy::ScopeFqnWalk => {
                // Walk up the scope FQN trying scope.name at each level
                scope_fqn_walk(ctx, result, name)
            }
            ImportStrategy::ExplicitImport => {
                // Check if there's an import that brings this name into scope
                explicit_import_lookup(ctx, file_idx, name)
            }
            ImportStrategy::WildcardImport => wildcard_import_lookup(ctx, file_idx, name),
            ImportStrategy::SamePackage => same_package_lookup(ctx, result, name),
            ImportStrategy::SameFile => {
                // Look up by name within the same file
                ctx.definitions
                    .lookup_name(name)
                    .iter()
                    .filter(|r| r.file_idx == file_idx)
                    .copied()
                    .collect()
            }
            ImportStrategy::GlobalName { max_candidates } => {
                let candidates = ctx.definitions.lookup_name(name);
                if candidates.len() <= *max_candidates {
                    candidates.to_vec()
                } else {
                    vec![]
                }
            }
            ImportStrategy::FilePath => {
                // Python-style: resolve import path to file, find definitions there
                // This requires the file tree — handled separately
                vec![]
            }
        };

        if !candidates.is_empty() {
            return candidates;
        }
    }

    vec![]
}

/// Resolve an import to terminal definitions.
fn resolve_import<A>(
    _rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
    import: &CanonicalImport,
    _importing_file: &str,
) -> Vec<DefRef> {
    let symbol_name = import
        .alias
        .as_deref()
        .or(import.name.as_deref())
        .unwrap_or("");

    if symbol_name.is_empty() || symbol_name == "*" {
        return vec![];
    }

    // Try full import path + symbol name as FQN
    let full_fqn = if import.path.is_empty() {
        symbol_name.to_string()
    } else {
        format!("{}.{}", import.path, symbol_name)
    };

    let by_fqn = ctx.definitions.lookup_fqn(&full_fqn);
    if !by_fqn.is_empty() {
        return by_fqn.to_vec();
    }

    // Try just the import path as FQN (for `import X` style)
    if !import.path.is_empty() {
        let by_path = ctx.definitions.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return by_path.to_vec();
        }
    }

    // Try the symbol name as a bare name
    let by_name = ctx.definitions.lookup_name(symbol_name);
    if by_name.len() <= 3 {
        return by_name.to_vec();
    }

    vec![]
}

/// Walk up scope FQN segments trying `scope.name` at each level.
fn scope_fqn_walk<A>(
    ctx: &ResolutionContext<A>,
    result: &CanonicalResult,
    name: &str,
) -> Vec<DefRef> {
    // Try each top-level definition's FQN as a potential scope
    for def in &result.definitions {
        if def.is_top_level {
            let candidate = format!("{}.{}", def.fqn, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }

    // Also try walking up from longer FQNs
    for def in &result.definitions {
        let fqn_str = def.fqn.to_string();
        let mut current = fqn_str.as_str();
        loop {
            let candidate = format!("{}.{}", current, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
            match current.rfind('.') {
                Some(pos) => current = &current[..pos],
                None => break,
            }
        }
    }

    vec![]
}

/// Check explicit imports for a name.
fn explicit_import_lookup<A>(
    ctx: &ResolutionContext<A>,
    file_idx: usize,
    name: &str,
) -> Vec<DefRef> {
    let result = &ctx.results[file_idx];
    for imp in &result.imports {
        let imp_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
        if imp_name == name {
            let defs = resolve_import_to_defs(ctx, imp);
            if !defs.is_empty() {
                return defs;
            }
        }
    }
    vec![]
}

/// Resolve a single import to definitions by FQN matching.
fn resolve_import_to_defs<A>(ctx: &ResolutionContext<A>, import: &CanonicalImport) -> Vec<DefRef> {
    let symbol = import.name.as_deref().unwrap_or("");
    let full_fqn = if import.path.is_empty() {
        symbol.to_string()
    } else {
        format!("{}.{}", import.path, symbol)
    };

    let by_fqn = ctx.definitions.lookup_fqn(&full_fqn);
    if !by_fqn.is_empty() {
        return by_fqn.to_vec();
    }

    // Try path alone (for `import X` where X is a module/class)
    if !import.path.is_empty() {
        let by_path = ctx.definitions.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return by_path.to_vec();
        }
    }

    vec![]
}

/// Check wildcard imports for a name.
fn wildcard_import_lookup<A>(
    ctx: &ResolutionContext<A>,
    file_idx: usize,
    name: &str,
) -> Vec<DefRef> {
    let result = &ctx.results[file_idx];
    for imp in &result.imports {
        if imp.import_type == "WildcardImport" || imp.import_type == "RelativeWildcardImport" {
            let candidate = format!("{}.{}", imp.path, name);
            let matches = ctx.definitions.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    vec![]
}

/// Same-package lookup: extract package from file's top-level FQN.
fn same_package_lookup<A>(
    ctx: &ResolutionContext<A>,
    result: &CanonicalResult,
    name: &str,
) -> Vec<DefRef> {
    for def in &result.definitions {
        if def.is_top_level {
            let fqn_str = def.fqn.to_string();
            if let Some(dot_pos) = fqn_str.rfind('.') {
                let pkg = &fqn_str[..dot_pos];
                let candidate = format!("{}.{}", pkg, name);
                let matches = ctx.definitions.lookup_fqn(&candidate);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
    }
    vec![]
}
