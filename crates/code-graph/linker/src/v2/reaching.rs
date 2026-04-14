//! `ReachingResolver` — generic resolver that uses the SSA graph + declarative
//! rules to produce call edges.
//!
//! This is the integration layer: it runs the walker to build the SSA graph,
//! then for each reference, resolves the reaching definitions to concrete
//! definitions via import strategies and chain resolution.

use code_graph_types::{CanonicalImport, CanonicalResult, EdgeKind, NodeKind, Relationship};

use super::context::{DefRef, ResolutionContext};
use super::edges::{EdgeSource, ResolvedEdge};
use super::resolver::ReferenceResolver;
use super::rules::{ImportStrategy, ResolutionRules};
use super::ssa::{BlockId, ReachingDefs, SsaResolver, Value};
use super::walker::{walk_files, AsAst};

/// Trait to get rules from the type parameter.
/// Each language implements this on a zero-sized struct.
pub trait HasRules {
    fn rules() -> ResolutionRules;
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
        let rules = R::rules();
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

        let result = &ctx.results[read.file_idx];
        let reference = &result.references[read.ref_idx];

        // Find enclosing class for implicit-this resolution
        let enclosing_class_fqn = find_enclosing_class(ctx, read.file_idx, reference);

        // If the reference has an expression chain, walk it to resolve
        // through types. Otherwise fall back to bare name resolution.
        let resolved_defs = if let Some(ref chain) = reference.expression {
            resolve_expression_chain(
                rules,
                ctx,
                &mut walk_result.ssa,
                read.file_idx,
                read.block,
                chain,
                enclosing_class_fqn.as_deref(),
            )
        } else {
            resolve_reaching_defs(
                rules,
                ctx,
                read.file_idx,
                &read.name,
                &reaching,
                enclosing_class_fqn.as_deref(),
            )
        };

        let source_enclosing = ctx.scopes.enclosing_scope(
            &result.file_path,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        let (source, source_node, source_def_kind) = match source_enclosing {
            Some(s) => {
                let def_ref = DefRef {
                    file_idx: s.file_idx,
                    def_idx: s.def_idx,
                };
                let (def, _) = ctx.resolve_def(def_ref);
                (
                    EdgeSource::Definition(def_ref),
                    NodeKind::Definition,
                    Some(def.kind),
                )
            }
            None => (EdgeSource::File(read.file_idx), NodeKind::File, None),
        };

        for target in resolved_defs {
            let (target_def, _) = ctx.resolve_def(target);

            edges.push(ResolvedEdge {
                relationship: Relationship {
                    edge_kind: EdgeKind::Calls,
                    source_node,
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
    enclosing_class_fqn: Option<&str>,
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
                let import = &ctx.results[*f].imports[*i];
                let import_defs = resolve_import(ctx, import);
                result.extend(import_defs);
            }
            Value::Type(type_name) => {
                // Type-flow: look up `name` as a member of the type.
                let member_defs =
                    ctx.members
                        .lookup_member_with_supers(type_name, name, &ctx.definitions);
                if !member_defs.is_empty() {
                    result.extend(member_defs);
                } else {
                    let fqn = format!("{}.{}", type_name, name);
                    for def_ref in ctx.definitions.lookup_fqn(&fqn) {
                        result.push(*def_ref);
                    }
                }
            }
            _ => {}
        }
    }

    // Fallback 1: import strategies
    if result.is_empty() {
        result = apply_import_strategies(rules, ctx, file_idx, name);
    }

    // Fallback 2: implicit this — try member lookup on the enclosing class.
    // In Java/Kotlin, `helper()` inside a method body means `this.helper()`.
    if result.is_empty() {
        if let Some(class_fqn) = enclosing_class_fqn {
            let member_defs =
                ctx.members
                    .lookup_member_with_supers(class_fqn, name, &ctx.definitions);
            result.extend(member_defs);
        }
    }

    // Deduplicate
    let mut seen = rustc_hash::FxHashSet::default();
    result.retain(|r| seen.insert((r.file_idx, r.def_idx)));

    result
}

/// Walk an `ExpressionStep` chain left-to-right, threading the resolved
/// type through each step.
///
/// e.g. `[Ident("svc"), Call("query")]`:
///   1. Resolve "svc" via SSA → Value::Type("UserService")
///   2. Look up "query" as member of UserService → UserService.query
///
/// e.g. `[Ident("factory"), Call("getService"), Call("query")]`:
///   1. Resolve "factory" via SSA → Value::Def(Factory)
///   2. Factory is a class → look up "getService" as member → Factory.getService
///   3. getService has return_type "UserService" → look up "query" → UserService.query
#[allow(clippy::too_many_arguments)]
fn resolve_expression_chain<A: AsAst>(
    rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
    ssa: &mut SsaResolver,
    file_idx: usize,
    block: BlockId,
    chain: &[code_graph_types::ExpressionStep],
    enclosing_class_fqn: Option<&str>,
) -> Vec<DefRef> {
    use code_graph_types::ExpressionStep;

    if chain.is_empty() {
        return vec![];
    }

    // Resolve the base (first step) to a set of type names
    let mut current_types: Vec<String> = Vec::new();

    match &chain[0] {
        ExpressionStep::Ident(name) => {
            let reaching = ssa.read_variable_stateless(name, block);
            for value in &reaching.values {
                match value {
                    Value::Type(t) => current_types.push(t.clone()),
                    Value::Def(f, d) => {
                        let def = &ctx.results[*f].definitions[*d];
                        match def.kind {
                            code_graph_types::DefKind::Class
                            | code_graph_types::DefKind::Interface => {
                                current_types.push(def.fqn.to_string());
                            }
                            _ => {
                                // Use return_type if it's a method/function
                                if let Some(meta) = &def.metadata {
                                    if let Some(rt) = &meta.return_type {
                                        current_types.push(rt.clone());
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        ExpressionStep::This => {
            if let Some(class_fqn) = enclosing_class_fqn {
                current_types.push(class_fqn.to_string());
            }
        }
        ExpressionStep::Super => {
            let reaching = ssa.read_variable_stateless("super", block);
            for value in &reaching.values {
                if let Value::Type(t) = value {
                    current_types.push(t.clone());
                }
            }
        }
        ExpressionStep::New(type_name) => {
            current_types.push(type_name.clone());
        }
        _ => {}
    }

    if current_types.is_empty() {
        // Can't resolve the base — fall back to bare name on the last step
        if let Some(last) = chain.last() {
            let name = match last {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => n.as_str(),
                _ => return vec![],
            };
            let reaching = ssa.read_variable_stateless(name, block);
            return resolve_reaching_defs(
                rules,
                ctx,
                file_idx,
                name,
                &reaching,
                enclosing_class_fqn,
            );
        }
        return vec![];
    }

    // Walk remaining steps, resolving each through the current type(s)
    for step in &chain[1..] {
        let member_name = match step {
            ExpressionStep::Call(n) | ExpressionStep::Field(n) => n,
            _ => continue,
        };

        let mut next_types = Vec::new();
        let mut found_members = Vec::new();

        for type_name in &current_types {
            let members =
                ctx.members
                    .lookup_member_with_supers(type_name, member_name, &ctx.definitions);
            for def_ref in &members {
                let def = &ctx.results[def_ref.file_idx].definitions[def_ref.def_idx];
                // For Call steps, advance to the return type
                if matches!(step, ExpressionStep::Call(_)) {
                    if let Some(meta) = &def.metadata {
                        if let Some(rt) = &meta.return_type {
                            next_types.push(rt.clone());
                        }
                    }
                    // If it's a class/constructor, the "return type" is the class itself
                    if matches!(
                        def.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Constructor
                    ) {
                        next_types.push(def.fqn.to_string());
                    }
                }
                // For Field steps, advance to the field's type annotation
                if matches!(step, ExpressionStep::Field(_)) {
                    if let Some(meta) = &def.metadata {
                        if let Some(ta) = &meta.type_annotation {
                            next_types.push(ta.clone());
                        }
                    }
                }
            }
            found_members.extend(members);
        }

        // If this is the last step, return the found members as the result
        if std::ptr::eq(step, chain.last().unwrap()) {
            let mut seen = rustc_hash::FxHashSet::default();
            found_members.retain(|r| seen.insert((r.file_idx, r.def_idx)));
            return found_members;
        }

        current_types = next_types;
        if current_types.is_empty() {
            break;
        }
    }

    vec![]
}

/// Find the enclosing class FQN for a reference, for implicit-this resolution.
fn find_enclosing_class<A>(
    ctx: &ResolutionContext<A>,
    file_idx: usize,
    reference: &code_graph_types::CanonicalReference,
) -> Option<String> {
    let result = &ctx.results[file_idx];
    let byte_start = reference.range.byte_offset.0;
    let byte_end = reference.range.byte_offset.1;

    // Find all containing scopes, then pick the innermost class/interface
    let containing = ctx
        .scopes
        .containing_scopes(&result.file_path, byte_start, byte_end);
    for scope in containing.iter().rev() {
        let def = &result.definitions[scope.def_idx];
        if matches!(
            def.kind,
            code_graph_types::DefKind::Class | code_graph_types::DefKind::Interface
        ) {
            return Some(def.fqn.to_string());
        }
    }
    None
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
fn resolve_import<A>(ctx: &ResolutionContext<A>, import: &CanonicalImport) -> Vec<DefRef> {
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
