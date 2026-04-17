//! Cross-file reference resolution from SSA-annotated `ReferenceEvent`s.
//!
//! Takes `ParseValue` indices and maps them to graph `NodeIndex` targets,
//! producing edges. This is the Phase C of the single-pass pipeline.

use code_graph_types::ssa::{ParseValue, ReferenceEvent};
use code_graph_types::{EdgeKind, ExpressionStep, NodeKind, Relationship};
use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

use super::graph::{CodeGraph, GraphEdge};
use super::imports::{self, ResolveSettings, apply_import_strategies};
use super::rules::{ImportStrategy, ResolutionRules, ResolveStage};
use super::state::ScratchBuf;

/// Result of resolving one file's references.
pub struct ResolveResult {
    pub edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
}

/// Per-file resolution context with caches.
struct ResolveCtx<'a> {
    graph: &'a CodeGraph,
    file_node: NodeIndex,
    def_nodes: &'a [NodeIndex],
    import_nodes: &'a [NodeIndex],
    import_map: FxHashMap<String, Vec<NodeIndex>>,
    rules: &'a ResolutionRules,
    settings: &'a ResolveSettings,
    scratch: ScratchBuf,
    import_cache: FxHashMap<NodeIndex, Vec<NodeIndex>>,
    nested_cache: FxHashMap<(String, String), Vec<NodeIndex>>,
}

impl<'a> ResolveCtx<'a> {
    fn new(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        rules: &'a ResolutionRules,
        settings: &'a ResolveSettings,
    ) -> Self {
        let import_map = pre_resolve_imports(graph, file_node, import_nodes);
        Self {
            graph,
            file_node,
            def_nodes,
            import_nodes,
            import_map,
            rules,
            settings,
            scratch: ScratchBuf::new(),
            import_cache: FxHashMap::default(),
            nested_cache: FxHashMap::default(),
        }
    }

    fn resolve_import_cached(&mut self, import_node: NodeIndex) -> Vec<NodeIndex> {
        if let Some(cached) = self.import_cache.get(&import_node) {
            return cached.clone();
        }
        let result = imports::resolve_import(
            self.graph,
            import_node,
            self.rules.fqn_separator,
            &mut self.scratch,
        );
        self.import_cache.insert(import_node, result.clone());
        result
    }

    fn lookup_nested_cached(
        &mut self,
        scope_fqn: &str,
        member_name: &str,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        let key = (scope_fqn.to_string(), member_name.to_string());
        if let Some(cached) = self.nested_cache.get(&key) {
            if !cached.is_empty() {
                out.extend_from_slice(cached);
                return true;
            }
            return false;
        }
        let mut result = Vec::new();
        self.graph
            .lookup_nested_with_hierarchy(scope_fqn, member_name, &mut result);
        let found = !result.is_empty();
        if found {
            out.extend_from_slice(&result);
        }
        self.nested_cache.insert(key, result);
        found
    }
}

/// Resolve all references for a single file.
///
/// Maps `ParseValue` indices to graph nodes and produces edges.
/// This runs in parallel across files — no shared mutable state.
pub fn resolve_file_references(
    graph: &CodeGraph,
    refs: &[ReferenceEvent],
    file_node: NodeIndex,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    rules: &ResolutionRules,
    settings: &ResolveSettings,
) -> ResolveResult {
    if refs.is_empty() {
        return ResolveResult { edges: Vec::new() };
    }

    let mut edges = Vec::new();
    let mut ctx = ResolveCtx::new(graph, file_node, def_nodes, import_nodes, rules, settings);

    let deadline = settings
        .per_file_timeout
        .map(|d| std::time::Instant::now() + d);

    for ref_event in refs {
        if deadline.is_some_and(|d| std::time::Instant::now() > d) {
            break;
        }
        let source_node = ref_event
            .enclosing_def
            .and_then(|i| def_nodes.get(i as usize).copied())
            .unwrap_or(file_node);

        let targets = resolve_single(&mut ctx, ref_event);
        if targets.len() > 100 {
            eprintln!(
                "[resolve-explosion] ref={:?} chain={} reaching={:?} targets={}",
                ref_event.name,
                ref_event.chain.is_some(),
                ref_event.reaching,
                targets.len(),
            );
        }

        let (source_node_kind, source_def_kind) = ref_event
            .enclosing_def
            .and_then(|i| def_nodes.get(i as usize))
            .and_then(|&n| graph.graph[n].def_id())
            .map(|did| (NodeKind::Definition, Some(graph.defs[did.0 as usize].kind)))
            .unwrap_or((NodeKind::File, None));

        for target in targets {
            let target_def_kind = graph.graph[target]
                .def_id()
                .map(|did| graph.defs[did.0 as usize].kind);
            edges.push((
                source_node,
                target,
                GraphEdge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Calls,
                        source_node: source_node_kind,
                        target_node: NodeKind::Definition,
                        source_def_kind,
                        target_def_kind,
                    },
                },
            ));
        }
    }

    ResolveResult { edges }
}

/// Resolve a single reference event to target nodes.
fn resolve_single(ctx: &mut ResolveCtx<'_>, ref_event: &ReferenceEvent) -> Vec<NodeIndex> {
    if ref_event.chain.is_some() {
        resolve_chain(ctx, ref_event)
    } else {
        resolve_bare(ctx, ref_event)
    }
}

/// Resolve a bare reference (no chain) using the configured stages.
fn resolve_bare(ctx: &mut ResolveCtx<'_>, ref_event: &ReferenceEvent) -> Vec<NodeIndex> {
    // Early skip: if no SSA reaching defs AND name not in any definition, nothing can resolve.
    if ref_event.reaching.is_empty() && !ctx.graph.indexes.by_name.contains(&ref_event.name) {
        return vec![];
    }

    for stage in &ctx.rules.bare_stages.clone() {
        let result = match stage {
            ResolveStage::SSA => {
                if ref_event.reaching.is_empty() {
                    vec![]
                } else {
                    resolve_from_reaching(ctx, &ref_event.reaching, &ref_event.name)
                }
            }
            ResolveStage::ImportStrategies => {
                // Skip if name doesn't exist as any definition
                if !ctx.graph.indexes.by_name.contains(&ref_event.name) {
                    continue;
                }
                apply_import_strategies(
                    &ctx.rules.import_strategies,
                    ctx.graph,
                    ctx.file_node,
                    &ref_event.name,
                    ctx.rules.fqn_separator,
                    &ctx.import_map,
                    &mut ctx.scratch,
                )
            }
            ResolveStage::ImplicitMember => {
                if !ctx.graph.indexes.by_name.contains(&ref_event.name) {
                    continue;
                }
                if let Some(enclosing_idx) = ref_event.enclosing_def
                    && let Some(&enclosing_node) = ctx.def_nodes.get(enclosing_idx as usize)
                {
                    resolve_implicit_member(
                        ctx.graph,
                        enclosing_node,
                        &ref_event.name,
                        ctx.rules.fqn_separator,
                    )
                } else {
                    vec![]
                }
            }
        };
        if !result.is_empty() {
            return result;
        }
    }
    vec![]
}

/// Map ParseValue reaching defs to concrete graph nodes.
fn resolve_from_reaching(
    ctx: &mut ResolveCtx<'_>,
    reaching: &[ParseValue],
    ref_name: &str,
) -> Vec<NodeIndex> {
    let mut result = Vec::new();
    for value in reaching {
        match value {
            ParseValue::LocalDef(i) => {
                if let Some(&node) = ctx.def_nodes.get(*i as usize) {
                    if let Some(did) = ctx.graph.graph[node].def_id() {
                        let gdef = &ctx.graph.defs[did.0 as usize];
                        if gdef.kind.is_type_container() {
                            let fqn = ctx.graph.str(gdef.fqn);
                            ctx.lookup_nested_cached(fqn, ref_name, &mut result);
                        } else {
                            result.push(node);
                        }
                    }
                }
            }
            ParseValue::ImportRef(i) => {
                if let Some(&import_node) = ctx.import_nodes.get(*i as usize) {
                    result.extend(ctx.resolve_import_cached(import_node));
                }
            }
            ParseValue::Type(type_fqn) => {
                ctx.lookup_nested_cached(type_fqn, ref_name, &mut result);
            }
            ParseValue::Opaque => {}
        }
    }
    result
}

/// Resolve a chained reference using FQN-based type flow.
fn resolve_chain(ctx: &mut ResolveCtx<'_>, ref_event: &ReferenceEvent) -> Vec<NodeIndex> {
    let chain = ref_event.chain.as_deref().unwrap_or(&[]);
    if chain.is_empty() {
        return vec![];
    }

    let mut current_types: Vec<String> =
        resolve_base_type_fqns(ctx, &chain[0], &ref_event.reaching);

    if current_types.is_empty() {
        if ctx.settings.chain_fallback {
            if let Some(last_name) = chain.last().and_then(|s| match s {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => Some(n.as_str()),
                _ => None,
            }) {
                let fallback_event = ReferenceEvent {
                    name: last_name.to_string(),
                    chain: None,
                    reaching: smallvec::smallvec![],
                    enclosing_def: ref_event.enclosing_def,
                    range: ref_event.range,
                };
                return resolve_bare(ctx, &fallback_event);
            }
        }
        return vec![];
    }

    for (depth, step) in chain[1..].iter().enumerate() {
        if depth >= ctx.settings.max_chain_depth || current_types.is_empty() {
            break;
        }
        let member_name = match step {
            ExpressionStep::Call(n) | ExpressionStep::Field(n) => n.as_str(),
            _ => continue,
        };

        let is_last = depth == chain.len() - 2;

        let mut found_nodes = Vec::new();
        let mut next_types = Vec::new();

        for type_fqn in &current_types {
            let before = found_nodes.len();
            ctx.lookup_nested_cached(type_fqn, member_name, &mut found_nodes);

            for &def_idx in &found_nodes[before..] {
                if let Some(did) = ctx.graph.graph[def_idx].def_id() {
                    let gdef = &ctx.graph.defs[did.0 as usize];
                    if matches!(step, ExpressionStep::Call(_)) {
                        if let Some(meta) = &gdef.metadata
                            && let Some(rt) = meta.return_type
                        {
                            next_types.push(ctx.graph.str(rt).to_string());
                        }
                        if gdef.kind.is_type_container() {
                            next_types.push(ctx.graph.str(gdef.fqn).to_string());
                        }
                    }
                    if matches!(step, ExpressionStep::Field(_)) {
                        if let Some(meta) = &gdef.metadata
                            && let Some(ta) = meta.type_annotation
                        {
                            next_types.push(ctx.graph.str(ta).to_string());
                        }
                    }
                }
            }
        }

        if is_last {
            return found_nodes;
        }

        current_types = next_types;
    }

    vec![]
}

/// Resolve the base of a chain to type FQN strings.
fn resolve_base_type_fqns(
    ctx: &mut ResolveCtx<'_>,
    base_step: &ExpressionStep,
    reaching: &[ParseValue],
) -> Vec<String> {
    match base_step {
        ExpressionStep::Ident(_) | ExpressionStep::Call(_) | ExpressionStep::This => {
            let mut types = Vec::new();
            for value in reaching {
                match value {
                    ParseValue::Type(fqn) => {
                        types.push(fqn.clone());
                    }
                    ParseValue::LocalDef(i) => {
                        if let Some(&node) = ctx.def_nodes.get(*i as usize)
                            && let Some(did) = ctx.graph.graph[node].def_id()
                        {
                            let gdef = &ctx.graph.defs[did.0 as usize];
                            if gdef.kind.is_type_container() {
                                types.push(ctx.graph.str(gdef.fqn).to_string());
                            } else if let Some(meta) = &gdef.metadata
                                && let Some(rt) = meta.return_type
                            {
                                types.push(ctx.graph.str(rt).to_string());
                            }
                        }
                    }
                    ParseValue::ImportRef(i) => {
                        if let Some(&import_node) = ctx.import_nodes.get(*i as usize) {
                            let resolved = ctx.resolve_import_cached(import_node);
                            for def_idx in resolved {
                                if let Some(did) = ctx.graph.graph[def_idx].def_id() {
                                    let gdef = &ctx.graph.defs[did.0 as usize];
                                    if gdef.kind.is_type_container() {
                                        types.push(ctx.graph.str(gdef.fqn).to_string());
                                    }
                                }
                            }
                        }
                    }
                    ParseValue::Opaque => {}
                }
            }
            types
        }
        ExpressionStep::Super => reaching
            .iter()
            .filter_map(|v| match v {
                ParseValue::Type(fqn) => Some(fqn.clone()),
                _ => None,
            })
            .collect(),
        ExpressionStep::New(type_name) => {
            let fqn_matches = ctx.graph.indexes.by_fqn.lookup(type_name, |idx| {
                ctx.graph.graph[idx]
                    .def_id()
                    .is_some_and(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn) == *type_name)
            });
            if !fqn_matches.is_empty() {
                return vec![type_name.clone()];
            }
            let name_matches = ctx.graph.indexes.by_name.lookup(type_name, |idx| {
                ctx.graph.graph[idx]
                    .def_id()
                    .is_some_and(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].name) == *type_name)
            });
            name_matches
                .iter()
                .filter_map(|&idx| {
                    ctx.graph.graph[idx]
                        .def_id()
                        .map(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string())
                })
                .collect()
        }
        _ => vec![],
    }
}

/// Pre-resolve imports for a file: build name → [NodeIndex] map.
fn pre_resolve_imports(
    graph: &CodeGraph,
    file_node: NodeIndex,
    import_nodes: &[NodeIndex],
) -> FxHashMap<String, Vec<NodeIndex>> {
    let mut map: FxHashMap<String, Vec<NodeIndex>> = FxHashMap::default();
    for &import_node in import_nodes {
        if let Some(iid) = graph.graph[import_node].import_id() {
            let gimp = &graph.imports[iid.0 as usize];
            let effective_name = gimp
                .alias
                .or(gimp.name)
                .map(|s| graph.str(s).to_string())
                .unwrap_or_default();
            if !effective_name.is_empty() {
                let fqn = match gimp.name {
                    Some(n) => format!("{}{}{}", graph.str(gimp.path), ".", graph.str(n),),
                    None => graph.str(gimp.path).to_string(),
                };
                let targets = graph.indexes.by_fqn.lookup(&fqn, |idx| {
                    graph.graph[idx]
                        .def_id()
                        .is_some_and(|d| graph.str(graph.defs[d.0 as usize].fqn) == fqn)
                });
                if !targets.is_empty() {
                    map.entry(effective_name).or_default().extend(targets);
                }
            }
        }
    }
    map
}

/// Look up a name as a member of the enclosing scope (implicit this/self).
fn resolve_implicit_member(
    graph: &CodeGraph,
    enclosing_node: NodeIndex,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    let mut result = Vec::new();
    if let Some(did) = graph.graph[enclosing_node].def_id() {
        let gdef = &graph.defs[did.0 as usize];
        let fqn = graph.str(gdef.fqn);
        // Walk up the FQN chain looking for a container with this member
        let mut scope = fqn;
        loop {
            graph.indexes.nested.lookup_into(
                scope,
                name,
                |idx| {
                    graph.graph[idx]
                        .def_id()
                        .is_some_and(|d| graph.str(graph.defs[d.0 as usize].name) == name)
                },
                &mut result,
            );
            if !result.is_empty() {
                break;
            }
            match scope.rfind(sep) {
                Some(pos) => scope = &scope[..pos],
                None => break,
            }
        }
    }
    result
}
