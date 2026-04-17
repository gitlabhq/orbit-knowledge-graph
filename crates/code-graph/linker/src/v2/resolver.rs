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
    let mut edges = Vec::new();
    let mut scratch = ScratchBuf::new();

    // Pre-resolve imports for this file (same as the old walker)
    let import_map = pre_resolve_imports(graph, file_node, import_nodes);

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

        let targets = resolve_single(
            graph,
            ref_event,
            file_node,
            def_nodes,
            import_nodes,
            &import_map,
            rules,
            settings,
            &mut scratch,
        );

        for target in targets {
            if target != source_node {
                edges.push((
                    source_node,
                    target,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Calls,
                            source_node: NodeKind::Definition,
                            target_node: NodeKind::Definition,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                    },
                ));
            }
        }
    }

    ResolveResult { edges }
}

/// Resolve a single reference event to target nodes.
fn resolve_single(
    graph: &CodeGraph,
    ref_event: &ReferenceEvent,
    file_node: NodeIndex,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    import_map: &FxHashMap<String, Vec<NodeIndex>>,
    rules: &ResolutionRules,
    settings: &ResolveSettings,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    if ref_event.chain.is_some() {
        resolve_chain(
            graph,
            ref_event,
            file_node,
            def_nodes,
            import_nodes,
            import_map,
            rules,
            settings,
            scratch,
        )
    } else {
        resolve_bare(
            graph,
            ref_event,
            file_node,
            def_nodes,
            import_nodes,
            import_map,
            rules,
            scratch,
        )
    }
}

/// Resolve a bare reference (no chain) using the configured stages.
fn resolve_bare(
    graph: &CodeGraph,
    ref_event: &ReferenceEvent,
    file_node: NodeIndex,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    import_map: &FxHashMap<String, Vec<NodeIndex>>,
    rules: &ResolutionRules,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    for stage in &rules.bare_stages {
        let result = match stage {
            ResolveStage::SSA => resolve_from_reaching(
                graph,
                &ref_event.reaching,
                &ref_event.name,
                def_nodes,
                import_nodes,
                import_map,
                rules,
                scratch,
            ),
            ResolveStage::ImportStrategies => apply_import_strategies(
                &rules.import_strategies,
                graph,
                file_node,
                &ref_event.name,
                rules.fqn_separator,
                import_map,
                scratch,
            ),
            ResolveStage::ImplicitMember => {
                if let Some(enclosing_idx) = ref_event.enclosing_def
                    && let Some(&enclosing_node) = def_nodes.get(enclosing_idx as usize)
                {
                    resolve_implicit_member(
                        graph,
                        enclosing_node,
                        &ref_event.name,
                        rules.fqn_separator,
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
    graph: &CodeGraph,
    reaching: &[ParseValue],
    ref_name: &str,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    import_map: &FxHashMap<String, Vec<NodeIndex>>,
    rules: &ResolutionRules,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    let mut result = Vec::new();
    for value in reaching {
        match value {
            ParseValue::LocalDef(i) => {
                if let Some(&node) = def_nodes.get(*i as usize) {
                    if let Some(did) = graph.graph[node].def_id() {
                        let gdef = &graph.defs[did.0 as usize];
                        if gdef.kind.is_type_container() {
                            let fqn = graph.str(gdef.fqn);
                            graph.indexes.nested.lookup_into(
                                fqn,
                                ref_name,
                                |idx| {
                                    graph.graph[idx].def_id().is_some_and(|d| {
                                        graph.str(graph.defs[d.0 as usize].name) == ref_name
                                    })
                                },
                                &mut result,
                            );
                        } else {
                            result.push(node);
                        }
                    }
                }
            }
            ParseValue::ImportRef(i) => {
                if let Some(&import_node) = import_nodes.get(*i as usize) {
                    result.extend(imports::resolve_import(
                        graph,
                        import_node,
                        rules.fqn_separator,
                        scratch,
                    ));
                }
            }
            ParseValue::Type(type_fqn) => {
                // Nested member lookup: find ref_name as a member of type_fqn
                graph.lookup_nested_with_hierarchy(type_fqn, ref_name, &mut result);
            }
            ParseValue::Opaque => {}
        }
    }
    result
}

/// Resolve a chained reference using FQN-based type flow.
/// Mirrors the walker's resolve_base → walk_step pattern.
fn resolve_chain(
    graph: &CodeGraph,
    ref_event: &ReferenceEvent,
    file_node: NodeIndex,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    import_map: &FxHashMap<String, Vec<NodeIndex>>,
    rules: &ResolutionRules,
    settings: &ResolveSettings,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    let chain = ref_event.chain.as_deref().unwrap_or(&[]);
    if chain.is_empty() {
        return vec![];
    }

    // Resolve base to type FQNs (not NodeIndex)
    let mut current_types: Vec<String> = resolve_base_type_fqns(
        graph,
        &chain[0],
        &ref_event.reaching,
        def_nodes,
        import_nodes,
        rules,
        scratch,
    );

    if current_types.is_empty() {
        // Chain fallback: resolve the last step as a bare reference
        if settings.chain_fallback {
            if let Some(last_name) = chain.last().and_then(|s| match s {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => Some(n.as_str()),
                _ => None,
            }) {
                let fallback_event = ReferenceEvent {
                    name: last_name.to_string(),
                    chain: None,
                    reaching: ref_event.reaching.clone(),
                    enclosing_def: ref_event.enclosing_def,
                    range: ref_event.range,
                };
                return resolve_bare(
                    graph,
                    &fallback_event,
                    file_node,
                    def_nodes,
                    import_nodes,
                    import_map,
                    rules,
                    scratch,
                );
            }
        }
        return vec![];
    }

    // Walk chain steps using FQN-based nested lookup
    for (depth, step) in chain[1..].iter().enumerate() {
        if depth >= settings.max_chain_depth || current_types.is_empty() {
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
            graph.lookup_nested_with_hierarchy(type_fqn, member_name, &mut found_nodes);

            // Derive next types from found defs
            for &def_idx in &found_nodes[before..] {
                if let Some(did) = graph.graph[def_idx].def_id() {
                    let gdef = &graph.defs[did.0 as usize];
                    if matches!(step, ExpressionStep::Call(_)) {
                        if let Some(meta) = &gdef.metadata
                            && let Some(rt) = meta.return_type
                        {
                            next_types.push(graph.str(rt).to_string());
                        }
                        if gdef.kind.is_type_container() {
                            next_types.push(graph.str(gdef.fqn).to_string());
                        }
                    }
                    if matches!(step, ExpressionStep::Field(_)) {
                        if let Some(meta) = &gdef.metadata
                            && let Some(ta) = meta.type_annotation
                        {
                            next_types.push(graph.str(ta).to_string());
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
/// Mirrors the walker's `resolve_base` → `value_types` pattern.
fn resolve_base_type_fqns(
    graph: &CodeGraph,
    base_step: &ExpressionStep,
    reaching: &[ParseValue],
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
    rules: &ResolutionRules,
    scratch: &mut ScratchBuf,
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
                        if let Some(&node) = def_nodes.get(*i as usize)
                            && let Some(did) = graph.graph[node].def_id()
                        {
                            let gdef = &graph.defs[did.0 as usize];
                            if gdef.kind.is_type_container() {
                                types.push(graph.str(gdef.fqn).to_string());
                            } else if let Some(meta) = &gdef.metadata
                                && let Some(rt) = meta.return_type
                            {
                                types.push(graph.str(rt).to_string());
                            }
                        }
                    }
                    ParseValue::ImportRef(i) => {
                        if let Some(&import_node) = import_nodes.get(*i as usize) {
                            let resolved = imports::resolve_import(
                                graph,
                                import_node,
                                rules.fqn_separator,
                                scratch,
                            );
                            for def_idx in resolved {
                                if let Some(did) = graph.graph[def_idx].def_id() {
                                    let gdef = &graph.defs[did.0 as usize];
                                    if gdef.kind.is_type_container() {
                                        types.push(graph.str(gdef.fqn).to_string());
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
        ExpressionStep::Super => {
            // Super types come from reaching defs (written as Type(super_fqn))
            reaching
                .iter()
                .filter_map(|v| match v {
                    ParseValue::Type(fqn) => Some(fqn.clone()),
                    _ => None,
                })
                .collect()
        }
        ExpressionStep::New(type_name) => {
            // Constructor: find type by FQN or name
            let fqn_matches = graph.indexes.by_fqn.lookup(type_name, |idx| {
                graph.graph[idx]
                    .def_id()
                    .is_some_and(|d| graph.str(graph.defs[d.0 as usize].fqn) == *type_name)
            });
            if !fqn_matches.is_empty() {
                return vec![type_name.clone()];
            }
            let name_matches = graph.indexes.by_name.lookup(type_name, |idx| {
                graph.graph[idx]
                    .def_id()
                    .is_some_and(|d| graph.str(graph.defs[d.0 as usize].name) == *type_name)
            });
            name_matches
                .iter()
                .filter_map(|&idx| {
                    graph.graph[idx]
                        .def_id()
                        .map(|d| graph.str(graph.defs[d.0 as usize].fqn).to_string())
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
