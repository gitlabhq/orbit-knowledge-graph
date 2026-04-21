//! Cross-file reference resolution.
//!
//! `FileResolver` resolves one reference at a time via `resolve()`.
//! No intermediate collections — the caller drives iteration.

use crate::v2::trace::{TraceEvent, Tracer};
use crate::v2::types::ssa::ParseValue;
use crate::v2::types::{DefKind, EdgeKind, ExpressionStep, NodeKind, Relationship};
use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

use super::graph::{CodeGraph, GraphEdge};
use super::imports::{self, ResolveSettings, apply_import_strategies};
use super::rules::{ResolutionRules, ResolveStage};
use super::state::ScratchBuf;

/// Borrowed reference data for resolution. No allocations.
pub struct RefData<'a> {
    pub name: &'a str,
    pub chain: Option<&'a [ExpressionStep]>,
    pub reaching: &'a [ParseValue],
    pub enclosing_def: Option<u32>,
}

/// Per-file resolver with caches. Create once per file, call `resolve()` per ref.
pub struct FileResolver<'a> {
    ctx: ResolveCtx<'a>,
    deadline: Option<std::time::Instant>,
    /// Edges from definitions to external imported symbols. Stored
    /// separately so they don't interfere with the failed_chains check.
    import_edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
}

impl<'a> FileResolver<'a> {
    pub fn new(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        rules: &'a ResolutionRules,
        settings: &'a ResolveSettings,
        tracer: &'a Tracer,
    ) -> Self {
        let ctx = ResolveCtx::new(
            graph,
            file_node,
            def_nodes,
            import_nodes,
            rules,
            settings,
            tracer,
        );
        let deadline = settings
            .per_file_timeout
            .map(|d| std::time::Instant::now() + d);
        Self {
            ctx,
            deadline,
            import_edges: Vec::new(),
        }
    }

    /// Drain accumulated Definition → ImportedSymbol edges.
    pub fn drain_import_edges(&mut self) -> Vec<(NodeIndex, NodeIndex, GraphEdge)> {
        std::mem::take(&mut self.import_edges)
    }

    /// Dump resolver trace events to stderr. Call after all refs are resolved.
    pub fn dump_trace(&self, header: &str) {
        self.ctx.tracer.dump_grouped(header);
    }

    /// Register inferred return types from the current file's Phase 2 pass.
    /// Maps def index → return type name. The resolver checks these when
    /// the graph's metadata has no explicit return type.
    pub fn set_inferred_returns(&mut self, returns: &[(u32, String)]) {
        for (def_idx, rt) in returns {
            if let Some(&node) = self.ctx.def_nodes.get(*def_idx as usize) {
                self.ctx.inferred_returns.insert(node, rt.clone());
            }
        }
    }

    /// Resolve a single reference, returning (source, target, edge) triples.
    pub fn resolve(
        &mut self,
        name: &str,
        chain: Option<&[ExpressionStep]>,
        reaching: &[ParseValue],
        enclosing_def: Option<u32>,
        edges: &mut Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    ) {
        if self.deadline.is_some_and(|d| std::time::Instant::now() > d) {
            return;
        }

        let ref_data = RefData {
            name,
            chain,
            reaching,
            enclosing_def,
        };
        let targets = resolve_single(&mut self.ctx, &ref_data);
        if targets.is_empty() {
            let mut imp_edges = Vec::new();
            self.emit_imported_symbol_edges(reaching, enclosing_def, &mut imp_edges);
            self.import_edges.extend(imp_edges);
            return;
        }

        let graph = self.ctx.graph;
        let source_node = enclosing_def
            .and_then(|i| self.ctx.def_nodes.get(i as usize).copied())
            .unwrap_or(self.ctx.file_node);

        let (source_node_kind, source_def_kind) = enclosing_def
            .and_then(|i| self.ctx.def_nodes.get(i as usize))
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

    /// Emit Definition → ImportedSymbol edges for external (unresolved) refs.
    fn emit_imported_symbol_edges(
        &self,
        reaching: &[ParseValue],
        enclosing_def: Option<u32>,
        edges: &mut Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    ) {
        let Some(enc) = enclosing_def else { return };
        let Some(&src) = self.ctx.def_nodes.get(enc as usize) else {
            return;
        };
        let src_def_kind = self.ctx.graph.graph[src]
            .def_id()
            .map(|did| self.ctx.graph.defs[did.0 as usize].kind);
        let rel = Relationship {
            edge_kind: EdgeKind::Calls,
            source_node: NodeKind::Definition,
            target_node: NodeKind::ImportedSymbol,
            source_def_kind: src_def_kind,
            target_def_kind: None,
        };
        // (a) Explicit imports from reaching defs
        let before = edges.len();
        for pv in reaching {
            if let ParseValue::ImportRef(i) = pv
                && let Some(&import_node) = self.ctx.import_nodes.get(*i as usize)
            {
                edges.push((src, import_node, GraphEdge { relationship: rel }));
            }
        }
        // (b) Wildcard imports: when no explicit import matched, any wildcard
        // import from this file could cover the ref name.
        if edges.len() == before {
            for &import_node in self.ctx.import_nodes {
                let imp = self.ctx.graph.import(import_node);
                if imp.wildcard {
                    edges.push((src, import_node, GraphEdge { relationship: rel }));
                }
            }
        }
    }
}

// ── internals ───────────────────────────────────────────────────

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
    inferred_returns: FxHashMap<NodeIndex, String>,
    tracer: &'a Tracer,
}

impl<'a> ResolveCtx<'a> {
    fn new(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        rules: &'a ResolutionRules,
        settings: &'a ResolveSettings,
        tracer: &'a Tracer,
    ) -> Self {
        let import_map = pre_resolve_imports(graph, import_nodes, rules.fqn_separator);
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
            inferred_returns: FxHashMap::default(),
            tracer,
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
        if let Some(iid) = self.graph.graph[import_node].import_id() {
            let gimp = &self.graph.imports[iid.0 as usize];
            let path = self.graph.str(gimp.path);
            let name = gimp.name.map(|n| self.graph.str(n)).unwrap_or("");
            let fqn = if path.is_empty() {
                name.to_string()
            } else {
                format!("{path}{}{name}", self.rules.fqn_separator)
            };
            let result_fqns: Vec<String> = result
                .iter()
                .filter_map(|&n| {
                    self.graph.graph[n].def_id().map(|d| {
                        self.graph
                            .str(self.graph.defs[d.0 as usize].fqn)
                            .to_string()
                    })
                })
                .collect();
            self.tracer.event(TraceEvent::ImportResolve {
                import_fqn: fqn,
                found: !result.is_empty(),
                result_fqns,
            });
        }
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

        // Fallback: try implicit sub-scopes (e.g. Kotlin Companion objects).
        // Foo.bar() → Foo.Companion.bar()
        if result.is_empty() {
            for &sub in self.rules.implicit_sub_scopes {
                let sub_scope = format!("{scope_fqn}{}{sub}", self.rules.fqn_separator);
                self.graph
                    .lookup_nested_with_hierarchy(&sub_scope, member_name, &mut result);
                if !result.is_empty() {
                    self.tracer.event(TraceEvent::ImplicitSubScope {
                        scope_fqn: scope_fqn.to_string(),
                        sub_scope: sub.to_string(),
                        member_name: member_name.to_string(),
                        found: true,
                    });
                    break;
                }
            }
        }

        // Fallback: find methods whose receiver_type matches scope_fqn
        // or any of its ancestors. This handles Go methods
        // (`func (s *Service) Run()`) and promoted methods from embedded
        // structs (`Service` embeds `Logger`, `svc.Log()` resolves via
        // Logger's receiver type).
        if result.is_empty() {
            self.graph.lookup_by_receiver_type(
                scope_fqn,
                member_name,
                self.rules.fqn_separator,
                &mut result,
            );
            // Walk ancestors for receiver type lookup (struct embedding,
            // extension functions on parent types). Collects ALL matches
            // across the full ancestor chain — doesn't early-exit so that
            // diamond/multiple inheritance cases surface all candidates.
            if result.is_empty() {
                let scope_nodes = self.graph.resolve_scope_nodes(scope_fqn);
                for &scope_node in &scope_nodes {
                    if let Some(ancestors) = self.graph.ancestors(scope_node) {
                        for &ancestor in ancestors {
                            let ancestor_fqn = self.graph.def_fqn(ancestor);
                            self.graph.lookup_by_receiver_type(
                                ancestor_fqn,
                                member_name,
                                self.rules.fqn_separator,
                                &mut result,
                            );
                        }
                    }
                }
            }
            if !result.is_empty() {
                self.tracer.event(TraceEvent::ReceiverTypeLookup {
                    type_name: scope_fqn.to_string(),
                    member_name: member_name.to_string(),
                    found_count: result.len(),
                });
            }
        }

        let found = !result.is_empty();
        let result_fqns: Vec<String> = result
            .iter()
            .filter_map(|&n| {
                self.graph.graph[n].def_id().map(|d| {
                    self.graph
                        .str(self.graph.defs[d.0 as usize].fqn)
                        .to_string()
                })
            })
            .collect();
        self.tracer.event(TraceEvent::NestedLookup {
            scope_fqn: scope_fqn.to_string(),
            member_name: member_name.to_string(),
            found,
            result_fqns,
        });
        if found {
            out.extend_from_slice(&result);
        }
        self.nested_cache.insert(key, result);
        found
    }
}

fn format_reaching(reaching: &[ParseValue]) -> Vec<String> {
    reaching
        .iter()
        .map(|v| match v {
            ParseValue::LocalDef(i) => format!("LocalDef({i})"),
            ParseValue::ImportRef(i) => format!("ImportRef({i})"),
            ParseValue::Type(t) => format!("Type({t})"),
            ParseValue::Opaque => "Opaque".to_string(),
        })
        .collect()
}

fn format_chain(chain: Option<&[ExpressionStep]>) -> Option<Vec<String>> {
    chain.map(|c| c.iter().map(|s| format!("{s:?}")).collect())
}

fn resolve_single(ctx: &mut ResolveCtx<'_>, r: &RefData<'_>) -> Vec<NodeIndex> {
    ctx.tracer.event(TraceEvent::ResolveStart {
        name: r.name.to_string(),
        chain: format_chain(r.chain),
        reaching: format_reaching(r.reaching),
        enclosing_def: r.enclosing_def.and_then(|i| {
            ctx.def_nodes
                .get(i as usize)
                .and_then(|&n| ctx.graph.graph[n].def_id())
                .map(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string())
        }),
    });

    let result = if r.chain.is_some() {
        resolve_chain(ctx, r)
    } else {
        resolve_bare(ctx, r)
    };

    let target_fqns: Vec<String> = result
        .iter()
        .filter_map(|&n| {
            ctx.graph.graph[n]
                .def_id()
                .map(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string())
        })
        .collect();
    ctx.tracer.event(TraceEvent::ResolveResult {
        name: r.name.to_string(),
        targets: target_fqns,
    });

    result
}

fn resolve_bare(ctx: &mut ResolveCtx<'_>, r: &RefData<'_>) -> Vec<NodeIndex> {
    if r.reaching.is_empty() && !ctx.graph.indexes.by_name.contains(r.name) {
        return vec![];
    }

    for stage in &ctx.rules.bare_stages.clone() {
        let stage_name = format!("{stage:?}");
        let result = match stage {
            ResolveStage::SSA => {
                if r.reaching.is_empty() {
                    vec![]
                } else {
                    resolve_from_reaching(ctx, r.reaching, r.name)
                }
            }
            ResolveStage::ImportStrategies => {
                if !ctx.graph.indexes.by_name.contains(r.name) {
                    continue;
                }
                apply_import_strategies(
                    &ctx.rules.import_strategies,
                    ctx.graph,
                    ctx.file_node,
                    r.name,
                    ctx.rules.fqn_separator,
                    &ctx.import_map,
                    &mut ctx.scratch,
                )
            }
            ResolveStage::ImplicitMember => {
                if !ctx.graph.indexes.by_name.contains(r.name) {
                    continue;
                }
                if let Some(enclosing_idx) = r.enclosing_def
                    && let Some(&enclosing_node) = ctx.def_nodes.get(enclosing_idx as usize)
                {
                    resolve_implicit_member(
                        ctx.graph,
                        enclosing_node,
                        r.name,
                        ctx.rules.fqn_separator,
                    )
                } else {
                    vec![]
                }
            }
        };
        let result_fqns: Vec<String> = result
            .iter()
            .filter_map(|&n| {
                ctx.graph.graph[n]
                    .def_id()
                    .map(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string())
            })
            .collect();
        ctx.tracer.event(TraceEvent::ResolveBareStage {
            stage: stage_name,
            name: r.name.to_string(),
            result_count: result.len(),
            result_fqns,
        });
        if !result.is_empty() {
            let mut seen = rustc_hash::FxHashSet::default();
            let mut result = result;
            result.retain(|n| seen.insert(*n));
            return result;
        }
    }
    vec![]
}

fn resolve_from_reaching(
    ctx: &mut ResolveCtx<'_>,
    reaching: &[ParseValue],
    ref_name: &str,
) -> Vec<NodeIndex> {
    let mut result = Vec::new();
    for value in reaching {
        match value {
            ParseValue::LocalDef(i) => {
                if let Some(&node) = ctx.def_nodes.get(*i as usize)
                    && let Some(did) = ctx.graph.graph[node].def_id()
                {
                    let gdef = &ctx.graph.defs[did.0 as usize];
                    if gdef.kind.is_type_container() {
                        let name = ctx.graph.str(gdef.name);
                        let fqn = ctx.graph.str(gdef.fqn);
                        if ref_name == name {
                            ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                result: format!("constructor -> {fqn}"),
                            });
                            result.push(node);
                        } else if !ctx.lookup_nested_cached(fqn, ref_name, &mut result) {
                            if let Some(call_method) = ctx.rules.hooks.call_method {
                                let before = result.len();
                                ctx.lookup_nested_cached(fqn, call_method, &mut result);
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: if result.len() > before {
                                        format!("call_method({call_method}) found")
                                    } else {
                                        format!("{fqn}.{ref_name} not found, call_method({call_method}) not found")
                                    },
                                });
                            } else {
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: format!("{fqn}.{ref_name} not found"),
                                });
                            }
                        } else {
                            ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                result: format!("nested {fqn}.{ref_name} found"),
                            });
                        }
                    } else {
                        let fqn = ctx.graph.str(gdef.fqn);
                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                            value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                            result: format!("direct -> {fqn}"),
                        });
                        result.push(node);
                    }
                }
            }
            ParseValue::ImportRef(i) => {
                if let Some(&import_node) = ctx.import_nodes.get(*i as usize) {
                    let resolved = ctx.resolve_import_cached(import_node);
                    let fqns: Vec<String> = resolved
                        .iter()
                        .filter_map(|&n| {
                            ctx.graph.graph[n].def_id().map(|d| {
                                ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string()
                            })
                        })
                        .collect();
                    ctx.tracer.event(TraceEvent::ReachingDefResolved {
                        value: format!("ImportRef({i})"),
                        result: if fqns.is_empty() {
                            "import not resolved".to_string()
                        } else {
                            format!("import -> [{}]", fqns.join(", "))
                        },
                    });
                    result.extend(resolved);
                }
            }
            ParseValue::Type(type_fqn) => {
                let before = result.len();
                if !ctx.lookup_nested_cached(type_fqn, ref_name, &mut result) {
                    if let Some(call_method) = ctx.rules.hooks.call_method {
                        ctx.lookup_nested_cached(type_fqn, call_method, &mut result);
                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                            value: format!("Type({type_fqn})"),
                            result: if result.len() > before {
                                format!("call_method({call_method}) found")
                            } else {
                                format!("{type_fqn}.{ref_name} not found, call_method({call_method}) not found")
                            },
                        });
                    } else {
                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                            value: format!("Type({type_fqn})"),
                            result: format!("{type_fqn}.{ref_name} not found"),
                        });
                    }
                } else {
                    ctx.tracer.event(TraceEvent::ReachingDefResolved {
                        value: format!("Type({type_fqn})"),
                        result: format!("nested {type_fqn}.{ref_name} found"),
                    });
                }
            }
            ParseValue::Opaque => {}
        }
    }
    result
}

fn resolve_chain(ctx: &mut ResolveCtx<'_>, r: &RefData<'_>) -> Vec<NodeIndex> {
    let chain = r.chain.unwrap_or(&[]);
    if chain.is_empty() {
        return vec![];
    }

    let mut current_types: Vec<String> = resolve_base_type_fqns(ctx, &chain[0], r.reaching);

    ctx.tracer.event(TraceEvent::ResolveChainBase {
        step: format!("{:?}", chain[0]),
        types: current_types.clone(),
    });

    if current_types.is_empty() {
        return chain_fallback(ctx, r, chain);
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
                        let mut has_return_type = false;
                        if let Some(meta) = &gdef.metadata
                            && let Some(rt) = meta.return_type
                        {
                            next_types.push(ctx.graph.str(rt).to_string());
                            has_return_type = true;
                        }
                        if !has_return_type && let Some(rt) = ctx.inferred_returns.get(&def_idx) {
                            next_types.push(rt.clone());
                            has_return_type = true;
                        }
                        if !has_return_type && gdef.kind.is_type_container() {
                            next_types.push(ctx.graph.str(gdef.fqn).to_string());
                        }
                    }
                    if matches!(step, ExpressionStep::Field(_)) {
                        if let Some(meta) = &gdef.metadata
                            && let Some(ta) = meta.type_annotation
                        {
                            next_types.push(ctx.graph.str(ta).to_string());
                        } else if let Some(meta) = &gdef.metadata
                            && let Some(rt) = meta.return_type
                        {
                            next_types.push(ctx.graph.str(rt).to_string());
                        } else if gdef.kind == DefKind::EnumEntry {
                            // Enum constant: propagate the parent enum's FQN
                            let fqn = ctx.graph.str(gdef.fqn);
                            if let Some((parent, _)) = fqn.rsplit_once(ctx.rules.fqn_separator) {
                                next_types.push(parent.to_string());
                            }
                        } else if gdef.kind.is_type_container() {
                            // Nested type access (e.g. Outer.Inner.method()):
                            // propagate the type's own FQN so the chain continues
                            next_types.push(ctx.graph.str(gdef.fqn).to_string());
                        }
                    }
                }
            }
        }

        // Constructor method hook: when Call("new") (or similar) finds no
        // nested member, the call returns an instance of the receiver type.
        if found_nodes.is_empty()
            && matches!(step, ExpressionStep::Call(_))
            && ctx.rules.hooks.constructor_methods.contains(&member_name)
        {
            next_types.extend(current_types.iter().cloned());
        }

        let found_fqns: Vec<String> = found_nodes
            .iter()
            .filter_map(|&n| {
                ctx.graph.graph[n]
                    .def_id()
                    .map(|d| ctx.graph.str(ctx.graph.defs[d.0 as usize].fqn).to_string())
            })
            .collect();
        ctx.tracer.event(TraceEvent::ResolveChainStep {
            depth,
            step: format!("{step:?}"),
            member_name: member_name.to_string(),
            scope_types: current_types.clone(),
            found_count: found_nodes.len(),
            found_fqns,
            next_types: next_types.clone(),
        });

        if is_last {
            let mut seen = rustc_hash::FxHashSet::default();
            found_nodes.retain(|n| seen.insert(*n));
            return found_nodes;
        }

        {
            let mut seen = rustc_hash::FxHashSet::default();
            next_types.retain(|t| seen.insert(t.clone()));
        }
        current_types = next_types;
    }

    chain_fallback(ctx, r, chain)
}

/// When chain resolution fails (empty base or mid-chain step failure),
/// fall back to bare resolution of the terminal name. Handles extension
/// functions, static methods, and other cases where the def isn't nested
/// under the chain's resolved type.
fn chain_fallback(
    ctx: &mut ResolveCtx<'_>,
    r: &RefData<'_>,
    chain: &[ExpressionStep],
) -> Vec<NodeIndex> {
    if !ctx.settings.chain_fallback {
        return vec![];
    }
    let Some(last_name) = chain.last().and_then(|s| match s {
        ExpressionStep::Call(n) | ExpressionStep::Field(n) => Some(n.as_str()),
        _ => None,
    }) else {
        return vec![];
    };
    ctx.tracer.event(TraceEvent::ResolveChainFallback {
        name: last_name.to_string(),
    });
    let fallback = RefData {
        name: last_name,
        chain: None,
        reaching: &[],
        enclosing_def: r.enclosing_def,
    };
    resolve_bare(ctx, &fallback)
}

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
                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                            value: format!("Type({fqn})"),
                            result: format!("base type -> {fqn}"),
                        });
                        types.push(fqn.clone());
                    }
                    ParseValue::LocalDef(i) => {
                        if let Some(&node) = ctx.def_nodes.get(*i as usize)
                            && let Some(did) = ctx.graph.graph[node].def_id()
                        {
                            let gdef = &ctx.graph.defs[did.0 as usize];
                            let fqn = ctx.graph.str(gdef.fqn);
                            if gdef.kind.is_type_container() {
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: format!("base type (container) -> {fqn}"),
                                });
                                types.push(fqn.to_string());
                            } else if let Some(meta) = &gdef.metadata
                                && let Some(rt) = meta.return_type
                            {
                                let rt_str = ctx.graph.str(rt);
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: format!("base type (return_type) -> {rt_str}"),
                                });
                                types.push(rt_str.to_string());
                            } else if let Some(rt) = ctx.inferred_returns.get(&node) {
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: format!("base type (inferred) -> {rt}"),
                                });
                                types.push(rt.clone());
                            } else {
                                ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: "no type info".to_string(),
                                });
                            }
                        }
                    }
                    ParseValue::ImportRef(i) => {
                        if let Some(&import_node) = ctx.import_nodes.get(*i as usize) {
                            let resolved = ctx.resolve_import_cached(import_node);
                            for def_idx in resolved {
                                if let Some(did) = ctx.graph.graph[def_idx].def_id() {
                                    let gdef = &ctx.graph.defs[did.0 as usize];
                                    let fqn = ctx.graph.str(gdef.fqn);
                                    if gdef.kind.is_type_container() {
                                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                            value: format!(
                                                "ImportRef({i}) -> {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (container) -> {fqn}"),
                                        });
                                        types.push(fqn.to_string());
                                    } else if let Some(meta) = &gdef.metadata
                                        && let Some(rt) = meta.return_type
                                    {
                                        let rt_str = ctx.graph.str(rt);
                                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                            value: format!(
                                                "ImportRef({i}) -> {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (return_type) -> {rt_str}"),
                                        });
                                        types.push(rt_str.to_string());
                                    } else if let Some(rt) = ctx.inferred_returns.get(&def_idx) {
                                        ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                            value: format!(
                                                "ImportRef({i}) -> {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (inferred) -> {rt}"),
                                        });
                                        types.push(rt.clone());
                                    }
                                }
                            }
                        }
                    }
                    ParseValue::Opaque => {}
                }
            }
            // Fallback: when reaching defs produce no types (e.g. chain base
            // is an untracked name like a same-package class), try resolving
            // the base name via import strategies to find its FQN, then
            // global name lookup as a last resort.
            if types.is_empty()
                && let ExpressionStep::Ident(name) | ExpressionStep::Call(name) = base_step
            {
                let fallback = RefData {
                    name,
                    chain: None,
                    reaching: &[],
                    enclosing_def: None,
                };
                let mut nodes = resolve_bare(ctx, &fallback);
                // GlobalName: last-resort lookup for chain bases only.
                // Not in import_strategies to avoid O(candidates) scans
                // on every bare identifier ref.
                if nodes.is_empty() {
                    nodes = super::imports::global_name(ctx.graph, ctx.file_node, name, 10);
                }
                for n in nodes {
                    if let Some(did) = ctx.graph.graph[n].def_id() {
                        let gdef = &ctx.graph.defs[did.0 as usize];
                        if gdef.kind.is_type_container() {
                            let fqn = ctx.graph.str(gdef.fqn).to_string();
                            ctx.tracer.event(TraceEvent::ReachingDefResolved {
                                value: format!("bare({name})"),
                                result: format!("base type (import fallback) -> {fqn}"),
                            });
                            types.push(fqn);
                        }
                    }
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

fn pre_resolve_imports(
    graph: &CodeGraph,
    import_nodes: &[NodeIndex],
    sep: &str,
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
                    Some(n) => format!("{}{}{}", graph.str(gimp.path), sep, graph.str(n)),
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
