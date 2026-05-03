//! Cross-file reference resolution.
//!
//! `FileResolver` resolves one reference at a time via `resolve()`.
//! No intermediate collections — the caller drives iteration.

use crate::trace;
use crate::v2::pipeline::LanguageContext;
use crate::v2::sentinel::{FileGuard, Killed};
use crate::v2::trace::Tracer;
use crate::v2::types::ssa::ParseValue;
use crate::v2::types::{
    DefKind, EdgeKind, ExpressionStep, ImportBindingKind, NodeKind, Relationship,
};
use petgraph::graph::NodeIndex;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;

use super::graph::{CodeGraph, GraphEdge};
use super::imports::{ImportResolver, ResolveSettings};
use super::rules::{AmbientImportFallback, ResolutionRules, ResolveStage};
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
    /// Sentinel guard — sets kill flag on timeout, sends FileDone on drop.
    /// None when running without a sentinel (tests, custom pipelines).
    _guard: Option<FileGuard>,
    /// Edges from definitions to external imported symbols. Stored
    /// separately so they don't interfere with the failed_chains check.
    import_edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    import_edge_keys: FxHashSet<(NodeIndex, NodeIndex, EdgeKind)>,
}

impl<'a> FileResolver<'a> {
    pub fn new(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        lang_ctx: &'a Arc<LanguageContext>,
        guard: Option<FileGuard>,
    ) -> Self {
        let kill_flag = guard
            .as_ref()
            .map(|g| g.kill_flag())
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let ctx = ResolveCtx::new(
            graph,
            file_node,
            def_nodes,
            import_nodes,
            lang_ctx,
            kill_flag,
        );
        Self {
            ctx,
            _guard: guard,
            import_edges: Vec::new(),
            import_edge_keys: FxHashSet::default(),
        }
    }

    /// Construct without a `LanguageContext` — used by custom pipelines
    /// (e.g. JS) that manage their own rules and settings.
    pub fn from_parts(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        rules: &'a ResolutionRules,
        settings: &'a ResolveSettings,
        tracer: &'a Tracer,
    ) -> Self {
        let import_map = pre_resolve_imports(graph, import_nodes);
        let ctx = ResolveCtx {
            graph,
            file_node,
            def_nodes,
            import_nodes,
            import_map,
            lang_ctx: None,
            rules,
            settings,
            tracer,
            kill_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            scratch: ScratchBuf::new(),
            import_cache: FxHashMap::default(),
            nested_cache: FxHashMap::default(),
            inferred_returns: FxHashMap::default(),
        };
        Self {
            ctx,
            _guard: None,
            import_edges: Vec::new(),
            import_edge_keys: FxHashSet::default(),
        }
    }

    /// Drain accumulated Definition → ImportedSymbol edges.
    pub fn drain_import_edges(&mut self) -> Vec<(NodeIndex, NodeIndex, GraphEdge)> {
        self.import_edge_keys.clear();
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
    /// Returns `Err(Killed)` if the sentinel has timed out this file.
    pub fn resolve(
        &mut self,
        name: &str,
        chain: Option<&[ExpressionStep]>,
        reaching: &[ParseValue],
        enclosing_def: Option<u32>,
        edges: &mut Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    ) -> Result<(), Killed> {
        self.ctx.check_killed()?;

        let ref_data = RefData {
            name,
            chain,
            reaching,
            enclosing_def,
        };
        let targets = self.ctx.resolve_single(&ref_data)?;
        if targets.is_empty() {
            self.emit_imported_symbol_edges(&ref_data);
            return Ok(());
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
        Ok(())
    }

    /// Emit Definition → ImportedSymbol edges for external (unresolved) refs.
    fn emit_imported_symbol_edges(&mut self, r: &RefData<'_>) {
        let Some(enc) = r.enclosing_def else { return };
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

        let policy = self.ctx.rules.hooks.imported_symbol_fallback;

        let mut saw_explicit_import = false;
        if policy.explicit_reaching_imports {
            for pv in r.reaching {
                if let ParseValue::ImportRef(i) = pv
                    && let Some(&import_node) = self.ctx.import_nodes.get(*i as usize)
                {
                    saw_explicit_import = true;
                    if !self.ctx.import_resolves_locally(import_node) {
                        self.push_imported_symbol_edge(src, import_node, rel);
                    }
                }
            }
        }

        if saw_explicit_import
            || self
                .ctx
                .rules
                .hooks
                .excluded_ambient_imported_symbol_names
                .contains(&r.name)
        {
            return;
        }

        match policy.ambient {
            AmbientImportFallback::None => {}
            AmbientImportFallback::Wildcard => {
                let candidates: Vec<NodeIndex> = self
                    .ctx
                    .import_nodes
                    .iter()
                    .copied()
                    .filter(|&import_node| self.ctx.graph.import(import_node).wildcard)
                    .collect();
                if candidates.is_empty()
                    || candidates.len() > policy.max_ambient_candidates
                    || policy.max_ambient_candidates == 0
                {
                    return;
                }
                for import_node in candidates {
                    self.push_imported_symbol_edge(src, import_node, rel);
                }
            }
        }
    }

    fn push_imported_symbol_edge(
        &mut self,
        src: NodeIndex,
        import_node: NodeIndex,
        relationship: Relationship,
    ) {
        if self
            .import_edge_keys
            .insert((src, import_node, relationship.edge_kind))
        {
            self.import_edges
                .push((src, import_node, GraphEdge { relationship }));
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
    #[allow(dead_code)]
    lang_ctx: Option<&'a Arc<LanguageContext>>,
    rules: &'a ResolutionRules,
    settings: &'a ResolveSettings,
    tracer: &'a Tracer,
    kill_flag: Arc<std::sync::atomic::AtomicBool>,
    scratch: ScratchBuf,
    import_cache: FxHashMap<NodeIndex, Vec<NodeIndex>>,
    nested_cache: FxHashMap<(String, String), Vec<NodeIndex>>,
    inferred_returns: FxHashMap<NodeIndex, String>,
}

impl<'a> ResolveCtx<'a> {
    fn new(
        graph: &'a CodeGraph,
        file_node: NodeIndex,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
        lang_ctx: &'a Arc<LanguageContext>,
        kill_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        let rules = &*lang_ctx.rules;
        let settings = &rules.settings;
        let tracer = lang_ctx.tracer();
        let import_map = pre_resolve_imports(graph, import_nodes);
        Self {
            graph,
            file_node,
            def_nodes,
            import_nodes,
            import_map,
            lang_ctx: Some(lang_ctx),
            rules,
            settings,
            tracer,
            kill_flag,
            scratch: ScratchBuf::new(),
            import_cache: FxHashMap::default(),
            nested_cache: FxHashMap::default(),
            inferred_returns: FxHashMap::default(),
        }
    }

    /// Check if the sentinel has killed this file. Returns `Err(Killed)`
    /// if so, allowing `?` propagation to unwind cleanly.
    #[inline]
    fn check_killed(&self) -> Result<(), Killed> {
        if self.kill_flag.load(std::sync::atomic::Ordering::Relaxed) {
            Err(Killed)
        } else {
            Ok(())
        }
    }

    /// Build a temporary `ImportResolver` from this context's state.
    fn import_resolver(&mut self) -> ImportResolver<'_> {
        ImportResolver {
            graph: self.graph,
            file_node: self.file_node,
            import_map: &self.import_map,
            scratch: &mut self.scratch,
            settings: self.settings,
        }
    }

    fn resolve_import_cached(&mut self, import_node: NodeIndex) -> Vec<NodeIndex> {
        if let Some(cached) = self.import_cache.get(&import_node) {
            return cached.clone();
        }
        let result = self.import_resolver().resolve_import(import_node);
        if let Some(iid) = self.graph.graph[import_node].import_id() {
            let gimp = &self.graph.imports[iid.0 as usize];
            let path = self.graph.str(gimp.path);
            let name = gimp.name.map(|n| self.graph.str(n)).unwrap_or("");
            let fqn = if path.is_empty() {
                name.to_string()
            } else {
                format!("{path}{}{name}", self.graph.sep())
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
            trace!(
                self.tracer,
                ImportResolve {
                    import_fqn: fqn,
                    found: !result.is_empty(),
                    result_fqns: result_fqns,
                }
            );
        }
        self.import_cache.insert(import_node, result.clone());
        result
    }

    fn import_resolves_locally(&mut self, import_node: NodeIndex) -> bool {
        !self.resolve_import_cached(import_node).is_empty()
    }

    fn has_unresolved_import_ref(&mut self, reaching: &[ParseValue]) -> bool {
        for value in reaching {
            if let ParseValue::ImportRef(i) = value
                && let Some(&import_node) = self.import_nodes.get(*i as usize)
                && !self.import_resolves_locally(import_node)
            {
                return true;
            }
        }
        false
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

        if result.is_empty() {
            let direct_fqn = format!("{}{member_name}", self.scope_member_prefix(scope_fqn));
            let direct = self.graph.indexes.by_fqn.lookup(&direct_fqn, |idx| {
                self.graph.graph[idx].def_id().is_some_and(|d| {
                    self.graph.str(self.graph.defs[d.0 as usize].fqn) == direct_fqn
                })
            });
            result.extend_from_slice(&direct);
        }

        // Fallback: try implicit sub-scopes (e.g. Kotlin Companion objects).
        // Foo.bar() → Foo.Companion.bar()
        if result.is_empty() {
            for &sub in self.rules.implicit_sub_scopes {
                let sub_scope = format!("{scope_fqn}{}{sub}", self.rules.fqn_separator);
                self.graph
                    .lookup_nested_with_hierarchy(&sub_scope, member_name, &mut result);
                if !result.is_empty() {
                    trace!(
                        self.tracer,
                        ImplicitSubScope {
                            scope_fqn: scope_fqn.to_string(),
                            sub_scope: sub.to_string(),
                            member_name: member_name.to_string(),
                            found: true,
                        }
                    );
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
            self.graph
                .lookup_by_receiver_type(scope_fqn, member_name, &mut result);
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
                                &mut result,
                            );
                        }
                    }
                }
            }
            if !result.is_empty() {
                trace!(
                    self.tracer,
                    ReceiverTypeLookup {
                        type_name: scope_fqn.to_string(),
                        member_name: member_name.to_string(),
                        found_count: result.len(),
                    }
                );
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
        trace!(
            self.tracer,
            NestedLookup {
                scope_fqn: scope_fqn.to_string(),
                member_name: member_name.to_string(),
                found: found,
                result_fqns: result_fqns,
            }
        );
        if found {
            out.extend_from_slice(&result);
        }
        self.nested_cache.insert(key, result);
        found
    }

    // ── Resolution methods (formerly free functions) ────────────

    fn resolve_single(&mut self, r: &RefData<'_>) -> Result<Vec<NodeIndex>, Killed> {
        trace!(
            self.tracer,
            ResolveStart {
                name: r.name.to_string(),
                chain: format_chain(r.chain),
                reaching: format_reaching(r.reaching),
                enclosing_def: r.enclosing_def.and_then(|i| {
                    self.def_nodes
                        .get(i as usize)
                        .and_then(|&n| self.graph.graph[n].def_id())
                        .map(|d| {
                            self.graph
                                .str(self.graph.defs[d.0 as usize].fqn)
                                .to_string()
                        })
                }),
            }
        );

        self.check_killed()?;
        let result = if r.chain.is_some() {
            self.resolve_chain(r)?
        } else {
            self.resolve_bare(r)?
        };

        let target_fqns: Vec<String> = result
            .iter()
            .filter_map(|&n| {
                self.graph.graph[n].def_id().map(|d| {
                    self.graph
                        .str(self.graph.defs[d.0 as usize].fqn)
                        .to_string()
                })
            })
            .collect();
        trace!(
            self.tracer,
            ResolveResult {
                name: r.name.to_string(),
                targets: target_fqns,
            }
        );

        Ok(result)
    }

    fn resolve_bare(&mut self, r: &RefData<'_>) -> Result<Vec<NodeIndex>, Killed> {
        self.check_killed()?;
        if r.reaching.is_empty() && !self.graph.indexes.by_name.contains(r.name) {
            return Ok(vec![]);
        }

        for stage in &self.rules.bare_stages.clone() {
            let stage_name = format!("{stage:?}");
            let result = match stage {
                ResolveStage::SSA => {
                    if r.reaching.is_empty() {
                        vec![]
                    } else {
                        self.resolve_from_reaching(r.reaching, r.name)?
                    }
                }
                ResolveStage::ImportStrategies => {
                    if !self.graph.indexes.by_name.contains(r.name) {
                        continue;
                    }
                    {
                        let strategies = self.rules.import_strategies.clone();
                        self.import_resolver().apply_strategies(&strategies, r.name)
                    }
                }
                ResolveStage::ImplicitMember => {
                    if !self.graph.indexes.by_name.contains(r.name) {
                        continue;
                    }
                    if let Some(enclosing_idx) = r.enclosing_def
                        && let Some(&enclosing_node) = self.def_nodes.get(enclosing_idx as usize)
                    {
                        self.resolve_implicit_member(enclosing_node, r.name)
                    } else {
                        vec![]
                    }
                }
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
            trace!(
                self.tracer,
                ResolveBareStage {
                    stage: stage_name,
                    name: r.name.to_string(),
                    result_count: result.len(),
                    result_fqns: result_fqns,
                }
            );
            if !result.is_empty() {
                let mut seen = rustc_hash::FxHashSet::default();
                let mut result = result;
                result.retain(|n| seen.insert(*n));
                return Ok(result);
            }
        }
        Ok(vec![])
    }

    fn resolve_from_reaching(
        &mut self,
        reaching: &[ParseValue],
        ref_name: &str,
    ) -> Result<Vec<NodeIndex>, Killed> {
        self.check_killed()?;
        let mut result = Vec::new();
        for value in reaching {
            match value {
                ParseValue::LocalDef(i) => {
                    if let Some(&node) = self.def_nodes.get(*i as usize)
                        && let Some(did) = self.graph.graph[node].def_id()
                    {
                        let gdef = &self.graph.defs[did.0 as usize];
                        if gdef.kind.is_type_container() {
                            let name = self.graph.str(gdef.name);
                            let fqn = self.graph.str(gdef.fqn);
                            if ref_name == name {
                                trace!(
                                    self.tracer,
                                    ReachingDefResolved {
                                        value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                        result: format!("constructor -> {fqn}"),
                                    }
                                );
                                result.push(node);
                            } else if !self.lookup_nested_cached(fqn, ref_name, &mut result) {
                                if let Some(call_method) = self.rules.hooks.call_method {
                                    let before = result.len();
                                    self.lookup_nested_cached(fqn, call_method, &mut result);
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: if result.len() > before {
                                                format!("call_method({call_method}) found")
                                            } else {
                                                format!(
                                                    "{fqn}.{ref_name} not found, call_method({call_method}) not found"
                                                )
                                            },
                                        }
                                    );
                                } else {
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("{fqn}.{ref_name} not found"),
                                        }
                                    );
                                }
                            } else {
                                trace!(
                                    self.tracer,
                                    ReachingDefResolved {
                                        value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                        result: format!("nested {fqn}.{ref_name} found"),
                                    }
                                );
                            }
                        } else {
                            let fqn = self.graph.str(gdef.fqn);
                            trace!(
                                self.tracer,
                                ReachingDefResolved {
                                    value: format!("LocalDef({i}) = {fqn} ({:?})", gdef.kind),
                                    result: format!("direct -> {fqn}"),
                                }
                            );
                            result.push(node);
                        }
                    }
                }
                ParseValue::ImportRef(i) => {
                    if let Some(&import_node) = self.import_nodes.get(*i as usize) {
                        let resolved = self.resolve_import_cached(import_node);
                        let fqns: Vec<String> = resolved
                            .iter()
                            .filter_map(|&n| {
                                self.graph.graph[n].def_id().map(|d| {
                                    self.graph
                                        .str(self.graph.defs[d.0 as usize].fqn)
                                        .to_string()
                                })
                            })
                            .collect();
                        trace!(
                            self.tracer,
                            ReachingDefResolved {
                                value: format!("ImportRef({i})"),
                                result: if fqns.is_empty() {
                                    "import not resolved".to_string()
                                } else {
                                    format!("import -> [{}]", fqns.join(", "))
                                },
                            }
                        );
                        result.extend(resolved);
                    }
                }
                ParseValue::Type(type_fqn) => {
                    let before = result.len();
                    if !self.lookup_nested_cached(type_fqn, ref_name, &mut result) {
                        if let Some(call_method) = self.rules.hooks.call_method {
                            self.lookup_nested_cached(type_fqn, call_method, &mut result);
                            trace!(
                                self.tracer,
                                ReachingDefResolved {
                                    value: format!("Type({type_fqn})"),
                                    result: if result.len() > before {
                                        format!("call_method({call_method}) found")
                                    } else {
                                        format!(
                                            "{type_fqn}.{ref_name} not found, call_method({call_method}) not found"
                                        )
                                    },
                                }
                            );
                        } else {
                            trace!(
                                self.tracer,
                                ReachingDefResolved {
                                    value: format!("Type({type_fqn})"),
                                    result: format!("{type_fqn}.{ref_name} not found"),
                                }
                            );
                        }
                    } else {
                        trace!(
                            self.tracer,
                            ReachingDefResolved {
                                value: format!("Type({type_fqn})"),
                                result: format!("nested {type_fqn}.{ref_name} found"),
                            }
                        );
                    }
                }
                ParseValue::Opaque => {}
            }
        }
        Ok(result)
    }

    fn resolve_chain(&mut self, r: &RefData<'_>) -> Result<Vec<NodeIndex>, Killed> {
        self.check_killed()?;
        let chain = r.chain.unwrap_or(&[]);
        if chain.is_empty() {
            return Ok(vec![]);
        }

        let mut current_types: Vec<String> = self.resolve_base_type_fqns(&chain[0], r.reaching)?;
        let base_has_unresolved_import_ref = self.has_unresolved_import_ref(r.reaching);

        trace!(
            self.tracer,
            ResolveChainBase {
                step: format!("{:?}", chain[0]),
                types: current_types.clone(),
            }
        );

        if current_types.is_empty() {
            if base_has_unresolved_import_ref {
                return Ok(vec![]);
            }
            return self.chain_fallback(r, chain);
        }

        for (depth, step) in chain[1..].iter().enumerate() {
            if depth >= self.settings.max_chain_depth || current_types.is_empty() {
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
                self.lookup_nested_cached(type_fqn, member_name, &mut found_nodes);

                for &def_idx in &found_nodes[before..] {
                    if let Some(did) = self.graph.graph[def_idx].def_id() {
                        let gdef = &self.graph.defs[did.0 as usize];
                        if matches!(step, ExpressionStep::Call(_)) {
                            let mut has_return_type = false;
                            if let Some(meta) = &gdef.metadata
                                && let Some(rt) = meta.return_type
                            {
                                next_types.push(self.graph.str(rt).to_string());
                                has_return_type = true;
                            }
                            if !has_return_type
                                && let Some(rt) = self.inferred_returns.get(&def_idx)
                            {
                                next_types.push(rt.clone());
                                has_return_type = true;
                            }
                            if !has_return_type && gdef.kind.is_type_container() {
                                next_types.push(self.graph.str(gdef.fqn).to_string());
                            }
                        }
                        if matches!(step, ExpressionStep::Field(_)) {
                            if let Some(meta) = &gdef.metadata
                                && let Some(ta) = meta.type_annotation
                            {
                                next_types.push(self.graph.str(ta).to_string());
                            } else if let Some(meta) = &gdef.metadata
                                && let Some(rt) = meta.return_type
                            {
                                next_types.push(self.graph.str(rt).to_string());
                            } else if gdef.kind == DefKind::EnumEntry {
                                let fqn = self.graph.str(gdef.fqn);
                                if let Some((parent, _)) = fqn.rsplit_once(self.rules.fqn_separator)
                                {
                                    next_types.push(parent.to_string());
                                }
                            } else if gdef.kind.is_type_container() {
                                next_types.push(self.graph.str(gdef.fqn).to_string());
                            }
                        }
                    }
                }
            }

            // Constructor method hook: when Call("new") (or similar) finds no
            // nested member, the call returns an instance of the receiver type.
            if found_nodes.is_empty()
                && matches!(step, ExpressionStep::Call(_))
                && self.rules.hooks.constructor_methods.contains(&member_name)
            {
                next_types.extend(current_types.iter().cloned());
            }

            let found_fqns: Vec<String> = found_nodes
                .iter()
                .filter_map(|&n| {
                    self.graph.graph[n].def_id().map(|d| {
                        self.graph
                            .str(self.graph.defs[d.0 as usize].fqn)
                            .to_string()
                    })
                })
                .collect();
            trace!(
                self.tracer,
                ResolveChainStep {
                    depth: depth,
                    step: format!("{step:?}"),
                    member_name: member_name.to_string(),
                    scope_types: current_types.clone(),
                    found_count: found_nodes.len(),
                    found_fqns: found_fqns,
                    next_types: next_types.clone(),
                }
            );

            if is_last {
                let mut seen = rustc_hash::FxHashSet::default();
                found_nodes.retain(|n| seen.insert(*n));
                return Ok(found_nodes);
            }

            {
                let mut seen = rustc_hash::FxHashSet::default();
                next_types.retain(|t| seen.insert(t.clone()));
            }
            current_types = next_types;
        }

        if base_has_unresolved_import_ref {
            return Ok(vec![]);
        }

        self.chain_fallback(r, chain)
    }

    fn chain_fallback(
        &mut self,
        r: &RefData<'_>,
        chain: &[ExpressionStep],
    ) -> Result<Vec<NodeIndex>, Killed> {
        if !self.settings.chain_fallback {
            return Ok(vec![]);
        }
        let Some(last_name) = chain.last().and_then(|s| match s {
            ExpressionStep::Call(n) | ExpressionStep::Field(n) => Some(n.as_str()),
            _ => None,
        }) else {
            return Ok(vec![]);
        };
        trace!(
            self.tracer,
            ResolveChainFallback {
                name: last_name.to_string(),
            }
        );
        let fallback = RefData {
            name: last_name,
            chain: None,
            reaching: &[],
            enclosing_def: r.enclosing_def,
        };
        self.resolve_bare(&fallback)
    }

    fn resolve_base_type_fqns(
        &mut self,
        base_step: &ExpressionStep,
        reaching: &[ParseValue],
    ) -> Result<Vec<String>, Killed> {
        self.check_killed()?;
        match base_step {
            ExpressionStep::Ident(_) | ExpressionStep::Call(_) | ExpressionStep::This => {
                let mut types = Vec::new();
                for value in reaching {
                    match value {
                        ParseValue::Type(fqn) => {
                            trace!(
                                self.tracer,
                                ReachingDefResolved {
                                    value: format!("Type({fqn})"),
                                    result: format!("base type -> {fqn}"),
                                }
                            );
                            types.push(fqn.to_string());
                        }
                        ParseValue::LocalDef(i) => {
                            if let Some(&node) = self.def_nodes.get(*i as usize)
                                && let Some(did) = self.graph.graph[node].def_id()
                            {
                                let gdef = &self.graph.defs[did.0 as usize];
                                let fqn = self.graph.str(gdef.fqn);
                                if gdef.kind.is_type_container() {
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (container) -> {fqn}"),
                                        }
                                    );
                                    types.push(fqn.to_string());
                                } else if let Some(meta) = &gdef.metadata
                                    && let Some(rt) = meta.return_type
                                {
                                    let rt_str = self.graph.str(rt);
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (return_type) -> {rt_str}"),
                                        }
                                    );
                                    types.push(rt_str.to_string());
                                } else if let Some(rt) = self.inferred_returns.get(&node) {
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: format!("base type (inferred) -> {rt}"),
                                        }
                                    );
                                    types.push(rt.clone());
                                } else {
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!(
                                                "LocalDef({i}) = {fqn} ({:?})",
                                                gdef.kind
                                            ),
                                            result: "no type info".to_string(),
                                        }
                                    );
                                }
                            }
                        }
                        ParseValue::ImportRef(i) => {
                            if let Some(&import_node) = self.import_nodes.get(*i as usize) {
                                let before = types.len();
                                let resolved = self.resolve_import_cached(import_node);
                                for def_idx in resolved {
                                    if let Some(did) = self.graph.graph[def_idx].def_id() {
                                        let gdef = &self.graph.defs[did.0 as usize];
                                        let fqn = self.graph.str(gdef.fqn);
                                        if gdef.kind.is_type_container() {
                                            trace!(
                                                self.tracer,
                                                ReachingDefResolved {
                                                    value: format!(
                                                        "ImportRef({i}) -> {fqn} ({:?})",
                                                        gdef.kind
                                                    ),
                                                    result: format!(
                                                        "base type (container) -> {fqn}"
                                                    ),
                                                }
                                            );
                                            types.push(fqn.to_string());
                                        } else if let Some(meta) = &gdef.metadata
                                            && let Some(rt) = meta.return_type
                                        {
                                            let rt_str = self.graph.str(rt);
                                            trace!(
                                                self.tracer,
                                                ReachingDefResolved {
                                                    value: format!(
                                                        "ImportRef({i}) -> {fqn} ({:?})",
                                                        gdef.kind
                                                    ),
                                                    result: format!(
                                                        "base type (return_type) -> {rt_str}"
                                                    ),
                                                }
                                            );
                                            types.push(rt_str.to_string());
                                        } else if let Some(rt) = self.inferred_returns.get(&def_idx)
                                        {
                                            trace!(
                                                self.tracer,
                                                ReachingDefResolved {
                                                    value: format!(
                                                        "ImportRef({i}) -> {fqn} ({:?})",
                                                        gdef.kind
                                                    ),
                                                    result: format!("base type (inferred) -> {rt}"),
                                                }
                                            );
                                            types.push(rt.clone());
                                        }
                                    }
                                }
                                if types.len() == before
                                    && let Some(import_type) =
                                        self.imported_symbol_type_fqn(import_node)
                                {
                                    trace!(
                                        self.tracer,
                                        ReachingDefResolved {
                                            value: format!("ImportRef({i})"),
                                            result: format!(
                                                "base type (imported symbol) -> {import_type}"
                                            ),
                                        }
                                    );
                                    types.push(import_type);
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
                    let mut nodes = self.resolve_bare(&fallback)?;
                    // GlobalName: last-resort lookup for chain bases only.
                    // Not in import_strategies to avoid O(candidates) scans
                    // on every bare identifier ref.
                    if nodes.is_empty() {
                        nodes = self.import_resolver().global_name(name);
                    }
                    for n in nodes {
                        if let Some(did) = self.graph.graph[n].def_id() {
                            let gdef = &self.graph.defs[did.0 as usize];
                            if gdef.kind.is_type_container() {
                                let fqn = self.graph.str(gdef.fqn).to_string();
                                trace!(
                                    self.tracer,
                                    ReachingDefResolved {
                                        value: format!("bare({name})"),
                                        result: format!("base type (import fallback) -> {fqn}"),
                                    }
                                );
                                types.push(fqn);
                            }
                        }
                    }
                }
                Ok(types)
            }
            ExpressionStep::Super => Ok(reaching
                .iter()
                .filter_map(|v| match v {
                    ParseValue::Type(fqn) => Some(fqn.to_string()),
                    _ => None,
                })
                .collect()),
            ExpressionStep::New(type_name) => {
                let fqn_matches = self.graph.indexes.by_fqn.lookup(type_name, |idx| {
                    self.graph.graph[idx].def_id().is_some_and(|d| {
                        self.graph.str(self.graph.defs[d.0 as usize].fqn) == *type_name
                    })
                });
                if !fqn_matches.is_empty() {
                    return Ok(vec![type_name.to_string()]);
                }
                let name_matches = self.graph.indexes.by_name.lookup(type_name, |idx| {
                    self.graph.graph[idx].def_id().is_some_and(|d| {
                        self.graph.str(self.graph.defs[d.0 as usize].name) == *type_name
                    })
                });
                Ok(name_matches
                    .iter()
                    .filter_map(|&idx| {
                        self.graph.graph[idx].def_id().map(|d| {
                            self.graph
                                .str(self.graph.defs[d.0 as usize].fqn)
                                .to_string()
                        })
                    })
                    .collect())
            }
            _ => Ok(vec![]),
        }
    }

    fn imported_symbol_type_fqn(&self, import_node: NodeIndex) -> Option<String> {
        if let Some(hook) = self.rules.hooks.external_import_type {
            return hook(self.graph, import_node);
        }

        let imp = self.graph.import(import_node);
        if imp.wildcard || matches!(imp.binding_kind, ImportBindingKind::SideEffect) {
            return None;
        }

        let path = self.graph.str(imp.path);
        let name = imp.name.map(|id| self.graph.str(id));
        match (path.is_empty(), name) {
            (true, Some(name)) => Some(name.to_string()),
            (true, None) => imp.alias.map(|id| self.graph.str(id).to_string()),
            (false, Some(name)) => Some(format!("{}{name}", self.scope_member_prefix(path))),
            (false, None) => Some(path.to_string()),
        }
    }

    fn scope_member_prefix(&self, scope_fqn: &str) -> String {
        format!("{}{}", scope_fqn, self.rules.fqn_separator)
    }

    fn resolve_implicit_member(&self, enclosing_node: NodeIndex, name: &str) -> Vec<NodeIndex> {
        let sep = self.graph.sep();
        let mut result = Vec::new();
        if let Some(did) = self.graph.graph[enclosing_node].def_id() {
            let gdef = &self.graph.defs[did.0 as usize];
            let fqn = self.graph.str(gdef.fqn);
            let mut scope = fqn;
            loop {
                self.graph.indexes.nested.lookup_into(
                    scope,
                    name,
                    |idx| {
                        self.graph.graph[idx].def_id().is_some_and(|d| {
                            self.graph.str(self.graph.defs[d.0 as usize].name) == name
                        })
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
}

// ── Utility functions ───────────────────────────────────────────

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

fn pre_resolve_imports(
    graph: &CodeGraph,
    import_nodes: &[NodeIndex],
) -> FxHashMap<String, Vec<NodeIndex>> {
    let sep = graph.sep();
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
