//! Fused walker: single AST walk combining scope tracking, SSA
//! construction (Braun et al.), and reference resolution.
//!
//! Phase 2 of the two-phase architecture:
//!   Phase 1: parse + extract defs/imports → add to graph (parallel)
//!   Phase 2: re-parse + fused walk+resolve (parallel, this module)
//!
//! Eliminates: CanonicalResult refs/bindings/cf, FileWalkResult,
//! RecordedRead. Resolution happens inline during the AST walk.

use code_graph_types::{BindingKind, EdgeKind, ExpressionStep, IStr, NodeKind, Relationship};
use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;
use smallvec::{SmallVec, smallvec};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::graph::{CodeGraph, GraphEdge, GraphNode};
use super::resolve::{ResolveSettings, apply_import_strategies, resolve_import};
use super::rules::ResolutionRules;
use super::ssa::{BlockId, SsaResolver, Value};
use super::stats::ResolveStats;

const MIN_STACK_REMAINING: usize = 128 * 1024;

/// Result of a fused walk+resolve for a single file.
pub struct FusedWalkResult {
    pub edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    pub stats: ResolveStats,
    pub num_refs: usize,
}

/// Run Phase 2 on a single file: fused walk + inline resolve.
///
/// The graph must be finalized (defs indexed, extends linked,
/// ancestors pre-computed). Read-only during this phase.
pub fn fused_walk_file(
    rules: &ResolutionRules,
    graph: &CodeGraph,
    root: &Node<StrDoc<SupportLang>>,
    file_node: NodeIndex,
    settings: &ResolveSettings,
) -> FusedWalkResult {
    let spec = rules
        .language_spec
        .as_ref()
        .expect("fused walk requires LanguageSpec");
    let sep = rules.fqn_separator;

    let mut ssa = SsaResolver::new();
    let module_block = ssa.add_block();
    ssa.seal_block(module_block);

    // Build import name→FQN map for chain expression building.
    let mut import_name_map: FxHashMap<String, String> = FxHashMap::default();

    // Initialize SSA with all defs and imports from graph neighbors.
    // This handles forward references: all defs visible from module block.
    let mut defs_by_byte: FxHashMap<usize, (NodeIndex, bool)> = FxHashMap::default();

    for neighbor in graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
    {
        match &graph.graph[neighbor] {
            GraphNode::Definition { id, .. } => {
                let def = &graph.defs[id.0 as usize];
                ssa.write_variable(&def.name, module_block, Value::Def(neighbor));
                defs_by_byte.insert(
                    def.range.byte_offset.0,
                    (neighbor, def.kind.is_type_container()),
                );
            }
            GraphNode::Import { id, .. } => {
                let import = &graph.imports[id.0 as usize];
                if !import.wildcard {
                    let name = import
                        .alias
                        .as_deref()
                        .or(import.name.as_deref())
                        .unwrap_or("");
                    if !name.is_empty() {
                        ssa.write_variable(name, module_block, Value::Import(neighbor));
                        if !import.path.is_empty() {
                            import_name_map
                                .insert(name.to_string(), format!("{}{sep}{name}", import.path));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let import_map = graph.pre_resolve_file_imports(file_node, sep);

    let mut walker = FusedFileWalker {
        spec,
        rules,
        graph,
        ssa: &mut ssa,
        file_node,
        settings,
        import_map: &import_map,
        import_name_map: &import_name_map,
        defs_by_byte: &defs_by_byte,
        current_block: module_block,
        scope_stack: vec![ScopeEntry {
            block: module_block,
            is_type_scope: false,
            name: None,
            def_node: None,
            enclosing_scope_fqn: None,
        }],
        edges: Vec::new(),
        stats: ResolveStats::default(),
        num_refs: 0,
        import_cache: FxHashMap::default(),
        nested_cache: FxHashMap::default(),
        buf: String::with_capacity(128),
        sep,
        last_bare_path: ResolvePath::None,
        last_chain_path: ResolvePath::None,
    };

    walker.walk_node(root);
    walker.ssa.seal_remaining();
    walker.stats.ssa.merge(&walker.ssa.stats);

    FusedWalkResult {
        edges: walker.edges,
        stats: walker.stats,
        num_refs: walker.num_refs,
    }
}

// ── Scope stack entry ───────────────────────────────────────────

struct ScopeEntry {
    block: BlockId,
    is_type_scope: bool,
    name: Option<String>,
    def_node: Option<NodeIndex>,
    enclosing_scope_fqn: Option<IStr>,
}

// ── Fused walker ────────────────────────────────────────────────

struct FusedFileWalker<'a> {
    spec: &'a parser_core::dsl::types::LanguageSpec,
    rules: &'a ResolutionRules,
    graph: &'a CodeGraph,
    ssa: &'a mut SsaResolver,
    file_node: NodeIndex,
    settings: &'a ResolveSettings,
    import_map: &'a FxHashMap<String, Vec<NodeIndex>>,
    import_name_map: &'a FxHashMap<String, String>,
    defs_by_byte: &'a FxHashMap<usize, (NodeIndex, bool)>,

    current_block: BlockId,
    scope_stack: Vec<ScopeEntry>,
    edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    stats: ResolveStats,
    num_refs: usize,

    import_cache: FxHashMap<NodeIndex, Vec<NodeIndex>>,
    nested_cache: FxHashMap<(String, String), Vec<NodeIndex>>,
    buf: String,
    sep: &'static str,
    last_bare_path: ResolvePath,
    last_chain_path: ResolvePath,
}

#[derive(Debug, Clone, Copy)]
enum ResolvePath {
    None,
    BareSsa,
    BareImport,
    BareImplicit,
    Chain,
    ChainFallback,
}

impl<'a> FusedFileWalker<'a> {
    // ── AST walk ────────────────────────────────────────────

    fn walk_node(&mut self, node: &Node<StrDoc<SupportLang>>) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < MIN_STACK_REMAINING {
            return;
        }

        let node_kind = node.kind();
        let node_kind_ref = node_kind.as_ref();

        // 1. Scope matching
        if let Some(info) =
            self.spec
                .match_scope(node, node_kind_ref, self.import_name_map, self.sep)
        {
            self.enter_scope(node, &info.name, info.creates_scope, info.is_type_scope);
            self.walk_children(node);
            if info.creates_scope {
                self.exit_scope();
            }
            return;
        }

        // 2. Branch matching → SSA blocks
        if let Some(&rule_idx) = self
            .spec
            .branch_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
        {
            self.walk_branch(node, rule_idx);
            return;
        }

        // 3. Loop matching → SSA blocks
        if let Some(&rule_idx) = self
            .spec
            .loop_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
        {
            self.walk_loop(node, rule_idx);
            return;
        }

        // 4. Binding matching → SSA writeVariable
        if let Some(&rule_idx) = self
            .spec
            .binding_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
        {
            self.handle_binding(node, rule_idx);
        }

        // 5. Reference matching → resolve inline
        if let Some((name, expression)) =
            self.spec
                .match_reference(node, node_kind_ref, self.import_name_map, self.sep)
        {
            self.num_refs += 1;
            self.resolve_reference_inline(&name, expression.as_deref());
        }

        self.walk_children(node);
    }

    fn walk_children(&mut self, node: &Node<StrDoc<SupportLang>>) {
        for child in node.children() {
            self.walk_node(&child);
        }
    }

    // ── Scope enter/exit ────────────────────────────────────

    fn enter_scope(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        name: &str,
        creates_scope: bool,
        is_type_scope: bool,
    ) {
        if !creates_scope {
            return;
        }

        let new_block = self.ssa.add_block();
        self.ssa.add_predecessor(new_block, self.current_block);
        self.ssa.seal_block(new_block);

        // Match this scope to its graph definition by byte offset.
        let byte_start = node.range().start;
        let def_info = self.defs_by_byte.get(&byte_start).copied();
        let def_node = def_info.map(|(idx, _)| idx);

        // Compute enclosing_scope_fqn for this scope.
        let enclosing_scope_fqn = if is_type_scope {
            if let Some((idx, _)) = def_info {
                Some(self.graph.def_fqn(idx))
            } else {
                self.scope_stack.last().and_then(|e| e.enclosing_scope_fqn)
            }
        } else {
            self.scope_stack.last().and_then(|e| e.enclosing_scope_fqn)
        };

        // Write self/super SSA variables for type scopes
        if is_type_scope {
            let scope_fqn = if let Some((idx, _)) = def_info {
                self.graph.def(idx).fqn.to_string()
            } else {
                self.build_fqn(name)
            };
            for &self_name in self.rules.self_names {
                self.ssa
                    .write_variable(self_name, new_block, Value::type_of(&scope_fqn));
            }
            if let Some(super_name) = self.rules.super_name
                && let Some(super_type) = self.find_super_type(name)
            {
                self.ssa
                    .write_variable(super_name, new_block, Value::type_of(&super_type));
            }
        }

        self.scope_stack.push(ScopeEntry {
            block: new_block,
            is_type_scope,
            name: Some(name.to_string()),
            def_node,
            enclosing_scope_fqn,
        });
        self.current_block = new_block;
    }

    fn exit_scope(&mut self) {
        if self.scope_stack.pop().is_some()
            && let Some(parent) = self.scope_stack.last()
        {
            self.current_block = parent.block;
        }
    }

    fn build_fqn(&self, name: &str) -> String {
        let mut parts: Vec<&str> = self
            .scope_stack
            .iter()
            .filter_map(|e| e.name.as_deref())
            .collect();
        parts.push(name);
        parts.join(self.sep)
    }

    fn find_super_type(&self, class_name: &str) -> Option<String> {
        // Look up the def by name in graph neighbors
        for neighbor in self
            .graph
            .graph
            .neighbors_directed(self.file_node, petgraph::Direction::Outgoing)
        {
            if let GraphNode::Definition { id, .. } = &self.graph.graph[neighbor] {
                let def = &self.graph.defs[id.0 as usize];
                if def.name == class_name
                    && let Some(meta) = &def.metadata
                    && let Some(st) = meta.super_types.first()
                {
                    return Some(st.clone());
                }
            }
        }
        None
    }

    // ── Branch handling (Braun et al. §2.3 Fig 3b) ─────────

    fn walk_branch(&mut self, node: &Node<StrDoc<SupportLang>>, rule_idx: usize) {
        let rule = &self.spec.branches[rule_idx];
        let pre_block = self.current_block;

        // Walk condition in current block
        if let Some(cond_field) = rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_node(&cond_node);
        }

        let has_catch_all = rule
            .catch_all_kind
            .is_some_and(|ck| node.children().any(|c| c.kind().as_ref() == ck));

        // Identify condition ranges to skip
        let cond_range = rule
            .condition_field
            .and_then(|f| node.field(f))
            .map(|n| (n.range().start, n.range().end));

        let mut branch_blocks = Vec::new();
        for child in node.children() {
            let ck = child.kind();
            if rule.branch_kinds.iter().any(|&k| k == ck.as_ref()) {
                let branch_block = self.ssa.add_block();
                self.ssa.add_predecessor(branch_block, pre_block);
                self.ssa.seal_block(branch_block);

                self.current_block = branch_block;
                self.walk_node(&child);
                branch_blocks.push(self.current_block);
            } else {
                let cs = child.range().start;
                let ce = child.range().end;
                let is_condition = cond_range.is_some_and(|(s, e)| cs >= s && ce <= e);
                if !is_condition {
                    self.current_block = pre_block;
                    self.walk_node(&child);
                }
            }
        }

        let join_block = self.ssa.add_block();
        for &bb in &branch_blocks {
            self.ssa.add_predecessor(join_block, bb);
        }
        if !has_catch_all {
            self.ssa.add_predecessor(join_block, pre_block);
        }
        self.ssa.seal_block(join_block);
        self.current_block = join_block;
    }

    // ── Loop handling (Braun et al. §2.3 Fig 3a) ───────────

    fn walk_loop(&mut self, node: &Node<StrDoc<SupportLang>>, rule_idx: usize) {
        let rule = &self.spec.loops[rule_idx];
        let pre_block = self.current_block;

        // Walk iteration expression in pre_block
        if let Some(iter_field) = rule.iter_field
            && let Some(iter_node) = node.field(iter_field)
        {
            self.walk_node(&iter_node);
        }

        // Loop header — DON'T seal (back-edge coming)
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, pre_block);

        let body_block = self.ssa.add_block();
        self.ssa.add_predecessor(body_block, header);
        self.ssa.seal_block(body_block);
        self.current_block = body_block;

        // Walk body
        if let Some(body_node) = node.field(rule.body_field) {
            self.walk_node(&body_node);
        } else {
            self.walk_children(node);
        }

        // Back-edge + seal header
        self.ssa.add_predecessor(header, self.current_block);
        self.ssa.seal_block(header);

        let exit_block = self.ssa.add_block();
        self.ssa.add_predecessor(exit_block, header);
        self.ssa.seal_block(exit_block);
        self.current_block = exit_block;
    }

    // ── Binding handling ────────────────────────────────────

    fn handle_binding(&mut self, node: &Node<StrDoc<SupportLang>>, rule_idx: usize) {
        let rule = &self.spec.bindings[rule_idx];
        let Some(name) = rule.extract_name(node) else {
            return;
        };

        let value = match rule.binding_kind {
            BindingKind::Deletion | BindingKind::ForTarget => Value::Opaque,
            BindingKind::Parameter | BindingKind::Assignment | BindingKind::WithAlias => {
                if let Some(ref type_ann) = rule.extract_type_annotation(node) {
                    Value::type_of(type_ann)
                } else if let Some(ref rhs) = rule.extract_rhs_name(node, self.spec) {
                    Value::Alias(IStr::from(rhs.as_str()))
                } else {
                    Value::Opaque
                }
            }
        };

        let target_block = if rule
            .instance_attr_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            self.enclosing_class_block().unwrap_or(self.current_block)
        } else {
            self.current_block
        };
        self.ssa.write_variable(&name, target_block, value);
    }

    // ── Inline reference resolution ─────────────────────────

    fn resolve_reference_inline(&mut self, name: &str, expression: Option<&[ExpressionStep]>) {
        let enclosing_def = self.innermost_def();
        let enclosing_scope_fqn = self.scope_stack.last().and_then(|e| e.enclosing_scope_fqn);

        let (resolved_defs, path) = if let Some(chain) = expression {
            self.stats.chain_refs += 1;
            let defs = self.resolve_chain(chain, enclosing_scope_fqn);
            (defs, self.last_chain_path)
        } else {
            self.stats.bare_refs += 1;
            let iname = IStr::from(name);
            let defs = self.resolve_bare(&iname, enclosing_scope_fqn);
            (defs, self.last_bare_path)
        };

        let (source_idx, source_node_kind, source_def_kind) = match enclosing_def {
            Some(def_node) => {
                let def = self.graph.def(def_node);
                (def_node, NodeKind::Definition, Some(def.kind))
            }
            None => (self.file_node, NodeKind::File, None),
        };

        let edge_count = resolved_defs.len() as u64;
        for target_idx in resolved_defs {
            let target_def = self.graph.def(target_idx);
            self.edges.push((
                source_idx,
                target_idx,
                GraphEdge {
                    relationship: Relationship {
                        edge_kind: EdgeKind::Calls,
                        source_node: source_node_kind,
                        target_node: NodeKind::Definition,
                        source_def_kind,
                        target_def_kind: Some(target_def.kind),
                    },
                },
            ));
        }

        match path {
            ResolvePath::BareSsa => self.stats.edges_from_bare_ssa += edge_count,
            ResolvePath::BareImport => self.stats.edges_from_bare_import += edge_count,
            ResolvePath::BareImplicit => self.stats.edges_from_bare_implicit += edge_count,
            ResolvePath::Chain => self.stats.edges_from_chain += edge_count,
            ResolvePath::ChainFallback => self.stats.edges_from_chain_fallback += edge_count,
            ResolvePath::None => {}
        }
    }

    // ── Bare resolution (SSA → import strategies → implicit) ─
    fn resolve_bare(&mut self, name: &IStr, enclosing_scope_fqn: Option<IStr>) -> Vec<NodeIndex> {
        use super::rules::ResolveStage;
        self.last_bare_path = ResolvePath::None;

        for stage in &self.rules.bare_stages {
            let result = match stage {
                ResolveStage::SSA => self.resolve_bare_ssa(name),
                ResolveStage::ImportStrategies => {
                    if self.graph.lookup_name(name).is_empty() {
                        self.stats.bare_early_exit_unknown += 1;
                        continue;
                    }
                    let r = apply_import_strategies(
                        &self.rules.import_strategies,
                        self.graph,
                        self.file_node,
                        name,
                        self.sep,
                        self.import_map,
                    );
                    if !r.is_empty() {
                        self.stats.bare_import_resolved += 1;
                        self.last_bare_path = ResolvePath::BareImport;
                    }
                    r
                }
                ResolveStage::ImplicitMember => {
                    let mut r = Vec::new();
                    if let Some(type_fqn) = &enclosing_scope_fqn
                        && self.lookup_nested_cached(type_fqn, name, &mut r)
                    {
                        self.stats.bare_implicit_scope_resolved += 1;
                        self.last_bare_path = ResolvePath::BareImplicit;
                    }
                    r
                }
            };

            if !result.is_empty() {
                let mut result = result;
                dedup(&mut result);
                return result;
            }
        }

        self.stats.bare_unresolved += 1;
        vec![]
    }

    fn resolve_bare_ssa(&mut self, name: &IStr) -> Vec<NodeIndex> {
        let reaching = self.ssa.read_variable_stateless(name, self.current_block);
        let mut result = Vec::new();

        for value in &reaching.values {
            match value {
                Value::Def(idx) => {
                    self.stats.bare_ssa_def += 1;
                    result.push(*idx);
                }
                Value::Import(idx) => {
                    self.stats.bare_ssa_import += 1;
                    result.extend(self.resolve_import_cached(*idx));
                }
                Value::Type(type_name) => {
                    self.stats.bare_ssa_type += 1;
                    self.lookup_nested_cached(type_name, name, &mut result);
                }
                Value::Alias(alias_name) => {
                    let alias_reaching = self
                        .ssa
                        .read_variable_stateless(alias_name, self.current_block);
                    for av in &alias_reaching.values {
                        match av {
                            Value::Def(idx) => result.push(*idx),
                            Value::Import(idx) => {
                                result.extend(self.resolve_import_cached(*idx));
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if !result.is_empty() {
            self.stats.bare_ssa_resolved += 1;
            self.last_bare_path = ResolvePath::BareSsa;
        }
        result
    }

    // ── Chain resolution ────────────────────────────────────

    fn resolve_chain(
        &mut self,
        chain: &[ExpressionStep],
        enclosing_scope_fqn: Option<IStr>,
    ) -> Vec<NodeIndex> {
        self.last_chain_path = ResolvePath::None;
        if chain.is_empty() {
            return vec![];
        }

        let max_depth = self.settings.max_chain_depth;
        let effective_chain = if chain.len() > max_depth {
            &chain[chain.len() - max_depth..]
        } else {
            chain
        };

        match &effective_chain[0] {
            ExpressionStep::Ident(_) => self.stats.chain_base_ident += 1,
            ExpressionStep::This => self.stats.chain_base_this += 1,
            ExpressionStep::Super => self.stats.chain_base_super += 1,
            ExpressionStep::New(_) => self.stats.chain_base_new += 1,
            _ => self.stats.chain_base_other += 1,
        }

        let enclosing_str = enclosing_scope_fqn.as_ref().map(|s| s.as_ref());
        let mut current_types = self.resolve_base(&effective_chain[0], enclosing_str);

        if current_types.is_empty() {
            if self.settings.chain_fallback {
                self.stats.chain_fallback_fired += 1;
                let result = self.chain_fallback(chain, enclosing_scope_fqn);
                if !result.is_empty() {
                    self.stats.chain_fallback_resolved += 1;
                    self.last_chain_path = ResolvePath::ChainFallback;
                }
                return result;
            }
            return vec![];
        }

        let mut compound_key = if self.settings.compound_key_recovery {
            self.compound_key_base(&effective_chain[0])
        } else {
            String::new()
        };

        for (i, step) in effective_chain[1..].iter().enumerate() {
            let is_last = i == effective_chain.len() - 2;
            let member_name = match step {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => n,
                _ => continue,
            };

            let (mut next_types, found_nested) = self.walk_step(&current_types, step, member_name);

            if is_last && !found_nested.is_empty() {
                self.stats.chain_resolved += 1;
                self.last_chain_path = ResolvePath::Chain;
                let mut result = found_nested;
                dedup(&mut result);
                return result;
            }

            if next_types.is_empty() && found_nested.is_empty() {
                let recovered = self.compound_key_step(&mut compound_key, member_name);
                if !recovered.is_empty() {
                    self.stats.chain_compound_key_recovered += 1;
                    current_types = recovered;
                    continue;
                }
            } else {
                compound_key.clear();
            }

            {
                let mut seen = rustc_hash::FxHashSet::default();
                next_types.retain(|t| seen.insert(*t));
            }
            current_types = next_types;
            if current_types.is_empty() {
                self.stats.chain_mid_break += 1;
                break;
            }
        }
        vec![]
    }

    fn resolve_base(
        &mut self,
        step: &ExpressionStep,
        enclosing: Option<&str>,
    ) -> SmallVec<[IStr; 2]> {
        match step {
            ExpressionStep::Ident(name) | ExpressionStep::Call(name) => {
                let reaching = self.ssa.read_variable_stateless(name, self.current_block);
                let values = self.resolve_aliases(&reaching.values);
                let mut types: SmallVec<[IStr; 2]> =
                    values.iter().flat_map(|v| self.value_types(v)).collect();

                if types.is_empty()
                    && self.settings.implicit_scope_on_base
                    && self
                        .rules
                        .bare_stages
                        .contains(&super::rules::ResolveStage::ImplicitMember)
                    && let Some(fqn) = enclosing
                {
                    let mut members = Vec::new();
                    self.lookup_nested_cached(fqn, name, &mut members);
                    for &def_idx in &members {
                        let def = self.graph.def(def_idx);
                        types.extend(self.def_types(def));
                    }
                }
                types
            }
            ExpressionStep::This => enclosing
                .map(|fqn| smallvec![IStr::from(fqn)])
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|name| {
                    let reaching = self.ssa.read_variable_stateless(name, self.current_block);
                    reaching
                        .values
                        .iter()
                        .filter_map(|v| match v {
                            Value::Type(t) => Some(*t),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ExpressionStep::New(type_name) => smallvec![IStr::from(type_name.as_ref())],
            _ => SmallVec::new(),
        }
    }

    fn walk_step(
        &mut self,
        current_types: &[IStr],
        step: &ExpressionStep,
        member_name: &str,
    ) -> (SmallVec<[IStr; 2]>, Vec<NodeIndex>) {
        let mut next_types = SmallVec::new();
        let mut found_nested = Vec::new();

        for type_name in current_types {
            let before = found_nested.len();
            self.lookup_nested_cached(type_name, member_name, &mut found_nested);
            for &def_idx in &found_nested[before..] {
                let def = self.graph.def(def_idx);
                if matches!(step, ExpressionStep::Call(_)) {
                    if let Some(meta) = &def.metadata
                        && let Some(rt) = &meta.return_type
                    {
                        next_types.push(IStr::from(rt.as_str()));
                    }
                    if matches!(
                        def.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Constructor
                    ) {
                        next_types.push(def.fqn.as_istr());
                    }
                }
                if matches!(step, ExpressionStep::Field(_))
                    && let Some(meta) = &def.metadata
                    && let Some(ta) = &meta.type_annotation
                {
                    next_types.push(IStr::from(ta.as_str()));
                }
            }
        }
        (next_types, found_nested)
    }

    fn compound_key_base(&self, step: &ExpressionStep) -> String {
        match step {
            ExpressionStep::Ident(n) => n.clone(),
            ExpressionStep::This => self
                .rules
                .self_names
                .first()
                .map(|s| s.to_string())
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|s| s.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }

    fn compound_key_step(
        &mut self,
        compound_key: &mut String,
        member_name: &str,
    ) -> SmallVec<[IStr; 2]> {
        if compound_key.is_empty() {
            return SmallVec::new();
        }
        self.buf.clear();
        self.buf.push_str(compound_key);
        self.buf.push_str(self.sep);
        self.buf.push_str(member_name);
        std::mem::swap(compound_key, &mut self.buf);
        let reaching = self
            .ssa
            .read_variable_stateless(compound_key, self.current_block);
        reaching
            .values
            .iter()
            .flat_map(|v| self.value_types(v))
            .collect()
    }

    fn chain_fallback(
        &mut self,
        chain: &[ExpressionStep],
        enclosing_scope_fqn: Option<IStr>,
    ) -> Vec<NodeIndex> {
        let last = match chain.last() {
            Some(ExpressionStep::Call(n) | ExpressionStep::Field(n)) => n,
            _ => return vec![],
        };
        let iname = IStr::from(last.as_str());
        self.resolve_bare(&iname, enclosing_scope_fqn)
    }

    // ── Resolution helpers ──────────────────────────────────

    fn resolve_import_cached(&mut self, import_idx: NodeIndex) -> Vec<NodeIndex> {
        if let Some(cached) = self.import_cache.get(&import_idx) {
            return cached.clone();
        }
        let result = resolve_import(self.graph, import_idx, self.sep);
        self.import_cache.insert(import_idx, result.clone());
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

    fn def_types(&self, def: &code_graph_types::CanonicalDefinition) -> SmallVec<[IStr; 2]> {
        if def.kind.is_type_container() {
            smallvec![def.fqn.as_istr()]
        } else if let Some(meta) = &def.metadata
            && let Some(rt) = &meta.return_type
        {
            smallvec![IStr::from(rt.as_str())]
        } else {
            SmallVec::new()
        }
    }

    fn resolve_aliases(&mut self, values: &[Value]) -> SmallVec<[Value; 4]> {
        let mut out = SmallVec::new();
        for v in values {
            match v {
                Value::Alias(name) => {
                    let reaching = self.ssa.read_variable_stateless(name, self.current_block);
                    for av in &reaching.values {
                        out.push(av.clone());
                    }
                }
                other => out.push(other.clone()),
            }
        }
        out
    }

    fn value_types(&mut self, value: &Value) -> SmallVec<[IStr; 2]> {
        match value {
            Value::Type(t) => smallvec![*t],
            Value::Def(idx) => {
                let def = self.graph.def(*idx);
                self.def_types(def)
            }
            Value::Import(idx) => {
                let defs = self.resolve_import_cached(*idx);
                defs.iter()
                    .flat_map(|&di| {
                        let def = self.graph.def(di);
                        self.def_types(def)
                    })
                    .collect()
            }
            _ => SmallVec::new(),
        }
    }

    fn innermost_def(&self) -> Option<NodeIndex> {
        self.scope_stack.iter().rev().find_map(|e| e.def_node)
    }

    fn enclosing_class_block(&self) -> Option<BlockId> {
        self.scope_stack
            .iter()
            .rev()
            .find(|e| e.is_type_scope)
            .map(|e| e.block)
    }
}

fn dedup(result: &mut Vec<NodeIndex>) {
    if result.len() <= 4 {
        let mut i = 0;
        while i < result.len() {
            let mut j = i + 1;
            while j < result.len() {
                if result[j] == result[i] {
                    result.swap_remove(j);
                } else {
                    j += 1;
                }
            }
            i += 1;
        }
    } else {
        let mut seen = rustc_hash::FxHashSet::default();
        result.retain(|r| seen.insert(*r));
    }
}
