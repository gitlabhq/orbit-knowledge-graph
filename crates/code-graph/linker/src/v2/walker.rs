//! SSA walker that interprets `ResolutionRules` and drives the `SsaResolver`.
//!
//! Walks each file's tree-sitter AST, creating SSA blocks for scopes and
//! control flow per Braun et al. (CC 2013). Writes variable definitions,
//! processes bindings, and records reference reads. The output is a
//! populated `SsaResolver` with all reaching definitions computed.

use code_graph_types::{
    BindingKind, CanonicalBinding, CanonicalControlFlow, CanonicalResult, ControlFlowKind, IStr,
};
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use petgraph::graph::NodeIndex;

use super::rules::ResolutionRules;
use super::ssa::{BlockId, SsaResolver, Value};

/// Trait for AST types that can provide a tree-sitter root for walking.
pub trait HasRoot {
    fn as_root(&self) -> Option<Node<'_, StrDoc<SupportLang>>>;
}

impl HasRoot for treesitter_visit::Root<StrDoc<SupportLang>> {
    fn as_root(&self) -> Option<Node<'_, StrDoc<SupportLang>>> {
        Some(self.root())
    }
}

/// Minimum remaining stack space (bytes) before the walker stops recursing.
/// Prevents stack overflow on deeply nested ASTs.
const MIN_STACK_REMAINING: usize = 128 * 1024;

/// A recorded reference read, linking back to the canonical data.
#[derive(Debug, Clone)]
pub struct RecordedRead {
    pub file_idx: usize,
    pub ref_idx: usize,
    pub block: BlockId,
    /// Interned reference name — avoids 2.2M String clones on elasticsearch.
    pub name: IStr,
    /// Pre-computed enclosing definition (for edge source). None = file-level.
    pub enclosing_def: Option<NodeIndex>,
    /// Pre-computed enclosing type scope FQN (for implicit this / This chains).
    pub enclosing_type_fqn: Option<IStr>,
}

/// Per-file walk result: owned SSA state + recorded reads.
/// AST is dropped after walking, only this survives.
pub struct FileWalkResult {
    pub ssa: SsaResolver,
    pub reads: Vec<RecordedRead>,
}

impl FileWalkResult {
    /// Empty result for files without an AST (custom pipelines, parse failures).
    pub fn empty() -> Self {
        Self {
            ssa: SsaResolver::new(),
            reads: Vec::new(),
        }
    }
}

/// Walk a single file's AST and build its SSA graph.
///
/// Called in the parallel phase (one per file). The AST can be dropped
/// after this returns -- only the SSA state and reads are needed for
/// resolution.
pub fn walk_file(
    rules: &ResolutionRules,
    file_idx: usize,
    result: &CanonicalResult,
    root: &Node<StrDoc<SupportLang>>,
    def_nodes: &[NodeIndex],
    import_nodes: &[NodeIndex],
) -> FileWalkResult {
    let mut ssa = SsaResolver::new();
    let mut walker = FileWalker::new(rules, &mut ssa, file_idx, result, def_nodes, import_nodes);
    walker.walk_node(root);
    let reads = walker.reads;
    ssa.seal_remaining();
    FileWalkResult { ssa, reads }
}

// ── AST walker (Braun et al.) ───────────────────────────────────

/// An entry on the scope stack, tracking the block, kind, and name.
struct ScopeEntry {
    block: BlockId,
    is_type_scope: bool,
    name: Option<String>,
    def_node: Option<NodeIndex>,
    enclosing_type_fqn: Option<IStr>,
}

struct FileWalker<'a> {
    rules: &'a ResolutionRules,
    ssa: &'a mut SsaResolver,
    file_idx: usize,
    result: &'a CanonicalResult,
    def_nodes: &'a [NodeIndex],
    #[allow(dead_code)]
    import_nodes: &'a [NodeIndex],

    current_block: BlockId,
    scope_stack: Vec<ScopeEntry>,
    ref_by_range_start: FxHashMap<usize, Vec<usize>>,
    def_by_byte_start: FxHashMap<usize, usize>,
    binding_by_byte_start: FxHashMap<usize, usize>,
    cf_by_byte_start: FxHashMap<usize, usize>,

    reads: Vec<RecordedRead>,
}

impl<'a> FileWalker<'a> {
    fn new(
        rules: &'a ResolutionRules,
        ssa: &'a mut SsaResolver,
        file_idx: usize,
        result: &'a CanonicalResult,
        def_nodes: &'a [NodeIndex],
        import_nodes: &'a [NodeIndex],
    ) -> Self {
        let module_block = ssa.add_block();
        ssa.seal_block(module_block);

        for (di, def) in result.definitions.iter().enumerate() {
            ssa.write_variable(&def.name, module_block, Value::Def(def_nodes[di]));
        }

        for (ii, imp) in result.imports.iter().enumerate() {
            if imp.wildcard {
                continue;
            }
            let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
            if !name.is_empty() {
                ssa.write_variable(name, module_block, Value::Import(import_nodes[ii]));
            }
        }

        // Index canonical references by byte offset for matching
        let mut ref_by_range_start: FxHashMap<usize, Vec<usize>> = FxHashMap::default();
        for (idx, r) in result.references.iter().enumerate() {
            ref_by_range_start
                .entry(r.range.byte_offset.0)
                .or_default()
                .push(idx);
        }

        // Index canonical definitions by byte offset for scope matching
        let mut def_by_byte_start: FxHashMap<usize, usize> = FxHashMap::default();
        for (idx, d) in result.definitions.iter().enumerate() {
            def_by_byte_start.insert(d.range.byte_offset.0, idx);
        }

        // Index parsed bindings by byte offset
        let mut binding_by_byte_start: FxHashMap<usize, usize> = FxHashMap::default();
        for (idx, b) in result.bindings.iter().enumerate() {
            binding_by_byte_start.insert(b.range.byte_offset.0, idx);
        }

        // Index parsed control flow by byte offset
        let mut cf_by_byte_start: FxHashMap<usize, usize> = FxHashMap::default();
        for (idx, cf) in result.control_flow.iter().enumerate() {
            cf_by_byte_start.insert(cf.byte_range.0, idx);
        }

        Self {
            rules,
            ssa,
            file_idx,
            result,
            def_nodes,
            import_nodes,
            current_block: module_block,
            scope_stack: vec![ScopeEntry {
                block: module_block,
                is_type_scope: false,
                name: None,
                def_node: None,
                enclosing_type_fqn: None,
            }],
            ref_by_range_start,
            def_by_byte_start,
            binding_by_byte_start,
            cf_by_byte_start,
            reads: Vec::new(),
        }
    }

    fn walk_node(&mut self, node: &Node<StrDoc<SupportLang>>) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < MIN_STACK_REMAINING {
            return;
        }

        let byte_start = node.range().start;

        // Scope-creating nodes (matched by AST node kind)
        let kind = node.kind();
        let kind_ref = kind.as_ref();
        if let Some(scope_rule) = self.rules.scopes().iter().find(|s| s.node_kind == kind_ref) {
            self.enter_scope(node, scope_rule.is_type_scope);
            self.walk_children(node);
            self.exit_scope();
            return;
        }

        // Control flow: branches and loops (matched by byte offset + node kind
        // against parsed CanonicalControlFlow).
        if let Some(&cf_idx) = self.cf_by_byte_start.get(&byte_start) {
            let cf = &self.result.control_flow[cf_idx];
            if cf.node_kind == kind_ref {
                self.cf_by_byte_start.remove(&byte_start);
                match cf.kind {
                    ControlFlowKind::Branch { has_catch_all } => {
                        self.walk_branch_from_cf(node, cf, has_catch_all);
                    }
                    ControlFlowKind::Loop => {
                        self.walk_loop_from_cf(node, cf);
                    }
                }
                return;
            }
        }

        // Bindings (matched by byte offset against parsed CanonicalBinding).
        // Pure writeVariable — no AST inspection, no SSA reads.
        if let Some(binding_idx) = self.binding_by_byte_start.remove(&byte_start) {
            let binding = &self.result.bindings[binding_idx];
            self.handle_binding_from_parsed(binding);
        }

        // References (matched by byte offset against parsed CanonicalReference).
        if let Some(ref_indices) = self.ref_by_range_start.remove(&byte_start) {
            let enclosing_def = self.innermost_def();
            let enclosing_type_fqn = self.scope_stack.last().and_then(|e| e.enclosing_type_fqn);

            for ref_idx in ref_indices {
                let reference = &self.result.references[ref_idx];
                self.reads.push(RecordedRead {
                    file_idx: self.file_idx,
                    ref_idx,
                    block: self.current_block,
                    name: IStr::from(reference.name.as_str()),
                    enclosing_def,
                    enclosing_type_fqn,
                });
            }
        }

        self.walk_children(node);
    }

    fn walk_children(&mut self, node: &Node<StrDoc<SupportLang>>) {
        for child in node.children() {
            self.walk_node(&child);
        }
    }

    fn enter_scope(&mut self, node: &Node<StrDoc<SupportLang>>, is_type_scope: bool) {
        let new_block = self.ssa.add_block();
        self.ssa.add_predecessor(new_block, self.current_block);
        self.ssa.seal_block(new_block);

        let scope_name = node.field("name").map(|n| n.text().to_string());

        // Match this scope to a canonical definition by byte offset.
        let byte_start = node.range().start;
        let def_idx = self.def_by_byte_start.get(&byte_start).copied();

        // Compute enclosing_type_fqn for this scope.
        // Prefer the canonical definition's FQN (source of truth) over build_fqn.
        let enclosing_type_fqn = if is_type_scope {
            if let Some(di) = def_idx {
                Some(self.result.definitions[di].fqn.as_istr())
            } else if scope_name.is_some() {
                // Type scope matched by AST but no canonical def found — fall back.
                scope_name
                    .as_ref()
                    .map(|name| IStr::from(self.build_fqn(name).as_str()))
            } else {
                self.scope_stack.last().and_then(|e| e.enclosing_type_fqn)
            }
        } else {
            // Non-type scope — inherit parent's enclosing type.
            self.scope_stack.last().and_then(|e| e.enclosing_type_fqn)
        };

        if is_type_scope && let Some(ref name) = scope_name {
            // Use canonical FQN for self/super SSA writes (matches MemberIndex keys).
            let class_fqn = if let Some(di) = def_idx {
                self.result.definitions[di].fqn.to_string()
            } else {
                self.build_fqn(name)
            };
            for &self_name in self.rules.self_names {
                self.ssa
                    .write_variable(self_name, new_block, Value::type_of(&class_fqn));
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
            name: scope_name,
            def_node: def_idx.map(|di| self.def_nodes[di]),
            enclosing_type_fqn,
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

    /// Build a dotted FQN from the scope stack names + a new name.
    fn build_fqn(&self, name: &str) -> String {
        let sep = self.rules.fqn_separator;
        let mut parts: Vec<&str> = self
            .scope_stack
            .iter()
            .filter_map(|e| e.name.as_deref())
            .collect();
        parts.push(name);
        parts.join(sep)
    }

    /// Look up the first super_type for a class by name from canonical defs.
    fn find_super_type(&self, class_name: &str) -> Option<String> {
        self.result
            .definitions
            .iter()
            .find(|d| d.name == class_name)
            .and_then(|d| d.metadata.as_ref())
            .and_then(|m| m.super_types.first())
            .cloned()
    }

    /// Branch handling per Braun et al. §2.3 (Figure 3b), driven by
    /// parsed `CanonicalControlFlow`. No AST rule matching — the parser
    /// already identified the branch structure.
    fn walk_branch_from_cf(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        cf: &CanonicalControlFlow,
        has_catch_all: bool,
    ) {
        let pre_block = self.current_block;

        // Walk condition children in current block
        for child_cf in &cf.children {
            if child_cf.is_condition {
                for child in node.children() {
                    let cs = child.range().start;
                    let ce = child.range().end;
                    if cs >= child_cf.byte_range.0 && ce <= child_cf.byte_range.1 {
                        self.walk_node(&child);
                    }
                }
            }
        }

        // Create a block for each branch arm
        let branch_ranges: Vec<(usize, usize)> = cf
            .children
            .iter()
            .filter(|c| !c.is_condition)
            .map(|c| c.byte_range)
            .collect();

        let mut branch_blocks = Vec::new();
        for child in node.children() {
            let cs = child.range().start;
            let ce = child.range().end;
            if branch_ranges.iter().any(|&(s, e)| cs >= s && ce <= e) {
                let branch_block = self.ssa.add_block();
                self.ssa.add_predecessor(branch_block, pre_block);
                self.ssa.seal_block(branch_block);

                self.current_block = branch_block;
                self.walk_node(&child);
                branch_blocks.push(self.current_block);
            } else {
                // Condition or other non-branch child — walk in pre_block
                let is_condition = cf
                    .children
                    .iter()
                    .any(|c| c.is_condition && cs >= c.byte_range.0 && ce <= c.byte_range.1);
                if !is_condition {
                    self.current_block = pre_block;
                    self.walk_node(&child);
                }
            }
        }

        // Join block
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

    /// Loop handling per Braun et al. §2.3 (Figure 3a), driven by
    /// parsed `CanonicalControlFlow`.
    fn walk_loop_from_cf(&mut self, node: &Node<StrDoc<SupportLang>>, cf: &CanonicalControlFlow) {
        let pre_block = self.current_block;

        // Walk iteration expression (is_condition children) in pre_block
        for child_cf in &cf.children {
            if child_cf.is_condition {
                for child in node.children() {
                    let cs = child.range().start;
                    let ce = child.range().end;
                    if cs >= child_cf.byte_range.0 && ce <= child_cf.byte_range.1 {
                        self.walk_node(&child);
                    }
                }
            }
        }

        // Loop header — DON'T seal (back-edge coming)
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, pre_block);

        // Body block
        let body_block = self.ssa.add_block();
        self.ssa.add_predecessor(body_block, header);
        self.ssa.seal_block(body_block);
        self.current_block = body_block;

        // Walk body children
        let body_ranges: Vec<(usize, usize)> = cf
            .children
            .iter()
            .filter(|c| !c.is_condition)
            .map(|c| c.byte_range)
            .collect();

        if !body_ranges.is_empty() {
            let mut matched = false;
            for child in node.children() {
                let cs = child.range().start;
                let ce = child.range().end;
                if body_ranges.iter().any(|&(s, e)| cs >= s && ce <= e) {
                    matched = true;
                    self.walk_node(&child);
                }
            }
            if !matched {
                self.walk_children(node);
            }
        } else {
            self.walk_children(node);
        }

        // Back-edge + seal header
        self.ssa.add_predecessor(header, self.current_block);
        self.ssa.seal_block(header);

        // Exit block
        let exit_block = self.ssa.add_block();
        self.ssa.add_predecessor(exit_block, header);
        self.ssa.seal_block(exit_block);
        self.current_block = exit_block;
    }

    /// Write a binding to SSA from parsed `CanonicalBinding`.
    /// No AST inspection, no SSA reads — pure writeVariable.
    fn handle_binding_from_parsed(&mut self, binding: &CanonicalBinding) {
        let value = match binding.kind {
            BindingKind::Deletion | BindingKind::ForTarget => Value::Opaque,
            BindingKind::Parameter | BindingKind::Assignment | BindingKind::WithAlias => {
                if let Some(ref type_ann) = binding.type_annotation {
                    Value::type_of(type_ann)
                } else if let Some(ref rhs) = binding.rhs_name {
                    Value::Alias(IStr::from(rhs.as_str()))
                } else {
                    Value::Opaque
                }
            }
        };

        let target_block = if binding.instance_attr {
            self.enclosing_class_block().unwrap_or(self.current_block)
        } else {
            self.current_block
        };
        self.ssa.write_variable(&binding.name, target_block, value);
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
