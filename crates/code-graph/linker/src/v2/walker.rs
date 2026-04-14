//! Generic AST walker that interprets `ResolutionRules` and drives the `SsaResolver`.
//!
//! The walker visits each file's AST (or canonical data when no AST is available),
//! creating SSA blocks for scopes and control flow, writing variable definitions,
//! and reading references. The output is a populated `SsaResolver` with all
//! reaching definitions computed.

use code_graph_types::CanonicalResult;
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::rules::{BindingKind, ResolutionRules, ScopeKind};
use super::ssa::{BlockId, SsaResolver, Value};

/// A recorded reference read, linking back to the canonical data.
#[derive(Debug, Clone)]
pub struct RecordedRead {
    pub file_idx: usize,
    pub ref_idx: usize,
    pub block: BlockId,
    pub name: String,
}

/// Result of walking all files: the populated SSA resolver + recorded reads.
pub struct WalkResult {
    pub ssa: SsaResolver,
    pub reads: Vec<RecordedRead>,
}

/// Walk all files using the given rules and build the SSA graph.
///
/// For files with a retained AST, walks the AST to discover control flow.
/// For files without an AST, uses a flat single-block model where all
/// definitions are visible everywhere.
pub fn walk_files<A>(
    rules: &ResolutionRules,
    results: &[CanonicalResult],
    asts: &FxHashMap<String, A>,
) -> WalkResult
where
    A: AsAst,
{
    let mut ssa = SsaResolver::new();
    let mut reads = Vec::new();

    for (file_idx, result) in results.iter().enumerate() {
        let ast = asts.get(&result.file_path);

        match ast.and_then(|a| a.as_root()) {
            Some(root) => {
                // Full AST walk with control flow
                let mut walker = FileWalker::new(rules, &mut ssa, file_idx, result);
                walker.walk_node(&root);
                walker.finalize();
                reads.extend(walker.reads);
            }
            None => {
                // No AST: flat model from canonical data
                walk_flat(rules, &mut ssa, &mut reads, file_idx, result);
            }
        }
    }

    WalkResult { ssa, reads }
}

/// Trait for extracting a tree-sitter root from the AST type.
/// `()` returns None (no AST), concrete roots return Some.
pub trait AsAst {
    fn as_root(&self) -> Option<Node<'_, StrDoc<SupportLang>>>;
}

impl AsAst for () {
    fn as_root(&self) -> Option<Node<'_, StrDoc<SupportLang>>> {
        None
    }
}

impl AsAst for treesitter_visit::Root<StrDoc<SupportLang>> {
    fn as_root(&self) -> Option<Node<'_, StrDoc<SupportLang>>> {
        Some(self.root())
    }
}

// ── Scoped walker (no AST) ───────────────────────────────────────

/// Walk canonical data without an AST. Creates one SSA block per scope
/// (function/class) so that local bindings don't leak across scopes.
/// Each scope's block has an edge from its parent scope, so names
/// defined in outer scopes are visible via SSA's recursive lookup.
fn walk_flat(
    _rules: &ResolutionRules,
    ssa: &mut SsaResolver,
    reads: &mut Vec<RecordedRead>,
    file_idx: usize,
    result: &CanonicalResult,
) {
    // Build scope hierarchy: scope_fqn → SSA block
    let mut scope_blocks: FxHashMap<String, BlockId> = FxHashMap::default();

    // Module-level block (root scope)
    let module_block = ssa.add_block();
    ssa.seal_block(module_block);
    scope_blocks.insert(String::new(), module_block);

    // Create a block for each definition that creates a scope (functions, classes)
    // ordered by FQN depth so parents are created before children
    let mut scoped_defs: Vec<(usize, &code_graph_types::CanonicalDefinition)> = result
        .definitions
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            matches!(
                d.kind,
                code_graph_types::DefKind::Function
                    | code_graph_types::DefKind::Method
                    | code_graph_types::DefKind::Class
                    | code_graph_types::DefKind::Constructor
            )
        })
        .collect();
    scoped_defs.sort_by_key(|(_, d)| d.fqn.parts().len());

    for (_def_idx, def) in &scoped_defs {
        let fqn_str = def.fqn.to_string();
        let parent_fqn: String = def.fqn.parent().map(|p| p.to_string()).unwrap_or_default();

        let parent_block = scope_blocks
            .get(&parent_fqn)
            .copied()
            .unwrap_or(module_block);

        let block = ssa.add_block();
        ssa.add_predecessor(block, parent_block);
        ssa.seal_block(block);
        scope_blocks.insert(fqn_str, block);
    }

    // Write definitions to their scope's block
    for (def_idx, def) in result.definitions.iter().enumerate() {
        let parent_fqn = def.fqn.parent().map(|p| p.to_string()).unwrap_or_default();
        let block = scope_blocks
            .get(&parent_fqn)
            .copied()
            .unwrap_or(module_block);
        ssa.write_variable(&def.name, block, Value::Def(file_idx, def_idx));
    }

    // Write imports to module block
    for (import_idx, imp) in result.imports.iter().enumerate() {
        let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
        if !name.is_empty() && name != "*" {
            ssa.write_variable(name, module_block, Value::Import(file_idx, import_idx));
        }
    }

    // Process bindings in their scope's block
    for binding in &result.bindings {
        let scope_fqn = binding
            .scope_fqn
            .as_ref()
            .map(|f| f.to_string())
            .unwrap_or_default();
        let block = scope_blocks
            .get(&scope_fqn)
            .copied()
            .unwrap_or(module_block);

        let value = if let Some(ref val_name) = binding.value {
            let reaching = ssa.read_variable_stateless(val_name, block);
            if !reaching.values.is_empty() {
                reaching.values[0].clone()
            } else {
                Value::Opaque
            }
        } else {
            Value::Opaque
        };
        ssa.write_variable(&binding.name, block, value);
    }

    // Read references in their scope's block
    for (ref_idx, reference) in result.references.iter().enumerate() {
        let scope_fqn = reference
            .scope_fqn
            .as_ref()
            .map(|f| f.to_string())
            .unwrap_or_default();
        let block = scope_blocks
            .get(&scope_fqn)
            .copied()
            .unwrap_or(module_block);

        reads.push(RecordedRead {
            file_idx,
            ref_idx,
            block,
            name: reference.name.clone(),
        });
    }
}

// ── AST walker ──────────────────────────────────────────────────

struct FileWalker<'a> {
    rules: &'a ResolutionRules,
    ssa: &'a mut SsaResolver,
    file_idx: usize,
    result: &'a CanonicalResult,

    /// Current SSA block we're writing into.
    current_block: BlockId,
    /// Stack of (scope_block, scope_kind) for nested scopes.
    scope_stack: Vec<(BlockId, ScopeKind)>,
    /// References indexed by byte-offset start.
    ref_by_range_start: FxHashMap<usize, Vec<usize>>,

    /// Recorded reads to return.
    reads: Vec<RecordedRead>,
}

impl<'a> FileWalker<'a> {
    fn new(
        rules: &'a ResolutionRules,
        ssa: &'a mut SsaResolver,
        file_idx: usize,
        result: &'a CanonicalResult,
    ) -> Self {
        // Create the module-level block
        let module_block = ssa.add_block();
        ssa.seal_block(module_block);

        // Write all definitions to the module block (they're globally visible within the file)
        for (def_idx, def) in result.definitions.iter().enumerate() {
            ssa.write_variable(&def.name, module_block, Value::Def(file_idx, def_idx));
        }

        // Write all imports
        for (import_idx, imp) in result.imports.iter().enumerate() {
            let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
            if !name.is_empty() && name != "*" {
                ssa.write_variable(name, module_block, Value::Import(file_idx, import_idx));
            }
        }

        // Build index for matching AST nodes to canonical references
        let mut ref_by_range_start: FxHashMap<usize, Vec<usize>> = FxHashMap::default();
        for (idx, r) in result.references.iter().enumerate() {
            ref_by_range_start
                .entry(r.range.byte_offset.0)
                .or_default()
                .push(idx);
        }

        Self {
            rules,
            ssa,
            file_idx,
            result,
            current_block: module_block,
            scope_stack: vec![(module_block, ScopeKind::Module)],
            ref_by_range_start,
            reads: Vec::new(),
        }
    }

    fn walk_node(&mut self, node: &Node<StrDoc<SupportLang>>) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < 128 * 1024 {
            return;
        }

        let kind = node.kind();
        let kind_ref = kind.as_ref();

        // Check for scope-creating nodes
        if let Some(scope_rule) = self.rules.scopes.iter().find(|s| s.node_kind == kind_ref) {
            self.enter_scope(node, scope_rule.scope_kind);
            self.walk_children(node);
            self.exit_scope();
            return;
        }

        // Check for branch nodes (if/else, try/catch, match)
        if let Some(branch_rule) = self.rules.branches.iter().find(|b| b.node_kind == kind_ref) {
            self.walk_branch(node, branch_rule);
            return;
        }

        // Check for loop nodes
        if let Some(loop_rule) = self.rules.loops.iter().find(|l| l.node_kind == kind_ref) {
            self.walk_loop(node, loop_rule);
            return;
        }

        // Check for binding nodes (assignments, parameters)
        if let Some(binding_rule) = self.rules.bindings.iter().find(|b| b.node_kind == kind_ref) {
            self.handle_binding(node, binding_rule);
        }

        // Check for reference nodes (calls)
        if let Some(_ref_rule) = self
            .rules
            .references
            .iter()
            .find(|r| r.node_kind == kind_ref)
        {
            self.handle_reference(node);
        }

        // Also check if this node corresponds to a canonical reference by range
        let byte_start = node.range().start;
        if let Some(ref_indices) = self.ref_by_range_start.get(&byte_start).cloned() {
            for ref_idx in ref_indices {
                let reference = &self.result.references[ref_idx];
                self.reads.push(RecordedRead {
                    file_idx: self.file_idx,
                    ref_idx,
                    block: self.current_block,
                    name: reference.name.clone(),
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

    fn enter_scope(&mut self, _node: &Node<StrDoc<SupportLang>>, scope_kind: ScopeKind) {
        let new_block = self.ssa.add_block();
        self.ssa.add_predecessor(new_block, self.current_block);
        self.ssa.seal_block(new_block);

        self.scope_stack.push((new_block, scope_kind));
        self.current_block = new_block;
    }

    fn exit_scope(&mut self) {
        if let Some((_block, _kind)) = self.scope_stack.pop() {
            // Restore parent block
            if let Some(&(parent_block, _)) = self.scope_stack.last() {
                self.current_block = parent_block;
            }
        }
    }

    fn walk_branch(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        branch_rule: &super::rules::BranchRule,
    ) {
        let pre_block = self.current_block;

        // Walk the condition in the current block (if there is one)
        if let Some(cond_field) = branch_rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_node(&cond_node);
        }

        // Create a block for each branch
        let mut branch_blocks = Vec::new();
        for child in node.children() {
            let child_kind = child.kind();
            if branch_rule
                .branch_kinds
                .iter()
                .any(|&k| k == child_kind.as_ref())
            {
                let branch_block = self.ssa.add_block();
                self.ssa.add_predecessor(branch_block, pre_block);
                self.ssa.seal_block(branch_block);

                self.current_block = branch_block;
                self.walk_children(&child);
                branch_blocks.push(self.current_block); // may have changed within the branch
            } else if branch_rule.condition_field.is_some_and(|f| {
                node.field(f)
                    .is_some_and(|n| n.range().start == child.range().start)
            }) {
                // Skip condition node, already walked above
            } else {
                // Walk non-branch children in pre_block
                self.current_block = pre_block;
                self.walk_node(&child);
            }
        }

        // Create join block
        let join_block = self.ssa.add_block();
        for &bb in &branch_blocks {
            self.ssa.add_predecessor(join_block, bb);
        }
        // If no catch-all branch, the pre_block also flows to the join
        // (the condition might not match any branch)
        let has_catch_all = branch_rule
            .catch_all_kind
            .is_some_and(|catch_kind| node.children().any(|c| c.kind().as_ref() == catch_kind));
        if !has_catch_all {
            self.ssa.add_predecessor(join_block, pre_block);
        }
        self.ssa.seal_block(join_block);

        self.current_block = join_block;
    }

    fn walk_loop(&mut self, node: &Node<StrDoc<SupportLang>>, loop_rule: &super::rules::LoopRule) {
        let pre_block = self.current_block;

        // Walk iteration expression in pre_block (e.g. the iterable in `for x in iter`)
        if let Some(iter_field) = loop_rule.iter_field
            && let Some(iter_node) = node.field(iter_field)
        {
            self.walk_node(&iter_node);
        }

        // Create loop header (unsealed — back edge will come from body)
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, pre_block);
        // Don't seal header yet

        // Create body block
        let body_block = self.ssa.add_block();
        self.ssa.add_predecessor(body_block, header);
        self.ssa.seal_block(body_block);

        self.current_block = body_block;

        // Walk loop body
        if let Some(body_node) = node.field(loop_rule.body_field) {
            self.walk_children(&body_node);
        } else {
            // Walk all children if no explicit body field
            self.walk_children(node);
        }

        // Add back edge and seal header
        self.ssa.add_predecessor(header, self.current_block);
        self.ssa.seal_block(header);

        // Create exit block
        let exit_block = self.ssa.add_block();
        self.ssa.add_predecessor(exit_block, header);
        self.ssa.seal_block(exit_block);

        self.current_block = exit_block;
    }

    fn handle_binding(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        binding_rule: &super::rules::BindingRule,
    ) {
        // Extract the name being bound
        let name = match node.field(binding_rule.name_field) {
            Some(name_node) => name_node.text().to_string(),
            None => return,
        };

        let value = match binding_rule.binding_kind {
            BindingKind::Parameter | BindingKind::Deletion | BindingKind::ForTarget => {
                Value::Opaque
            }
            BindingKind::Assignment | BindingKind::WithAlias => {
                // For value-flow: try to find what this assignment refers to
                if let Some(value_field) = binding_rule.value_field {
                    if let Some(value_node) = node.field(value_field) {
                        let value_text = value_node.text().to_string();
                        // Check if the value is a known identifier (alias)
                        // For now, read it from the SSA to get the current value
                        let reaching = self.ssa.read_variable(&value_text, self.current_block);
                        if !reaching.values.is_empty() {
                            // Alias to whatever the RHS resolves to
                            reaching.values[0].clone()
                        } else {
                            Value::Opaque
                        }
                    } else {
                        Value::Opaque
                    }
                } else {
                    Value::Opaque
                }
            }
        };

        self.ssa.write_variable(&name, self.current_block, value);
    }

    fn handle_reference(&mut self, node: &Node<StrDoc<SupportLang>>) {
        // Find matching canonical reference by byte offset
        let byte_start = node.range().start;
        if let Some(ref_indices) = self.ref_by_range_start.get(&byte_start).cloned() {
            for ref_idx in ref_indices {
                let reference = &self.result.references[ref_idx];
                self.reads.push(RecordedRead {
                    file_idx: self.file_idx,
                    ref_idx,
                    block: self.current_block,
                    name: reference.name.clone(),
                });
            }
        }
    }

    fn finalize(&mut self) {
        // Nothing to do — all blocks should already be sealed
    }
}
