//! SSA walker that interprets `ResolutionRules` and drives the `SsaResolver`.
//!
//! Walks each file's tree-sitter AST, creating SSA blocks for scopes and
//! control flow per Braun et al. (CC 2013). Writes variable definitions,
//! processes bindings, and records reference reads. The output is a
//! populated `SsaResolver` with all reaching definitions computed.

use code_graph_types::CanonicalResult;
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::rules::{BindingKind, ChainMode, ResolutionRules, ScopeKind};
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

/// Walk all files and build the SSA graph.
///
/// For files with a retained AST, walks the AST using `rules` to discover
/// control flow (branches, loops, scopes) per Braun et al.
/// For files without an AST (custom pipelines), falls back to a flat
/// single-block-per-scope model from `CanonicalResult` data.
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
                let mut walker = FileWalker::new(rules, &mut ssa, file_idx, result);
                walker.walk_node(&root);
                walker.finalize();
                reads.extend(walker.reads);
            }
            None => {
                walk_flat(&mut ssa, &mut reads, file_idx, result);
            }
        }
    }

    WalkResult { ssa, reads }
}

/// Trait for extracting a tree-sitter root from the AST type.
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

// ── Flat walker (fallback for custom pipelines without AST) ─────

/// Minimal fallback when no AST is available. One block per scope,
/// no control flow modeling. Used only by custom pipelines (e.g. Ruby)
/// that implement `LanguagePipeline` directly with `Ast = ()`.
fn walk_flat(
    ssa: &mut SsaResolver,
    reads: &mut Vec<RecordedRead>,
    file_idx: usize,
    result: &CanonicalResult,
) {
    let mut scope_blocks: FxHashMap<String, BlockId> = FxHashMap::default();

    let module_block = ssa.add_block();
    ssa.seal_block(module_block);
    scope_blocks.insert(String::new(), module_block);

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
        let parent_fqn = def.fqn.parent().map(|p| p.to_string()).unwrap_or_default();
        let parent_block = scope_blocks
            .get(&parent_fqn)
            .copied()
            .unwrap_or(module_block);

        let block = ssa.add_block();
        ssa.add_predecessor(block, parent_block);
        ssa.seal_block(block);
        scope_blocks.insert(fqn_str, block);
    }

    for (def_idx, def) in result.definitions.iter().enumerate() {
        let parent_fqn = def.fqn.parent().map(|p| p.to_string()).unwrap_or_default();
        let block = scope_blocks
            .get(&parent_fqn)
            .copied()
            .unwrap_or(module_block);
        ssa.write_variable(&def.name, block, Value::Def(file_idx, def_idx));
    }

    for (import_idx, imp) in result.imports.iter().enumerate() {
        let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
        if !name.is_empty() && name != "*" {
            ssa.write_variable(name, module_block, Value::Import(file_idx, import_idx));
        }
    }

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

// ── AST walker (Braun et al.) ───────────────────────────────────

/// An entry on the scope stack, tracking the block, kind, and name.
struct ScopeEntry {
    block: BlockId,
    kind: ScopeKind,
    name: Option<String>,
}

struct FileWalker<'a> {
    rules: &'a ResolutionRules,
    ssa: &'a mut SsaResolver,
    file_idx: usize,
    result: &'a CanonicalResult,

    current_block: BlockId,
    scope_stack: Vec<ScopeEntry>,
    ref_by_range_start: FxHashMap<usize, Vec<usize>>,

    reads: Vec<RecordedRead>,
}

impl<'a> FileWalker<'a> {
    fn new(
        rules: &'a ResolutionRules,
        ssa: &'a mut SsaResolver,
        file_idx: usize,
        result: &'a CanonicalResult,
    ) -> Self {
        let module_block = ssa.add_block();
        ssa.seal_block(module_block);

        // Write all definitions to the module block
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

        // Index canonical references by byte offset for matching
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
            scope_stack: vec![ScopeEntry {
                block: module_block,
                kind: ScopeKind::Module,
                name: None,
            }],
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

        // Scope-creating nodes
        if let Some(scope_rule) = self.rules.scopes.iter().find(|s| s.node_kind == kind_ref) {
            self.enter_scope(node, scope_rule.scope_kind);
            self.walk_children(node);
            self.exit_scope();
            return;
        }

        // Branch nodes (if/else, try/catch, match)
        if let Some(branch_rule) = self.rules.branches.iter().find(|b| b.node_kind == kind_ref) {
            self.walk_branch(node, branch_rule);
            return;
        }

        // Loop nodes
        if let Some(loop_rule) = self.rules.loops.iter().find(|l| l.node_kind == kind_ref) {
            self.walk_loop(node, loop_rule);
            return;
        }

        // Binding nodes
        if let Some(binding_rule) = self.rules.bindings.iter().find(|b| b.node_kind == kind_ref) {
            self.handle_binding(node, binding_rule);
        }

        // Match canonical references by byte offset — this is the primary
        // mechanism for recording reads. The DSL parser already extracted
        // references; we just need to assign them to the correct SSA block.
        let byte_start = node.range().start;
        if let Some(ref_indices) = self.ref_by_range_start.remove(&byte_start) {
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

    fn enter_scope(&mut self, node: &Node<StrDoc<SupportLang>>, scope_kind: ScopeKind) {
        let new_block = self.ssa.add_block();
        self.ssa.add_predecessor(new_block, self.current_block);
        self.ssa.seal_block(new_block);

        let scope_name = node.field("name").map(|n| n.text().to_string());

        if scope_kind == ScopeKind::Class {
            if let Some(ref name) = scope_name {
                let class_fqn = self.build_fqn(name);
                self.ssa
                    .write_variable("this", new_block, Value::Type(class_fqn.clone()));
                self.ssa
                    .write_variable("self", new_block, Value::Type(class_fqn));
                // super → look up first super_type from canonical definitions
                if let Some(super_type) = self.find_super_type(name) {
                    self.ssa
                        .write_variable("super", new_block, Value::Type(super_type));
                }
            }
        }

        self.scope_stack.push(ScopeEntry {
            block: new_block,
            kind: scope_kind,
            name: scope_name,
        });
        self.current_block = new_block;
    }

    fn exit_scope(&mut self) {
        if self.scope_stack.pop().is_some() {
            if let Some(parent) = self.scope_stack.last() {
                self.current_block = parent.block;
            }
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

    /// Find the enclosing class FQN by walking up the scope stack.
    fn enclosing_class_fqn(&self) -> Option<String> {
        for entry in self.scope_stack.iter().rev() {
            if entry.kind == ScopeKind::Class {
                if let Some(ref name) = entry.name {
                    // Rebuild the FQN up to this scope
                    let sep = self.rules.fqn_separator;
                    let parts: Vec<&str> = self
                        .scope_stack
                        .iter()
                        .take_while(|e| !std::ptr::eq(*e, entry))
                        .filter_map(|e| e.name.as_deref())
                        .chain(std::iter::once(name.as_str()))
                        .collect();
                    return Some(parts.join(sep));
                }
            }
        }
        None
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

    /// Branch handling per Braun et al. §2.3 (Figure 3b).
    /// Each arm gets its own block; they merge at a join point.
    fn walk_branch(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        branch_rule: &super::rules::BranchRule,
    ) {
        let pre_block = self.current_block;

        // Walk condition in current block
        if let Some(cond_field) = branch_rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_node(&cond_node);
        }

        // Create a block for each branch arm
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
                branch_blocks.push(self.current_block);
            } else if branch_rule.condition_field.is_some_and(|f| {
                node.field(f)
                    .is_some_and(|n| n.range().start == child.range().start)
            }) {
                // Skip condition — already walked
            } else {
                self.current_block = pre_block;
                self.walk_node(&child);
            }
        }

        // Create join block
        let join_block = self.ssa.add_block();
        for &bb in &branch_blocks {
            self.ssa.add_predecessor(join_block, bb);
        }
        // No catch-all → pre_block also flows to join
        let has_catch_all = branch_rule
            .catch_all_kind
            .is_some_and(|catch_kind| node.children().any(|c| c.kind().as_ref() == catch_kind));
        if !has_catch_all {
            self.ssa.add_predecessor(join_block, pre_block);
        }
        self.ssa.seal_block(join_block);

        self.current_block = join_block;
    }

    /// Loop handling per Braun et al. §2.3 (Figure 3a).
    /// Unsealed header → body → back-edge → seal header → exit.
    fn walk_loop(&mut self, node: &Node<StrDoc<SupportLang>>, loop_rule: &super::rules::LoopRule) {
        let pre_block = self.current_block;

        // Walk iteration expression in pre_block
        if let Some(iter_field) = loop_rule.iter_field
            && let Some(iter_node) = node.field(iter_field)
        {
            self.walk_node(&iter_node);
        }

        // Create loop header — DON'T seal (back-edge coming)
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, pre_block);

        // Create body block
        let body_block = self.ssa.add_block();
        self.ssa.add_predecessor(body_block, header);
        self.ssa.seal_block(body_block);

        self.current_block = body_block;

        // Walk loop body
        if let Some(body_node) = node.field(loop_rule.body_field) {
            self.walk_children(&body_node);
        } else {
            self.walk_children(node);
        }

        // Add back-edge and seal header
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
        let name = match node.field(binding_rule.name_field) {
            Some(name_node) => name_node.text().to_string(),
            None => return,
        };

        let value = match binding_rule.binding_kind {
            BindingKind::Parameter => {
                self.extract_type_value(node).unwrap_or(Value::Opaque)
            }
            BindingKind::Deletion | BindingKind::ForTarget => Value::Opaque,
            BindingKind::Assignment | BindingKind::WithAlias => {
                self.extract_type_value(node)
                    .unwrap_or_else(|| self.resolve_binding_value(node, binding_rule))
            }
        };

        self.ssa.write_variable(&name, self.current_block, value);
    }

    /// Extract a type annotation from a node, producing `Value::Type`.
    /// Only active for `ChainMode::TypeFlow` — reads field names and
    /// skip list from the per-language config.
    fn extract_type_value(&self, node: &Node<StrDoc<SupportLang>>) -> Option<Value> {
        let (type_fields, skip_types) = match &self.rules.chain_mode {
            ChainMode::TypeFlow {
                type_fields,
                skip_types,
            } => (type_fields, skip_types),
            ChainMode::ValueFlow => return None,
        };

        for &field_name in type_fields.iter() {
            if let Some(type_node) = node.field(field_name) {
                let type_text = type_node.text().to_string();
                if !skip_types.iter().any(|&s| s == type_text) {
                    return Some(Value::Type(type_text));
                }
            }
        }
        None
    }

    /// Resolve a binding's RHS value through the SSA (value-flow).
    /// For TypeFlow, if the resolved value is a Def with return_type
    /// metadata, promote to Value::Type so downstream member access
    /// chains resolve through the return type.
    fn resolve_binding_value(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        binding_rule: &super::rules::BindingRule,
    ) -> Value {
        if let Some(value_field) = binding_rule.value_field {
            if let Some(value_node) = node.field(value_field) {
                let value_text = value_node.text().to_string();
                let reaching = self.ssa.read_variable(&value_text, self.current_block);
                if !reaching.values.is_empty() {
                    let value = reaching.values[0].clone();
                    self.maybe_promote_to_type(value)
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

    /// For TypeFlow: if a value is Def and the definition has return_type
    /// metadata, promote to Value::Type(return_type). This allows
    /// `x = getService(); x.query()` to resolve through the return type.
    fn maybe_promote_to_type(&self, value: Value) -> Value {
        if !matches!(self.rules.chain_mode, ChainMode::TypeFlow { .. }) {
            return value;
        }
        match &value {
            Value::Def(file_idx, def_idx) if *file_idx == self.file_idx => {
                if let Some(def) = self.result.definitions.get(*def_idx) {
                    if let Some(meta) = &def.metadata {
                        if let Some(return_type) = &meta.return_type {
                            return Value::Type(return_type.clone());
                        }
                    }
                }
                value
            }
            _ => value,
        }
    }

    fn finalize(&mut self) {
        // All blocks should already be sealed
    }
}
