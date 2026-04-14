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

use parser_core::dsl::types::Rule as DslRule;

use super::rules::{BindingKind, ChainMode, ResolutionRules};
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
    pub name: String,
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
) -> FileWalkResult {
    let mut ssa = SsaResolver::new();
    let mut walker = FileWalker::new(rules, &mut ssa, file_idx, result);
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

        // Write all imports (skip wildcards — they have no single name to bind)
        for (import_idx, imp) in result.imports.iter().enumerate() {
            if imp.wildcard {
                continue;
            }
            let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
            if !name.is_empty() {
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
                is_type_scope: false,
                name: None,
            }],
            ref_by_range_start,
            reads: Vec::new(),
        }
    }

    fn walk_node(&mut self, node: &Node<StrDoc<SupportLang>>) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < MIN_STACK_REMAINING {
            return;
        }

        let kind = node.kind();
        let kind_ref = kind.as_ref();

        // Scope-creating nodes
        if let Some(scope_rule) = self.rules.scopes().iter().find(|s| s.node_kind == kind_ref) {
            self.enter_scope(node, scope_rule.is_type_scope);
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

    fn enter_scope(&mut self, node: &Node<StrDoc<SupportLang>>, is_type_scope: bool) {
        let new_block = self.ssa.add_block();
        self.ssa.add_predecessor(new_block, self.current_block);
        self.ssa.seal_block(new_block);

        let scope_name = node.field("name").map(|n| n.text().to_string());

        if is_type_scope && let Some(ref name) = scope_name {
            let class_fqn = self.build_fqn(name);
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
        let name = match Self::walk_field_chain(node, binding_rule.name_fields) {
            Some(n) => n.text().to_string(),
            None => return,
        };

        let value = match binding_rule.binding_kind {
            BindingKind::Parameter => self.extract_type_value(node).unwrap_or(Value::Opaque),
            BindingKind::Deletion | BindingKind::ForTarget => Value::Opaque,
            BindingKind::Assignment | BindingKind::WithAlias => self
                .extract_type_value(node)
                .unwrap_or_else(|| self.resolve_binding_value(node, binding_rule)),
        };

        // Instance attribute bindings (e.g. self.db = ...) are written to
        // the enclosing class block so sibling methods can see them.
        let is_instance_attr = binding_rule
            .instance_attr_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix));
        let target_block = if is_instance_attr {
            self.enclosing_class_block().unwrap_or(self.current_block)
        } else {
            self.current_block
        };
        self.ssa.write_variable(&name, target_block, value);
    }

    /// Walk a chain of field names to reach a nested node.
    /// e.g. `&["declarator", "name"]` → `node.field("declarator")?.field("name")?`
    fn walk_field_chain<'b>(
        node: &Node<'b, StrDoc<SupportLang>>,
        fields: &[&str],
    ) -> Option<Node<'b, StrDoc<SupportLang>>> {
        let mut current = node.clone();
        for &field in fields {
            current = current.field(field)?;
        }
        Some(current)
    }

    /// Find the enclosing class scope's block.
    fn enclosing_class_block(&self) -> Option<BlockId> {
        self.scope_stack
            .iter()
            .rev()
            .find(|e| e.is_type_scope)
            .map(|e| e.block)
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
                    return Some(Value::type_of(&type_text));
                }
            }
        }
        None
    }

    /// Resolve a binding's RHS value through the SSA.
    ///
    /// Extracts the meaningful name from the RHS expression — unwrapping
    /// call expressions to get the callee name (e.g. `Database()` → `Database`).
    /// For TypeFlow, promotes Def values with return_type to Value::Type.
    fn resolve_binding_value(
        &mut self,
        node: &Node<StrDoc<SupportLang>>,
        binding_rule: &super::rules::BindingRule,
    ) -> Value {
        if let Some(value_field) = binding_rule.value_field {
            if let Some(value_node) = node.field(value_field) {
                let name = self.extract_rhs_name(&value_node);
                if let Some(name) = name {
                    let reaching = self.ssa.read_variable_stateless(&name, self.current_block);
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
        } else {
            Value::Opaque
        }
    }

    /// Extract the resolvable name from an RHS expression node.
    ///
    /// Unwraps call expressions to find the callee:
    /// - `Database()` → `"Database"` (call whose function is an identifier)
    /// - `foo` → `"foo"` (bare identifier)
    /// - `a + b` → `None` (not a simple name)
    ///
    /// Uses the language spec's reference rules and chain config to
    /// identify calls and identifiers — no hardcoded node kinds.
    fn extract_rhs_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        let kind = node.kind();
        let kind_ref = kind.as_ref();

        let spec = self.rules.language_spec.as_ref();

        // Check reference rules from the language spec (call expressions)
        if let Some(spec) = spec
            && let Some(ref_rule) = spec.refs.iter().find(|r| r.kind() == kind_ref)
        {
            return ref_rule.extract_name(node);
        }

        // Check if it's a known identifier node kind
        if let Some(spec) = spec
            && let Some(cc) = &spec.chain_config
            && cc.ident_kinds.contains(&kind_ref)
        {
            return Some(node.text().to_string());
        }

        None
    }

    /// For TypeFlow: if a value is Def and the definition has return_type
    /// metadata, promote to Value::Type(return_type). This allows
    /// `x = getService(); x.query()` to resolve through the return type.
    fn maybe_promote_to_type(&self, value: Value) -> Value {
        if !matches!(self.rules.chain_mode, ChainMode::TypeFlow { .. }) {
            return value;
        }
        match &value {
            Value::Def(file_idx, def_idx) if *file_idx == self.file_idx => self
                .result
                .definitions
                .get(*def_idx)
                .and_then(|d| d.metadata.as_ref())
                .and_then(|m| m.return_type.as_ref())
                .map(|rt| Value::type_of(rt))
                .unwrap_or(value),
            // Cross-file defs can't be checked here (walker only has
            // current file). The chain resolver handles cross-file
            // return_type lookup via ctx.results.
            _ => value,
        }
    }
}
