use std::sync::Arc;

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use code_graph_config::Language;
use code_graph_types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, ExpressionStep, Fqn,
};

use crate::utils::node_to_range;

use super::types::{LanguageSpec, Rule};

/// Result of a defs-only parse. Just definitions and imports.
pub struct ParsedDefs {
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
}

struct ScopeMatch {
    name: String,
    label: &'static str,
    def_kind: DefKind,
    range: crate::utils::Range,
    creates_scope: bool,
    metadata: Option<Box<DefinitionMetadata>>,
}

impl LanguageSpec {
    /// Parse source for defs+imports only. Used by Phase 1.
    pub fn parse_defs_only(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
    ) -> crate::Result<ParsedDefs> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = language.parse_ast(source_str);
        let root = ast.root();
        let sep = language.fqn_separator();

        let mut defs = Vec::new();
        let mut imports = Vec::new();
        let mut scope_stack: Vec<Arc<str>> = Vec::new();
        let mut import_map = rustc_hash::FxHashMap::default();

        if let Some(f) = self.hooks.module_scope
            && let Some(module) = f(file_path, sep)
        {
            scope_stack.push(Arc::from(module.as_str()));
        }

        let top_level_depth = scope_stack.len();
        self.walk_defs_only(
            &root,
            &mut scope_stack,
            top_level_depth,
            &mut defs,
            &mut imports,
            &mut import_map,
            sep,
        );

        Ok(ParsedDefs {
            definitions: defs,
            imports,
        })
    }

    /// Lightweight walk: only scope + import rules. No refs, bindings, or
    /// control flow. Used by `parse_defs_only` for Phase 1.
    #[allow(clippy::too_many_arguments)]
    fn walk_defs_only(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        scope_stack: &mut Vec<Arc<str>>,
        top_level_depth: usize,
        defs: &mut Vec<CanonicalDefinition>,
        imports: &mut Vec<CanonicalImport>,
        import_map: &mut rustc_hash::FxHashMap<String, String>,
        sep: &'static str,
    ) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
            return;
        }

        let node_kind = node.kind();
        let node_kind_ref = node_kind.as_ref();
        let mut pushed_scope = false;

        if let Some((pkg_kind, ref pkg_extract)) = self.package_node
            && node_kind_ref == pkg_kind
            && let Some(name) = pkg_extract.extract_name(node)
        {
            scope_stack.push(Arc::from(name.as_str()));
        }

        if let Some(m) = self.evaluate_scope(node, node_kind_ref, import_map, sep) {
            let is_top_level = scope_stack.len() <= top_level_depth;

            if m.creates_scope {
                scope_stack.push(Arc::from(m.name.as_str()));
                pushed_scope = true;
            }

            let fqn = if m.creates_scope {
                Fqn::from_parts(
                    &scope_stack.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
                    sep,
                )
            } else {
                Fqn::from_scope(scope_stack, &m.name, sep)
            };

            defs.push(CanonicalDefinition {
                definition_type: m.label,
                kind: m.def_kind,
                name: m.name,
                fqn,
                range: canonical_range(&m.range),
                is_top_level,
                metadata: m.metadata,
            });
        }

        let custom_scope_handled = self
            .hooks
            .on_scope
            .is_some_and(|f| f(node, defs, scope_stack, sep));

        if !custom_scope_handled {
            let import_count_before = imports.len();
            let handled = self.hooks.on_import.is_some_and(|f| f(node, imports));
            if !handled {
                self.evaluate_imports(node, node_kind_ref, imports);
            }
            for imp in &imports[import_count_before..] {
                if !imp.wildcard && !imp.path.is_empty() {
                    let name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
                    if !name.is_empty() {
                        import_map.insert(name.to_string(), format!("{}{}{}", imp.path, sep, name));
                    }
                }
            }
        }

        for child in node.children() {
            self.walk_defs_only(
                &child,
                scope_stack,
                top_level_depth,
                defs,
                imports,
                import_map,
                sep,
            );
        }

        if pushed_scope {
            scope_stack.pop();
        }
    }

    fn evaluate_scope(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
        import_map: &rustc_hash::FxHashMap<String, String>,
        sep: &'static str,
    ) -> Option<ScopeMatch> {
        let indices = self.scope_dispatch.get(node_kind)?;
        let rule = indices
            .iter()
            .rev()
            .map(|&i| &self.scopes[i])
            .find(|r| r.condition().is_none_or(|c| c.test(node)))?;

        let name = rule.extract_name(node)?;
        Some(ScopeMatch {
            name,
            label: rule.resolve_label(node),
            def_kind: rule.resolve_def_kind(),
            range: node_to_range(node),
            creates_scope: rule.creates_scope,
            metadata: rule.extract_metadata(node, import_map, sep),
        })
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
        import_map: &rustc_hash::FxHashMap<String, String>,
        sep: &str,
    ) -> Option<(String, crate::utils::Range, Option<Vec<ExpressionStep>>)> {
        let indices = self.ref_dispatch.get(node_kind)?;
        let rule = indices
            .iter()
            .map(|&i| &self.refs[i])
            .find(|r| r.condition().is_none_or(|c| c.test(node)))?;
        let name = rule.extract_name(node)?;

        // Build expression chain if the rule declares an object field
        // and the spec has a ChainConfig
        let expression = rule
            .receiver_extract
            .as_ref()
            .zip(self.chain_config.as_ref())
            .and_then(|(extract, cc)| {
                let receiver_node = extract.resolve(node)?;
                let mut chain = Vec::new();
                self.build_expression_chain(&receiver_node, &mut chain, cc, import_map, sep);
                chain.push(ExpressionStep::Call(name.clone()));
                if chain.len() > 1 { Some(chain) } else { None }
            });

        Some((name, node_to_range(node), expression))
    }

    /// Recursively walk a receiver expression, building the chain
    /// from innermost (base) to outermost (final call).
    /// All node kind recognition is driven by `ChainConfig`.
    /// Type names in `New` steps are resolved via `import_map`.
    fn build_expression_chain(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        chain: &mut Vec<ExpressionStep>,
        cc: &crate::dsl::types::ChainConfig,
        import_map: &rustc_hash::FxHashMap<String, String>,
        sep: &str,
    ) {
        let kind = node.kind();
        let kind_ref = kind.as_ref();

        // Identifier base
        if cc.ident_kinds.contains(&kind_ref) {
            chain.push(ExpressionStep::Ident(node.text().to_string()));
            return;
        }

        // this/self
        if cc.this_kinds.contains(&kind_ref) {
            chain.push(ExpressionStep::This);
            return;
        }

        // super
        if cc.super_kinds.contains(&kind_ref) {
            chain.push(ExpressionStep::Super);
            return;
        }

        // Constructor (new Foo())
        for &(ctor_kind, type_field) in cc.constructor {
            if kind_ref == ctor_kind {
                if let Some(type_node) = node.field(type_field) {
                    let bare = type_node.text().to_string();
                    let resolved =
                        crate::dsl::extractors::resolve_type_via_map(&bare, import_map, sep);
                    chain.push(ExpressionStep::New(resolved));
                }
                return;
            }
        }

        // Field access (obj.field)
        for &(fa_kind, obj_field, member_field) in cc.field_access {
            if kind_ref == fa_kind {
                if let Some(obj) = node.field(obj_field) {
                    self.build_expression_chain(&obj, chain, cc, import_map, sep);
                }
                if let Some(field) = node.field(member_field) {
                    chain.push(ExpressionStep::Field(field.text().to_string()));
                }
                return;
            }
        }

        // Call expression with object field (method_invocation, call_expression)
        if let Some(&rule_idx) = self.ref_dispatch.get(kind_ref).and_then(|v| v.first()) {
            let rule = &self.refs[rule_idx];
            if let Some(extract) = &rule.receiver_extract
                && let Some(recv) = extract.resolve(node)
            {
                self.build_expression_chain(&recv, chain, cc, import_map, sep);
            }
            if let Some(name) = rule.extract_name(node) {
                chain.push(ExpressionStep::Call(name));
            }
            return;
        }

        // Fallback: treat as identifier
        let text = node.text().to_string();
        if !text.is_empty() {
            chain.push(ExpressionStep::Ident(text));
        }
    }

    fn evaluate_imports(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
        imports: &mut Vec<CanonicalImport>,
    ) {
        let Some(indices) = self.import_dispatch.get(node_kind) else {
            return;
        };
        let Some(rule) = indices
            .iter()
            .map(|&i| &self.imports[i])
            .find(|r| r.condition().is_none_or(|c| c.test(node)))
        else {
            return;
        };

        let range = canonical_range(&node_to_range(node));
        let label = rule.resolve_label(node);

        if let Some(child_kinds) = rule.multi_child_kinds {
            let base_path = rule.extract_name(node).unwrap_or_default();
            let alias_kind = rule.alias_child_kind;

            for child in node.children() {
                let ck = child.kind();

                if alias_kind.is_some_and(|ak| ak == ck.as_ref()) {
                    if let Some(name_node) = child.field("name") {
                        let alias = child.field("alias").map(|a| a.text().to_string());
                        imports.push(CanonicalImport {
                            import_type: label,
                            path: base_path.clone(),
                            name: Some(name_node.text().to_string()),
                            alias,
                            scope_fqn: None,
                            range,
                            wildcard: false,
                        });
                    }
                } else if child_kinds.iter().any(|&k| k == ck.as_ref()) {
                    let child_text = child.text().to_string();
                    if !base_path.is_empty() && child_text == base_path {
                        continue;
                    }
                    let (path, name) = if base_path.is_empty() {
                        (child_text, None)
                    } else {
                        (base_path.clone(), Some(child_text))
                    };
                    imports.push(CanonicalImport {
                        import_type: label,
                        path,
                        name,
                        alias: None,
                        scope_fqn: None,
                        range,
                        wildcard: false,
                    });
                } else if rule.wildcard_child_kind.is_some_and(|wk| wk == ck.as_ref()) {
                    imports.push(CanonicalImport {
                        import_type: label,
                        path: base_path.clone(),
                        name: Some(rule.wildcard_symbol.to_string()),
                        alias: None,
                        scope_fqn: None,
                        range,
                        wildcard: true,
                    });
                }
            }
        } else if let Some(full_path) = rule.extract_name(node) {
            // Check for wildcard child (e.g. `asterisk` in `import com.example.*`).
            let has_wildcard_child = rule
                .wildcard_child_kind
                .is_some_and(|wk| node.children().any(|c| c.kind().as_ref() == wk));

            if has_wildcard_child {
                // Wildcard import: path is the full extracted name, no split needed.
                imports.push(CanonicalImport {
                    import_type: label,
                    path: full_path,
                    name: None,
                    alias: None,
                    scope_fqn: None,
                    range,
                    wildcard: true,
                });
            } else {
                let (path, name) = if rule.should_split() {
                    rule.split_path_name(&full_path)
                } else {
                    (full_path, rule.extract_symbol(node))
                };
                let is_wildcard = name.as_deref() == Some(rule.wildcard_symbol);
                imports.push(CanonicalImport {
                    import_type: label,
                    path,
                    name,
                    alias: rule.extract_alias(node),
                    scope_fqn: None,
                    range,
                    wildcard: is_wildcard,
                });
            }
        }
    }

    // ── parse_full_and_resolve: single walk with SSA + inline callback ──

    /// Parse source with SSA, then call `on_ref` for each resolved reference.
    /// No intermediate collections — each ref is dispatched as soon as its
    /// reaching defs are computed.
    pub fn parse_full_and_resolve<F>(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
        mut on_ref: F,
    ) -> crate::Result<()>
    where
        F: FnMut(
            &str,                                        // name
            Option<&[code_graph_types::ExpressionStep]>, // chain
            &[code_graph_types::ssa::ParseValue],        // reaching defs
            Option<u32>,                                 // enclosing_def index
        ),
    {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = language.parse_ast(source_str);
        let root = ast.root();
        let sep = language.fqn_separator();

        let arena = bumpalo::Bump::new();
        let mut state = WalkFullState::new(&arena);

        if let Some(f) = self.hooks.module_scope
            && let Some(module) = f(file_path, sep)
        {
            state.scope_stack.push(Arc::from(module.as_str()));
        }
        state.top_level_depth = state.scope_stack.len();

        self.walk_full(&root, &mut state, sep);

        state.ssa.seal_remaining();

        // Dispatch each pending ref directly — no Vec<ReferenceEvent> materialized
        for pending in state.pending_refs.drain(..) {
            let reaching = state
                .ssa
                .read_variable_stateless(pending.ssa_key, pending.block);
            let parse_values: smallvec::SmallVec<[code_graph_types::ssa::ParseValue; 2]> = reaching
                .values
                .iter()
                .filter_map(|v| v.to_parse_value())
                .collect();
            on_ref(
                &pending.name,
                pending.chain.as_deref(),
                &parse_values,
                pending.enclosing_def,
            );
        }

        Ok(())
    }

    fn walk_full<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        state: &mut WalkFullState<'a>,
        sep: &'static str,
    ) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
            return;
        }

        let node_kind = node.kind();
        let nk = node_kind.as_ref();
        let mut pushed_scope = false;

        // Package node
        if let Some((pkg_kind, ref pkg_extract)) = self.package_node
            && nk == pkg_kind
            && let Some(name) = pkg_extract.extract_name(node)
        {
            state.scope_stack.push(Arc::from(name.as_str()));
        }

        // Scope matching → push def + optional SSA self/super writes
        if let Some(m) = self.evaluate_scope(node, nk, &state.import_map, sep) {
            let is_top_level = state.scope_stack.len() <= state.top_level_depth;
            let def_index = state.defs.len() as u32;

            if m.creates_scope {
                state.scope_stack.push(Arc::from(m.name.as_str()));
                pushed_scope = true;

                // Create new SSA block for this scope (isolates bindings)
                let new_block = state.ssa.add_block();
                state.ssa.add_predecessor(new_block, state.current_block);
                state.ssa.seal_block(new_block);
                state.saved_blocks.push(state.current_block);
                state.current_block = new_block;
            }

            let fqn = if m.creates_scope {
                Fqn::from_parts(
                    &state
                        .scope_stack
                        .iter()
                        .map(|s| s.as_ref())
                        .collect::<Vec<_>>(),
                    sep,
                )
            } else {
                Fqn::from_scope(&state.scope_stack, &m.name, sep)
            };

            let is_type_scope = m.def_kind.is_type_container();

            let def_name = m.name.clone();
            state.defs.push(CanonicalDefinition {
                definition_type: m.label,
                kind: m.def_kind,
                name: m.name,
                fqn,
                range: canonical_range(&m.range),
                is_top_level,
                metadata: m.metadata,
            });

            // Write def name to SSA in the parent block so sibling scopes can see it.
            let parent_block = if pushed_scope {
                *state.saved_blocks.last().unwrap_or(&state.current_block)
            } else {
                state.current_block
            };
            let ssa_name = state.arena.alloc_str(&def_name);
            state.ssa.write_variable(
                ssa_name,
                parent_block,
                super::ssa::SsaValue::LocalDef(def_index),
            );

            // Write self/this/super SSA variables for type scopes
            if is_type_scope {
                let scope_fqn = {
                    let parts: Vec<&str> = state.scope_stack.iter().map(|s| s.as_ref()).collect();
                    state.arena.alloc_str(&parts.join(sep))
                };
                for &self_name in self.ssa_config.self_names {
                    let name = state.arena.alloc_str(self_name);
                    state.ssa.write_variable(
                        name,
                        state.current_block,
                        super::ssa::SsaValue::Type(scope_fqn),
                    );
                }
                if let Some(super_name) = self.ssa_config.super_name
                    && let Some(meta) = &state.defs[def_index as usize].metadata
                    && let Some(super_type) = meta.super_types.first()
                {
                    let st = state.arena.alloc_str(super_type);
                    let name = state.arena.alloc_str(super_name);
                    state.ssa.write_variable(
                        name,
                        state.current_block,
                        super::ssa::SsaValue::Type(st),
                    );
                }
            }

            // Track enclosing def for references
            if m.creates_scope {
                state.enclosing_def_stack.push(def_index);
            }
        }

        // Custom scope handling (e.g. Ruby attr_accessor)
        let custom_handled = self
            .hooks
            .on_scope
            .is_some_and(|f| f(node, &mut state.defs, &state.scope_stack, sep));

        if !custom_handled {
            // Branch matching → SSA fork/join (handles own children)
            if let Some(&rule_idx) = self.branch_dispatch.get(nk).and_then(|v| v.first()) {
                self.walk_full_branch(node, rule_idx, state, sep);
                if pushed_scope {
                    state.scope_stack.pop();
                    state.enclosing_def_stack.pop();
                    if let Some(saved) = state.saved_blocks.pop() {
                        state.current_block = saved;
                    }
                }
                return;
            }

            // Loop matching → SSA header/body/exit (handles own children)
            if let Some(&rule_idx) = self.loop_dispatch.get(nk).and_then(|v| v.first()) {
                self.walk_full_loop(node, rule_idx, state, sep);
                if pushed_scope {
                    state.scope_stack.pop();
                    state.enclosing_def_stack.pop();
                    if let Some(saved) = state.saved_blocks.pop() {
                        state.current_block = saved;
                    }
                }
                return;
            }

            // Import handling → also write to SSA
            let import_count_before = state.imports.len();
            let handled = self
                .hooks
                .on_import
                .is_some_and(|f| f(node, &mut state.imports));
            if !handled {
                self.evaluate_imports(node, nk, &mut state.imports);
            }
            for idx in import_count_before..state.imports.len() {
                let imp = &state.imports[idx];
                let import_idx = idx as u32;
                if !imp.wildcard && !imp.path.is_empty() {
                    let effective_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
                    if !effective_name.is_empty() {
                        state.import_map.insert(
                            effective_name.to_string(),
                            format!("{}{}{}", imp.path, sep, effective_name),
                        );
                        // Write import to SSA so alias chasing finds it
                        let ssa_name = state.arena.alloc_str(effective_name);
                        state.ssa.write_variable(
                            ssa_name,
                            state.current_block,
                            super::ssa::SsaValue::ImportRef(import_idx),
                        );
                    }
                }
            }

            // Binding handling → SSA write
            if let Some(&rule_idx) = self.binding_dispatch.get(nk).and_then(|v| v.first()) {
                let rule = &self.bindings[rule_idx];
                if let Some(name) = rule.extract_name(node) {
                    let val = if let Some(type_ann) = rule.extract_type_annotation(node) {
                        // Type annotation → Type(bare_name), matching walker behavior
                        super::ssa::SsaValue::Type(state.arena.alloc_str(&type_ann))
                    } else if let Some(rhs_name) = rule.extract_rhs_name(node, self) {
                        // RHS callee name → Alias for SSA copy propagation
                        super::ssa::SsaValue::Alias(state.arena.alloc_str(&rhs_name))
                    } else {
                        super::ssa::SsaValue::Opaque
                    };

                    let ssa_name = state.arena.alloc_str(&name);
                    state.ssa.write_variable(ssa_name, state.current_block, val);
                }
            }

            // Reference handling → SSA read → PendingRef
            if let Some((name, _range, expression)) =
                self.evaluate_reference(node, nk, &state.import_map, sep)
            {
                // For chains, read SSA for the base identifier (not the terminal).
                // For bare refs, read SSA for the name itself.
                let ssa_key = if let Some(chain) = &expression {
                    match chain.first() {
                        Some(ExpressionStep::Ident(base) | ExpressionStep::Call(base)) => {
                            state.arena.alloc_str(base)
                        }
                        Some(ExpressionStep::This) => self
                            .ssa_config
                            .self_names
                            .first()
                            .map(|&s| state.arena.alloc_str(s))
                            .unwrap_or(state.arena.alloc_str(&name)),
                        Some(ExpressionStep::Super) => self
                            .ssa_config
                            .super_name
                            .map(|s| state.arena.alloc_str(s))
                            .unwrap_or(state.arena.alloc_str(&name)),
                        Some(ExpressionStep::New(type_name)) => state.arena.alloc_str(type_name),
                        _ => state.arena.alloc_str(&name),
                    }
                } else {
                    state.arena.alloc_str(&name)
                };

                state.pending_refs.push(PendingRef {
                    name,
                    chain: expression,
                    ssa_key,
                    block: state.current_block,
                    enclosing_def: state.enclosing_def_stack.last().copied(),
                });
            }
        }

        // Recurse children
        for child in node.children() {
            self.walk_full(&child, state, sep);
        }

        if pushed_scope {
            state.scope_stack.pop();
            state.enclosing_def_stack.pop();
            if let Some(saved) = state.saved_blocks.pop() {
                state.current_block = saved;
            }
        }
    }

    fn walk_full_branch<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        rule_idx: usize,
        state: &mut WalkFullState<'a>,
        sep: &'static str,
    ) {
        let rule = &self.branches[rule_idx];
        let pre_block = state.current_block;

        // Walk condition in pre-branch block
        if let Some(cond_field) = rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_full(&cond_node, state, sep);
        }

        let has_catch_all = rule
            .catch_all_kind
            .is_some_and(|ck| node.children().any(|c| c.kind().as_ref() == ck));

        // Identify condition byte range to skip (already walked above)
        let cond_range = rule
            .condition_field
            .and_then(|f| node.field(f))
            .map(|n| (n.range().start, n.range().end));

        let mut end_blocks = smallvec::SmallVec::<[super::ssa::BlockId; 4]>::new();

        for child in node.children() {
            let ck = child.kind();
            if rule.branch_kinds.iter().any(|&k| k == ck.as_ref()) {
                let arm_block = state.ssa.add_block();
                state.ssa.add_predecessor(arm_block, pre_block);
                state.ssa.seal_block(arm_block);
                state.current_block = arm_block;

                // Walk arm contents
                for arm_child in child.children() {
                    self.walk_full(&arm_child, state, sep);
                }

                end_blocks.push(state.current_block);
            } else {
                // Non-branch child: walk in pre-block (skip condition, already walked)
                let cs = child.range().start;
                let ce = child.range().end;
                let is_condition = cond_range.is_some_and(|(s, e)| cs >= s && ce <= e);
                if !is_condition {
                    state.current_block = pre_block;
                    self.walk_full(&child, state, sep);
                }
            }
        }

        // Join block
        let join = state.ssa.add_block();
        for &end in &end_blocks {
            state.ssa.add_predecessor(join, end);
        }
        if !has_catch_all {
            state.ssa.add_predecessor(join, pre_block);
        }
        state.ssa.seal_block(join);
        state.current_block = join;
    }

    fn walk_full_loop<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        rule_idx: usize,
        state: &mut WalkFullState<'a>,
        sep: &'static str,
    ) {
        let rule = &self.loops[rule_idx];
        let pre_block = state.current_block;

        // Walk iteration expression in pre-loop block
        if let Some(iter_field) = rule.iter_field
            && let Some(iter_node) = node.field(iter_field)
        {
            self.walk_full(&iter_node, state, sep);
        }

        // Header block (NOT sealed yet — back edge comes after body)
        let header = state.ssa.add_block();
        state.ssa.add_predecessor(header, pre_block);
        state.current_block = header;

        // Body block
        let body = state.ssa.add_block();
        state.ssa.add_predecessor(body, header);
        state.ssa.seal_block(body);
        state.current_block = body;

        // Walk body contents
        if let Some(body_node) = node.field(rule.body_field) {
            self.walk_full(&body_node, state, sep);
        } else {
            // No explicit body field — walk all children
            for child in node.children() {
                self.walk_full(&child, state, sep);
            }
        }

        // Back edge + seal header
        state.ssa.add_predecessor(header, state.current_block);
        state.ssa.seal_block(header);

        // Exit block
        let exit = state.ssa.add_block();
        state.ssa.add_predecessor(exit, header);
        state.ssa.seal_block(exit);
        state.current_block = exit;
    }
}

// ── Walk state for parse_full ───────────────────────────────────

/// A reference whose SSA reaching defs haven't been resolved yet.
/// Stored during the walk, resolved after seal_remaining().
struct PendingRef<'a> {
    name: String,
    chain: Option<Vec<ExpressionStep>>,
    ssa_key: &'a str,
    block: super::ssa::BlockId,
    enclosing_def: Option<u32>,
}

struct WalkFullState<'a> {
    ssa: super::ssa::SsaEngine<'a>,
    arena: &'a bumpalo::Bump,
    current_block: super::ssa::BlockId,
    scope_stack: Vec<Arc<str>>,
    enclosing_def_stack: Vec<u32>,
    defs: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
    pending_refs: Vec<PendingRef<'a>>,
    saved_blocks: Vec<super::ssa::BlockId>,
    import_map: rustc_hash::FxHashMap<String, String>,
    top_level_depth: usize,
}

impl<'a> WalkFullState<'a> {
    fn new(arena: &'a bumpalo::Bump) -> Self {
        let mut ssa = super::ssa::SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);

        Self {
            ssa,
            arena,
            current_block: entry,
            scope_stack: Vec::new(),
            enclosing_def_stack: Vec::new(),
            defs: Vec::new(),
            imports: Vec::new(),
            pending_refs: Vec::new(),
            saved_blocks: Vec::new(),
            import_map: rustc_hash::FxHashMap::default(),
            top_level_depth: 0,
        }
    }
}

fn canonical_range(r: &crate::utils::Range) -> code_graph_types::Range {
    code_graph_types::Range::new(
        code_graph_types::Position::new(r.start.line, r.start.column),
        code_graph_types::Position::new(r.end.line, r.end.column),
        r.byte_offset,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::extractors::field;
    use crate::dsl::predicates::*;
    use crate::dsl::types::*;

    fn parse_with(spec: &LanguageSpec, code: &str) -> ParsedDefs {
        spec.parse_defs_only(code.as_bytes(), "test.py", Language::Python)
            .unwrap()
    }

    #[test]
    fn scope_matching_and_fqn() {
        let spec = LanguageSpec::new(
            "test",
            vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "Method").when(grandparent_is("class_definition")),
            ],
            vec![],
            vec![],
        );
        let result = parse_with(&spec, "class A:\n    def b(self): pass\ndef c(): pass");

        assert_eq!(result.definitions.len(), 3);

        let b = result.definitions.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type, "Method");
        assert_eq!(b.fqn.to_string(), "A.b");

        let c = result.definitions.iter().find(|d| d.name == "c").unwrap();
        assert_eq!(c.definition_type, "Function");
        assert_eq!(c.fqn.to_string(), "c");
    }

    #[test]
    fn reference_extraction() {
        let spec = LanguageSpec::new(
            "test",
            vec![scope("function_definition", "Function")],
            vec![reference("call").name_from(field("function"))],
            vec![],
        );
        let mut ref_names = Vec::new();
        spec.parse_full_and_resolve(
            b"def foo(): pass\nfoo()",
            "test.py",
            Language::Python,
            |name, _chain, _reaching, _enclosing| {
                ref_names.push(name.to_string());
            },
        )
        .unwrap();

        assert_eq!(ref_names.len(), 1);
        assert_eq!(ref_names[0], "foo");
    }

    #[test]
    fn no_scope_definition() {
        let spec = LanguageSpec::new(
            "test",
            vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "FlatMethod")
                    .when(grandparent_is("class_definition"))
                    .no_scope(),
            ],
            vec![],
            vec![],
        );
        let result = parse_with(&spec, "class A:\n    def method(self): pass");

        let method = result
            .definitions
            .iter()
            .find(|d| d.name == "method")
            .unwrap();
        assert_eq!(method.fqn.to_string(), "A.method");
        assert_eq!(method.definition_type, "FlatMethod");
    }

    #[test]
    fn conditional_scope_rules() {
        let spec = LanguageSpec::new(
            "test",
            vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "Method").when(grandparent_is("class_definition")),
            ],
            vec![],
            vec![],
        );
        let result = parse_with(&spec, "class A:\n    def b(self): pass\ndef c(): pass");

        assert_eq!(result.definitions.len(), 3);

        let a = result.definitions.iter().find(|d| d.name == "A").unwrap();
        assert_eq!(a.definition_type, "Class");

        let b = result.definitions.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type, "Method");

        let c = result.definitions.iter().find(|d| d.name == "c").unwrap();
        assert_eq!(c.definition_type, "Function");
    }
}
