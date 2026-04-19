use std::sync::Arc;

use treesitter_visit::Axis;
use treesitter_visit::Match;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::config::Language;
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, ExpressionStep, Fqn,
};

use crate::utils::node_to_range;

use super::types::{LanguageSpec, Rule};

/// Complete analysis of a single source file — defs, imports, refs, and
/// inferred return types. Produced by `LanguageSpec::analyze()`.
///
/// This is the single output of one parse. The pipeline uses `defs` and
/// `imports` for graph construction, then `refs` for resolution.
pub struct FileAnalysis {
    pub defs: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub refs: Vec<AnalyzedRef>,
    pub inferred_returns: Vec<(u32, String)>,
}

/// A reference with its SSA reaching definitions already computed.
/// Ready for cross-file resolution — no re-parsing needed.
pub struct AnalyzedRef {
    pub name: String,
    pub chain: Option<Vec<ExpressionStep>>,
    pub reaching: smallvec::SmallVec<[crate::v2::types::ssa::ParseValue; 2]>,
    pub enclosing_def: Option<u32>,
    pub is_return: bool,
}

struct ScopeMatch {
    name: String,
    label: &'static str,
    def_kind: DefKind,
    range: crate::utils::Range,
    creates_scope: bool,
    metadata: Option<Box<DefinitionMetadata>>,
    adopt_siblings: &'static [&'static str],
}

impl LanguageSpec {
    fn evaluate_scope(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        actions: &[super::types::ActionRef],
        resolve: impl Fn(String, &Node<StrDoc<SupportLang>>) -> String,
    ) -> Option<ScopeMatch> {
        let rule = actions
            .iter()
            .rev()
            .filter_map(|a| match a {
                super::types::ActionRef::Scope(i) => Some(&self.scopes[*i]),
                _ => None,
            })
            .find(|r| r.condition().is_none_or(|c| c.test(node)))?;

        let name = rule.extract().apply(node)?;
        Some(ScopeMatch {
            name,
            label: rule.resolve_label(node),
            def_kind: rule.resolve_def_kind(),
            range: node_to_range(node),
            creates_scope: rule.creates_scope,
            metadata: rule.extract_metadata(node, &resolve),
            adopt_siblings: rule.adopt_siblings,
        })
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        actions: &[super::types::ActionRef],
        import_map: &rustc_hash::FxHashMap<String, String>,
        module_prefix: Option<&str>,
        sep: &str,
    ) -> Option<(String, crate::utils::Range, Option<Vec<ExpressionStep>>)> {
        let rule = actions
            .iter()
            .filter_map(|a| match a {
                super::types::ActionRef::Ref(i) => Some(&self.refs[*i]),
                _ => None,
            })
            .find(|r| r.condition().is_none_or(|c| c.test(node)))?;
        let name = rule.extract().apply(node)?;

        // Build expression chain if the rule declares an object field
        // and the spec has a ChainConfig
        let expression = rule
            .receiver_extract
            .as_ref()
            .zip(self.chain_config.as_ref())
            .and_then(|(extract, cc)| {
                let receiver_node = extract.navigate(node)?;
                let mut chain = Vec::new();
                self.build_expression_chain(
                    &receiver_node,
                    &mut chain,
                    cc,
                    import_map,
                    module_prefix,
                    sep,
                );
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
        cc: &super::types::ChainConfig,
        import_map: &rustc_hash::FxHashMap<String, String>,
        module_prefix: Option<&str>,
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
                    let resolved = if let Some(fqn) = import_map.get(&bare) {
                        fqn.clone()
                    } else if let Some(prefix) = module_prefix {
                        format!("{prefix}{sep}{bare}")
                    } else {
                        bare
                    };
                    chain.push(ExpressionStep::New(resolved));
                }
                return;
            }
        }

        // Field access (obj.field)
        for &(fa_kind, obj_field, member_field) in cc.field_access {
            if kind_ref == fa_kind {
                // Named field lookup. Falls back to child-of-kind for grammars
                // without named fields (e.g. Kotlin navigation_expression).
                let obj = node.field(obj_field);
                let member = node
                    .field(member_field)
                    .or_else(|| node.child_of_kind(member_field));

                if let Some(obj) = obj {
                    self.build_expression_chain(&obj, chain, cc, import_map, module_prefix, sep);
                } else if let Some(ref member_node) = member {
                    // No named field for the object — use the first named child
                    // that isn't the member node.
                    let mr = member_node.range();
                    if let Some(obj) = node.children().find(|c| c.is_named() && c.range() != mr) {
                        self.build_expression_chain(
                            &obj,
                            chain,
                            cc,
                            import_map,
                            module_prefix,
                            sep,
                        );
                    }
                }
                if let Some(field) = member {
                    // Use default_name to extract the identifier from wrapper
                    // nodes like navigation_suffix (skip the "." punctuation).
                    let name = treesitter_visit::extract::default_name()
                        .apply(&field)
                        .unwrap_or_else(|| field.text().to_string());
                    chain.push(ExpressionStep::Field(name));
                }
                return;
            }
        }

        // Call expression with object field (method_invocation, call_expression)
        if let Some(rule_idx) = self.dispatch.get(kind_ref).and_then(|acts| {
            acts.iter().find_map(|a| match a {
                super::types::ActionRef::Ref(i) => Some(*i),
                _ => None,
            })
        }) {
            let rule = &self.refs[rule_idx];
            if let Some(extract) = &rule.receiver_extract
                && let Some(recv) = extract.navigate(node)
            {
                self.build_expression_chain(&recv, chain, cc, import_map, module_prefix, sep);
            }
            if let Some(name) = rule.extract().apply(node) {
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
        actions: &[super::types::ActionRef],
        imports: &mut Vec<CanonicalImport>,
        module_scope: Option<&str>,
        sep: &str,
    ) {
        let Some(rule) = actions
            .iter()
            .filter_map(|a| match a {
                super::types::ActionRef::Import(i) => Some(&self.imports[*i]),
                _ => None,
            })
            .find(|r| r.condition().is_none_or(|c| c.test(node)))
        else {
            return;
        };

        let range = canonical_range(&node_to_range(node));
        let label = rule.resolve_label(node);

        if let Some(child_kinds) = rule.multi_child_kinds {
            let raw_path = rule.extract().apply(node).unwrap_or_default();
            let base_path = if let Some(resolve) = self.resolve_import_path
                && let Some(ms) = module_scope
            {
                resolve(&raw_path, ms, sep).unwrap_or(raw_path)
            } else {
                raw_path
            };
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
        } else if let Some(raw_path) = rule.extract().apply(node) {
            let full_path = if let Some(resolve) = self.resolve_import_path
                && let Some(ms) = module_scope
            {
                resolve(&raw_path, ms, sep).unwrap_or(raw_path)
            } else {
                raw_path
            };
            // Check for wildcard child (e.g. `asterisk` in `import com.example.*`).
            let has_wildcard_child = rule
                .wildcard_child_kind
                .is_some_and(|wk| node.has(Axis::Child, Match::Kind(wk)));

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

    // ── analyze: single parse, data out ───────────────────────────

    /// Parse source once. Returns definitions, imports, analyzed references
    /// (with SSA reaching defs), and inferred return types.
    ///
    /// Single-pass replacement for the former `parse_defs_only` + `parse_full_and_resolve`.
    pub fn analyze(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
    ) -> crate::legacy::parser::Result<FileAnalysis> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::legacy::parser::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = language.parse_ast(source_str);
        let root = ast.root();
        let sep = language.fqn_separator();

        let arena = bumpalo::Bump::new();
        let mut state = WalkCtx::new(&arena);

        if let Some(f) = self.hooks.module_scope
            && let Some(module) = f(file_path, sep)
        {
            state.scope_stack.push(Arc::from(module.as_str()));
        }
        state.top_level_depth = state.scope_stack.len();

        self.walk_full(&root, &mut state, sep);

        state.seal_remaining();

        // Infer return types from bare-call / bare-identifier returns.
        let pending_refs: Vec<_> = state.pending_refs.drain(..).collect();
        for pending in &pending_refs {
            if !pending.is_return || pending.chain.is_some() {
                continue;
            }
            let Some(enclosing_idx) = pending.enclosing_def else {
                continue;
            };
            if state.defs[enclosing_idx as usize]
                .metadata
                .as_ref()
                .and_then(|m| m.return_type.as_ref())
                .is_some()
            {
                continue;
            }
            let reaching = state
                .ssa
                .read_variable_stateless(pending.ssa_key, pending.block);
            let inferred = reaching.values.iter().find_map(|v| {
                let pv = v.to_parse_value()?;
                match pv {
                    crate::v2::types::ssa::ParseValue::Type(fqn) => Some(fqn),
                    crate::v2::types::ssa::ParseValue::LocalDef(i) => state
                        .defs
                        .get(i as usize)
                        .map(|d| d.fqn.as_str().to_string()),
                    crate::v2::types::ssa::ParseValue::ImportRef(i) => {
                        state.imports.get(i as usize).and_then(|imp| {
                            let name = imp.name.as_deref()?;
                            state
                                .import_map
                                .get(name)
                                .cloned()
                                .or_else(|| Some(name.to_string()))
                        })
                    }
                    crate::v2::types::ssa::ParseValue::Opaque => None,
                }
            });
            if let Some(rt) = inferred {
                state.defs[enclosing_idx as usize]
                    .metadata
                    .get_or_insert_with(Box::default)
                    .return_type = Some(rt);
            }
        }

        // Collect inferred return types
        let inferred_returns: Vec<(u32, String)> = state
            .defs
            .iter()
            .enumerate()
            .filter_map(|(i, def)| {
                def.metadata
                    .as_ref()?
                    .return_type
                    .as_ref()
                    .map(|rt| (i as u32, rt.clone()))
            })
            .collect();

        // Build analyzed refs with reaching defs
        let refs: Vec<AnalyzedRef> = pending_refs
            .iter()
            .map(|pending| {
                let reaching = state
                    .ssa
                    .read_variable_stateless(pending.ssa_key, pending.block);
                let mut parse_values: smallvec::SmallVec<[crate::v2::types::ssa::ParseValue; 2]> =
                    reaching
                        .values
                        .iter()
                        .filter_map(|v| v.to_parse_value())
                        .collect();

                // Instance attr rewrite: check compound SSA keys for self.field patterns
                let mut chain_owned: Option<Vec<ExpressionStep>> = pending.chain.clone();
                if let Some(chain) = &chain_owned
                    && chain.len() >= 3
                    && parse_values
                        .iter()
                        .any(|v| matches!(v, crate::v2::types::ssa::ParseValue::Type(_)))
                {
                    let field_steps: Vec<usize> = chain[1..]
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| {
                            if matches!(s, ExpressionStep::Field(_)) {
                                Some(i + 1)
                            } else {
                                None
                            }
                        })
                        .collect();

                    for &field_idx in field_steps.iter().rev() {
                        let mut compound = pending.ssa_key.to_string();
                        for step in &chain[1..=field_idx] {
                            if let ExpressionStep::Field(name) = step {
                                compound.push('.');
                                compound.push_str(name);
                            }
                        }
                        let key = state.alloc(&compound);
                        let r = state.ssa.read_variable_stateless(key, pending.block);
                        let compound_values: smallvec::SmallVec<
                            [crate::v2::types::ssa::ParseValue; 2],
                        > = r.values.iter().filter_map(|v| v.to_parse_value()).collect();
                        if !compound_values.is_empty()
                            && !compound_values
                                .iter()
                                .all(|v| matches!(v, crate::v2::types::ssa::ParseValue::Opaque))
                        {
                            parse_values = compound_values;
                            let remaining = &chain[field_idx + 1..];
                            chain_owned = if remaining.len() <= 1 {
                                None
                            } else {
                                Some(remaining.to_vec())
                            };
                            break;
                        }
                    }
                }

                AnalyzedRef {
                    name: pending.name.clone(),
                    chain: chain_owned,
                    reaching: parse_values,
                    enclosing_def: pending.enclosing_def,
                    is_return: pending.is_return,
                }
            })
            .collect();

        Ok(FileAnalysis {
            defs: state.defs,
            imports: state.imports,
            refs,
            inferred_returns,
        })
    }

    fn walk_full<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        state: &mut WalkCtx<'a>,
        sep: &'static str,
    ) {
        if stacker::remaining_stack().unwrap_or(usize::MAX)
            < crate::legacy::parser::MINIMUM_STACK_REMAINING
        {
            return;
        }

        let node_kind = node.kind();
        let nk = node_kind.as_ref();
        let mut pushed_scope = false;

        // Package node
        if let Some((pkg_kind, ref pkg_extract)) = self.package_node
            && nk == pkg_kind
            && let Some(name) = pkg_extract.apply(node)
        {
            state.scope_stack.push(Arc::from(name.as_str()));
        }

        // Module-level scope prefix for FQN resolution (package/module, not class/method)
        let module_prefix: Option<String> = if state.top_level_depth > 0 {
            Some(
                state.scope_stack[..state.top_level_depth]
                    .iter()
                    .map(|s| s.as_ref())
                    .collect::<Vec<_>>()
                    .join(sep),
            )
        } else {
            None
        };

        // Single dispatch lookup for all rule types
        let empty_actions = smallvec::SmallVec::<[super::types::ActionRef; 3]>::new();
        let actions = self.dispatch.get(nk).unwrap_or(&empty_actions);

        // Scope matching → push def + SSA writes via WalkCtx
        if let Some(m) = self.evaluate_scope(node, actions, |bare, _origin| {
            if let Some(fqn) = state.import_map.get(&bare) {
                return fqn.clone();
            }
            if let Some(prefix) = &module_prefix {
                return format!("{prefix}{sep}{bare}");
            }
            bare
        }) {
            let is_top_level = state.scope_stack.len() <= state.top_level_depth;
            let def_index = state.defs.len() as u32;

            if m.creates_scope {
                state.scope_stack.push(Arc::from(m.name.as_str()));
                pushed_scope = true;
                state.enter_scope();
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

            // Def name visible in parent block so siblings can see it
            if pushed_scope {
                state.bind_in_parent(&def_name, super::ssa::SsaValue::LocalDef(def_index));
            } else {
                state.bind(&def_name, super::ssa::SsaValue::LocalDef(def_index));
            }

            // self/this/super SSA variables for type scopes
            if is_type_scope {
                let scope_fqn = {
                    let parts: Vec<&str> = state.scope_stack.iter().map(|s| s.as_ref()).collect();
                    state.alloc(&parts.join(sep))
                };
                for &self_name in self.ssa_config.self_names {
                    state.bind(self_name, super::ssa::SsaValue::Type(scope_fqn));
                }
                if let Some(super_name) = self.ssa_config.super_name
                    && let Some(meta) = &state.defs[def_index as usize].metadata
                    && let Some(super_type) = meta.super_types.first()
                {
                    let st = state.alloc(super_type);
                    state.bind(super_name, super::ssa::SsaValue::Type(st));
                }
            }

            // Track enclosing def + adopt sibling refs
            if m.creates_scope {
                state.enclosing_def_stack.push(def_index);

                if !m.adopt_siblings.is_empty() {
                    if let Some(parent) = node.parent() {
                        for sibling in parent.children() {
                            let sk = sibling.kind();
                            if m.adopt_siblings.contains(&sk.as_ref()) {
                                if let Some(name) =
                                    treesitter_visit::extract::default_name().apply(&sibling)
                                {
                                    let ssa_key = state.alloc(&name);
                                    state.record_ref(name, None, ssa_key, Some(def_index), false);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Custom scope handling (e.g. Ruby attr_accessor)
        let custom_handled = self
            .hooks
            .on_scope
            .is_some_and(|f| f(node, &mut state.defs, &state.scope_stack, sep));

        if !custom_handled {
            // Branch matching → SSA fork/join (handles own children)
            if let Some(rule_idx) = actions.iter().find_map(|a| match a {
                super::types::ActionRef::Branch(i) => Some(*i),
                _ => None,
            }) {
                self.walk_full_branch(node, rule_idx, state, sep);
                if pushed_scope {
                    state.scope_stack.pop();
                    state.enclosing_def_stack.pop();
                    state.exit_scope();
                }
                return;
            }

            // Loop matching → SSA header/body/exit (handles own children)
            if let Some(rule_idx) = actions.iter().find_map(|a| match a {
                super::types::ActionRef::Loop(i) => Some(*i),
                _ => None,
            }) {
                self.walk_full_loop(node, rule_idx, state, sep);
                if pushed_scope {
                    state.scope_stack.pop();
                    state.enclosing_def_stack.pop();
                    state.exit_scope();
                }
                return;
            }

            // Import handling
            let import_count_before = state.imports.len();
            let handled = self
                .hooks
                .on_import
                .is_some_and(|f| f(node, &mut state.imports));
            if !handled {
                let ms = state.scope_stack.first().map(|s| s.as_ref());
                self.evaluate_imports(node, actions, &mut state.imports, ms, sep);
            }
            for idx in import_count_before..state.imports.len() {
                let import_idx = idx as u32;
                let wildcard = state.imports[idx].wildcard;
                let path_empty = state.imports[idx].path.is_empty();
                let effective_name = state.imports[idx]
                    .alias
                    .as_deref()
                    .or(state.imports[idx].name.as_deref())
                    .unwrap_or("")
                    .to_string();
                if !wildcard && !path_empty && !effective_name.is_empty() {
                    let fqn = format!("{}{}{}", state.imports[idx].path, sep, effective_name);
                    state.import_map.insert(effective_name.clone(), fqn);
                    state.bind(&effective_name, super::ssa::SsaValue::ImportRef(import_idx));
                }
            }

            // Binding handling
            if let Some(rule_idx) = actions.iter().find_map(|a| match a {
                super::types::ActionRef::Binding(i) => Some(*i),
                _ => None,
            }) {
                let rule = &self.bindings[rule_idx];
                if let Some(name) = rule.extract_name(node) {
                    let val = if let Some(type_ann) = rule.extract_type_annotation(node) {
                        let resolved = if let Some(fqn) = state.import_map.get(&type_ann) {
                            fqn.clone()
                        } else if let Some(prefix) = &module_prefix {
                            format!("{prefix}{sep}{type_ann}")
                        } else {
                            type_ann
                        };
                        super::ssa::SsaValue::Type(state.alloc(&resolved))
                    } else if let Some(rhs_name) = rule.extract_rhs_name(node, self) {
                        super::ssa::SsaValue::Alias(state.alloc(&rhs_name))
                    } else {
                        super::ssa::SsaValue::Opaque
                    };

                    let is_instance_attr = rule
                        .instance_attr_prefixes
                        .iter()
                        .any(|p| name.starts_with(p));
                    if is_instance_attr {
                        state.bind_in_parent(&name, val);
                    } else {
                        state.bind(&name, val);
                    }
                }
            }

            // Return statement context + inline return type inference
            if !self.return_kinds.is_empty() && self.return_kinds.contains(&nk) {
                state.in_return = true;

                if let Some(enclosing_idx) = state.enclosing_def_stack.last().copied()
                    && state.defs[enclosing_idx as usize]
                        .metadata
                        .as_ref()
                        .and_then(|m| m.return_type.as_ref())
                        .is_none()
                    && let Some(cc) = &self.chain_config
                    && node
                        .children()
                        .find(|c| c.is_named())
                        .is_some_and(|c| cc.ident_kinds.contains(&c.kind().as_ref()))
                    && let Some(ident) = find_first_ident(node, cc.ident_kinds)
                {
                    let reaching = state.read_reaching(&ident);
                    let inferred = reaching.values.iter().find_map(|v| {
                        let pv = v.to_parse_value()?;
                        match pv {
                            crate::v2::types::ssa::ParseValue::Type(fqn) => Some(fqn),
                            crate::v2::types::ssa::ParseValue::LocalDef(i) => state
                                .defs
                                .get(i as usize)
                                .map(|d| d.fqn.as_str().to_string()),
                            _ => None,
                        }
                    });
                    if let Some(rt) = inferred {
                        state.defs[enclosing_idx as usize]
                            .metadata
                            .get_or_insert_with(Box::default)
                            .return_type = Some(rt);
                    }
                }
            }

            // Reference handling
            if let Some((name, _range, expression)) = self.evaluate_reference(
                node,
                actions,
                &state.import_map,
                module_prefix.as_deref(),
                sep,
            ) {
                let ssa_key = if let Some(chain) = &expression {
                    match chain.first() {
                        Some(ExpressionStep::Ident(base) | ExpressionStep::Call(base)) => {
                            state.alloc(base)
                        }
                        Some(ExpressionStep::This) => self
                            .ssa_config
                            .self_names
                            .first()
                            .map(|&s| state.alloc(s))
                            .unwrap_or(state.alloc(&name)),
                        Some(ExpressionStep::Super) => self
                            .ssa_config
                            .super_name
                            .map(|s| state.alloc(s))
                            .unwrap_or(state.alloc(&name)),
                        Some(ExpressionStep::New(type_name)) => state.alloc(type_name),
                        _ => state.alloc(&name),
                    }
                } else {
                    state.alloc(&name)
                };

                state.record_ref(
                    name,
                    expression,
                    ssa_key,
                    state.enclosing_def_stack.last().copied(),
                    state.in_return,
                );
            }
        }

        // Recurse children
        for child in node.children() {
            self.walk_full(&child, state, sep);
        }

        // Clear return context after children
        if !self.return_kinds.is_empty() && self.return_kinds.contains(&nk) {
            state.in_return = false;
        }

        if pushed_scope {
            state.scope_stack.pop();
            state.enclosing_def_stack.pop();
            state.exit_scope();
        }
    }

    fn walk_full_branch<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        rule_idx: usize,
        state: &mut WalkCtx<'a>,
        sep: &'static str,
    ) {
        let rule = &self.branches[rule_idx];

        // Walk condition in pre-branch block
        if let Some(cond_field) = rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_full(&cond_node, state, sep);
        }

        let has_catch_all = rule
            .catch_all_kind
            .is_some_and(|ck| node.has(Axis::Child, Match::Kind(ck)));

        let cond_range = rule
            .condition_field
            .and_then(|f| node.field(f))
            .map(|n| (n.range().start, n.range().end));

        let mut branch = state.enter_branch();

        for child in node.children() {
            let ck = child.kind();
            if rule.branch_kinds.iter().any(|&k| k == ck.as_ref()) {
                state.enter_arm(&branch);
                for arm_child in child.children() {
                    self.walk_full(&arm_child, state, sep);
                }
                state.exit_arm(&mut branch);
            } else {
                let cs = child.range().start;
                let ce = child.range().end;
                let is_condition = cond_range.is_some_and(|(s, e)| cs >= s && ce <= e);
                if !is_condition {
                    state.in_pre_block(&branch);
                    self.walk_full(&child, state, sep);
                }
            }
        }

        state.exit_branch(branch, has_catch_all);
    }

    fn walk_full_loop<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        rule_idx: usize,
        state: &mut WalkCtx<'a>,
        sep: &'static str,
    ) {
        let rule = &self.loops[rule_idx];

        // Walk iteration expression in pre-loop block
        if let Some(iter_field) = rule.iter_field
            && let Some(iter_node) = node.field(iter_field)
        {
            self.walk_full(&iter_node, state, sep);
        }

        let loop_ctx = state.enter_loop();

        // Walk body contents
        if let Some(body_node) = node.field(rule.body_field) {
            self.walk_full(&body_node, state, sep);
        } else {
            for child in node.children() {
                self.walk_full(&child, state, sep);
            }
        }

        state.exit_loop(loop_ctx);
    }
}

// ── WalkCtx: high-level walk state with SSA abstraction ────────

/// A reference whose SSA reaching defs haven't been resolved yet.
/// Stored during the walk, resolved after seal.
struct PendingRef<'a> {
    name: String,
    chain: Option<Vec<ExpressionStep>>,
    ssa_key: &'a str,
    block: super::ssa::BlockId,
    enclosing_def: Option<u32>,
    is_return: bool,
}

/// Saved state for an in-progress branch (if/match/switch).
/// Returned by `enter_branch`, consumed by `exit_branch`.
pub(crate) struct BranchCtx {
    pre_block: super::ssa::BlockId,
    end_blocks: smallvec::SmallVec<[super::ssa::BlockId; 4]>,
}

/// Saved state for an in-progress loop (for/while).
/// Returned by `enter_loop`, consumed by `exit_loop`.
pub(crate) struct LoopCtx {
    header: super::ssa::BlockId,
}

struct WalkCtx<'a> {
    // SSA internals — hidden behind methods below
    ssa: super::ssa::SsaEngine<'a>,
    arena: &'a bumpalo::Bump,
    current_block: super::ssa::BlockId,
    saved_blocks: Vec<super::ssa::BlockId>,

    // Walk state
    scope_stack: Vec<Arc<str>>,
    enclosing_def_stack: Vec<u32>,
    defs: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
    pending_refs: Vec<PendingRef<'a>>,
    import_map: rustc_hash::FxHashMap<String, String>,
    top_level_depth: usize,
    in_return: bool,
}

impl<'a> WalkCtx<'a> {
    fn new(arena: &'a bumpalo::Bump) -> Self {
        let mut ssa = super::ssa::SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);

        Self {
            ssa,
            arena,
            current_block: entry,
            saved_blocks: Vec::new(),
            scope_stack: Vec::new(),
            enclosing_def_stack: Vec::new(),
            defs: Vec::new(),
            imports: Vec::new(),
            pending_refs: Vec::new(),
            import_map: rustc_hash::FxHashMap::default(),
            top_level_depth: 0,
            in_return: false,
        }
    }

    // ── Scope lifecycle ─────────────────────────────────────

    /// Create an isolated SSA block for a new scope (class/function).
    fn enter_scope(&mut self) {
        let new = self.ssa.add_block();
        self.ssa.add_predecessor(new, self.current_block);
        self.ssa.seal_block(new);
        self.saved_blocks.push(self.current_block);
        self.current_block = new;
    }

    /// Restore the parent scope's SSA block.
    fn exit_scope(&mut self) {
        if let Some(saved) = self.saved_blocks.pop() {
            self.current_block = saved;
        }
    }

    // ── Variable operations ─────────────────────────────────

    /// Write a variable in the current block.
    fn bind(&mut self, name: &str, value: super::ssa::SsaValue<'a>) {
        let key = self.arena.alloc_str(name);
        self.ssa.write_variable(key, self.current_block, value);
    }

    /// Write a variable in the parent scope's block.
    /// Used for instance attrs (self.x) that should be visible to sibling methods.
    fn bind_in_parent(&mut self, name: &str, value: super::ssa::SsaValue<'a>) {
        let block = *self.saved_blocks.last().unwrap_or(&self.current_block);
        let key = self.arena.alloc_str(name);
        self.ssa.write_variable(key, block, value);
    }

    /// Read reaching definitions for a key at the current block.
    /// Used for inline return-type inference during the walk.
    fn read_reaching(&mut self, key: &str) -> super::ssa::ReachingDefs<'a> {
        let k = self.arena.alloc_str(key);
        self.ssa.read_variable_stateless(k, self.current_block)
    }

    // ── Reference recording ─────────────────────────────────

    /// Allocate a string in the arena (outlives the current node).
    fn alloc(&self, s: &str) -> &'a str {
        self.arena.alloc_str(s)
    }

    /// Record a reference for post-walk resolution.
    /// Snapshots the current block automatically.
    fn record_ref(
        &mut self,
        name: String,
        chain: Option<Vec<ExpressionStep>>,
        ssa_key: &'a str,
        enclosing_def: Option<u32>,
        is_return: bool,
    ) {
        self.pending_refs.push(PendingRef {
            name,
            chain,
            ssa_key,
            block: self.current_block,
            enclosing_def,
            is_return,
        });
    }

    // ── Branch CFG (if/match/switch) ────────────────────────

    /// Start a branch. Returns a context to pass to arm/exit methods.
    fn enter_branch(&self) -> BranchCtx {
        BranchCtx {
            pre_block: self.current_block,
            end_blocks: smallvec::SmallVec::new(),
        }
    }

    /// Enter one arm of a branch.
    fn enter_arm(&mut self, branch: &BranchCtx) {
        let arm = self.ssa.add_block();
        self.ssa.add_predecessor(arm, branch.pre_block);
        self.ssa.seal_block(arm);
        self.current_block = arm;
    }

    /// Finish one arm. Records the current block as an arm endpoint.
    fn exit_arm(&mut self, branch: &mut BranchCtx) {
        branch.end_blocks.push(self.current_block);
    }

    /// Walk a non-arm child in the pre-branch block (e.g. condition already walked).
    fn in_pre_block(&mut self, branch: &BranchCtx) {
        self.current_block = branch.pre_block;
    }

    /// Close a branch: create the join block from all arm endpoints.
    fn exit_branch(&mut self, branch: BranchCtx, has_catch_all: bool) {
        let join = self.ssa.add_block();
        for &end in &branch.end_blocks {
            self.ssa.add_predecessor(join, end);
        }
        if !has_catch_all {
            self.ssa.add_predecessor(join, branch.pre_block);
        }
        self.ssa.seal_block(join);
        self.current_block = join;
    }

    // ── Loop CFG (for/while) ────────────────────────────────

    /// Start a loop: create header (unsealed) + body block.
    fn enter_loop(&mut self) -> LoopCtx {
        let header = self.ssa.add_block();
        self.ssa.add_predecessor(header, self.current_block);
        self.current_block = header;

        let body = self.ssa.add_block();
        self.ssa.add_predecessor(body, header);
        self.ssa.seal_block(body);
        self.current_block = body;

        LoopCtx { header }
    }

    /// Close a loop: add back edge, seal header, create exit block.
    fn exit_loop(&mut self, ctx: LoopCtx) {
        self.ssa.add_predecessor(ctx.header, self.current_block);
        self.ssa.seal_block(ctx.header);

        let exit = self.ssa.add_block();
        self.ssa.add_predecessor(exit, ctx.header);
        self.ssa.seal_block(exit);
        self.current_block = exit;
    }

    // ── Finalization ────────────────────────────────────────

    fn seal_remaining(&mut self) {
        self.ssa.seal_remaining();
    }
}

fn canonical_range(r: &crate::utils::Range) -> crate::v2::types::Range {
    crate::v2::types::Range::new(
        crate::v2::types::Position::new(r.start.line, r.start.column),
        crate::v2::types::Position::new(r.end.line, r.end.column),
        r.byte_offset,
    )
}

/// Find the first identifier node in an expression tree (DFS).
/// Uses the language's `ident_kinds` from chain config to detect identifiers
/// generically across languages.
fn find_first_ident(node: &Node<StrDoc<SupportLang>>, ident_kinds: &[&str]) -> Option<String> {
    node.find_descendant(|n| n.is_named() && ident_kinds.contains(&n.kind().as_ref()))
        .map(|n| n.text().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::dsl::types::*;
    use treesitter_visit::extract::field;
    use treesitter_visit::predicate::*;

    fn analyze_with(spec: &LanguageSpec, code: &str) -> FileAnalysis {
        spec.analyze(code.as_bytes(), "test.py", Language::Python)
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
        let result = analyze_with(&spec, "class A:\n    def b(self): pass\ndef c(): pass");

        assert_eq!(result.defs.len(), 3);

        let b = result.defs.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type, "Method");
        assert_eq!(b.fqn.to_string(), "A.b");

        let c = result.defs.iter().find(|d| d.name == "c").unwrap();
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
        let result = spec
            .analyze(b"def foo(): pass\nfoo()", "test.py", Language::Python)
            .unwrap();

        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
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
        let result = analyze_with(&spec, "class A:\n    def method(self): pass");

        let method = result.defs.iter().find(|d| d.name == "method").unwrap();
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
        let result = analyze_with(&spec, "class A:\n    def b(self): pass\ndef c(): pass");

        assert_eq!(result.defs.len(), 3);

        let a = result.defs.iter().find(|d| d.name == "A").unwrap();
        assert_eq!(a.definition_type, "Class");

        let b = result.defs.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type, "Method");

        let c = result.defs.iter().find(|d| d.name == "c").unwrap();
        assert_eq!(c.definition_type, "Function");
    }
}
