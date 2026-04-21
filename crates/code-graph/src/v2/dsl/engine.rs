use std::sync::Arc;

use treesitter_visit::Axis;
use treesitter_visit::Match;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::config::Language;
use crate::v2::trace::{TraceEvent, Tracer};

/// Resolve a bare or dotted type name to its FQN using import_map,
/// separator-based splitting, and module_prefix fallback.
///
/// Resolution order:
/// 1. Direct import_map lookup for the full name
/// 2. Split on separator, resolve first segment via imports, append rest
/// 3. Prepend module_prefix (same-package/module fallback)
/// 4. Return bare name unchanged
fn resolve_type_name(
    name: &str,
    import_map: &rustc_hash::FxHashMap<String, String>,
    module_prefix: Option<&str>,
    sep: &str,
) -> String {
    if let Some(fqn) = import_map.get(name) {
        return fqn.clone();
    }
    if name.contains(sep)
        && let Some((first, rest)) = name.split_once(sep)
        && let Some(fqn) = import_map.get(first)
    {
        return format!("{fqn}{sep}{rest}");
    }
    if let Some(prefix) = module_prefix {
        return format!("{prefix}{sep}{name}");
    }
    name.to_string()
}

use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, ExpressionStep, Fqn,
    ImportBindingKind, ImportMode,
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

fn infer_import_binding_kind(
    name: Option<&str>,
    alias: Option<&str>,
    wildcard: bool,
) -> ImportBindingKind {
    if wildcard {
        ImportBindingKind::Named
    } else if name.is_none() && alias.is_none() {
        ImportBindingKind::SideEffect
    } else {
        ImportBindingKind::Named
    }
}

impl LanguageSpec {
    /// Parse source for defs+imports only. Used by Phase 1.
    pub fn parse_defs_only(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
    ) -> crate::legacy::parser::Result<ParsedDefs> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::legacy::parser::Error::Parse(format!("Invalid UTF-8: {e}")))?;

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
        if stacker::remaining_stack().unwrap_or(usize::MAX)
            < crate::legacy::parser::MINIMUM_STACK_REMAINING
        {
            return;
        }

        let node_kind = node.kind();
        let node_kind_ref = node_kind.as_ref();
        let mut pushed_scope = false;

        if let Some((pkg_kind, ref pkg_extract)) = self.package_node
            && node_kind_ref == pkg_kind
            && let Some(name) = pkg_extract.apply(node)
        {
            scope_stack.push(Arc::from(name.as_str()));
        }

        let module_prefix: Option<String> = if top_level_depth > 0 {
            Some(
                scope_stack[..top_level_depth]
                    .iter()
                    .map(|s| s.as_ref())
                    .collect::<Vec<_>>()
                    .join(sep),
            )
        } else {
            None
        };
        if let Some(m) = self.evaluate_scope(node, node_kind_ref, |bare, _origin| {
            if let Some(fqn) = import_map.get(&bare) {
                return fqn.clone();
            }
            if let Some(prefix) = &module_prefix {
                return format!("{prefix}{sep}{bare}");
            }
            bare
        }) {
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
                let ms = scope_stack.first().map(|s| s.as_ref());
                self.evaluate_imports(node, node_kind_ref, imports, ms, sep);
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
        resolve: impl Fn(String, &Node<StrDoc<SupportLang>>) -> String,
    ) -> Option<ScopeMatch> {
        let indices = self.scope_dispatch.get(node_kind)?;
        let rule = indices
            .iter()
            .rev()
            .map(|&i| &self.scopes[i])
            .find(|r| r.condition().is_none_or(|c| c.test(node)))?;

        let name = rule
            .extract()
            .apply(node)
            .or_else(|| rule.default_name.map(|s| s.to_string()))?;
        Some(ScopeMatch {
            name,
            label: rule.resolve_label(node),
            def_kind: rule.resolve_def_kind(),
            range: node_to_range(node),
            creates_scope: rule.creates_scope,
            metadata: rule.extract_metadata(node, &resolve),
        })
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
        import_map: &rustc_hash::FxHashMap<String, String>,
        module_prefix: Option<&str>,
        sep: &str,
        tracer: &Tracer,
    ) -> Option<(String, crate::utils::Range, Option<Vec<ExpressionStep>>)> {
        let indices = self.ref_dispatch.get(node_kind)?;
        let rule = indices
            .iter()
            .map(|&i| &self.refs[i])
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
                    tracer,
                );
                chain.push(ExpressionStep::Call(name.clone()));
                if chain.len() > 1 { Some(chain) } else { None }
            });

        Some((name, node_to_range(node), expression))
    }

    /// Iteratively walk a receiver expression, building the chain
    /// from innermost (base) to outermost (final call).
    /// All node kind recognition is driven by `ChainConfig`.
    /// Type names in `New` steps are resolved via `import_map`.
    #[allow(clippy::too_many_arguments)]
    fn build_expression_chain(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        chain: &mut Vec<ExpressionStep>,
        cc: &super::types::ChainConfig,
        import_map: &rustc_hash::FxHashMap<String, String>,
        module_prefix: Option<&str>,
        sep: &str,
        tracer: &Tracer,
    ) {
        // Collect deferred steps (outermost first) while walking inward to
        // the base. Then push the base, then the deferred steps in reverse.
        let mut deferred: Vec<ExpressionStep> = Vec::new();
        let mut current = node.clone();

        loop {
            let kind: String = current.kind().to_string();
            let kind_ref: &str = &kind;

            // ── Terminal cases (base of the chain) ──

            // Identifier base
            if cc.ident_kinds.contains(&kind_ref) {
                let text = current.text().to_string();
                tracer.event(TraceEvent::ChainStepMatched {
                    node_kind: kind_ref.to_string(),
                    category: "Ident".to_string(),
                    text: text.clone(),
                });
                chain.push(ExpressionStep::Ident(text));
                break;
            }

            // this/self
            if cc.this_kinds.contains(&kind_ref) {
                tracer.event(TraceEvent::ChainStepMatched {
                    node_kind: kind_ref.to_string(),
                    category: "This".to_string(),
                    text: current.text().to_string(),
                });
                chain.push(ExpressionStep::This);
                break;
            }

            // super
            if cc.super_kinds.contains(&kind_ref) {
                tracer.event(TraceEvent::ChainStepMatched {
                    node_kind: kind_ref.to_string(),
                    category: "Super".to_string(),
                    text: current.text().to_string(),
                });
                chain.push(ExpressionStep::Super);
                break;
            }

            // Qualified type reference (e.g. Outer.Inner as receiver in new Outer.Inner()).
            if cc.qualified_type_kinds.contains(&kind_ref) {
                let mut segments = current.children().filter(|c| c.is_named());
                if let Some(first) = segments.next() {
                    let name = first.text().to_string();
                    let resolved = resolve_type_name(&name, import_map, module_prefix, sep);
                    tracer.event(TraceEvent::ChainStepMatched {
                        node_kind: kind_ref.to_string(),
                        category: "Ident(qualified)".to_string(),
                        text: resolved.clone(),
                    });
                    chain.push(ExpressionStep::Ident(resolved));
                    for seg in segments {
                        let seg_text = seg.text().to_string();
                        tracer.event(TraceEvent::ChainStepMatched {
                            node_kind: kind_ref.to_string(),
                            category: "Field(qualified)".to_string(),
                            text: seg_text.clone(),
                        });
                        chain.push(ExpressionStep::Field(seg_text));
                    }
                }
                break;
            }

            // Constructor (new Foo() or new Outer.Inner())
            let mut matched_ctor = false;
            for &(ctor_kind, type_field) in cc.constructor {
                if kind_ref == ctor_kind {
                    if let Some(type_node) = current.field(type_field) {
                        let tk = type_node.kind();
                        if cc.qualified_type_kinds.contains(&tk.as_ref()) {
                            let mut segments = type_node.children().filter(|c| c.is_named());
                            if let Some(first) = segments.next() {
                                let name = first.text().to_string();
                                let resolved =
                                    resolve_type_name(&name, import_map, module_prefix, sep);
                                tracer.event(TraceEvent::ChainStepMatched {
                                    node_kind: kind_ref.to_string(),
                                    category: "New(qualified)".to_string(),
                                    text: resolved.clone(),
                                });
                                chain.push(ExpressionStep::New(resolved));
                                for seg in segments {
                                    chain.push(ExpressionStep::Field(seg.text().to_string()));
                                }
                            }
                        } else {
                            let bare = type_node.text().to_string();
                            let resolved = resolve_type_name(&bare, import_map, module_prefix, sep);
                            tracer.event(TraceEvent::ChainStepMatched {
                                node_kind: kind_ref.to_string(),
                                category: "New".to_string(),
                                text: resolved.clone(),
                            });
                            chain.push(ExpressionStep::New(resolved));
                        }
                    }
                    matched_ctor = true;
                    break;
                }
            }
            if matched_ctor {
                break;
            }

            // ── Recursive cases (defer step, advance inward) ──

            // Field access (obj.field) — defer the Field step, advance to obj
            let mut matched_fa = false;
            for fa in &cc.field_access {
                if kind_ref == fa.kind {
                    if let Some(name) = fa.member.apply(&current) {
                        tracer.event(TraceEvent::ChainStepMatched {
                            node_kind: kind_ref.to_string(),
                            category: "Field".to_string(),
                            text: name.clone(),
                        });
                        deferred.push(ExpressionStep::Field(name));
                    }
                    if let Some(obj) = fa.object.navigate(&current) {
                        current = obj;
                        matched_fa = true;
                    }
                    break;
                }
            }
            if matched_fa {
                continue;
            }

            // Call expression — defer the Call step, advance to receiver
            if let Some(&rule_idx) = self.ref_dispatch.get(kind_ref).and_then(|v| v.first()) {
                let rule = &self.refs[rule_idx];
                if let Some(name) = rule.extract().apply(&current) {
                    tracer.event(TraceEvent::ChainStepMatched {
                        node_kind: kind_ref.to_string(),
                        category: "Call".to_string(),
                        text: name.clone(),
                    });
                    deferred.push(ExpressionStep::Call(name));
                }
                if let Some(extract) = &rule.receiver_extract
                    && let Some(recv) = extract.navigate(&current)
                {
                    current = recv;
                    continue;
                }
                // No receiver — this call is the base
                break;
            }

            // Fallback: treat as identifier
            let text = current.text().to_string();
            if !text.is_empty() {
                tracer.event(TraceEvent::ChainStepMatched {
                    node_kind: kind_ref.to_string(),
                    category: "Fallback".to_string(),
                    text: text.clone(),
                });
                chain.push(ExpressionStep::Ident(text));
            }
            break;
        }

        // Append deferred steps in reverse (innermost was deferred last)
        chain.extend(deferred.into_iter().rev());
    }

    fn evaluate_imports(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
        imports: &mut Vec<CanonicalImport>,
        module_scope: Option<&str>,
        sep: &str,
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
            let raw_path = rule.extract().apply(node).unwrap_or_default();
            let base_path = if let Some(resolve) = self.hooks.resolve_import_path
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
                        let name = name_node.text().to_string();
                        let binding_kind =
                            infer_import_binding_kind(Some(name.as_str()), alias.as_deref(), false);
                        imports.push(CanonicalImport {
                            import_type: label,
                            path: base_path.clone(),
                            binding_kind,
                            mode: ImportMode::Declarative,
                            name: Some(name),
                            alias,
                            scope_fqn: None,
                            range,
                            is_type_only: false,
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
                    let binding_kind = infer_import_binding_kind(name.as_deref(), None, false);
                    imports.push(CanonicalImport {
                        import_type: label,
                        binding_kind,
                        mode: ImportMode::Declarative,
                        path,
                        name,
                        alias: None,
                        scope_fqn: None,
                        range,
                        is_type_only: false,
                        wildcard: false,
                    });
                } else if rule.wildcard_child_kind.is_some_and(|wk| wk == ck.as_ref()) {
                    imports.push(CanonicalImport {
                        import_type: label,
                        binding_kind: ImportBindingKind::Named,
                        mode: ImportMode::Declarative,
                        path: base_path.clone(),
                        name: Some(rule.wildcard_symbol.to_string()),
                        alias: None,
                        scope_fqn: None,
                        range,
                        is_type_only: false,
                        wildcard: true,
                    });
                }
            }
        } else if let Some(raw_path) = rule.extract().apply(node) {
            let full_path = if let Some(resolve) = self.hooks.resolve_import_path
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
                    binding_kind: ImportBindingKind::Named,
                    mode: ImportMode::Declarative,
                    path: full_path,
                    name: None,
                    alias: None,
                    scope_fqn: None,
                    range,
                    is_type_only: false,
                    wildcard: true,
                });
            } else {
                let (path, name) = if rule.should_split() {
                    rule.split_path_name(&full_path)
                } else {
                    (full_path, rule.extract_symbol(node))
                };
                let alias = rule.extract_alias(node);
                let is_wildcard = name.as_deref() == Some(rule.wildcard_symbol);
                let binding_kind =
                    infer_import_binding_kind(name.as_deref(), alias.as_deref(), is_wildcard);
                imports.push(CanonicalImport {
                    import_type: label,
                    binding_kind,
                    mode: ImportMode::Declarative,
                    path,
                    name,
                    alias,
                    scope_fqn: None,
                    range,
                    is_type_only: false,
                    wildcard: is_wildcard,
                });
            }
        }
    }

    // ── parse_full_and_resolve: single walk with SSA + inline callback ──

    /// Parse source with SSA, then call `on_ref` for each resolved reference.
    /// No intermediate collections — each ref is dispatched as soon as its
    /// reaching defs are computed.
    ///
    /// When `graph` is provided, constructor chains (e.g. `Parent.Child.Foo()`)
    /// are resolved eagerly after sealing, and the resolved types are written
    /// back to SSA so subsequent bindings can use them.
    pub fn parse_full_and_resolve<F>(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
        on_ref: &mut F,
        tracer: &Tracer,
        graph: Option<&crate::v2::linker::graph::CodeGraph>,
    ) -> crate::legacy::parser::Result<Vec<(u32, String)>>
    where
        F: FnMut(
            &str,                                        // name
            Option<&[crate::v2::types::ExpressionStep]>, // chain
            &[crate::v2::types::ssa::ParseValue],        // reaching defs
            Option<u32>,                                 // enclosing_def index
            &[(u32, String)],                            // inferred return types
        ),
    {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::legacy::parser::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = language.parse_ast(source_str);
        let root = ast.root();
        let sep = language.fqn_separator();

        let arena = bumpalo::Bump::new();
        let mut state = WalkFullState::new(&arena, tracer);

        if let Some(f) = self.hooks.module_scope
            && let Some(module) = f(file_path, sep)
        {
            state.scope_stack.push(Arc::from(module.as_str()));
            tracer.event(TraceEvent::PackageMatched {
                name: module.clone(),
            });
        }
        state.top_level_depth = state.scope_stack.len();

        self.walk_full(&root, &mut state, sep);

        state.ssa.seal_remaining();
        state.ssa.remove_redundant_phi_sccs();

        let pending_refs: Vec<_> = state.pending_refs.drain(..).collect();

        // Pass 1: infer return types from bare-call / bare-identifier return refs.
        // Chain refs (e.g. `return foo.bar()`) are skipped — the ssa_key points
        // at the chain base, not the terminal call's return type.
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
                            // Use import_map to resolve to FQN (e.g. "UserService" → "models.UserService")
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
                tracer.event(TraceEvent::ReturnTypeInferred {
                    def_index: enclosing_idx,
                    def_fqn: state.defs[enclosing_idx as usize].fqn.as_str().to_string(),
                    return_type: rt.clone(),
                });
                state.defs[enclosing_idx as usize]
                    .metadata
                    .get_or_insert_with(Box::default)
                    .return_type = Some(rt);
            }
        }

        // Pass 1.5: eager intra-file resolution against the graph.
        //
        // After SSA is sealed, find alias targets that have no SSA value
        // (i.e. the alias can't be chased). For each, try to resolve the
        // target name via pending ref chains against the graph and write
        // the resolved type to SSA so alias chasing succeeds in Pass 2.
        if let Some(g) = graph {
            // Collect alias targets that need resolution: names referenced
            // by Alias() values but absent from SSA.
            let mut needed: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for pending in &pending_refs {
                let reaching = state
                    .ssa
                    .read_variable_stateless(pending.ssa_key, pending.block);
                for v in &reaching.values {
                    if let super::ssa::SsaValue::Alias(target) = v {
                        let target_vals = state.ssa.read_variable_stateless(target, pending.block);
                        let unresolved = target_vals.values.is_empty()
                            || target_vals.values.iter().all(|tv| {
                                matches!(
                                    tv,
                                    super::ssa::SsaValue::Opaque | super::ssa::SsaValue::Alias(_)
                                )
                            });
                        if unresolved {
                            needed.insert(target);
                        }
                    }
                }
            }

            // For each needed name, find a ref that resolves it.
            // Check both ref names (bare refs) and chain bases (chain refs
            // where the base ident matches the needed name).
            if !needed.is_empty() {
                for pending in &pending_refs {
                    // Match by ref name (bare ref) or chain base (chain ref)
                    let matched_name = if needed.contains(pending.name.as_str()) {
                        Some(pending.name.as_str())
                    } else if let Some(chain) = &pending.chain {
                        if let ExpressionStep::Ident(base) = &chain[0] {
                            if needed.contains(base.as_str()) {
                                Some(base.as_str())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let Some(name) = matched_name else {
                        continue;
                    };
                    // Resolve: for bare refs, look up the name directly.
                    // For chain bases, resolve just the base (not the full chain).
                    let resolved_fqn = {
                        let fqn = state
                            .import_map
                            .get(name)
                            .cloned()
                            .unwrap_or_else(|| name.to_string());
                        let nodes = g.resolve_scope_nodes(&fqn);
                        nodes
                            .first()
                            .filter(|&&n| g.def_kind(n).is_type_container())
                            .map(|&n| g.def_fqn(n).to_string())
                    };
                    if let Some(fqn) = resolved_fqn {
                        let key = state.arena.alloc_str(name);
                        let val = super::ssa::SsaValue::Type(state.arena.alloc_str(&fqn));
                        state.ssa.write_variable(key, pending.block, val);
                        needed.remove(name);
                    }
                }
            }
        }

        // Collect all inferred return types (from both call returns and
        // any future sources) into the sidecar for the resolver
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

        // Pass 2: dispatch refs to resolver
        for pending in &pending_refs {
            let reaching = state
                .ssa
                .read_variable_stateless(pending.ssa_key, pending.block);
            let mut parse_values: smallvec::SmallVec<[crate::v2::types::ssa::ParseValue; 2]> =
                reaching
                    .values
                    .iter()
                    .filter_map(|v| v.to_parse_value())
                    .collect();

            // Instance attr rewrite: for chains like [This, Field("db"), Call("execute")],
            // build compound SSA keys (e.g. "self.db") and check if they have a type.
            // This handles languages where instance fields are dynamic assignments
            // (Python self.x = ..., Ruby @x = ...) rather than explicit declarations.
            let mut chain_slice: Option<&[ExpressionStep]> = pending.chain.as_deref();
            if let Some(chain) = chain_slice
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
                    let key = state.arena.alloc_str(&compound);
                    let r = state.ssa.read_variable_stateless(key, pending.block);
                    let compound_values: smallvec::SmallVec<
                        [crate::v2::types::ssa::ParseValue; 2],
                    > = r.values.iter().filter_map(|v| v.to_parse_value()).collect();
                    let found = !compound_values.is_empty()
                        && !compound_values
                            .iter()
                            .all(|v| matches!(v, crate::v2::types::ssa::ParseValue::Opaque));
                    tracer.event(TraceEvent::InstanceAttrRewrite {
                        original_key: pending.ssa_key.to_string(),
                        compound_key: compound.clone(),
                        found_values: compound_values.iter().map(|v| format!("{v:?}")).collect(),
                        chain_trimmed: found,
                    });
                    if found {
                        parse_values = compound_values;
                        let remaining = &chain[field_idx + 1..];
                        chain_slice = if remaining.len() <= 1 {
                            None
                        } else {
                            Some(remaining)
                        };
                        break;
                    }
                }
            }

            on_ref(
                &pending.name,
                chain_slice,
                &parse_values,
                pending.enclosing_def,
                &inferred_returns,
            );
        }

        Ok(inferred_returns)
    }

    fn walk_full<'a>(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        state: &mut WalkFullState<'a>,
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

        // Scope matching → push def + optional SSA self/super writes
        if let Some(m) = self.evaluate_scope(node, nk, |bare, _origin| {
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
            state.tracer.event(TraceEvent::ScopePush {
                name: def_name.clone(),
                kind: format!("{:?}", m.def_kind),
                label: m.label.to_string(),
                fqn: fqn.as_str().to_string(),
                block_id: state.current_block.0,
            });
            state.defs.push(CanonicalDefinition {
                definition_type: m.label,
                kind: m.def_kind,
                name: m.name,
                fqn,
                range: canonical_range(&m.range),
                is_top_level,
                metadata: m.metadata,
            });
            // Emit def discovered with index for cross-referencing
            let last_def = &state.defs[def_index as usize];
            state.tracer.event(TraceEvent::DefDiscovered {
                name: last_def.name.clone(),
                fqn: last_def.fqn.as_str().to_string(),
                kind: format!("{:?}", last_def.kind),
                label: last_def.definition_type.to_string(),
                is_top_level,
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
                    let resolved = resolve_type_name(
                        super_type,
                        &state.import_map,
                        module_prefix.as_deref(),
                        sep,
                    );
                    let st = state.arena.alloc_str(&resolved);
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

                // Adopt sibling references: when decorators/annotations are
                // CST siblings of the scope node, emit refs attributed to
                // this def rather than the parent scope.
                if !self.hooks.adopt_sibling_refs.is_empty()
                    && let Some(parent) = node.parent()
                {
                    for sibling in parent.children() {
                        let sk = sibling.kind();
                        if sibling.range() != node.range()
                            && self.hooks.adopt_sibling_refs.contains(&sk.as_ref())
                            && let Some(name) =
                                treesitter_visit::extract::default_name().apply(&sibling)
                        {
                            state.tracer.event(TraceEvent::SiblingRefAdopted {
                                name: name.clone(),
                                def_index,
                            });
                            let ssa_key = state.arena.alloc_str(&name);
                            state.pending_refs.push(PendingRef {
                                name,
                                chain: None,
                                ssa_key,
                                block: state.current_block,
                                enclosing_def: Some(def_index),
                                is_return: false,
                            });
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

        // Expression-bodied functions: when a node like `function_body`
        // contains `=`, treat all refs within as implicit returns.
        let is_expression_body = !self.hooks.expression_body_kinds.is_empty()
            && self.hooks.expression_body_kinds.contains(&nk)
            && node.children().any(|c| c.kind().as_ref() == "=");

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
                let ms = state.scope_stack.first().map(|s| s.as_ref());
                self.evaluate_imports(node, nk, &mut state.imports, ms, sep);
            }
            for idx in import_count_before..state.imports.len() {
                let imp = &state.imports[idx];
                let import_idx = idx as u32;
                let effective_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
                state.tracer.event(TraceEvent::ImportRecorded {
                    path: imp.path.clone(),
                    name: imp.name.as_deref().unwrap_or("").to_string(),
                    alias: imp.alias.clone(),
                    wildcard: imp.wildcard,
                    ssa_name: if !imp.wildcard && !imp.path.is_empty() && !effective_name.is_empty()
                    {
                        Some(effective_name.to_string())
                    } else {
                        None
                    },
                    block_id: state.current_block.0,
                });
                if !imp.wildcard && !imp.path.is_empty() && !effective_name.is_empty() {
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

            // Binding handling → SSA write
            if let Some(&rule_idx) = self.binding_dispatch.get(nk).and_then(|v| v.first()) {
                let rule = &self.bindings[rule_idx];
                let names = rule.extract_names(node);
                for name in &names {
                    let val = if let Some(type_ann) = rule.extract_type_annotation(node) {
                        let resolved = resolve_type_name(
                            &type_ann,
                            &state.import_map,
                            module_prefix.as_deref(),
                            sep,
                        );
                        super::ssa::SsaValue::Type(state.arena.alloc_str(&resolved))
                    } else if let Some(ctor_type) = rule.extract_constructor_type(
                        node,
                        self,
                        self.ssa_config.constructor_methods,
                    ) {
                        let resolved = resolve_type_name(
                            &ctor_type,
                            &state.import_map,
                            module_prefix.as_deref(),
                            sep,
                        );
                        super::ssa::SsaValue::Type(state.arena.alloc_str(&resolved))
                    } else if let Some(rhs_name) = rule.extract_rhs_name(node, self) {
                        super::ssa::SsaValue::Alias(state.arena.alloc_str(&rhs_name))
                    } else {
                        super::ssa::SsaValue::Opaque
                    };

                    let ssa_name = state.arena.alloc_str(name);
                    let is_instance_attr = rule
                        .instance_attr_prefixes
                        .iter()
                        .any(|p| name.starts_with(p));
                    let target_block = if is_instance_attr {
                        // Write to parent class block so sibling methods can read it
                        *state.saved_blocks.last().unwrap_or(&state.current_block)
                    } else {
                        // If this variable already has a value in the current
                        // block, split: create a new block so the previous
                        // value is preserved for refs queued before this write.
                        if state
                            .ssa
                            .has_variable_in_block(ssa_name, state.current_block)
                        {
                            let new_block = state.ssa.add_block();
                            state.ssa.add_predecessor(new_block, state.current_block);
                            state.ssa.seal_block(new_block);
                            state.current_block = new_block;
                        }
                        state.current_block
                    };
                    state.tracer.event(TraceEvent::BindingWrite {
                        name: name.clone(),
                        value: val.trace_display(),
                        block_id: target_block.0,
                    });
                    state
                        .ssa
                        .write_variable(ssa_name, target_block, val.clone());

                    // Class field bindings: also write compound key (e.g. "this.myParameter")
                    // so that chains like this.myParameter.bar() can resolve via instance
                    // attr rewrite. Only fires when the binding is directly inside a type
                    // container scope (not nested inside a method).
                    if !is_instance_attr
                        && !self.ssa_config.self_names.is_empty()
                        && let Some(&enclosing_idx) = state.enclosing_def_stack.last()
                        && state.defs[enclosing_idx as usize].kind.is_type_container()
                    {
                        for &self_name in self.ssa_config.self_names {
                            let compound = format!("{self_name}.{name}");
                            let compound_key = state.arena.alloc_str(&compound);
                            state
                                .ssa
                                .write_variable(compound_key, target_block, val.clone());
                        }
                    }
                } // end for name in names
            }

            if is_expression_body {
                state.in_return = true;
            }

            // Track return statement context + infer return type from bare identifiers
            if !self.hooks.return_kinds.is_empty() && self.hooks.return_kinds.contains(&nk) {
                state.in_return = true;

                // For `return x` where x is a bare identifier, read its SSA
                // value. Only fires when the return expression's first named
                // child is itself an identifier kind — chains like `return
                // foo.bar()` are left to the resolver via PendingRef.is_return.
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
                    let ssa_key = state.arena.alloc_str(&ident);
                    let reaching = state
                        .ssa
                        .read_variable_stateless(ssa_key, state.current_block);
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

            // Reference handling → SSA read → PendingRef
            let ref_result = self.evaluate_reference(
                node,
                nk,
                &state.import_map,
                module_prefix.as_deref(),
                sep,
                state.tracer,
            );
            // Trace ref evaluation (only when we have a dispatch entry for this node kind)
            if self.ref_dispatch.contains_key(nk) {
                state.tracer.event(TraceEvent::RefEvaluated {
                    node_kind: nk.to_string(),
                    matched: ref_result.is_some(),
                    name: ref_result.as_ref().map(|(n, _, _)| n.clone()),
                    has_chain: ref_result.as_ref().is_some_and(|(_, _, e)| e.is_some()),
                });
            }
            if let Some((name, _range, expression)) = ref_result {
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

                state.tracer.event(TraceEvent::RefQueued {
                    name: name.clone(),
                    chain: expression
                        .as_ref()
                        .map(|c| c.iter().map(|s| format!("{s:?}")).collect()),
                    ssa_key: ssa_key.to_string(),
                    block_id: state.current_block.0,
                    enclosing_def: state.enclosing_def_stack.last().copied(),
                    is_return: state.in_return,
                });
                state.pending_refs.push(PendingRef {
                    name,
                    chain: expression,
                    ssa_key,
                    block: state.current_block,
                    enclosing_def: state.enclosing_def_stack.last().copied(),
                    is_return: state.in_return,
                });
            }
        }

        // Recurse children
        for child in node.children() {
            self.walk_full(&child, state, sep);
        }

        // Clear return context after children
        if (!self.hooks.return_kinds.is_empty() && self.hooks.return_kinds.contains(&nk))
            || is_expression_body
        {
            state.in_return = false;
        }

        if pushed_scope {
            if let Some(name) = state.scope_stack.last() {
                state.tracer.event(TraceEvent::ScopePop {
                    name: name.to_string(),
                });
            }
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
        state.tracer.event(TraceEvent::BranchEnter {
            node_kind: node.kind().to_string(),
            pre_block: pre_block.0,
        });

        // Walk condition in pre-branch block
        if let Some(cond_field) = rule.condition_field
            && let Some(cond_node) = node.field(cond_field)
        {
            self.walk_full(&cond_node, state, sep);
        }

        let has_catch_all = rule
            .catch_all_kind
            .is_some_and(|ck| node.has(Axis::Child, Match::Kind(ck)));

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
                state.tracer.event(TraceEvent::BranchArm {
                    block_id: arm_block.0,
                });

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
        state.tracer.event(TraceEvent::BranchJoin {
            block_id: join.0,
            arm_blocks: end_blocks.iter().map(|b| b.0).collect(),
        });
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
        state.tracer.event(TraceEvent::LoopEnter {
            node_kind: node.kind().to_string(),
            header_block: header.0,
            body_block: body.0,
        });

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
        state
            .tracer
            .event(TraceEvent::LoopExit { exit_block: exit.0 });
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
    /// True if this ref is inside a return statement.
    is_return: bool,
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
    in_return: bool,
    tracer: &'a Tracer,
}

impl<'a> WalkFullState<'a> {
    fn new(arena: &'a bumpalo::Bump, tracer: &'a Tracer) -> Self {
        let mut ssa = super::ssa::SsaEngine::new().with_tracer(tracer);
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
            in_return: false,
            tracer,
        }
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
        let tracer = crate::v2::trace::Tracer::new(false);
        spec.parse_full_and_resolve(
            b"def foo(): pass\nfoo()",
            "test.py",
            Language::Python,
            &mut |name: &str, _chain, _reaching, _enclosing, _inferred| {
                ref_names.push(name.to_string());
            },
            &tracer,
            None,
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
