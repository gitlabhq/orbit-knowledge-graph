use std::sync::Arc;

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use code_graph_config::Language;
use code_graph_types::{
    CanonicalBinding, CanonicalControlFlow, CanonicalDefinition, CanonicalImport,
    CanonicalReference, CanonicalResult, ControlFlowChild, ControlFlowKind, DefKind,
    DefinitionMetadata, ExpressionStep, Fqn, ReferenceStatus,
};

use crate::utils::node_to_range;

use super::types::{LanguageSpec, Rule};

struct ScopeMatch {
    name: String,
    label: &'static str,
    def_kind: DefKind,
    range: crate::utils::Range,
    creates_scope: bool,
    metadata: Option<Box<DefinitionMetadata>>,
}

impl LanguageSpec {
    /// Parse source bytes into a `CanonicalResult` and the retained AST.
    pub fn parse_canonical(
        &self,
        source: &[u8],
        file_path: &str,
        language: Language,
    ) -> crate::Result<(
        CanonicalResult,
        treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>>,
    )> {
        let source_str = std::str::from_utf8(source)
            .map_err(|e| crate::Error::Parse(format!("Invalid UTF-8: {e}")))?;

        let ast = language.parse_ast(source_str);
        let root = ast.root();
        let sep = language.fqn_separator();

        let mut defs = Vec::new();
        let mut refs = Vec::new();
        let mut imports = Vec::new();
        let mut bindings = Vec::new();
        let mut control_flow = Vec::new();
        let mut scope_stack: Vec<Arc<str>> = Vec::new();
        let mut import_map = rustc_hash::FxHashMap::default();

        // Derive module scope from file path for languages like Python.
        if self.module_from_path
            && let Some(module) = file_path_to_module(file_path, sep)
        {
            scope_stack.push(Arc::from(module.as_str()));
        }

        // Scope depth at which definitions are "top level" — accounts for
        // the module scope pushed by module_from_path.
        let top_level_depth = scope_stack.len();

        self.walk(
            &root,
            &mut scope_stack,
            top_level_depth,
            &mut defs,
            &mut refs,
            &mut imports,
            &mut bindings,
            &mut control_flow,
            &mut import_map,
            sep,
        );

        let extension = file_path
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_string())
            .unwrap_or_default();

        let result = CanonicalResult {
            file_path: file_path.to_string(),
            extension,
            file_size: source.len() as u64,
            language,
            definitions: defs,
            imports,
            references: refs,
            bindings,
            control_flow,
        };

        Ok((result, ast))
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        scope_stack: &mut Vec<Arc<str>>,
        top_level_depth: usize,
        defs: &mut Vec<CanonicalDefinition>,
        refs: &mut Vec<CanonicalReference>,
        imports: &mut Vec<CanonicalImport>,
        bindings: &mut Vec<CanonicalBinding>,
        control_flow: &mut Vec<CanonicalControlFlow>,
        import_map: &mut rustc_hash::FxHashMap<String, String>,
        sep: &'static str,
    ) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
            return;
        }

        let node_kind = node.kind();
        let node_kind_ref = node_kind.as_ref();
        let mut pushed_scope = false;

        // Check for package/namespace node (pushes scope, no definition)
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

        if let Some((name, range, expression)) =
            self.evaluate_reference(node, node_kind_ref, import_map, sep)
        {
            refs.push(CanonicalReference {
                reference_type: "Call",
                name,
                range: canonical_range(&range),
                scope_fqn: Fqn::from_scope_only(scope_stack, sep),
                status: ReferenceStatus::Unresolved,
                target_fqn: None,
                expression,
            });
        }

        let import_count_before = imports.len();
        let handled = self.custom_import_fn.is_some_and(|f| f(node, imports));
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

        // Extract bindings
        if let Some(&rule_idx) = self
            .binding_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
            && let rule = &self.bindings[rule_idx]
            && let Some(name) = rule.extract_name(node)
        {
            let type_annotation = rule.extract_type_annotation(node);
            let rhs_name = rule.extract_rhs_name(node, self);
            let instance_attr = rule
                .instance_attr_prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix));

            bindings.push(CanonicalBinding {
                name,
                kind: rule.binding_kind,
                range: canonical_range(&node_to_range(node)),
                type_annotation,
                rhs_name,
                instance_attr,
            });
        }

        // Extract branches
        if let Some(&rule_idx) = self
            .branch_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
        {
            let rule = &self.branches[rule_idx];
            let byte_range = (node.range().start, node.range().end);
            let mut children = Vec::new();

            // Condition child (walked in pre-branch block)
            if let Some(cond_field) = rule.condition_field
                && let Some(cond_node) = node.field(cond_field)
            {
                children.push(ControlFlowChild {
                    byte_range: (cond_node.range().start, cond_node.range().end),
                    is_condition: true,
                });
            }

            // Branch arms
            let has_catch_all = rule
                .catch_all_kind
                .is_some_and(|catch_kind| node.children().any(|c| c.kind().as_ref() == catch_kind));
            for child in node.children() {
                let ck = child.kind();
                if rule.branch_kinds.iter().any(|&k| k == ck.as_ref()) {
                    children.push(ControlFlowChild {
                        byte_range: (child.range().start, child.range().end),
                        is_condition: false,
                    });
                }
            }

            control_flow.push(CanonicalControlFlow {
                kind: ControlFlowKind::Branch { has_catch_all },
                node_kind: node_kind_ref.to_string(),
                byte_range,
                children,
            });
        }

        // Extract loops
        if let Some(&rule_idx) = self
            .loop_dispatch
            .get(node_kind_ref)
            .and_then(|v| v.first())
        {
            let rule = &self.loops[rule_idx];
            let byte_range = (node.range().start, node.range().end);
            let mut children = Vec::new();

            // Iteration expression (walked in pre-loop block)
            if let Some(iter_field) = rule.iter_field
                && let Some(iter_node) = node.field(iter_field)
            {
                children.push(ControlFlowChild {
                    byte_range: (iter_node.range().start, iter_node.range().end),
                    is_condition: true,
                });
            }

            // Loop body
            if let Some(body_node) = node.field(rule.body_field) {
                children.push(ControlFlowChild {
                    byte_range: (body_node.range().start, body_node.range().end),
                    is_condition: false,
                });
            }

            control_flow.push(CanonicalControlFlow {
                kind: ControlFlowKind::Loop,
                node_kind: node_kind_ref.to_string(),
                byte_range,
                children,
            });
        }

        for child in node.children() {
            self.walk(
                &child,
                scope_stack,
                top_level_depth,
                defs,
                refs,
                imports,
                bindings,
                control_flow,
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
}

/// Convert a file path to a module scope string.
/// `services/user_service.py` → `services.user_service`
/// `models/__init__.py` → `models`
/// `main.py` → `main`
fn file_path_to_module(file_path: &str, sep: &str) -> Option<String> {
    let path = std::path::Path::new(file_path);

    // Strip extension
    let stem = path.with_extension("");
    let stem_str = stem.to_str()?;

    // Convert path separators to module separator
    let module = stem_str.replace(['/', '\\'], sep);

    // Strip trailing __init__ (package init files)
    let module = module
        .strip_suffix(&format!("{sep}__init__"))
        .unwrap_or(&module);

    if module.is_empty() {
        return None;
    }

    Some(module.to_string())
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

    fn parse_with(spec: &LanguageSpec, code: &str) -> CanonicalResult {
        spec.parse_canonical(code.as_bytes(), "test.py", Language::Python)
            .unwrap()
            .0
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
        let result = parse_with(&spec, "def foo(): pass\nfoo()");

        assert_eq!(result.references.len(), 1);
        assert_eq!(result.references[0].name, "foo");
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
