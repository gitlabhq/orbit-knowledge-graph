use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use oxc::span::GetSpan;
use std::collections::{HashMap, HashSet};

use super::analyzer::Ctx;
use super::types::{
    ImportedName, JsCallConfidence, JsCallEdge, JsCallSite, JsCallTarget, JsDef, JsDefKind,
    JsImport, JsImportKind,
};

pub(super) fn build_class_hierarchy(nodes: &AstNodes) -> HashMap<String, Option<String>> {
    let mut hierarchy = HashMap::new();
    for node in nodes.iter() {
        if let AstKind::Class(class) = node.kind()
            && let Some(id) = &class.id
        {
            let parent = class.super_class.as_ref().and_then(|expr| {
                if let oxc::ast::ast::Expression::Identifier(ident) = expr {
                    Some(ident.name.to_string())
                } else {
                    None
                }
            });
            hierarchy.insert(id.name.to_string(), parent);
        }
    }
    hierarchy
}

pub(super) fn find_method_in_defs<'a>(
    class: &str,
    method: &str,
    hierarchy: &HashMap<String, Option<String>>,
    defs: &'a [JsDef],
) -> Option<&'a JsDef> {
    let mut current = Some(class.to_string());
    let mut seen = HashSet::new();
    while let Some(cls) = current {
        if !seen.insert(cls.clone()) {
            break;
        }
        let fqn = format!("{cls}::{method}");
        if let Some(d) = defs.iter().find(|d| d.fqn == fqn) {
            return Some(d);
        }
        current = hierarchy.get(&cls).and_then(|p| p.clone());
    }
    None
}

pub(super) fn build_variable_type_map(
    nodes: &AstNodes,
) -> HashMap<String, (String, JsCallConfidence)> {
    let mut map = HashMap::new();
    for node in nodes.iter() {
        match node.kind() {
            // P1: const x = new Foo() -> x is Foo (Inferred)
            AstKind::VariableDeclarator(decl) => {
                if let Some(init) = &decl.init
                    && let oxc::ast::ast::Expression::NewExpression(new_expr) = init
                    && let oxc::ast::ast::Expression::Identifier(callee) = &new_expr.callee
                    && let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &decl.id
                {
                    map.insert(
                        binding.name.to_string(),
                        (callee.name.to_string(), JsCallConfidence::Inferred),
                    );
                }
            }
            // P2: function f(svc: Service) -> svc is Service (Annotated)
            AstKind::FormalParameter(param) => {
                if let oxc::ast::ast::BindingPattern::BindingIdentifier(ident) = &param.pattern
                    && let Some(type_ann) = &param.type_annotation
                    && let oxc::ast::ast::TSType::TSTypeReference(type_ref) =
                        &type_ann.type_annotation
                    && let oxc::ast::ast::TSTypeName::IdentifierReference(id) = &type_ref.type_name
                {
                    map.insert(
                        ident.name.to_string(),
                        (id.name.to_string(), JsCallConfidence::Annotated),
                    );
                }
            }
            _ => {}
        }
    }
    map
}

pub(super) fn extract_call_edges(
    ctx: &Ctx,
    defs: &[JsDef],
    imports: &[JsImport],
    class_hierarchy: &HashMap<String, Option<String>>,
    variable_type_map: &HashMap<String, (String, JsCallConfidence)>,
) -> Vec<JsCallEdge> {
    let mut calls = Vec::new();

    let import_lookup: HashMap<&str, (&str, ImportedName)> = imports
        .iter()
        .map(|i| {
            let imported_name = match &i.kind {
                JsImportKind::Named { imported_name } => ImportedName::Named(imported_name.clone()),
                JsImportKind::Default => ImportedName::Default,
                JsImportKind::Namespace => ImportedName::Namespace,
                JsImportKind::CjsRequire { imported_name } => imported_name
                    .as_ref()
                    .map_or(ImportedName::Namespace, |n| ImportedName::Named(n.clone())),
            };
            (i.local_name.as_str(), (i.specifier.as_str(), imported_name))
        })
        .collect();

    // Phase 1: resolved symbol references
    for symbol_id in ctx.scoping.symbol_ids() {
        for ref_id in ctx.scoping.get_resolved_reference_ids(symbol_id) {
            let reference = ctx.scoping.get_reference(*ref_id);
            if !reference.flags().is_read() {
                continue;
            }

            let ref_node_id = reference.node_id();
            let ref_span = ctx.nodes.get_node(ref_node_id).span();
            let parent_kind = ctx.nodes.parent_kind(ref_node_id);

            let (is_call, is_callback) = match parent_kind {
                AstKind::CallExpression(call) => {
                    let is_callee = matches!(
                        &call.callee,
                        oxc::ast::ast::Expression::Identifier(id) if id.span == ref_span
                    );
                    (is_callee, !is_callee)
                }
                AstKind::NewExpression(new_expr) => {
                    let is_callee = matches!(
                        &new_expr.callee,
                        oxc::ast::ast::Expression::Identifier(id) if id.span == ref_span
                    );
                    (is_callee, !is_callee)
                }
                AstKind::TaggedTemplateExpression(_) | AstKind::JSXOpeningElement(_) => {
                    (true, false)
                }
                _ => (false, false),
            };

            if !is_call && !is_callback {
                // Member expression calls: obj.method()
                if let AstKind::StaticMemberExpression(member) = ctx.nodes.parent_kind(ref_node_id)
                    && let Some(call_node_id) = ctx.nodes.ancestor_ids(ref_node_id).nth(1)
                    && matches!(
                        ctx.nodes.kind(call_node_id),
                        AstKind::CallExpression(_) | AstKind::NewExpression(_)
                    )
                {
                    let name = ctx.scoping.symbol_name(symbol_id);
                    let method_name = member.property.name.as_str();
                    let call_range = ctx
                        .lt
                        .span_to_range(ctx.nodes.get_node(call_node_id).span());

                    // P0: namespace/CJS import
                    if let Some((specifier, ImportedName::Namespace)) = import_lookup.get(name) {
                        let caller_scope = reference.scope_id();
                        let caller_info = ctx.find_enclosing_def(caller_scope);
                        let caller = match caller_info {
                            Some((fqn, range)) => JsCallSite::Definition { fqn, range },
                            None => JsCallSite::ModuleLevel,
                        };
                        calls.push(JsCallEdge {
                            caller,
                            callee: JsCallTarget::ImportedCall {
                                local_name: name.to_string(),
                                specifier: specifier.to_string(),
                                imported_name: ImportedName::Named(method_name.to_string()),
                            },
                            call_range,
                            confidence: JsCallConfidence::Known,
                        });
                    }
                    // P1/P3: variable method or static method
                    else {
                        let resolved = variable_type_map
                            .get(name)
                            .map(|(cls, conf)| (cls.as_str(), *conf))
                            .or_else(|| {
                                class_hierarchy
                                    .contains_key(name)
                                    .then_some((name, JsCallConfidence::Known))
                            });
                        if let Some((class_name, confidence)) = resolved
                            && let Some(target) =
                                find_method_in_defs(class_name, method_name, class_hierarchy, defs)
                        {
                            let caller_scope = reference.scope_id();
                            let caller_info = ctx.find_enclosing_def(caller_scope);
                            let caller = match caller_info {
                                Some((fqn, range)) => JsCallSite::Definition { fqn, range },
                                None => JsCallSite::ModuleLevel,
                            };
                            calls.push(JsCallEdge {
                                caller,
                                callee: JsCallTarget::Direct {
                                    fqn: target.fqn.clone(),
                                    range: target.range,
                                },
                                call_range,
                                confidence,
                            });
                        }
                    }
                }
                continue;
            }

            let caller_scope = reference.scope_id();
            let caller_info = ctx.find_enclosing_def(caller_scope);
            let call_site_span = ctx.nodes.get_node(ref_node_id).span();
            let call_site_range = ctx.lt.span_to_range(call_site_span);
            // P4: callback-as-argument gets Guessed confidence
            let base_confidence = if is_callback {
                JsCallConfidence::Guessed
            } else {
                JsCallConfidence::Known
            };

            let caller = match caller_info {
                Some((fqn, range)) => JsCallSite::Definition { fqn, range },
                None => JsCallSite::ModuleLevel,
            };

            let callee_flags = ctx.scoping.symbol_flags(symbol_id);
            if callee_flags.is_import() {
                let callee_name = ctx.scoping.symbol_name(symbol_id);
                if let Some((specifier, imported_name)) = import_lookup.get(callee_name) {
                    calls.push(JsCallEdge {
                        caller,
                        callee: JsCallTarget::ImportedCall {
                            local_name: callee_name.to_string(),
                            specifier: specifier.to_string(),
                            imported_name: imported_name.clone(),
                        },
                        call_range: call_site_range,
                        confidence: base_confidence,
                    });
                }
                continue;
            }

            let callee_span = ctx.scoping.symbol_span(symbol_id);
            let callee_range = ctx.lt.span_to_range(callee_span);
            let callee_fqn = ctx.build_fqn(symbol_id);

            calls.push(JsCallEdge {
                caller,
                callee: JsCallTarget::Direct {
                    fqn: callee_fqn,
                    range: callee_range,
                },
                call_range: call_site_range,
                confidence: base_confidence,
            });
        }
    }

    // Phase 2: this.method() and super.method()
    for node in ctx.nodes.iter() {
        if let AstKind::CallExpression(call) = node.kind()
            && let oxc::ast::ast::Expression::StaticMemberExpression(member) = &call.callee
        {
            let method_name = member.property.name.as_str();
            let call_range = ctx.lt.span_to_range(call.span);

            let is_this = matches!(&member.object, oxc::ast::ast::Expression::ThisExpression(_));
            let is_super = matches!(&member.object, oxc::ast::ast::Expression::Super(_));

            if !is_this && !is_super {
                continue;
            }

            let mut enclosing_class: Option<String> = None;
            let mut caller_method: Option<String> = None;
            for aid in ctx.nodes.ancestor_ids(node.id()).skip(1) {
                match ctx.nodes.kind(aid) {
                    AstKind::MethodDefinition(method) if caller_method.is_none() => {
                        if let Some(name) = method.key.static_name() {
                            caller_method = Some(name.to_string());
                        }
                    }
                    // Vue Options API: method shorthand in { methods: { handleClick() {} } }
                    AstKind::ObjectProperty(prop) if caller_method.is_none() && prop.method => {
                        if let Some(name) = prop.key.static_name() {
                            caller_method = Some(name.to_string());
                        }
                    }
                    AstKind::Class(class) => {
                        if let Some(id) = &class.id {
                            enclosing_class = Some(id.name.to_string());
                        }
                        break;
                    }
                    // Vue Options API: export default { ... } acts as a virtual class
                    AstKind::ExportDefaultDeclaration(_)
                        if enclosing_class.is_none() && caller_method.is_some() =>
                    {
                        let method = caller_method.as_ref().unwrap();
                        for def in defs.iter() {
                            if let JsDefKind::Method { class_fqn, .. } = &def.kind
                                && def.name == *method
                            {
                                enclosing_class = Some(class_fqn.clone());
                                break;
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }

            let Some(class_name) = enclosing_class else {
                continue;
            };

            let target_def = if is_super {
                class_hierarchy
                    .get(&class_name)
                    .and_then(|p| p.as_ref())
                    .and_then(|parent_name| {
                        find_method_in_defs(parent_name, method_name, class_hierarchy, defs)
                    })
            } else {
                find_method_in_defs(&class_name, method_name, class_hierarchy, defs)
            };

            let caller_fqn_str = caller_method.map(|m| format!("{class_name}::{m}"));
            let caller_def = caller_fqn_str
                .as_ref()
                .and_then(|fqn| defs.iter().find(|d| d.fqn == *fqn));

            let caller = match caller_def {
                Some(d) => JsCallSite::Definition {
                    fqn: d.fqn.clone(),
                    range: d.range,
                },
                None => continue,
            };

            let callee = if is_super {
                JsCallTarget::SuperMethod {
                    method_name: method_name.to_string(),
                    resolved_fqn: target_def.map(|d| d.fqn.clone()),
                    resolved_range: target_def.map(|d| d.range),
                }
            } else {
                JsCallTarget::ThisMethod {
                    method_name: method_name.to_string(),
                    resolved_fqn: target_def.map(|d| d.fqn.clone()),
                    resolved_range: target_def.map(|d| d.range),
                }
            };

            calls.push(JsCallEdge {
                caller,
                callee,
                call_range,
                confidence: JsCallConfidence::Known,
            });
        }
    }

    calls
}
