use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use oxc::span::GetSpan;
use oxc::syntax::symbol::SymbolId;
use std::collections::{HashMap, HashSet};

use super::super::types::{
    ImportedName, JsCallConfidence, JsCallEdge, JsCallSite, JsCallTarget, JsDef, JsImport,
    JsImportKind, JsImportedBinding, JsImportedCall, JsInvocationKind, JsResolutionMode,
};
use super::analyzer::Ctx;

fn supports_direct_call_kind(
    expr: &oxc::ast::ast::Expression,
    call_kind: JsInvocationKind,
) -> bool {
    match expr.get_inner_expression() {
        oxc::ast::ast::Expression::ArrowFunctionExpression(_) => {
            !matches!(call_kind, JsInvocationKind::Construct)
        }
        oxc::ast::ast::Expression::FunctionExpression(_) => true,
        oxc::ast::ast::Expression::ClassExpression(_) => {
            matches!(
                call_kind,
                JsInvocationKind::Construct | JsInvocationKind::Jsx
            )
        }
        _ => false,
    }
}

fn symbol_supports_direct_call(
    ctx: &Ctx,
    symbol_id: SymbolId,
    call_kind: JsInvocationKind,
) -> bool {
    let flags = ctx.scoping.symbol_flags(symbol_id);

    if flags.is_import() || flags.is_function() {
        return true;
    }

    if flags.is_class() {
        return matches!(
            call_kind,
            JsInvocationKind::Construct | JsInvocationKind::Jsx
        );
    }

    if !flags.is_variable() {
        return false;
    }

    match ctx.nodes.kind(ctx.scoping.symbol_declaration(symbol_id)) {
        AstKind::VariableDeclarator(decl) => decl
            .init
            .as_ref()
            .is_some_and(|init| supports_direct_call_kind(init, call_kind)),
        _ => false,
    }
}

fn imported_binding_from_import(import: &JsImport) -> Option<JsImportedBinding> {
    if import.is_type {
        return None;
    }

    let imported_name = match &import.kind {
        JsImportKind::Named { imported_name } => ImportedName::Named(imported_name.clone()),
        JsImportKind::Default => ImportedName::Default,
        JsImportKind::Namespace => ImportedName::Namespace,
        JsImportKind::CjsRequire { imported_name } => imported_name
            .as_ref()
            .map_or(ImportedName::Namespace, |name| {
                ImportedName::Named(name.clone())
            }),
    };
    let resolution_mode = match import.kind {
        JsImportKind::CjsRequire { .. } => JsResolutionMode::Require,
        _ => JsResolutionMode::Import,
    };

    Some(JsImportedBinding {
        specifier: import.specifier.clone(),
        imported_name,
        resolution_mode,
    })
}

fn direct_import_binding_map(imports: &[JsImport]) -> HashMap<String, JsImportedBinding> {
    imports
        .iter()
        .filter_map(|import| {
            Some((
                import.local_name.clone(),
                imported_binding_from_import(import)?,
            ))
        })
        .collect()
}

fn binding_from_identifier_reference(
    ctx: &Ctx,
    ident: &oxc::ast::ast::IdentifierReference<'_>,
    import_bindings: &HashMap<SymbolId, JsImportedBinding>,
) -> Option<JsImportedBinding> {
    let reference_id = ident.reference_id.get()?;
    let symbol_id = ctx.scoping.get_reference(reference_id).symbol_id()?;
    import_bindings.get(&symbol_id).cloned()
}

fn imported_binding_from_expression(
    ctx: &Ctx,
    expression: &oxc::ast::ast::Expression<'_>,
    import_bindings: &HashMap<SymbolId, JsImportedBinding>,
) -> Option<JsImportedBinding> {
    match expression.get_inner_expression() {
        oxc::ast::ast::Expression::Identifier(identifier) => {
            binding_from_identifier_reference(ctx, identifier, import_bindings)
        }
        oxc::ast::ast::Expression::StaticMemberExpression(member) => {
            let object = match member.object.get_inner_expression() {
                oxc::ast::ast::Expression::Identifier(identifier) => identifier,
                _ => return None,
            };
            let base_binding = binding_from_identifier_reference(ctx, object, import_bindings)?;
            if !matches!(base_binding.imported_name, ImportedName::Namespace) {
                return None;
            }
            Some(JsImportedBinding {
                specifier: base_binding.specifier,
                imported_name: ImportedName::Named(member.property.name.to_string()),
                resolution_mode: base_binding.resolution_mode,
            })
        }
        _ => None,
    }
}

fn member_expression_invocation_kind(
    ctx: &Ctx,
    member_node_id: oxc::semantic::NodeId,
    call_node_id: oxc::semantic::NodeId,
) -> Option<JsInvocationKind> {
    let member_span = ctx.nodes.get_node(member_node_id).span();

    match ctx.nodes.kind(call_node_id) {
        AstKind::CallExpression(call) => matches!(
            &call.callee,
            oxc::ast::ast::Expression::StaticMemberExpression(callee)
                if callee.span == member_span
        )
        .then_some(JsInvocationKind::Call),
        AstKind::NewExpression(new_expr) => matches!(
            &new_expr.callee,
            oxc::ast::ast::Expression::StaticMemberExpression(callee)
                if callee.span == member_span
        )
        .then_some(JsInvocationKind::Construct),
        _ => None,
    }
}

fn called_member_expression_ancestor(
    ctx: &Ctx,
    reference_node_id: oxc::semantic::NodeId,
) -> Option<(oxc::semantic::NodeId, JsInvocationKind)> {
    let mut current_id = reference_node_id;
    let mut outermost_member_id = None;

    loop {
        let parent_id = ctx.nodes.parent_id(current_id);
        if parent_id == current_id {
            break;
        }
        if matches!(
            ctx.nodes.kind(parent_id),
            AstKind::StaticMemberExpression(_)
        ) {
            outermost_member_id = Some(parent_id);
            current_id = parent_id;
            continue;
        }
        break;
    }

    let member_node_id = outermost_member_id?;
    let call_node_id = ctx.nodes.parent_id(member_node_id);
    if call_node_id == member_node_id {
        return None;
    }
    let invocation_kind = member_expression_invocation_kind(ctx, member_node_id, call_node_id)?;

    Some((member_node_id, invocation_kind))
}

fn imported_call_from_member_expression(
    ctx: &Ctx,
    member: &oxc::ast::ast::StaticMemberExpression<'_>,
    import_bindings: &HashMap<SymbolId, JsImportedBinding>,
    invocation_kind: JsInvocationKind,
) -> Option<JsImportedCall> {
    let mut member_path = vec![member.property.name.to_string()];
    let mut current = member.object.get_inner_expression();

    loop {
        match current {
            oxc::ast::ast::Expression::StaticMemberExpression(parent) => {
                member_path.push(parent.property.name.to_string());
                current = parent.object.get_inner_expression();
            }
            oxc::ast::ast::Expression::Identifier(identifier) => {
                let mut binding =
                    binding_from_identifier_reference(ctx, identifier, import_bindings)?;
                member_path.reverse();

                if matches!(binding.imported_name, ImportedName::Namespace) {
                    let first_member = member_path.first()?.clone();
                    binding.imported_name = ImportedName::Named(first_member);
                    member_path.remove(0);
                }

                return Some(JsImportedCall {
                    binding,
                    member_path,
                    invocation_kind,
                });
            }
            _ => return None,
        }
    }
}

fn imported_namespace_binding_from_require_call(
    expression: &oxc::ast::ast::Expression<'_>,
) -> Option<JsImportedBinding> {
    let oxc::ast::ast::Expression::CallExpression(call) = expression.get_inner_expression() else {
        return None;
    };
    let specifier = call.common_js_require()?.value.to_string();
    Some(JsImportedBinding {
        specifier,
        imported_name: ImportedName::Namespace,
        resolution_mode: JsResolutionMode::Require,
    })
}

fn collect_aliases_from_binding_pattern(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    base_binding: &JsImportedBinding,
    aliases: &mut Vec<(SymbolId, JsImportedBinding)>,
) {
    match pattern {
        oxc::ast::ast::BindingPattern::BindingIdentifier(binding) => {
            if let Some(symbol_id) = binding.symbol_id.get() {
                aliases.push((symbol_id, base_binding.clone()));
            }
        }
        oxc::ast::ast::BindingPattern::AssignmentPattern(assignment) => {
            collect_aliases_from_binding_pattern(&assignment.left, base_binding, aliases);
        }
        oxc::ast::ast::BindingPattern::ObjectPattern(object) => {
            if !matches!(base_binding.imported_name, ImportedName::Namespace) {
                return;
            }

            for property in &object.properties {
                let Some(member_name) = property.key.static_name() else {
                    continue;
                };
                let member_binding = JsImportedBinding {
                    specifier: base_binding.specifier.clone(),
                    imported_name: ImportedName::Named(member_name.to_string()),
                    resolution_mode: base_binding.resolution_mode,
                };
                collect_aliases_from_binding_pattern(&property.value, &member_binding, aliases);
            }
        }
        oxc::ast::ast::BindingPattern::ArrayPattern(_) => {}
    }
}

fn build_import_binding_map(
    ctx: &Ctx,
    imports: &[JsImport],
) -> HashMap<SymbolId, JsImportedBinding> {
    let direct_imports = direct_import_binding_map(imports);
    let mut import_bindings: HashMap<SymbolId, JsImportedBinding> = ctx
        .scoping
        .symbol_ids()
        .filter_map(|symbol_id| {
            let flags = ctx.scoping.symbol_flags(symbol_id);
            flags
                .is_import()
                .then(|| {
                    let name = ctx.scoping.symbol_name(symbol_id);
                    Some((symbol_id, direct_imports.get(name)?.clone()))
                })
                .flatten()
        })
        .collect();

    let mut changed = true;
    while changed {
        changed = false;

        for node in ctx.nodes.iter() {
            let AstKind::VariableDeclarator(declarator) = node.kind() else {
                continue;
            };
            let Some(init) = &declarator.init else {
                continue;
            };
            let Some(base_binding) = imported_binding_from_expression(ctx, init, &import_bindings)
                .or_else(|| imported_namespace_binding_from_require_call(init))
            else {
                continue;
            };

            let mut discovered = Vec::new();
            collect_aliases_from_binding_pattern(&declarator.id, &base_binding, &mut discovered);
            for (symbol_id, binding) in discovered {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    import_bindings.entry(symbol_id)
                {
                    entry.insert(binding);
                    changed = true;
                }
            }
        }
    }

    import_bindings
}

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

fn def_owner_map(defs: &[JsDef]) -> HashMap<(usize, usize), (String, parser_core::utils::Range)> {
    defs.iter()
        .map(|def| {
            (
                (def.range.byte_offset.0, def.range.byte_offset.1),
                (def.fqn.clone(), def.range),
            )
        })
        .collect()
}

fn structural_call_owner(
    ctx: &Ctx,
    def_owners: &HashMap<(usize, usize), (String, parser_core::utils::Range)>,
    node_id: oxc::semantic::NodeId,
) -> Option<(String, parser_core::utils::Range)> {
    for ancestor_id in ctx.nodes.ancestor_ids(node_id).skip(1) {
        match ctx.nodes.kind(ancestor_id) {
            AstKind::MethodDefinition(method) => {
                let range = ctx.lt.span_to_range(method.span);
                if let Some(owner) = def_owners.get(&(range.byte_offset.0, range.byte_offset.1)) {
                    return Some(owner.clone());
                }
            }
            AstKind::ObjectProperty(property) => {
                let range = ctx.lt.span_to_range(property.span);
                if let Some(owner) = def_owners.get(&(range.byte_offset.0, range.byte_offset.1)) {
                    return Some(owner.clone());
                }
            }
            AstKind::VariableDeclarator(decl)
                if decl.init.as_ref().is_some_and(|init| {
                    matches!(
                        init.get_inner_expression(),
                        oxc::ast::ast::Expression::ArrowFunctionExpression(_)
                            | oxc::ast::ast::Expression::FunctionExpression(_)
                            | oxc::ast::ast::Expression::ClassExpression(_)
                    )
                }) =>
            {
                let oxc::ast::ast::BindingPattern::BindingIdentifier(binding) = &decl.id else {
                    continue;
                };
                let range = ctx.lt.span_to_range(binding.span);
                if let Some(owner) = def_owners.get(&(range.byte_offset.0, range.byte_offset.1)) {
                    return Some(owner.clone());
                }
            }
            AstKind::Function(function) => {
                let Some(id) = &function.id else {
                    continue;
                };
                let range = ctx.lt.span_to_range(id.span);
                if let Some(owner) = def_owners.get(&(range.byte_offset.0, range.byte_offset.1)) {
                    return Some(owner.clone());
                }
            }
            _ => {}
        }
    }

    None
}

fn resolve_caller_site(
    ctx: &Ctx,
    def_owners: &HashMap<(usize, usize), (String, parser_core::utils::Range)>,
    scope_id: oxc::syntax::scope::ScopeId,
    node_id: oxc::semantic::NodeId,
) -> JsCallSite {
    structural_call_owner(ctx, def_owners, node_id)
        .or_else(|| ctx.find_enclosing_def(scope_id))
        .map(|(fqn, range)| JsCallSite::Definition { fqn, range })
        .unwrap_or(JsCallSite::ModuleLevel)
}

pub(super) fn extract_call_edges(
    ctx: &Ctx,
    defs: &[JsDef],
    imports: &[JsImport],
    class_hierarchy: &HashMap<String, Option<String>>,
    variable_type_map: &HashMap<String, (String, JsCallConfidence)>,
) -> Vec<JsCallEdge> {
    let mut calls = Vec::new();
    let import_bindings = build_import_binding_map(ctx, imports);
    let def_owners = def_owner_map(defs);

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

            let call_kind = match parent_kind {
                AstKind::CallExpression(call) => {
                    let is_callee = matches!(
                        &call.callee,
                        oxc::ast::ast::Expression::Identifier(id) if id.span == ref_span
                    );
                    is_callee.then_some(JsInvocationKind::Call)
                }
                AstKind::NewExpression(new_expr) => {
                    let is_callee = matches!(
                        &new_expr.callee,
                        oxc::ast::ast::Expression::Identifier(id) if id.span == ref_span
                    );
                    is_callee.then_some(JsInvocationKind::Construct)
                }
                AstKind::TaggedTemplateExpression(_) => Some(JsInvocationKind::TaggedTemplate),
                AstKind::JSXOpeningElement(_) => Some(JsInvocationKind::Jsx),
                _ => None,
            };

            if call_kind.is_none() {
                // Member expression calls: obj.method() or ns.Component.configure()
                if let Some((member_node_id, invocation_kind)) =
                    called_member_expression_ancestor(ctx, ref_node_id)
                    && let AstKind::StaticMemberExpression(member) = ctx.nodes.kind(member_node_id)
                {
                    let name = ctx.scoping.symbol_name(symbol_id);
                    let method_name = member.property.name.as_str();
                    let call_node_id = ctx.nodes.parent_id(member_node_id);
                    let call_range = ctx
                        .lt
                        .span_to_range(ctx.nodes.get_node(call_node_id).span());

                    // P0: namespace/CJS import
                    if import_bindings.contains_key(&symbol_id) {
                        let caller = resolve_caller_site(
                            ctx,
                            &def_owners,
                            reference.scope_id(),
                            ref_node_id,
                        );
                        let Some(imported_call) = imported_call_from_member_expression(
                            ctx,
                            member,
                            &import_bindings,
                            invocation_kind,
                        ) else {
                            continue;
                        };
                        calls.push(JsCallEdge {
                            caller,
                            callee: JsCallTarget::ImportedCall { imported_call },
                            call_range,
                            confidence: JsCallConfidence::Known,
                        });
                    } else {
                        // P1/P3: variable method or static method
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
                            let caller = resolve_caller_site(
                                ctx,
                                &def_owners,
                                reference.scope_id(),
                                ref_node_id,
                            );
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

            let call_site_span = ctx.nodes.get_node(ref_node_id).span();
            let call_site_range = ctx.lt.span_to_range(call_site_span);
            let caller = resolve_caller_site(ctx, &def_owners, reference.scope_id(), ref_node_id);

            let Some(call_kind) = call_kind else {
                continue;
            };
            if let Some(binding) = import_bindings.get(&symbol_id) {
                calls.push(JsCallEdge {
                    caller,
                    callee: JsCallTarget::ImportedCall {
                        imported_call: JsImportedCall {
                            binding: binding.clone(),
                            member_path: Vec::new(),
                            invocation_kind: call_kind,
                        },
                    },
                    call_range: call_site_range,
                    confidence: JsCallConfidence::Known,
                });
                continue;
            }
            if !symbol_supports_direct_call(ctx, symbol_id, call_kind) {
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
                confidence: JsCallConfidence::Known,
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
                        let watch_name = format!("watch_{method}");
                        for def in defs.iter() {
                            if let Some(class_fqn) = def.kind.class_fqn()
                                && (def.name == *method || def.name == watch_name)
                            {
                                enclosing_class = Some(class_fqn.to_string());
                                if def.name == watch_name {
                                    caller_method = Some(watch_name.clone());
                                }
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
