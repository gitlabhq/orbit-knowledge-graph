use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use oxc::syntax::symbol::SymbolId;
use std::collections::HashMap;

use super::super::types::{
    ImportedName, JsImport, JsImportKind, JsImportedBinding, JsImportedCall, JsInvocationKind,
    JsResolutionMode,
};
use super::analyzer::Ctx;

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
            .map_or(ImportedName::Default, |name| {
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

pub(super) fn binding_from_identifier_reference(
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

pub(super) fn imported_call_from_member_expression(
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

                if matches!(binding.imported_name, ImportedName::Namespace)
                    || matches!(
                        (binding.resolution_mode, &binding.imported_name),
                        (JsResolutionMode::Require, ImportedName::Default)
                    )
                {
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

pub(super) fn build_import_binding_map(
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
            let Some(mut base_binding) =
                imported_binding_from_expression(ctx, init, &import_bindings)
                    .or_else(|| imported_namespace_binding_from_require_call(init))
            else {
                continue;
            };
            if matches!(
                (
                    &declarator.id,
                    base_binding.resolution_mode,
                    &base_binding.imported_name
                ),
                (
                    oxc::ast::ast::BindingPattern::BindingIdentifier(_),
                    JsResolutionMode::Require,
                    ImportedName::Namespace
                )
            ) {
                base_binding.imported_name = ImportedName::Default;
            }

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
            let extends = class.super_class.as_ref().and_then(|expr| match expr {
                oxc::ast::ast::Expression::Identifier(ident) => Some(ident.name.to_string()),
                _ => None,
            });
            hierarchy.insert(id.name.to_string(), extends);
        }
    }

    hierarchy
}
