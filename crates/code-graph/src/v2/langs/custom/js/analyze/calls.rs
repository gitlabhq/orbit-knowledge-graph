use oxc::ast::AstKind;
use oxc::syntax::symbol::SymbolId;
use std::collections::{HashMap, VecDeque};

use super::super::types::{
    ImportedName, JsImport, JsImportKind, JsImportedBinding, JsImportedCall, JsInvocationKind,
    JsResolutionMode,
};
use super::analyzer::{Ctx, NodeId};
use super::patterns::walk_binding_pattern_identifiers;

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
    let symbol_id = symbol_from_identifier_reference(ctx, ident)?;
    import_bindings.get(&symbol_id).cloned()
}

fn symbol_from_identifier_reference(
    ctx: &Ctx,
    ident: &oxc::ast::ast::IdentifierReference<'_>,
) -> Option<SymbolId> {
    let reference_id = ident.reference_id.get()?;
    ctx.scoping.get_reference(reference_id).symbol_id()
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
                let fallback_binding = binding.clone();
                member_path.reverse();

                retarget_imported_member_binding(&mut binding, &mut member_path)?;

                return Some(JsImportedCall {
                    fallback_binding,
                    binding,
                    member_path,
                    invocation_kind,
                });
            }
            _ => return None,
        }
    }
}

pub(super) fn imported_call_from_jsx_member_expression(
    ctx: &Ctx,
    member: &oxc::ast::ast::JSXMemberExpression<'_>,
    import_bindings: &HashMap<SymbolId, JsImportedBinding>,
    invocation_kind: JsInvocationKind,
) -> Option<JsImportedCall> {
    let mut member_path = vec![member.property.name.to_string()];
    let mut current = &member.object;

    loop {
        match current {
            oxc::ast::ast::JSXMemberExpressionObject::MemberExpression(parent) => {
                member_path.push(parent.property.name.to_string());
                current = &parent.object;
            }
            oxc::ast::ast::JSXMemberExpressionObject::IdentifierReference(identifier) => {
                let mut binding =
                    binding_from_identifier_reference(ctx, identifier, import_bindings)?;
                let fallback_binding = binding.clone();
                member_path.reverse();

                retarget_imported_member_binding(&mut binding, &mut member_path)?;

                return Some(JsImportedCall {
                    fallback_binding,
                    binding,
                    member_path,
                    invocation_kind,
                });
            }
            oxc::ast::ast::JSXMemberExpressionObject::ThisExpression(_) => return None,
        }
    }
}

fn retarget_imported_member_binding(
    binding: &mut JsImportedBinding,
    member_path: &mut Vec<String>,
) -> Option<()> {
    if matches!(binding.imported_name, ImportedName::Namespace) {
        let first_member = member_path.first()?.clone();
        binding.imported_name = if first_member == "default" {
            ImportedName::Default
        } else {
            ImportedName::Named(first_member)
        };
        member_path.remove(0);
        return Some(());
    }

    if matches!(
        (binding.resolution_mode, &binding.imported_name),
        (JsResolutionMode::Require, ImportedName::Default)
    ) {
        let first_member = member_path.first()?.clone();
        binding.imported_name = ImportedName::Named(first_member);
        member_path.remove(0);
    }

    Some(())
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
    walk_binding_pattern_identifiers(
        pattern,
        None,
        matches!(base_binding.imported_name, ImportedName::Namespace),
        false,
        &mut |binding, imported_name| {
            let Some(symbol_id) = binding.symbol_id.get() else {
                return;
            };
            let binding = imported_name.map_or_else(
                || base_binding.clone(),
                |member_name| JsImportedBinding {
                    specifier: base_binding.specifier.clone(),
                    imported_name: ImportedName::Named(member_name),
                    resolution_mode: base_binding.resolution_mode,
                },
            );
            aliases.push((symbol_id, binding));
        },
    );
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

    let alias_declarators = alias_declarators(ctx);
    let mut dependents = HashMap::<SymbolId, Vec<usize>>::new();
    let mut ready = VecDeque::new();
    let mut queued = vec![false; alias_declarators.len()];

    for (idx, declarator) in alias_declarators.iter().enumerate() {
        if let Some(symbol_id) = declarator.dependency {
            dependents.entry(symbol_id).or_default().push(idx);
        } else {
            ready.push_back(idx);
            queued[idx] = true;
        }
    }

    let mut available_symbols: VecDeque<_> = import_bindings.keys().copied().collect();
    while let Some(symbol_id) = available_symbols.pop_front() {
        if let Some(indices) = dependents.remove(&symbol_id) {
            for idx in indices {
                if !queued[idx] {
                    queued[idx] = true;
                    ready.push_back(idx);
                }
            }
        }
    }

    while let Some(idx) = ready.pop_front() {
        let declarator = &alias_declarators[idx];
        let AstKind::VariableDeclarator(variable_declarator) = ctx.nodes.kind(declarator.id) else {
            continue;
        };
        let Some(init) = &variable_declarator.init else {
            continue;
        };
        let Some(mut base_binding) = imported_binding_from_expression(ctx, init, &import_bindings)
            .or_else(|| imported_namespace_binding_from_require_call(init))
        else {
            continue;
        };
        if matches!(
            (
                &variable_declarator.id,
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
        collect_aliases_from_binding_pattern(
            &variable_declarator.id,
            &base_binding,
            &mut discovered,
        );
        for (symbol_id, binding) in discovered {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                import_bindings.entry(symbol_id)
            {
                entry.insert(binding);
                if let Some(indices) = dependents.remove(&symbol_id) {
                    for idx in indices {
                        if !queued[idx] {
                            queued[idx] = true;
                            ready.push_back(idx);
                        }
                    }
                }
            }
        }
    }

    import_bindings
}

struct AliasDeclarator {
    id: NodeId,
    dependency: Option<SymbolId>,
}

fn alias_declarators(ctx: &Ctx) -> Vec<AliasDeclarator> {
    ctx.nodes
        .iter()
        .filter_map(|node| match node.kind() {
            AstKind::VariableDeclarator(declarator) if declarator.init.is_some() => {
                Some(AliasDeclarator {
                    id: node.id(),
                    dependency: declarator
                        .init
                        .as_ref()
                        .and_then(|init| alias_dependency_symbol(ctx, init)),
                })
            }
            _ => None,
        })
        .collect()
}

fn alias_dependency_symbol(
    ctx: &Ctx,
    expression: &oxc::ast::ast::Expression<'_>,
) -> Option<SymbolId> {
    match expression.get_inner_expression() {
        oxc::ast::ast::Expression::Identifier(identifier) => {
            symbol_from_identifier_reference(ctx, identifier)
        }
        oxc::ast::ast::Expression::StaticMemberExpression(member) => {
            let oxc::ast::ast::Expression::Identifier(identifier) =
                member.object.get_inner_expression()
            else {
                return None;
            };
            symbol_from_identifier_reference(ctx, identifier)
        }
        _ => None,
    }
}
