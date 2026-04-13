use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use oxc::span::Span;
use parser_core::utils::Range;
use std::collections::HashMap;

use super::super::types::{CjsExport, JsImport, JsImportKind, JsInvocationSupport};

pub(super) fn extract_cjs_imports(
    nodes: &AstNodes,
    span_to_range: impl Fn(Span) -> Range,
    imports: &mut Vec<JsImport>,
) {
    for node in nodes.iter() {
        if let AstKind::CallExpression(call) = node.kind() {
            let Some(str_lit) = call.common_js_require() else {
                continue;
            };
            let specifier = str_lit.value.to_string();
            let range = span_to_range(call.span);

            let Some(bindings) = nodes.ancestor_ids(node.id()).find_map(|aid| {
                if let AstKind::VariableDeclarator(decl) = nodes.kind(aid) {
                    let mut bindings = Vec::new();
                    collect_cjs_bindings(&decl.id, &mut bindings, None);
                    return Some(bindings);
                }
                None
            }) else {
                continue;
            };

            for (local_name, imported_name) in bindings {
                imports.push(JsImport {
                    specifier: specifier.clone(),
                    kind: JsImportKind::CjsRequire { imported_name },
                    local_name,
                    range,
                    is_type: false,
                });
            }
        }
    }
}

fn collect_cjs_bindings(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    bindings: &mut Vec<(String, Option<String>)>,
    imported_name: Option<String>,
) {
    use oxc::ast::ast::BindingPattern;

    match pattern {
        BindingPattern::BindingIdentifier(ident) => {
            bindings.push((ident.name.to_string(), imported_name));
        }
        BindingPattern::AssignmentPattern(assign) => {
            collect_cjs_bindings(&assign.left, bindings, imported_name);
        }
        BindingPattern::ObjectPattern(object) => {
            for property in &object.properties {
                let property_name = property.key.static_name().map(|name| name.into_owned());
                collect_cjs_bindings(&property.value, bindings, property_name);
            }
            if let Some(rest) = &object.rest {
                collect_cjs_bindings(&rest.argument, bindings, None);
            }
        }
        BindingPattern::ArrayPattern(array) => {
            for element in array.elements.iter().flatten() {
                collect_cjs_bindings(element, bindings, None);
            }
            if let Some(rest) = &array.rest {
                collect_cjs_bindings(&rest.argument, bindings, None);
            }
        }
    }
}

pub(super) fn extract_cjs_exports(
    nodes: &AstNodes,
    span_to_range: impl Fn(Span) -> Range + Copy,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
) -> Vec<CjsExport> {
    use oxc::ast::ast::AssignmentTarget;

    let mut exports = Vec::new();

    for node in nodes.iter() {
        if let AstKind::AssignmentExpression(assign) = node.kind() {
            let invocation_support =
                invocation_support_for_assignment_rhs(&assign.right, invocation_support_by_name);
            let local_fqn = local_fqn_for_expression(&assign.right);

            match &assign.left {
                AssignmentTarget::AssignmentTargetIdentifier(_) => {}
                _ => {
                    if let AssignmentTarget::StaticMemberExpression(member) = &assign.left {
                        let prop_name = member.property.name.as_str();

                        if prop_name == "exports"
                            && let oxc::ast::ast::Expression::Identifier(ident) = &member.object
                            && ident.name == "module"
                        {
                            exports.push(CjsExport::Default {
                                local_fqn: local_fqn.clone(),
                                range: span_to_range(assign.span),
                                invocation_support,
                            });
                            if let oxc::ast::ast::Expression::ObjectExpression(object) =
                                assign.right.get_inner_expression()
                            {
                                collect_cjs_object_exports(
                                    object,
                                    invocation_support_by_name,
                                    span_to_range,
                                    &mut exports,
                                );
                            }
                            continue;
                        }

                        if let oxc::ast::ast::Expression::Identifier(ident) = &member.object
                            && ident.name == "exports"
                        {
                            exports.push(CjsExport::Named {
                                name: prop_name.to_string(),
                                local_fqn: local_fqn.clone(),
                                range: span_to_range(assign.span),
                                invocation_support,
                            });
                        }

                        if let oxc::ast::ast::Expression::StaticMemberExpression(inner) =
                            &member.object
                            && inner.property.name == "exports"
                            && let oxc::ast::ast::Expression::Identifier(ident) = &inner.object
                            && ident.name == "module"
                        {
                            exports.push(CjsExport::Named {
                                name: prop_name.to_string(),
                                local_fqn: local_fqn.clone(),
                                range: span_to_range(assign.span),
                                invocation_support,
                            });
                        }
                    }
                }
            }
        }
    }

    exports
}

fn collect_cjs_object_exports(
    object: &oxc::ast::ast::ObjectExpression<'_>,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
    span_to_range: impl Fn(Span) -> Range + Copy,
    exports: &mut Vec<CjsExport>,
) {
    use oxc::ast::ast::ObjectPropertyKind;

    for property in &object.properties {
        let ObjectPropertyKind::ObjectProperty(property) = property else {
            continue;
        };
        let Some(name) = property.key.static_name() else {
            continue;
        };

        exports.push(CjsExport::Named {
            name: name.to_string(),
            local_fqn: local_fqn_for_expression(&property.value),
            range: span_to_range(property.span),
            invocation_support: invocation_support_for_assignment_rhs(
                &property.value,
                invocation_support_by_name,
            ),
        });
    }
}

fn local_fqn_for_expression(expr: &oxc::ast::ast::Expression<'_>) -> Option<String> {
    match expr.get_inner_expression() {
        oxc::ast::ast::Expression::Identifier(ident) => Some(ident.name.to_string()),
        _ => None,
    }
}

fn invocation_support_for_assignment_rhs(
    expr: &oxc::ast::ast::Expression,
    invocation_support_by_name: &HashMap<String, JsInvocationSupport>,
) -> Option<JsInvocationSupport> {
    match expr.get_inner_expression() {
        oxc::ast::ast::Expression::ArrowFunctionExpression(_) => {
            Some(JsInvocationSupport::arrow_function())
        }
        oxc::ast::ast::Expression::FunctionExpression(_) => Some(JsInvocationSupport::function()),
        oxc::ast::ast::Expression::ClassExpression(_) => Some(JsInvocationSupport::class()),
        oxc::ast::ast::Expression::Identifier(ident) => {
            invocation_support_by_name.get(ident.name.as_str()).copied()
        }
        _ => None,
    }
}
