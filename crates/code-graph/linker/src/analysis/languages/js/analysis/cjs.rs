use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use oxc::span::Span;
use parser_core::utils::Range;

use super::super::types::{CjsExport, JsImport, JsImportKind};

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
    span_to_range: impl Fn(Span) -> Range,
) -> Vec<CjsExport> {
    use oxc::ast::ast::AssignmentTarget;

    let mut exports = Vec::new();

    for node in nodes.iter() {
        if let AstKind::AssignmentExpression(assign) = node.kind() {
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
                                range: span_to_range(assign.span),
                            });
                            continue;
                        }

                        if let oxc::ast::ast::Expression::Identifier(ident) = &member.object
                            && ident.name == "exports"
                        {
                            exports.push(CjsExport::Named {
                                name: prop_name.to_string(),
                                range: span_to_range(assign.span),
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
                                range: span_to_range(assign.span),
                            });
                        }
                    }
                }
            }
        }
    }

    exports
}
