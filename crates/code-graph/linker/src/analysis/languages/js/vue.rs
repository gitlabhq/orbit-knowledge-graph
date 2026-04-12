use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use parser_core::utils::Range;
use std::collections::HashMap;

use super::types::{JsDef, JsDefKind};

pub(super) fn extract_vue_options_api(
    nodes: &AstNodes,
    span_to_range: impl Fn(oxc::span::Span) -> Range,
    relative_path: &str,
    defs: &mut Vec<JsDef>,
    class_hierarchy: &mut HashMap<String, Option<String>>,
) {
    for node in nodes.iter() {
        let AstKind::ExportDefaultDeclaration(decl) = node.kind() else {
            continue;
        };
        let oxc::ast::ast::ExportDefaultDeclarationKind::ObjectExpression(obj) = &decl.declaration
        else {
            continue;
        };

        let component_name = obj
            .properties
            .iter()
            .find_map(|prop| {
                let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(p) = prop else {
                    return None;
                };
                if p.key.static_name().as_deref() != Some("name") {
                    return None;
                }
                if let oxc::ast::ast::Expression::StringLiteral(s) = &p.value {
                    Some(s.value.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                std::path::Path::new(relative_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("Component")
                    .to_string()
            });

        class_hierarchy.insert(component_name.clone(), None);

        defs.push(JsDef {
            name: component_name.clone(),
            fqn: component_name.clone(),
            kind: JsDefKind::Class,
            range: span_to_range(obj.span),
            is_exported: true,
            type_annotation: None,
        });

        for prop in &obj.properties {
            let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(p) = prop else {
                continue;
            };
            let key_name = p.key.static_name();
            let is_methods = key_name.as_deref() == Some("methods");
            let is_computed = key_name.as_deref() == Some("computed");
            if !is_methods && !is_computed {
                continue;
            }
            let oxc::ast::ast::Expression::ObjectExpression(methods_obj) = &p.value else {
                continue;
            };
            for method_prop in &methods_obj.properties {
                let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(mp) = method_prop else {
                    continue;
                };
                let Some(method_name) = mp.key.static_name() else {
                    continue;
                };
                let method_name = method_name.to_string();
                let fqn = format!("{component_name}::{method_name}");
                defs.push(JsDef {
                    name: method_name,
                    fqn,
                    kind: JsDefKind::Method {
                        class_fqn: component_name.clone(),
                        is_static: false,
                    },
                    range: span_to_range(mp.span),
                    is_exported: false,
                    type_annotation: None,
                });
            }
        }
    }
}
