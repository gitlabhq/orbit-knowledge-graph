use oxc::ast::AstKind;
use oxc::semantic::AstNodes;
use parser_core::utils::Range;
use std::collections::HashMap;

use super::super::types::{JsDef, JsDefKind};

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

        // Only create virtual class if the object has methods or computed
        let has_component_methods = obj.properties.iter().any(|prop| {
            let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(p) = prop else {
                return false;
            };
            let name = p.key.static_name();
            name.as_deref() == Some("methods") || name.as_deref() == Some("computed")
        });
        if !has_component_methods {
            continue;
        }

        class_hierarchy.insert(component_name.clone(), None);

        defs.push(JsDef {
            name: component_name.clone(),
            fqn: component_name.clone(),
            kind: JsDefKind::Class,
            range: span_to_range(obj.span),
            is_exported: true,
            type_annotation: None,
        });

        let lifecycle_hooks = [
            "beforeCreate",
            "created",
            "beforeMount",
            "mounted",
            "beforeUpdate",
            "updated",
            "beforeDestroy",
            "destroyed",
            "beforeUnmount",
            "unmounted",
            "activated",
            "deactivated",
            "errorCaptured",
        ];

        for prop in &obj.properties {
            let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(p) = prop else {
                continue;
            };
            let Some(key_name) = p.key.static_name() else {
                continue;
            };
            let key = key_name.as_ref();

            // methods: { ... } and computed: { ... } -- extract each child as a Method
            if key == "methods" || key == "computed" {
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
            // watch: { ... } -- extract each watcher as a Method
            else if key == "watch" {
                let oxc::ast::ast::Expression::ObjectExpression(watch_obj) = &p.value else {
                    continue;
                };
                for watch_prop in &watch_obj.properties {
                    let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(wp) = watch_prop else {
                        continue;
                    };
                    let Some(watcher_name) = wp.key.static_name() else {
                        continue;
                    };
                    let watcher_name = watcher_name.to_string();
                    let fqn = format!("{component_name}::watch_{watcher_name}");
                    defs.push(JsDef {
                        name: format!("watch_{watcher_name}"),
                        fqn,
                        kind: JsDefKind::Method {
                            class_fqn: component_name.clone(),
                            is_static: false,
                        },
                        range: span_to_range(wp.span),
                        is_exported: false,
                        type_annotation: None,
                    });
                }
            }
            // data() and lifecycle hooks -- extract the function itself as a Method
            else if key == "data" || lifecycle_hooks.contains(&key) {
                let fqn = format!("{component_name}::{key}");
                defs.push(JsDef {
                    name: key.to_string(),
                    fqn,
                    kind: JsDefKind::Method {
                        class_fqn: component_name.clone(),
                        is_static: false,
                    },
                    range: span_to_range(p.span),
                    is_exported: false,
                    type_annotation: None,
                });
            }
        }
    }
}
