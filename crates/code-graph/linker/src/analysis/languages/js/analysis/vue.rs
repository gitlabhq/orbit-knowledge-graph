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

        // Only create virtual class if the object has Vue component structure
        let vue_keys: &[&str] = &[
            "methods",
            "computed",
            "watch",
            "data",
            "mounted",
            "created",
            "beforeDestroy",
            "destroyed",
            "beforeMount",
            "updated",
            "beforeCreate",
            "beforeUnmount",
            "unmounted",
            "activated",
            "deactivated",
            "errorCaptured",
        ];
        let has_component_structure = obj.properties.iter().any(|prop| {
            let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(p) = prop else {
                return false;
            };
            p.key
                .static_name()
                .is_some_and(|n| vue_keys.contains(&n.as_ref()))
        });
        if !has_component_structure {
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

            // methods: { ... } -- extract each child as a Method
            if key == "methods" {
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
            // computed: { ... } -- extract each child as ComputedProperty
            else if key == "computed" {
                let oxc::ast::ast::Expression::ObjectExpression(computed_obj) = &p.value else {
                    continue;
                };
                for cp in &computed_obj.properties {
                    let oxc::ast::ast::ObjectPropertyKind::ObjectProperty(mp) = cp else {
                        continue;
                    };
                    let Some(prop_name) = mp.key.static_name() else {
                        continue;
                    };
                    let prop_name = prop_name.to_string();
                    let fqn = format!("{component_name}::{prop_name}");
                    defs.push(JsDef {
                        name: prop_name,
                        fqn,
                        kind: JsDefKind::ComputedProperty {
                            class_fqn: component_name.clone(),
                        },
                        range: span_to_range(mp.span),
                        is_exported: false,
                        type_annotation: None,
                    });
                }
            }
            // watch: { ... } -- extract each watcher as Watcher
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
                        kind: JsDefKind::Watcher {
                            class_fqn: component_name.clone(),
                        },
                        range: span_to_range(wp.span),
                        is_exported: false,
                        type_annotation: None,
                    });
                }
            }
            // data() -- extract as Method (it's a function returning reactive state)
            else if key == "data" {
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
            // lifecycle hooks -- extract as LifecycleHook
            else if lifecycle_hooks.contains(&key) {
                let fqn = format!("{component_name}::{key}");
                defs.push(JsDef {
                    name: key.to_string(),
                    fqn,
                    kind: JsDefKind::LifecycleHook {
                        class_fqn: component_name.clone(),
                    },
                    range: span_to_range(p.span),
                    is_exported: false,
                    type_annotation: None,
                });
            }
        }
    }
}
