use crate::utils::Range;
use oxc::ast::AstKind;
use oxc::ast::ast::{
    CallExpression, ExportDefaultDeclarationKind, Expression, ObjectExpression, ObjectPropertyKind,
};
use oxc::semantic::AstNodes;
use std::collections::HashMap;

use super::super::types::{JsDef, JsDefKind, JsInvocationSupport};

const VUE_LIFECYCLE_HOOKS: &[&str] = &[
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
    "serverPrefetch",
];

fn vue_component_object<'a>(
    declaration: &'a ExportDefaultDeclarationKind<'a>,
    allow_loose_detection: bool,
) -> Option<(&'a ObjectExpression<'a>, bool)> {
    match declaration {
        ExportDefaultDeclarationKind::ObjectExpression(object) => {
            allow_loose_detection.then_some((object, false))
        }
        ExportDefaultDeclarationKind::CallExpression(call) => {
            vue_component_object_from_call(call, allow_loose_detection).map(|object| (object, true))
        }
        _ => None,
    }
}

fn vue_component_object_from_call<'a>(
    call: &'a CallExpression<'a>,
    allow_loose_detection: bool,
) -> Option<&'a ObjectExpression<'a>> {
    if !allow_loose_detection && !is_known_vue_component_wrapper(call) {
        return None;
    }
    (call.arguments.len() == 1)
        .then_some(call.arguments.first()?.as_expression()?)
        .and_then(|expression| match expression.get_inner_expression() {
            Expression::ObjectExpression(object) => Some(object),
            _ => None,
        })
        .map(|object| &**object)
}

fn is_vue_like_path(relative_path: &str) -> bool {
    relative_path.ends_with(".vue") || relative_path.contains(".vue.")
}

fn is_known_vue_component_wrapper(call: &CallExpression<'_>) -> bool {
    match call.callee.get_inner_expression() {
        Expression::Identifier(identifier) => matches!(
            identifier.name.as_str(),
            "defineComponent" | "defineAsyncComponent" | "defineNuxtComponent"
        ),
        Expression::StaticMemberExpression(member) => {
            matches!(member.object.get_inner_expression(), Expression::Identifier(identifier) if identifier.name == "Vue")
                && member.property.name == "extend"
        }
        _ => false,
    }
}

fn explicit_component_name(obj: &ObjectExpression<'_>) -> Option<String> {
    obj.properties.iter().find_map(|prop| {
        let ObjectPropertyKind::ObjectProperty(p) = prop else {
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
}

fn is_function_like(expression: &Expression<'_>) -> bool {
    matches!(
        expression.get_inner_expression(),
        Expression::ArrowFunctionExpression(_) | Expression::FunctionExpression(_)
    )
}

fn is_contract_value(expression: &Expression<'_>) -> bool {
    matches!(
        expression.get_inner_expression(),
        Expression::ObjectExpression(_)
            | Expression::ArrayExpression(_)
            | Expression::ArrowFunctionExpression(_)
            | Expression::FunctionExpression(_)
    )
}

fn is_vue_lifecycle_hook(key: &str) -> bool {
    VUE_LIFECYCLE_HOOKS.contains(&key)
}

fn is_executable_vue_option(property: &oxc::ast::ast::ObjectProperty<'_>) -> bool {
    match property.key.static_name().as_deref() {
        Some("methods" | "computed" | "watch") => {
            matches!(
                property.value.get_inner_expression(),
                Expression::ObjectExpression(_)
            )
        }
        Some("data" | "setup" | "render") => is_function_like(&property.value),
        Some(key) if is_vue_lifecycle_hook(key) => is_function_like(&property.value),
        _ => false,
    }
}

fn is_contract_vue_option(property: &oxc::ast::ast::ObjectProperty<'_>) -> bool {
    match property.key.static_name().as_deref() {
        Some("props" | "emits" | "inject" | "provide" | "components") => {
            is_contract_value(&property.value)
        }
        _ => false,
    }
}

fn is_known_vue_option_key(property: &oxc::ast::ast::ObjectProperty<'_>) -> bool {
    matches!(
        property.key.static_name().as_deref(),
        Some(
            "name"
                | "methods"
                | "computed"
                | "watch"
                | "data"
                | "setup"
                | "render"
                | "props"
                | "emits"
                | "inject"
                | "provide"
                | "components"
        )
    ) || property
        .key
        .static_name()
        .as_deref()
        .is_some_and(is_vue_lifecycle_hook)
}

pub(super) fn extract_vue_options_api(
    nodes: &AstNodes,
    span_to_range: impl Fn(oxc::span::Span) -> Range,
    relative_path: &str,
    defs: &mut Vec<JsDef>,
    class_hierarchy: &mut HashMap<String, Option<String>>,
) {
    let is_vue_sfc = is_vue_like_path(relative_path);
    let allow_loose_detection = is_vue_sfc;

    for node in nodes.iter() {
        let AstKind::ExportDefaultDeclaration(decl) = node.kind() else {
            continue;
        };
        let Some((obj, is_wrapped)) =
            vue_component_object(&decl.declaration, allow_loose_detection)
        else {
            continue;
        };

        let explicit_name = explicit_component_name(obj);
        let object_properties: Vec<_> = obj
            .properties
            .iter()
            .filter_map(|prop| match prop {
                ObjectPropertyKind::ObjectProperty(property) => Some(property.as_ref()),
                _ => None,
            })
            .collect();
        let has_executable_options = object_properties
            .iter()
            .copied()
            .any(is_executable_vue_option);
        let has_contract_options = object_properties
            .iter()
            .copied()
            .any(is_contract_vue_option);
        let has_known_option_keys = object_properties
            .iter()
            .copied()
            .any(is_known_vue_option_key);
        let allows_contract_only = explicit_name.is_some()
            && has_contract_options
            && (is_wrapped || allow_loose_detection);
        let is_sfc_options_object = is_vue_sfc
            && (!has_known_option_keys
                || explicit_name.is_some()
                || has_executable_options
                || has_contract_options);
        if !has_executable_options && !allows_contract_only && !is_sfc_options_object {
            continue;
        }
        let component_name = explicit_name.unwrap_or_else(|| {
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
            invocation_support: Some(JsInvocationSupport::class()),
        });

        for prop in &obj.properties {
            let ObjectPropertyKind::ObjectProperty(p) = prop else {
                continue;
            };
            let Some(key_name) = p.key.static_name() else {
                continue;
            };
            let key = key_name.as_ref();

            // methods: { ... } -- extract each child as a Method
            if key == "methods" {
                let Expression::ObjectExpression(methods_obj) = &p.value else {
                    continue;
                };
                for method_prop in &methods_obj.properties {
                    let ObjectPropertyKind::ObjectProperty(mp) = method_prop else {
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
                        invocation_support: Some(JsInvocationSupport::function()),
                    });
                }
            }
            // computed: { ... } -- extract each child as ComputedProperty
            else if key == "computed" {
                let Expression::ObjectExpression(computed_obj) = &p.value else {
                    continue;
                };
                for cp in &computed_obj.properties {
                    let ObjectPropertyKind::ObjectProperty(mp) = cp else {
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
                        invocation_support: None,
                    });
                }
            }
            // watch: { ... } -- extract each watcher as Watcher
            else if key == "watch" {
                let Expression::ObjectExpression(watch_obj) = &p.value else {
                    continue;
                };
                for watch_prop in &watch_obj.properties {
                    let ObjectPropertyKind::ObjectProperty(wp) = watch_prop else {
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
                        invocation_support: Some(JsInvocationSupport::function()),
                    });
                }
            }
            // data(), setup(), render() -- extract as Method-style definitions.
            else if matches!(key, "data" | "setup" | "render") {
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
                    invocation_support: Some(JsInvocationSupport::function()),
                });
            }
            // lifecycle hooks -- extract as LifecycleHook
            else if is_vue_lifecycle_hook(key) {
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
                    invocation_support: Some(JsInvocationSupport::function()),
                });
            }
        }
    }
}
