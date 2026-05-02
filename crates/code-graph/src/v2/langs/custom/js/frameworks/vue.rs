//! Vue options-API detection and def synthesis.
//!
//! The public entry point, `extract_vue_options_api`, walks every
//! `export default { ... }` (or `defineComponent({ ... })`) in the
//! file and emits a virtual class def plus method / computed / watch /
//! lifecycle-hook children. All knowledge of the Vue options vocabulary
//! routes through `constants::VUE_*` so adding a new option is a single-
//! site edit.

use crate::utils::Range;
use oxc::ast::AstKind;
use oxc::ast::ast::{
    CallExpression, ExportDefaultDeclarationKind, Expression, ObjectExpression, ObjectProperty,
    ObjectPropertyKind,
};
use oxc::semantic::AstNodes;
use std::collections::HashMap;

use super::super::constants::{
    VUE_OPTION_CONTRACT_KEYS, VUE_OPTION_EXECUTABLE_FNS, VUE_OPTION_EXECUTABLE_MAPS,
    VUE_OPTION_IDENTIFIER_KEYS, is_vue_lifecycle_hook,
};
use super::super::types::{JsDef, JsDefKind, JsInvocationSupport};

/// Classification of a property key on a Vue component options object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VueOption {
    /// `methods` / `computed` / `watch` — value is an object whose
    /// members are executable.
    ExecutableMap(ExecutableMap),
    /// `data` / `setup` / `render` — value is itself executable.
    ExecutableFn,
    /// `props` / `emits` / `inject` / `provide` / `components` —
    /// contract metadata, not executable but component-identifying.
    Contract,
    /// `name` etc. — marks the object as a component but contributes
    /// no executable members.
    Identifier,
    /// Any recognised Vue lifecycle hook
    /// (see `constants::VUE_LIFECYCLE_HOOKS`).
    LifecycleHook,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutableMap {
    Methods,
    Computed,
    Watch,
}

impl VueOption {
    fn classify(key: &str) -> Option<Self> {
        if VUE_OPTION_EXECUTABLE_MAPS.contains(&key) {
            let kind = match key {
                "methods" => ExecutableMap::Methods,
                "computed" => ExecutableMap::Computed,
                "watch" => ExecutableMap::Watch,
                _ => unreachable!("VUE_OPTION_EXECUTABLE_MAPS covers these keys"),
            };
            return Some(Self::ExecutableMap(kind));
        }
        if VUE_OPTION_EXECUTABLE_FNS.contains(&key) {
            return Some(Self::ExecutableFn);
        }
        if VUE_OPTION_CONTRACT_KEYS.contains(&key) {
            return Some(Self::Contract);
        }
        if is_vue_lifecycle_hook(key) {
            return Some(Self::LifecycleHook);
        }
        if VUE_OPTION_IDENTIFIER_KEYS.contains(&key) {
            return Some(Self::Identifier);
        }
        None
    }
}

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

pub(in crate::v2::langs::custom::js) fn is_vue_like_path(relative_path: &str) -> bool {
    relative_path.ends_with(".vue") || relative_path.contains(".vue.")
}

pub(in crate::v2::langs::custom::js) fn vue_default_component_def(
    defs: &[JsDef],
    default_range: Range,
) -> Option<&JsDef> {
    defs.iter()
        .filter(|def| def.kind == JsDefKind::Class && def.is_exported)
        .find(|def| def.range.is_contained_within(default_range))
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

/// Returns `(classification, is_executable, is_contract)` for one property.
/// `is_executable` and `is_contract` require both a valid classification
/// *and* a value shape that matches the classification.
fn classify_property(property: &ObjectProperty<'_>) -> Option<(VueOption, bool, bool)> {
    let key = property.key.static_name()?;
    let classification = VueOption::classify(key.as_ref())?;
    let is_executable = match classification {
        VueOption::ExecutableMap(_) => matches!(
            property.value.get_inner_expression(),
            Expression::ObjectExpression(_)
        ),
        VueOption::ExecutableFn | VueOption::LifecycleHook => is_function_like(&property.value),
        _ => false,
    };
    let is_contract =
        matches!(classification, VueOption::Contract) && is_contract_value(&property.value);
    Some((classification, is_executable, is_contract))
}

pub(in crate::v2::langs::custom::js) fn extract_vue_options_api(
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

        // Classify every recognised property once. Unrecognised keys
        // drop out here; downstream iteration only sees classified
        // options and reuses the executability / contract booleans
        // derived during classification.
        struct ClassifiedOption<'a> {
            property: &'a ObjectProperty<'a>,
            key: String,
            classification: VueOption,
            is_executable: bool,
            is_contract: bool,
        }
        let classified: Vec<ClassifiedOption<'_>> = obj
            .properties
            .iter()
            .filter_map(|prop| {
                let ObjectPropertyKind::ObjectProperty(property) = prop else {
                    return None;
                };
                let property = property.as_ref();
                let (classification, is_executable, is_contract) = classify_property(property)?;
                let key = property.key.static_name()?.to_string();
                Some(ClassifiedOption {
                    property,
                    key,
                    classification,
                    is_executable,
                    is_contract,
                })
            })
            .collect();

        let has_executable_options = classified.iter().any(|o| o.is_executable);
        let has_contract_options = classified.iter().any(|o| o.is_contract);
        let has_known_option_keys = !classified.is_empty();
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
        // If a real class with this FQN already exists in the file
        // (hostile case: `export class Foo {}` alongside a Vue default
        // export whose component name defaults to the file stem `Foo`),
        // namespace the virtual class so it cannot collide. The `<vue>`
        // marker uses angle brackets which cannot appear in a valid JS
        // identifier.
        let collides = defs
            .iter()
            .any(|def| def.kind == JsDefKind::Class && def.fqn == component_name);
        let component_fqn = if collides {
            format!("<vue>::{component_name}")
        } else {
            component_name.clone()
        };

        class_hierarchy.insert(component_fqn.clone(), None);

        defs.push(JsDef {
            name: component_name.clone(),
            fqn: component_fqn.clone(),
            kind: JsDefKind::Class,
            range: span_to_range(obj.span),
            is_exported: true,
            type_annotation: None,
            invocation_support: Some(JsInvocationSupport::class()),
        });

        for option in &classified {
            match option.classification {
                VueOption::ExecutableMap(map) => {
                    emit_executable_map(option.property, map, &component_fqn, defs, &span_to_range)
                }
                VueOption::ExecutableFn => emit_executable_fn(
                    option.property,
                    &option.key,
                    &component_fqn,
                    defs,
                    &span_to_range,
                ),
                VueOption::LifecycleHook => emit_lifecycle_hook(
                    option.property,
                    &option.key,
                    &component_fqn,
                    defs,
                    &span_to_range,
                ),
                VueOption::Contract | VueOption::Identifier => {}
            }
        }
    }
}

fn emit_executable_map(
    property: &ObjectProperty<'_>,
    map: ExecutableMap,
    component_name: &str,
    defs: &mut Vec<JsDef>,
    span_to_range: impl Fn(oxc::span::Span) -> Range,
) {
    let Expression::ObjectExpression(members) = &property.value else {
        return;
    };
    for member in &members.properties {
        let ObjectPropertyKind::ObjectProperty(member) = member else {
            continue;
        };
        let Some(member_name) = member.key.static_name() else {
            continue;
        };
        let member_name = member_name.to_string();
        let (name, fqn, kind, invocation_support) = match map {
            ExecutableMap::Methods => (
                member_name.clone(),
                format!("{component_name}::{member_name}"),
                JsDefKind::Method {
                    class_fqn: component_name.to_string(),
                    is_static: false,
                },
                Some(JsInvocationSupport::function()),
            ),
            ExecutableMap::Computed => (
                member_name.clone(),
                format!("{component_name}::{member_name}"),
                JsDefKind::ComputedProperty {
                    class_fqn: component_name.to_string(),
                },
                None,
            ),
            ExecutableMap::Watch => (
                format!("watch_{member_name}"),
                format!("{component_name}::watch_{member_name}"),
                JsDefKind::Watcher {
                    class_fqn: component_name.to_string(),
                },
                Some(JsInvocationSupport::function()),
            ),
        };
        defs.push(JsDef {
            name,
            fqn,
            kind,
            range: span_to_range(member.span),
            is_exported: false,
            type_annotation: None,
            invocation_support,
        });
    }
}

fn emit_executable_fn(
    property: &ObjectProperty<'_>,
    key: &str,
    component_name: &str,
    defs: &mut Vec<JsDef>,
    span_to_range: impl Fn(oxc::span::Span) -> Range,
) {
    defs.push(JsDef {
        name: key.to_string(),
        fqn: format!("{component_name}::{key}"),
        kind: JsDefKind::Method {
            class_fqn: component_name.to_string(),
            is_static: false,
        },
        range: span_to_range(property.span),
        is_exported: false,
        type_annotation: None,
        invocation_support: Some(JsInvocationSupport::function()),
    });
}

fn emit_lifecycle_hook(
    property: &ObjectProperty<'_>,
    key: &str,
    component_name: &str,
    defs: &mut Vec<JsDef>,
    span_to_range: impl Fn(oxc::span::Span) -> Range,
) {
    defs.push(JsDef {
        name: key.to_string(),
        fqn: format!("{component_name}::{key}"),
        kind: JsDefKind::LifecycleHook {
            class_fqn: component_name.to_string(),
        },
        range: span_to_range(property.span),
        is_exported: false,
        type_annotation: None,
        invocation_support: Some(JsInvocationSupport::function()),
    });
}
