use oxc::ast::ast::{BindingIdentifier, BindingPattern, ObjectExpression, ObjectPropertyKind};
use oxc::span::Span;

pub(super) fn walk_binding_pattern_identifiers<'a>(
    pattern: &'a BindingPattern<'a>,
    imported_name: Option<String>,
    recurse_object_properties: bool,
    recurse_array_elements: bool,
    visit: &mut impl FnMut(&'a BindingIdentifier<'a>, Option<String>),
) {
    // Hostile source can nest object/array patterns arbitrarily deep
    // (`const {a:{a:{a:...}}} = x`). Grow the stack instead of overflowing.
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        walk_binding_pattern_identifiers_inner(
            pattern,
            imported_name,
            recurse_object_properties,
            recurse_array_elements,
            visit,
        )
    })
}

fn walk_binding_pattern_identifiers_inner<'a>(
    pattern: &'a BindingPattern<'a>,
    imported_name: Option<String>,
    recurse_object_properties: bool,
    recurse_array_elements: bool,
    visit: &mut impl FnMut(&'a BindingIdentifier<'a>, Option<String>),
) {
    match pattern {
        BindingPattern::BindingIdentifier(binding) => visit(binding, imported_name),
        BindingPattern::AssignmentPattern(assignment) => walk_binding_pattern_identifiers(
            &assignment.left,
            imported_name,
            recurse_object_properties,
            recurse_array_elements,
            visit,
        ),
        BindingPattern::ObjectPattern(object) => {
            if !recurse_object_properties {
                return;
            }

            for property in &object.properties {
                let property_name = property.key.static_name().map(|name| name.into_owned());
                walk_binding_pattern_identifiers(
                    &property.value,
                    property_name,
                    recurse_object_properties,
                    recurse_array_elements,
                    visit,
                );
            }

            if recurse_array_elements && let Some(rest) = &object.rest {
                walk_binding_pattern_identifiers(
                    &rest.argument,
                    None,
                    recurse_object_properties,
                    recurse_array_elements,
                    visit,
                );
            }
        }
        BindingPattern::ArrayPattern(array) => {
            if !recurse_array_elements {
                return;
            }

            for element in array.elements.iter().flatten() {
                walk_binding_pattern_identifiers(
                    element,
                    None,
                    recurse_object_properties,
                    recurse_array_elements,
                    visit,
                );
            }

            if let Some(rest) = &array.rest {
                walk_binding_pattern_identifiers(
                    &rest.argument,
                    None,
                    recurse_object_properties,
                    recurse_array_elements,
                    visit,
                );
            }
        }
    }
}

pub(super) fn for_each_static_object_property<'a>(
    object: &'a ObjectExpression<'a>,
    visit: &mut impl FnMut(String, &'a oxc::ast::ast::Expression<'a>, Span),
) {
    for property in &object.properties {
        let ObjectPropertyKind::ObjectProperty(property) = property else {
            continue;
        };
        let Some(name) = property.key.static_name() else {
            continue;
        };
        visit(name.into_owned(), &property.value, property.span);
    }
}
