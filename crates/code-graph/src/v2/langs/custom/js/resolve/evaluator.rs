use oxc::allocator::Allocator;
use oxc::ast::ast::{Expression, ObjectExpression, ObjectPropertyKind, Statement};
use oxc::parser::Parser;
use oxc::span::SourceType;
use rustc_hash::FxHashMap;
use std::path::{Path, PathBuf};

const MAX_EVAL_MODULES: usize = 64;
const MAX_EVAL_DEPTH: usize = 12;
const MAX_EVAL_FILE_BYTES: u64 = 256 * 1024;
const MAX_EVAL_TOTAL_BYTES: u64 = 1024 * 1024;
const MAX_EVAL_STATEMENTS: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum EvaluatedValue {
    Bool(bool),
    String(String),
    Object(FxHashMap<String, EvaluatedValue>),
    Array(Vec<EvaluatedValue>),
    PathModule,
    FsModule,
    Json,
    Process,
    Undefined,
}

struct AliasEvalContext<'a> {
    root_dir: &'a Path,
    config_dir: &'a Path,
    module_depth: usize,
}

#[derive(Default)]
struct AliasEvalState {
    vars: FxHashMap<String, EvaluatedValue>,
    named_exports: FxHashMap<String, EvaluatedValue>,
    module_exports: Option<EvaluatedValue>,
}

#[derive(Default)]
pub(super) struct ModuleEvalCache {
    exports: FxHashMap<PathBuf, Option<EvaluatedValue>>,
    total_bytes: u64,
}

pub(super) fn evaluate_module_exports(
    root_dir: &Path,
    module_path: &Path,
    cache: &mut ModuleEvalCache,
    depth: usize,
) -> Option<EvaluatedValue> {
    if depth > MAX_EVAL_DEPTH {
        return None;
    }

    let module_path =
        canonical_repo_existing_path(root_dir, &normalize_path(module_path.to_path_buf()))?;
    if let Some(cached) = cache.exports.get(&module_path) {
        return cached.clone();
    }

    if cache.exports.len() >= MAX_EVAL_MODULES {
        return None;
    }

    let module_len = std::fs::metadata(&module_path).ok()?.len();
    if module_len > MAX_EVAL_FILE_BYTES
        || cache.total_bytes.saturating_add(module_len) > MAX_EVAL_TOTAL_BYTES
    {
        return None;
    }

    cache.total_bytes += module_len;
    cache.exports.insert(module_path.clone(), None);

    let evaluated = if module_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    {
        std::fs::read_to_string(&module_path)
            .ok()
            .and_then(|source| serde_json::from_str::<serde_json::Value>(&source).ok())
            .and_then(json_to_evaluated)
    } else {
        evaluate_script_module(root_dir, &module_path, cache, depth)
    };

    cache.exports.insert(module_path, evaluated.clone());
    evaluated
}

fn evaluate_script_module(
    root_dir: &Path,
    module_path: &Path,
    cache: &mut ModuleEvalCache,
    depth: usize,
) -> Option<EvaluatedValue> {
    let source = std::fs::read_to_string(module_path).ok()?;
    let source_type = SourceType::from_path(module_path).ok()?;

    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, &source, source_type).parse();
    if parsed.panicked || parsed.program.body.len() > MAX_EVAL_STATEMENTS {
        return None;
    }

    let context = AliasEvalContext {
        root_dir,
        config_dir: module_path.parent().unwrap_or(root_dir),
        module_depth: depth,
    };
    let mut state = AliasEvalState::default();

    for statement in &parsed.program.body {
        evaluate_module_statement(statement, &context, &mut state, cache);
    }

    state.module_exports.or_else(|| {
        (!state.named_exports.is_empty()).then_some(EvaluatedValue::Object(state.named_exports))
    })
}

fn evaluate_module_statement(
    statement: &Statement<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        evaluate_module_statement_inner(statement, context, state, cache)
    })
}

fn evaluate_module_statement_inner(
    statement: &Statement<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match statement {
        Statement::BlockStatement(block) => {
            for statement in &block.body {
                evaluate_module_statement(statement, context, state, cache);
            }
        }
        Statement::VariableDeclaration(variable_declaration) => {
            for declarator in &variable_declaration.declarations {
                let Some(init) = &declarator.init else {
                    continue;
                };

                if let Some(value) = evaluate_value(init, context, state, cache) {
                    bind_pattern_value(&declarator.id, value, state);
                }
            }
        }
        Statement::IfStatement(if_statement) => {
            match evaluate_bool(&if_statement.test, context, state, cache) {
                Some(true) => {
                    evaluate_module_statement(&if_statement.consequent, context, state, cache)
                }
                Some(false) => {
                    if let Some(alternate) = &if_statement.alternate {
                        evaluate_module_statement(alternate, context, state, cache);
                    }
                }
                None => {}
            }
        }
        Statement::ExpressionStatement(expression_statement) => {
            evaluate_top_level_expression(&expression_statement.expression, context, state, cache);
        }
        Statement::ExportDefaultDeclaration(export_default) => {
            state.module_exports =
                evaluate_export_default(&export_default.declaration, context, state, cache);
        }
        Statement::ExportNamedDeclaration(export_named) => {
            if let Some(declaration) = &export_named.declaration {
                evaluate_exported_declaration(declaration, context, state, cache);
            }
        }
        _ => {}
    }
}

fn evaluate_export_default(
    declaration: &oxc::ast::ast::ExportDefaultDeclarationKind<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match declaration {
        oxc::ast::ast::ExportDefaultDeclarationKind::ObjectExpression(object) => {
            evaluate_object_expression(object, context, state, cache)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::CallExpression(call) => {
            evaluate_call_expression(call, context, state, cache)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::Identifier(identifier) => {
            evaluate_identifier(identifier.name.as_str(), context, state)
        }
        oxc::ast::ast::ExportDefaultDeclarationKind::FunctionDeclaration(_)
        | oxc::ast::ast::ExportDefaultDeclarationKind::ClassDeclaration(_)
        | oxc::ast::ast::ExportDefaultDeclarationKind::TSInterfaceDeclaration(_) => None,
        _ => None,
    }
}

fn evaluate_exported_declaration(
    declaration: &oxc::ast::ast::Declaration<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match declaration {
        oxc::ast::ast::Declaration::VariableDeclaration(variable_declaration) => {
            for declarator in &variable_declaration.declarations {
                let Some(init) = &declarator.init else {
                    continue;
                };
                let Some(value) = evaluate_value(init, context, state, cache) else {
                    continue;
                };
                bind_pattern_value(&declarator.id, value.clone(), state);
                collect_named_exports(&declarator.id, value, &mut state.named_exports);
            }
        }
        oxc::ast::ast::Declaration::FunctionDeclaration(function) => {
            if let Some(id) = &function.id {
                state
                    .named_exports
                    .insert(id.name.to_string(), EvaluatedValue::Undefined);
            }
        }
        _ => {}
    }
}

fn collect_named_exports(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    value: EvaluatedValue,
    named_exports: &mut FxHashMap<String, EvaluatedValue>,
) {
    match pattern {
        oxc::ast::ast::BindingPattern::BindingIdentifier(binding) => {
            named_exports.insert(binding.name.to_string(), value);
        }
        oxc::ast::ast::BindingPattern::AssignmentPattern(assignment) => {
            collect_named_exports(&assignment.left, value, named_exports);
        }
        oxc::ast::ast::BindingPattern::ObjectPattern(_)
        | oxc::ast::ast::BindingPattern::ArrayPattern(_) => {}
    }
}

fn evaluate_top_level_expression(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    match expression.get_inner_expression() {
        Expression::AssignmentExpression(assignment) => {
            apply_assignment(assignment, context, state, cache);
        }
        Expression::CallExpression(call) => {
            maybe_apply_object_assign(call, context, state, cache);
        }
        _ => {}
    }
}

fn bind_pattern_value(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    value: EvaluatedValue,
    state: &mut AliasEvalState,
) {
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        bind_pattern_value_inner(pattern, value, state)
    });
}

fn bind_pattern_value_inner(
    pattern: &oxc::ast::ast::BindingPattern<'_>,
    value: EvaluatedValue,
    state: &mut AliasEvalState,
) {
    match pattern {
        oxc::ast::ast::BindingPattern::BindingIdentifier(binding) => {
            state.vars.insert(binding.name.to_string(), value);
        }
        oxc::ast::ast::BindingPattern::AssignmentPattern(assignment) => {
            bind_pattern_value(&assignment.left, value, state);
        }
        oxc::ast::ast::BindingPattern::ObjectPattern(object) => {
            let Some(object_value) = as_object(&value) else {
                return;
            };
            for property in &object.properties {
                let Some(property_name) = property.key.static_name() else {
                    continue;
                };
                let Some(property_value) = object_value.get(property_name.as_ref()).cloned() else {
                    continue;
                };
                bind_pattern_value(&property.value, property_value, state);
            }
            if let Some(rest) = &object.rest {
                bind_pattern_value(&rest.argument, value, state);
            }
        }
        oxc::ast::ast::BindingPattern::ArrayPattern(array) => {
            let EvaluatedValue::Array(items) = value else {
                return;
            };
            for (index, element) in array.elements.iter().enumerate() {
                let Some(element) = element else {
                    continue;
                };
                let Some(item_value) = items.get(index).cloned() else {
                    continue;
                };
                bind_pattern_value(element, item_value, state);
            }
            if let Some(rest) = &array.rest {
                bind_pattern_value(&rest.argument, EvaluatedValue::Array(items), state);
            }
        }
    }
}

fn apply_assignment(
    assignment: &oxc::ast::ast::AssignmentExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    let Some(value) = evaluate_value(&assignment.right, context, state, cache) else {
        return;
    };

    if let Some(target) = assignment.left.as_simple_assignment_target() {
        if let Some(member) = target.as_member_expression()
            && let Some(path) = member_path(member.object(), member.static_property_name())
        {
            set_member_path_value(path, value, state);
            return;
        }
        if let Some(identifier) = target.get_identifier_name() {
            state.vars.insert(identifier.to_string(), value);
        }
    }
}

fn maybe_apply_object_assign(
    call: &oxc::ast::ast::CallExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &mut AliasEvalState,
    cache: &mut ModuleEvalCache,
) {
    let Some(member) = call.callee.get_member_expr() else {
        return;
    };
    if !member.is_specific_member_access("Object", "assign") {
        return;
    }
    let Some(target) = call.arguments.first() else {
        return;
    };
    let Some(path) = assignment_target_path_from_argument(target) else {
        return;
    };

    for argument in call.arguments.iter().skip(1) {
        let Some(value) = evaluate_argument_value(argument, context, state, cache) else {
            continue;
        };
        merge_value_into_target(path.as_slice(), value, state);
    }
}

fn assignment_target_path_from_argument(
    argument: &oxc::ast::ast::Argument<'_>,
) -> Option<Vec<String>> {
    match argument {
        oxc::ast::ast::Argument::Identifier(identifier) => Some(vec![identifier.name.to_string()]),
        oxc::ast::ast::Argument::StaticMemberExpression(member) => {
            member_path(&member.object, Some(member.property.name.as_str()))
        }
        _ => None,
    }
}

fn member_path(expression: &Expression<'_>, final_property: Option<&str>) -> Option<Vec<String>> {
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        let mut path = match expression.get_inner_expression() {
            Expression::Identifier(identifier) => vec![identifier.name.to_string()],
            Expression::StaticMemberExpression(member) => {
                member_path(&member.object, Some(member.property.name.as_str()))?
            }
            _ => return None,
        };
        if let Some(property) = final_property {
            path.push(property.to_string());
        }
        Some(path)
    })
}

fn set_member_path_value(path: Vec<String>, value: EvaluatedValue, state: &mut AliasEvalState) {
    if path.is_empty() {
        return;
    }
    if path == ["module".to_string(), "exports".to_string()] {
        state.module_exports = Some(value);
        return;
    }
    if path.first().is_some_and(|segment| segment == "exports") {
        let module_exports = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        set_object_path_value(module_exports, &path[1..], value.clone());
        if path.len() == 2 {
            state.named_exports.insert(path[1].clone(), value);
        }
        return;
    }
    if path.first().is_some_and(|segment| segment == "module")
        && path.get(1).is_some_and(|segment| segment == "exports")
    {
        let module_exports = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        set_object_path_value(module_exports, &path[2..], value.clone());
        if path.len() == 3 {
            state.named_exports.insert(path[2].clone(), value);
        }
        return;
    }

    if path.len() == 1 {
        state.vars.insert(path[0].clone(), value);
        return;
    }

    let object = state
        .vars
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
    set_object_path_value(object, &path[1..], value);
}

fn set_object_path_value(target: &mut EvaluatedValue, path: &[String], value: EvaluatedValue) {
    if path.is_empty() {
        *target = value;
        return;
    }
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(FxHashMap::default());
        set_object_path_value(target, path, value);
        return;
    };
    if path.len() == 1 {
        object.insert(path[0].clone(), value);
        return;
    }
    let child = object
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
    set_object_path_value(child, &path[1..], value);
}

fn merge_value_into_target(path: &[String], value: EvaluatedValue, state: &mut AliasEvalState) {
    let Some(source) = as_object(&value).cloned() else {
        return;
    };

    if path == ["module".to_string(), "exports".to_string()] {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        merge_object(target, source);
        return;
    }

    if path.first().is_some_and(|segment| segment == "exports") {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        let nested = ensure_object_path(target, &path[1..]);
        merge_object(nested, source.clone());
        for (key, value) in source {
            state.named_exports.insert(key, value);
        }
        return;
    }

    if path.first().is_some_and(|segment| segment == "module")
        && path.get(1).is_some_and(|segment| segment == "exports")
    {
        let target = state
            .module_exports
            .get_or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        let nested = ensure_object_path(target, &path[2..]);
        merge_object(nested, source.clone());
        if path.len() == 3 {
            for (key, value) in source {
                if key == path[2] {
                    state.named_exports.insert(key, value);
                }
            }
        }
        return;
    }

    let target = if path.len() == 1 {
        state
            .vars
            .entry(path[0].clone())
            .or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()))
    } else {
        let object = state
            .vars
            .entry(path[0].clone())
            .or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
        ensure_object_path(object, &path[1..])
    };
    merge_object(target, source);
}

fn ensure_object_path<'a>(
    target: &'a mut EvaluatedValue,
    path: &[String],
) -> &'a mut EvaluatedValue {
    if path.is_empty() {
        return target;
    }
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(FxHashMap::default());
        return ensure_object_path(target, path);
    };
    let child = object
        .entry(path[0].clone())
        .or_insert_with(|| EvaluatedValue::Object(FxHashMap::default()));
    ensure_object_path(child, &path[1..])
}

fn merge_object(target: &mut EvaluatedValue, source: FxHashMap<String, EvaluatedValue>) {
    let EvaluatedValue::Object(object) = target else {
        *target = EvaluatedValue::Object(source);
        return;
    };
    for (key, value) in source {
        object.insert(key, value);
    }
}

fn evaluate_value(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || {
        evaluate_value_inner(expression, context, state, cache)
    })
}

fn evaluate_value_inner(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match expression.get_inner_expression() {
        Expression::BooleanLiteral(boolean) => Some(EvaluatedValue::Bool(boolean.value)),
        Expression::StringLiteral(string) => Some(EvaluatedValue::String(string.value.to_string())),
        Expression::NullLiteral(_) => Some(EvaluatedValue::Undefined),
        Expression::TemplateLiteral(template) if template.expressions.is_empty() => Some(
            EvaluatedValue::String(template.quasis.first()?.value.cooked?.to_string()),
        ),
        Expression::ObjectExpression(object) => {
            evaluate_object_expression(object, context, state, cache)
        }
        Expression::ArrayExpression(array) => Some(EvaluatedValue::Array(
            array
                .elements
                .iter()
                .filter_map(|element| match element {
                    oxc::ast::ast::ArrayExpressionElement::SpreadElement(_) => None,
                    element => evaluate_array_element(element, context, state, cache),
                })
                .collect(),
        )),
        Expression::Identifier(identifier) => {
            evaluate_identifier(identifier.name.as_str(), context, state)
        }
        Expression::StaticMemberExpression(member) => {
            evaluate_static_member_expression(member, context, state, cache)
        }
        Expression::ComputedMemberExpression(member) => {
            evaluate_computed_member_expression(member, context, state, cache)
        }
        Expression::CallExpression(call) => evaluate_call_expression(call, context, state, cache),
        Expression::UnaryExpression(unary)
            if unary.operator == oxc::ast::ast::UnaryOperator::LogicalNot =>
        {
            Some(EvaluatedValue::Bool(!evaluate_bool(
                &unary.argument,
                context,
                state,
                cache,
            )?))
        }
        Expression::LogicalExpression(logical) => {
            let left = evaluate_value(&logical.left, context, state, cache)?;
            match logical.operator {
                oxc::ast::ast::LogicalOperator::And => {
                    if is_truthy(&left) {
                        evaluate_value(&logical.right, context, state, cache)
                    } else {
                        Some(left)
                    }
                }
                oxc::ast::ast::LogicalOperator::Or => {
                    if is_truthy(&left) {
                        Some(left)
                    } else {
                        evaluate_value(&logical.right, context, state, cache)
                    }
                }
                oxc::ast::ast::LogicalOperator::Coalesce => {
                    if !matches!(left, EvaluatedValue::Undefined) {
                        Some(left)
                    } else {
                        evaluate_value(&logical.right, context, state, cache)
                    }
                }
            }
        }
        Expression::BinaryExpression(binary) => {
            let left = evaluate_value(&binary.left, context, state, cache)?;
            let right = evaluate_value(&binary.right, context, state, cache)?;
            let value = match binary.operator {
                oxc::ast::ast::BinaryOperator::Equality
                | oxc::ast::ast::BinaryOperator::StrictEquality => left == right,
                oxc::ast::ast::BinaryOperator::Inequality
                | oxc::ast::ast::BinaryOperator::StrictInequality => left != right,
                _ => return None,
            };
            Some(EvaluatedValue::Bool(value))
        }
        _ => None,
    }
}

fn evaluate_identifier(
    name: &str,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
) -> Option<EvaluatedValue> {
    if let Some(value) = state.vars.get(name) {
        return Some(value.clone());
    }
    if let Some(value) = state.named_exports.get(name) {
        return Some(value.clone());
    }

    match name {
        "__dirname" => Some(EvaluatedValue::String(
            context.config_dir.to_string_lossy().to_string(),
        )),
        "JSON" => Some(EvaluatedValue::Json),
        "process" => Some(EvaluatedValue::Process),
        _ => None,
    }
}

fn evaluate_bool(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<bool> {
    evaluate_value(expression, context, state, cache).map(|value| is_truthy(&value))
}

fn evaluate_string(
    expression: &Expression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<String> {
    match evaluate_value(expression, context, state, cache)? {
        EvaluatedValue::String(value) => Some(value),
        _ => None,
    }
}

fn evaluate_object_expression(
    object: &ObjectExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let mut values = FxHashMap::default();

    for property in &object.properties {
        match property {
            ObjectPropertyKind::ObjectProperty(property) => {
                let Some(key) = property.key.static_name().map(|name| name.to_string()) else {
                    continue;
                };
                let Some(value) = evaluate_value(&property.value, context, state, cache) else {
                    continue;
                };
                values.insert(key, value);
            }
            ObjectPropertyKind::SpreadProperty(spread) => {
                let Some(spread_value) = evaluate_value(&spread.argument, context, state, cache)
                else {
                    continue;
                };
                let Some(spread_object) = as_object(&spread_value) else {
                    continue;
                };
                values.extend(spread_object.clone());
            }
        }
    }

    Some(EvaluatedValue::Object(values))
}

fn evaluate_array_element(
    element: &oxc::ast::ast::ArrayExpressionElement<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match element {
        oxc::ast::ast::ArrayExpressionElement::Elision(_) => Some(EvaluatedValue::Undefined),
        element => evaluate_value(element.as_expression()?, context, state, cache),
    }
}

fn evaluate_static_member_expression(
    member: &oxc::ast::ast::StaticMemberExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let object = evaluate_value(&member.object, context, state, cache)?;
    evaluate_member_value(object, member.property.name.as_str())
}

fn evaluate_computed_member_expression(
    member: &oxc::ast::ast::ComputedMemberExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    let object = evaluate_value(&member.object, context, state, cache)?;
    let property = evaluate_string(&member.expression, context, state, cache)?;
    evaluate_member_value(object, &property)
}

fn evaluate_member_value(object: EvaluatedValue, property: &str) -> Option<EvaluatedValue> {
    match object {
        EvaluatedValue::Process if property == "env" => {
            Some(EvaluatedValue::Object(FxHashMap::default()))
        }
        EvaluatedValue::Object(map) => map
            .get(property)
            .cloned()
            .or(Some(EvaluatedValue::Undefined)),
        _ => None,
    }
}

fn evaluate_call_expression(
    call: &oxc::ast::ast::CallExpression<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    if let Expression::Identifier(identifier) = call.callee.get_inner_expression()
        && identifier.name == "require"
    {
        let specifier = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
        return evaluate_require_call(&specifier, context, cache);
    }

    let member = call.callee.get_member_expr()?;
    let object = evaluate_value(member.object(), context, state, cache)?;
    let property = member.static_property_name()?;

    match (object, property) {
        (EvaluatedValue::PathModule, "join" | "resolve") => {
            let mut parts = Vec::with_capacity(call.arguments.len());
            for argument in &call.arguments {
                let value = evaluate_argument_string(argument, context, state, cache)?;
                parts.push(value);
            }
            Some(EvaluatedValue::String(normalize_joined_path(
                property, parts,
            )))
        }
        (EvaluatedValue::Json, "parse") => {
            let raw = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
            serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(json_to_evaluated)
        }
        (EvaluatedValue::FsModule, "existsSync") => {
            let path = evaluate_argument_string(call.arguments.first()?, context, state, cache)?;
            Some(EvaluatedValue::Bool(
                contained_repo_existing_path(context.root_dir, context.config_dir, &path).is_some(),
            ))
        }
        _ => None,
    }
}

fn evaluate_require_call(
    specifier: &str,
    context: &AliasEvalContext<'_>,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match specifier {
        "path" => Some(EvaluatedValue::PathModule),
        "fs" => Some(EvaluatedValue::FsModule),
        _ => {
            let module_path = resolve_local_module_path(context, specifier)?;
            evaluate_module_exports(
                context.root_dir,
                &module_path,
                cache,
                context.module_depth + 1,
            )
        }
    }
}

fn resolve_local_module_path(context: &AliasEvalContext<'_>, specifier: &str) -> Option<PathBuf> {
    if !(specifier.starts_with('.') || specifier.starts_with('/')) {
        return None;
    }

    let base = if Path::new(specifier).is_absolute() {
        PathBuf::from(specifier)
    } else {
        context.config_dir.join(specifier)
    };
    let base = normalize_path(base);

    if let Some(candidate) = canonical_repo_existing_path(context.root_dir, &base)
        && candidate.is_file()
    {
        return Some(candidate);
    }

    for extension in super::super::constants::EVAL_EXTENSIONS {
        let candidate = PathBuf::from(format!("{}.{}", base.to_string_lossy(), extension));
        if let Some(candidate) = canonical_repo_existing_path(context.root_dir, &candidate)
            && candidate.is_file()
        {
            return Some(candidate);
        }
    }

    if let Some(base_dir) = canonical_repo_existing_path(context.root_dir, &base)
        && base_dir.is_dir()
    {
        for extension in super::super::constants::EVAL_EXTENSIONS {
            let candidate = base_dir.join(format!("index.{extension}"));
            if let Some(candidate) = canonical_repo_existing_path(context.root_dir, &candidate)
                && candidate.is_file()
            {
                return Some(candidate);
            }
        }
    }

    None
}

fn evaluate_argument_string(
    argument: &oxc::ast::ast::Argument<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<String> {
    let expression = match argument {
        oxc::ast::ast::Argument::SpreadElement(_) => return None,
        argument => argument.as_expression()?,
    };
    match evaluate_value(expression, context, state, cache)? {
        EvaluatedValue::String(value) => Some(value),
        _ => None,
    }
}

fn evaluate_argument_value(
    argument: &oxc::ast::ast::Argument<'_>,
    context: &AliasEvalContext<'_>,
    state: &AliasEvalState,
    cache: &mut ModuleEvalCache,
) -> Option<EvaluatedValue> {
    match argument {
        oxc::ast::ast::Argument::SpreadElement(_) => None,
        argument => evaluate_value(argument.as_expression()?, context, state, cache),
    }
}

fn as_object(value: &EvaluatedValue) -> Option<&FxHashMap<String, EvaluatedValue>> {
    match value {
        EvaluatedValue::Object(object) => Some(object),
        _ => None,
    }
}

fn is_truthy(value: &EvaluatedValue) -> bool {
    match value {
        EvaluatedValue::Bool(value) => *value,
        EvaluatedValue::String(value) => !value.is_empty(),
        EvaluatedValue::Object(_) | EvaluatedValue::Array(_) => true,
        EvaluatedValue::Undefined => false,
        EvaluatedValue::PathModule
        | EvaluatedValue::FsModule
        | EvaluatedValue::Json
        | EvaluatedValue::Process => true,
    }
}

fn json_to_evaluated(value: serde_json::Value) -> Option<EvaluatedValue> {
    // `serde_json` parses without a recursion limit, so a 256 KB file of
    // nested arrays is possible. Grow the stack before re-descending.
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || match value {
        serde_json::Value::Bool(value) => Some(EvaluatedValue::Bool(value)),
        serde_json::Value::String(value) => Some(EvaluatedValue::String(value)),
        serde_json::Value::Array(values) => Some(EvaluatedValue::Array(
            values.into_iter().filter_map(json_to_evaluated).collect(),
        )),
        serde_json::Value::Object(values) => Some(EvaluatedValue::Object(
            values
                .into_iter()
                .filter_map(|(key, value)| Some((key, json_to_evaluated(value)?)))
                .collect(),
        )),
        serde_json::Value::Null => Some(EvaluatedValue::Undefined),
        serde_json::Value::Number(_) => None,
    })
}

fn normalize_joined_path(method: &str, parts: Vec<String>) -> String {
    let mut path = PathBuf::new();

    for part in parts {
        let part_path = Path::new(&part);
        if method == "resolve" && part_path.is_absolute() {
            path = PathBuf::from(part_path);
            continue;
        }
        path.push(part_path);
    }

    normalize_path(path).to_string_lossy().to_string()
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }

    normalized
}

/// Thin wrapper around `gkg_utils::fs::contained_canonical_path` so the
/// evaluator's existing call sites read unchanged. All security-relevant
/// path-containment logic lives in `crates/utils/src/fs.rs` as a SSOT.
fn canonical_repo_existing_path(root_dir: &Path, path: &Path) -> Option<PathBuf> {
    gkg_utils::fs::contained_canonical_path(root_dir, path)
}

pub(super) fn contained_repo_path(
    root_dir: &Path,
    config_dir: &Path,
    path: &str,
) -> Option<PathBuf> {
    let candidate = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        config_dir.join(path)
    };
    canonical_repo_existing_path(root_dir, &normalize_path(candidate))
}

fn contained_repo_existing_path(root_dir: &Path, config_dir: &Path, path: &str) -> Option<PathBuf> {
    contained_repo_path(root_dir, config_dir, path).filter(|candidate| candidate.exists())
}
