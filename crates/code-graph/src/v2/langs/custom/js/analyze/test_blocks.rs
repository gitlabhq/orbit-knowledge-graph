use orbit_utils::strings::truncate_bytes;
use oxc::ast::AstKind;
use oxc::ast::ast::{Argument, CallExpression, Expression};
use oxc::span::GetSpan;

use super::super::types::{JsDef, JsDefKind, JsDisplay};
use super::analyzer::Ctx;
use crate::v2::types::text;

const GROUPS: &[&str] = &["describe", "context", "suite"];
const CASES: &[&str] = &["it", "test", "specify"];
const VARIANTS: &[&str] = &["only", "skip", "concurrent", "todo", "failing"];

fn callee_word<'a>(call: &'a CallExpression<'a>) -> Option<&'a str> {
    match &call.callee {
        Expression::Identifier(id) => Some(id.name.as_str()),
        Expression::StaticMemberExpression(member)
            if VARIANTS.contains(&member.property.name.as_str()) =>
        {
            match &member.object {
                Expression::Identifier(id) => Some(id.name.as_str()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn description(call: &CallExpression<'_>) -> Option<String> {
    let name = match call.arguments.first()? {
        Argument::StringLiteral(s) => s.value.to_string(),
        Argument::TemplateLiteral(t) if t.expressions.is_empty() => {
            t.quasis.iter().map(|q| q.value.raw.as_str()).collect()
        }
        _ => return None,
    };
    let has_callback = call.arguments.iter().skip(1).any(|a| {
        matches!(
            a,
            Argument::ArrowFunctionExpression(_) | Argument::FunctionExpression(_)
        )
    });
    (has_callback && !name.is_empty())
        .then(|| truncate_bytes(&name, text::NAME_CAP, text::CUT, false).into_owned())
}

fn test_call<'a>(kind: AstKind<'a>) -> Option<(&'a CallExpression<'a>, bool, String)> {
    let AstKind::CallExpression(call) = kind else {
        return None;
    };
    let word = callee_word(call)?;
    let group = if GROUPS.contains(&word) {
        true
    } else if CASES.contains(&word) {
        false
    } else {
        return None;
    };
    Some((call, group, description(call)?))
}

pub(super) fn collect_test_defs(ctx: &Ctx<'_>) -> Vec<JsDef> {
    let mut defs = Vec::new();
    for node in ctx.nodes.iter() {
        let Some((call, group, name)) = test_call(node.kind()) else {
            continue;
        };
        let mut parts: Vec<String> = ctx
            .nodes
            .ancestor_kinds(node.id())
            .skip(1)
            .filter_map(|k| test_call(k).map(|(_, _, n)| n))
            .collect();
        parts.reverse();
        parts.push(name.clone());
        let span = call.span();
        let source = ctx.source;
        let signature = call
            .arguments
            .get(1)
            .map(|a| a.span().start)
            .and_then(|end| source.get(span.start as usize..end as usize))
            .map(|t| t.trim_end().trim_end_matches(',').to_string())
            .and_then(|t| text::signature(&t));
        defs.push(JsDef {
            fqn: parts.join("::"),
            name,
            kind: JsDefKind::Test { group },
            range: ctx.lt.span_to_range(span),
            is_exported: false,
            type_annotation: None,
            invocation_support: None,
            display: Box::new(JsDisplay {
                signature,
                ..JsDisplay::default()
            }),
        });
    }
    defs
}
