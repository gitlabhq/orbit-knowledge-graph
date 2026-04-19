use oxc::ast::AstKind;
use oxc::ast::ast::Expression;
use oxc::semantic::{AstNodes, NodeId};
use oxc::syntax::symbol::SymbolFlags;

use crate::v2::types::DefKind;

use super::super::types::{JsDefKind, JsInvocationSupport};

pub(in crate::v2::langs::custom::js) fn invocation_support_for_expression(
    expression: &Expression<'_>,
) -> Option<JsInvocationSupport> {
    match expression.get_inner_expression() {
        Expression::ArrowFunctionExpression(_) => Some(JsInvocationSupport::arrow_function()),
        Expression::FunctionExpression(_) => Some(JsInvocationSupport::function()),
        Expression::ClassExpression(_) => Some(JsInvocationSupport::class()),
        _ => None,
    }
}

pub(in crate::v2::langs::custom::js) fn invocation_support_for_symbol(
    flags: SymbolFlags,
    nodes: &AstNodes,
    decl_node_id: NodeId,
) -> Option<JsInvocationSupport> {
    if flags.is_class() {
        return Some(JsInvocationSupport::class());
    }
    if flags.is_function() {
        if matches!(
            nodes.parent_kind(decl_node_id),
            AstKind::MethodDefinition(_)
        ) {
            return None;
        }
        return Some(JsInvocationSupport::function());
    }
    if !flags.is_variable()
        || matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_))
    {
        return None;
    }

    match nodes.kind(decl_node_id) {
        AstKind::VariableDeclarator(decl) => decl
            .init
            .as_ref()
            .and_then(invocation_support_for_expression),
        _ => None,
    }
}

pub(in crate::v2::langs::custom::js) fn invocation_support_for_js_def_kind(
    kind: &JsDefKind,
) -> Option<JsInvocationSupport> {
    match kind {
        JsDefKind::Class => Some(JsInvocationSupport::class()),
        JsDefKind::Function
        | JsDefKind::Method { .. }
        | JsDefKind::LifecycleHook { .. }
        | JsDefKind::Watcher { .. }
        | JsDefKind::Getter { .. }
        | JsDefKind::Setter { .. } => Some(JsInvocationSupport::function()),
        _ => None,
    }
}

pub(in crate::v2::langs::custom::js) fn invocation_support_for_graph_def_kind(
    kind: DefKind,
) -> Option<JsInvocationSupport> {
    match kind {
        DefKind::Class => Some(JsInvocationSupport::class()),
        DefKind::Function | DefKind::Method | DefKind::Constructor => {
            Some(JsInvocationSupport::function())
        }
        _ => None,
    }
}
