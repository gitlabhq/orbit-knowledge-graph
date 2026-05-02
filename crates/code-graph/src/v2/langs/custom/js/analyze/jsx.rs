use std::collections::HashMap;

use oxc::ast::ast::{JSXElementName, JSXMemberExpressionObject};
use oxc::syntax::symbol::SymbolId;

use crate::v2::types::ExpressionStep;

use super::super::types::{JsImportedBinding, JsImportedCall, JsInvocationKind};
use super::analyzer::Ctx;
use super::calls::{binding_from_identifier_reference, imported_call_from_jsx_member_expression};

pub(super) enum JsxInvocation {
    Imported(JsImportedCall),
    Local {
        name: String,
        chain: Option<Vec<ExpressionStep>>,
    },
}

pub(super) fn invocation_from_name<'a>(
    ctx: &Ctx,
    name: &JSXElementName<'a>,
    import_bindings: &HashMap<SymbolId, JsImportedBinding>,
) -> Option<JsxInvocation> {
    match name {
        JSXElementName::IdentifierReference(identifier) => {
            if is_intrinsic_name(identifier.name.as_str()) {
                return None;
            }

            if let Some(binding) =
                binding_from_identifier_reference(ctx, identifier, import_bindings)
            {
                return Some(JsxInvocation::Imported(JsImportedCall {
                    binding,
                    member_path: Vec::new(),
                    invocation_kind: JsInvocationKind::Jsx,
                }));
            }

            Some(JsxInvocation::Local {
                name: identifier.name.to_string(),
                chain: None,
            })
        }
        JSXElementName::MemberExpression(member) => {
            if let Some(imported_call) = imported_call_from_jsx_member_expression(
                ctx,
                member,
                import_bindings,
                JsInvocationKind::Jsx,
            ) {
                return Some(JsxInvocation::Imported(imported_call));
            }

            let (name, chain) = local_invocation_target(name)?;
            Some(JsxInvocation::Local { name, chain })
        }
        JSXElementName::ThisExpression(_) => None,
        _ => None,
    }
}

fn is_intrinsic_name(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_lowercase())
}

fn local_invocation_target(
    name: &JSXElementName<'_>,
) -> Option<(String, Option<Vec<ExpressionStep>>)> {
    match name {
        JSXElementName::IdentifierReference(identifier) => {
            Some((identifier.name.to_string(), None))
        }
        JSXElementName::ThisExpression(_) => None,
        JSXElementName::MemberExpression(member) => {
            let mut chain = member_object_steps(&member.object)?;
            chain.push(ExpressionStep::Call(
                member.property.name.to_string().into(),
            ));
            Some((member.property.name.to_string(), Some(chain)))
        }
        _ => None,
    }
}

fn member_object_steps(object: &JSXMemberExpressionObject<'_>) -> Option<Vec<ExpressionStep>> {
    stacker::maybe_grow(32 * 1024, 1024 * 1024, || match object {
        JSXMemberExpressionObject::IdentifierReference(identifier) => {
            Some(vec![ExpressionStep::Ident(identifier.name.as_str().into())])
        }
        JSXMemberExpressionObject::MemberExpression(member) => {
            let mut chain = member_object_steps(&member.object)?;
            chain.push(ExpressionStep::Field(
                member.property.name.to_string().into(),
            ));
            Some(chain)
        }
        JSXMemberExpressionObject::ThisExpression(_) => Some(vec![ExpressionStep::This]),
    })
}
