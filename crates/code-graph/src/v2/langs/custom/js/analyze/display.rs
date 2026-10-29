use oxc::ast::AstKind;
use oxc::ast::ast::{
    ArrowFunctionExpression, Class, Expression, FormalParameters, Function, MethodDefinition,
    MethodDefinitionKind, MethodDefinitionType, PropertyDefinition, PropertyKey, TSAccessibility,
    TSTypeAnnotation, VariableDeclarator,
};
use oxc::semantic::AstNodes;
use oxc::span::{GetSpan, Span};

use super::super::types::JsDisplay;
use crate::v2::types::{DefKind, Modifier, Modifiers, Visibility, text};

const LEADING_KEYWORDS: &[&str] = &["export", "default", "declare"];

fn slice(source: &str, start: u32, end: u32) -> &str {
    source.get(start as usize..end as usize).unwrap_or("")
}

fn signature(source: &str, start: u32, body_start: Option<u32>, end: u32) -> Option<String> {
    let text = slice(source, start, body_start.unwrap_or(end));
    let text = text.trim_end();
    text::signature(text.strip_suffix("=>").unwrap_or(text))
}

fn parameters(source: &str, params: &FormalParameters<'_>) -> Vec<String> {
    let mut items: Vec<&str> = params
        .items
        .iter()
        .map(|p| slice(source, p.span.start, p.span.end))
        .collect();
    if let Some(rest) = &params.rest {
        items.push(slice(source, rest.span.start, rest.span.end));
    }
    text::parameters(items)
}

fn annotation(source: &str, ann: Option<&TSTypeAnnotation<'_>>, cap: usize) -> Option<String> {
    let ann = ann?;
    let span = ann.type_annotation.span();
    text::type_text(slice(source, span.start, span.end), cap)
}

fn accessibility(acc: Option<TSAccessibility>) -> Option<Visibility> {
    acc.map(|a| match a {
        TSAccessibility::Private => Visibility::Private,
        TSAccessibility::Protected => Visibility::Protected,
        TSAccessibility::Public => Visibility::Public,
    })
}

fn member_visibility(acc: Option<TSAccessibility>, key: &PropertyKey<'_>) -> Visibility {
    if matches!(key, PropertyKey::PrivateIdentifier(_)) {
        return Visibility::Private;
    }
    accessibility(acc).unwrap_or(Visibility::Public)
}

pub(super) fn doc_before(source: &str, start: u32) -> Option<String> {
    let mut head = source.get(..start as usize)?.trim_end();
    loop {
        let trimmed = LEADING_KEYWORDS.iter().find_map(|k| {
            head.strip_suffix(k)
                .filter(|rest| rest.ends_with(char::is_whitespace) || rest.is_empty())
        });
        match trimmed {
            Some(rest) => head = rest.trim_end(),
            None => break,
        }
    }
    let body = head.strip_suffix("*/")?;
    let open = body.rfind("/*")?;
    let comment = body[open..].strip_prefix("/**")?;
    text::doc_paragraph(text::strip_comment_markers(comment, &["*"]))
}

fn function_modifiers(is_async: bool, generator: bool) -> Modifiers {
    let mut modifiers = Modifiers::empty();
    if is_async {
        modifiers.insert(Modifier::Async);
    }
    if generator {
        modifiers.insert(Modifier::Generator);
    }
    modifiers
}

pub(super) fn function(
    source: &str,
    f: &Function<'_>,
    start: u32,
    visibility: Option<Visibility>,
) -> JsDisplay {
    JsDisplay {
        signature: signature(
            source,
            start,
            f.body.as_ref().map(|b| b.span.start),
            f.span.end,
        ),
        parameters: parameters(source, &f.params),
        return_type: annotation(source, f.return_type.as_deref(), text::RETURN_TYPE_CAP),
        doc_comment: doc_before(source, start),
        modifiers: function_modifiers(f.r#async, f.generator),
        visibility,
        ..JsDisplay::default()
    }
}

fn arrow(
    source: &str,
    a: &ArrowFunctionExpression<'_>,
    start: u32,
    visibility: Option<Visibility>,
) -> JsDisplay {
    JsDisplay {
        signature: signature(source, start, Some(a.body.span().start), a.span.end),
        parameters: parameters(source, &a.params),
        return_type: annotation(source, a.return_type.as_deref(), text::RETURN_TYPE_CAP),
        doc_comment: doc_before(source, start),
        modifiers: function_modifiers(a.r#async, false),
        visibility,
        language_kind: Some("ArrowFunction"),
        ..JsDisplay::default()
    }
}

pub(super) fn class(source: &str, c: &Class<'_>, visibility: Option<Visibility>) -> JsDisplay {
    let mut modifiers = Modifiers::empty();
    if c.r#abstract {
        modifiers.insert(Modifier::Abstract);
    }
    JsDisplay {
        signature: signature(
            source,
            c.decorators.last().map_or(c.span.start, |d| d.span.end),
            Some(c.body.span.start),
            c.span.end,
        ),
        doc_comment: doc_before(source, c.span.start),
        modifiers,
        visibility,
        ..JsDisplay::default()
    }
}

pub(super) fn method(source: &str, m: &MethodDefinition<'_>) -> JsDisplay {
    let mut modifiers = function_modifiers(m.value.r#async, m.value.generator);
    if m.r#static {
        modifiers.insert(Modifier::Static);
    }
    if m.r#override {
        modifiers.insert(Modifier::Override);
    }
    if m.r#type == MethodDefinitionType::TSAbstractMethodDefinition {
        modifiers.insert(Modifier::Abstract);
    }
    JsDisplay {
        signature: signature(
            source,
            m.decorators.last().map_or(m.span.start, |d| d.span.end),
            m.value.body.as_ref().map(|b| b.span.start),
            m.span.end,
        ),
        parameters: parameters(source, &m.value.params),
        return_type: annotation(
            source,
            m.value.return_type.as_deref(),
            text::RETURN_TYPE_CAP,
        ),
        doc_comment: doc_before(source, m.span.start),
        modifiers,
        visibility: Some(member_visibility(m.accessibility, &m.key)),
        language_kind: match m.kind {
            MethodDefinitionKind::Get => Some("Getter"),
            MethodDefinitionKind::Set => Some("Setter"),
            _ => None,
        },
        kind: matches!(
            m.kind,
            MethodDefinitionKind::Get | MethodDefinitionKind::Set
        )
        .then_some(DefKind::Field),
        ..JsDisplay::default()
    }
}

pub(super) fn property(source: &str, p: &PropertyDefinition<'_>) -> JsDisplay {
    let mut modifiers = Modifiers::empty();
    if p.r#static {
        modifiers.insert(Modifier::Static);
    }
    if p.readonly {
        modifiers.insert(Modifier::Readonly);
    }
    if p.r#override {
        modifiers.insert(Modifier::Override);
    }
    JsDisplay {
        signature: signature(
            source,
            p.decorators.last().map_or(p.span.start, |d| d.span.end),
            p.value.as_ref().map(|v| v.span().start),
            p.span.end,
        ),
        declared_type: annotation(
            source,
            p.type_annotation.as_deref(),
            text::DECLARED_TYPE_CAP,
        ),
        doc_comment: doc_before(source, p.span.start),
        modifiers,
        visibility: Some(member_visibility(p.accessibility, &p.key)),
        ..JsDisplay::default()
    }
}

pub(super) fn parameter_property(
    source: &str,
    span: Span,
    acc: Option<TSAccessibility>,
    readonly: bool,
    ann: Option<&TSTypeAnnotation<'_>>,
) -> JsDisplay {
    let mut modifiers = Modifiers::empty();
    if readonly {
        modifiers.insert(Modifier::Readonly);
    }
    JsDisplay {
        signature: signature(source, span.start, None, span.end),
        declared_type: annotation(source, ann, text::DECLARED_TYPE_CAP),
        modifiers,
        visibility: Some(accessibility(acc).unwrap_or(Visibility::Public)),
        ..JsDisplay::default()
    }
}

fn declarator(
    source: &str,
    nodes: &AstNodes<'_>,
    decl_node_id: oxc::semantic::NodeId,
    d: &VariableDeclarator<'_>,
    visibility: Option<Visibility>,
) -> JsDisplay {
    let start = match nodes.parent_kind(decl_node_id) {
        AstKind::VariableDeclaration(v) => v.span.start,
        _ => d.span.start,
    };
    let keyword = d.kind.as_str();
    match &d.init {
        Some(Expression::ArrowFunctionExpression(a)) => arrow(source, a, start, visibility),
        Some(Expression::FunctionExpression(f)) => JsDisplay {
            language_kind: Some("FunctionExpression"),
            ..function(source, f, start, visibility)
        },
        init => {
            let mut display = JsDisplay {
                signature: signature(
                    source,
                    start,
                    init.as_ref().map(|i| i.span().start),
                    d.span.end,
                ),
                declared_type: annotation(
                    source,
                    d.type_annotation.as_deref(),
                    text::DECLARED_TYPE_CAP,
                ),
                doc_comment: doc_before(source, start),
                visibility,
                ..JsDisplay::default()
            };
            if keyword == "const" {
                display.language_kind = Some("Constant");
                display.kind = Some(DefKind::Constant);
            }
            display
        }
    }
}

pub(super) fn for_declaration(
    source: &str,
    nodes: &AstNodes<'_>,
    decl_node_id: oxc::semantic::NodeId,
    visibility: Option<Visibility>,
) -> JsDisplay {
    match nodes.kind(decl_node_id) {
        AstKind::Function(f) => function(source, f, f.span.start, visibility),
        AstKind::Class(c) => class(source, c, visibility),
        AstKind::VariableDeclarator(d) => declarator(source, nodes, decl_node_id, d, visibility),
        AstKind::TSInterfaceDeclaration(i) => JsDisplay {
            signature: signature(source, i.span.start, Some(i.body.span.start), i.span.end),
            doc_comment: doc_before(source, i.span.start),
            visibility,
            ..JsDisplay::default()
        },
        AstKind::TSEnumDeclaration(e) => JsDisplay {
            signature: signature(source, e.span.start, Some(e.body.span.start), e.span.end),
            doc_comment: doc_before(source, e.span.start),
            visibility,
            ..JsDisplay::default()
        },
        AstKind::TSModuleDeclaration(m) => JsDisplay {
            signature: signature(
                source,
                m.span.start,
                m.body.as_ref().map(|b| b.span().start),
                m.span.end,
            ),
            doc_comment: doc_before(source, m.span.start),
            visibility,
            ..JsDisplay::default()
        },
        kind => {
            let span = kind.span();
            JsDisplay {
                signature: signature(source, span.start, None, span.end),
                doc_comment: doc_before(source, span.start),
                visibility,
                ..JsDisplay::default()
            }
        }
    }
}
