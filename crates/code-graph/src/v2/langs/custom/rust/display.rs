use ra_ap_syntax::{AstNode, NodeOrToken, SyntaxKind, SyntaxNode, ast};

use crate::v2::types::{DefinitionMetadata, Modifier, Modifiers, Visibility, text};

const BODY_KINDS: &[SyntaxKind] = &[
    SyntaxKind::BLOCK_EXPR,
    SyntaxKind::ASSOC_ITEM_LIST,
    SyntaxKind::RECORD_FIELD_LIST,
    SyntaxKind::VARIANT_LIST,
    SyntaxKind::ITEM_LIST,
];

const LEADING_KINDS: &[SyntaxKind] = &[
    SyntaxKind::ATTR,
    SyntaxKind::COMMENT,
    SyntaxKind::DOC_COMMENT,
    SyntaxKind::WHITESPACE,
];

fn signature(node: &SyntaxNode) -> Option<String> {
    let start = node
        .children_with_tokens()
        .find(|c| !LEADING_KINDS.contains(&c.kind()))?
        .text_range()
        .start();
    let stops_at_value = matches!(node.kind(), SyntaxKind::CONST | SyntaxKind::STATIC);
    let end = node
        .children_with_tokens()
        .find(|c| BODY_KINDS.contains(&c.kind()) || (stops_at_value && c.kind() == SyntaxKind::EQ))
        .map_or(node.text_range().end(), |c| c.text_range().start());
    let base = node.text_range().start();
    let text = node.text().to_string();
    let slice = text.get(usize::from(start - base)..usize::from(end - base))?;
    text::signature(slice)
}

fn doc_comment(node: &SyntaxNode) -> Option<String> {
    let mut lines = String::new();
    for child in node.children_with_tokens() {
        match child {
            NodeOrToken::Node(n) if n.kind() == SyntaxKind::DOC_COMMENT => {
                let text = n.text().to_string();
                if text.starts_with("///") || text.starts_with("/**") {
                    lines.push_str(&text);
                    lines.push('\n');
                }
            }
            NodeOrToken::Token(token)
                if matches!(token.kind(), SyntaxKind::WHITESPACE | SyntaxKind::COMMENT) => {}
            NodeOrToken::Node(n) if n.kind() == SyntaxKind::ATTR => {}
            _ => break,
        }
    }
    text::doc_paragraph(text::strip_comment_markers(
        &lines,
        &["///", "/**", "*/", "*"],
    ))
}

fn visibility(node: &SyntaxNode) -> Option<Visibility> {
    if node.kind() == SyntaxKind::VARIANT {
        return Some(Visibility::Public);
    }
    let in_trait_scope = node
        .parent()
        .filter(|p| p.kind() == SyntaxKind::ASSOC_ITEM_LIST)
        .and_then(|p| p.parent())
        .is_some_and(|owner| {
            owner.kind() == SyntaxKind::TRAIT
                || ast::Impl::cast(owner).is_some_and(|i| i.trait_().is_some())
        });
    if in_trait_scope {
        return Some(Visibility::Public);
    }
    let vis = node.children().find(|c| c.kind() == SyntaxKind::VISIBILITY);
    Some(match vis.map(|v| v.text().to_string()) {
        None => Visibility::Private,
        Some(text) => match text.replace(char::is_whitespace, "").as_str() {
            "pub" => Visibility::Public,
            "pub(crate)" => Visibility::Internal,
            "pub(self)" => Visibility::Private,
            _ => Visibility::Package,
        },
    })
}

pub(super) fn is_test(node: &SyntaxNode) -> bool {
    node.kind() == SyntaxKind::FN
        && node.children().filter_map(ast::Attr::cast).any(|attr| {
            attr.path()
                .and_then(|p| p.segment())
                .and_then(|s| s.name_ref())
                .is_some_and(|n| n.text() == "test")
        })
}

pub(super) fn metadata(node: &SyntaxNode) -> DefinitionMetadata {
    let mut meta = DefinitionMetadata {
        signature: signature(node),
        doc_comment: doc_comment(node),
        visibility: visibility(node),
        ..DefinitionMetadata::default()
    };
    if let Some(function) = ast::Fn::cast(node.clone()) {
        if let Some(params) = function.param_list() {
            let items: Vec<String> = params
                .self_param()
                .map(|p| p.syntax().text().to_string())
                .into_iter()
                .chain(params.params().map(|p| p.syntax().text().to_string()))
                .collect();
            meta.parameters = text::parameters(items.iter().map(String::as_str));
        }
        meta.declared_return_type = function
            .ret_type()
            .and_then(|r| r.ty())
            .and_then(|t| text::type_text(&t.syntax().text().to_string(), text::RETURN_TYPE_CAP));
        let mut modifiers = Modifiers::empty();
        if function.async_token().is_some() {
            modifiers.insert(Modifier::Async);
        }
        if function.unsafe_token().is_some() {
            modifiers.insert(Modifier::Unsafe);
        }
        if function.abi().is_some() {
            modifiers.insert(Modifier::Extern);
        }
        meta.modifiers = modifiers;
    }
    let declared = ast::Const::cast(node.clone())
        .and_then(|c| c.ty())
        .or_else(|| ast::Static::cast(node.clone()).and_then(|s| s.ty()))
        .or_else(|| ast::RecordField::cast(node.clone()).and_then(|f| f.ty()))
        .or_else(|| ast::TupleField::cast(node.clone()).and_then(|f| f.ty()));
    meta.declared_type = declared
        .and_then(|t| text::type_text(&t.syntax().text().to_string(), text::DECLARED_TYPE_CAP));
    if let Some(tr) = ast::Trait::cast(node.clone())
        && tr.unsafe_token().is_some()
    {
        meta.modifiers.insert(Modifier::Unsafe);
    }
    meta
}
