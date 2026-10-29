use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

pub use treesitter_visit::extract::{
    Extract, child_of_kind, default_name, descendant, field, field_chain, name_or_ident,
    no_extract, text,
};

/// Single-value fields use `Extract`. Multi-value fields use `fn(&Node) -> Vec<String>`.
pub struct MetadataRule {
    pub super_types: Option<fn(&N<'_>) -> Vec<String>>,
    pub return_type: Option<Extract>,
    pub type_annotation: Option<Extract>,
    pub receiver_type: Option<Extract>,
    pub decorators: Option<fn(&N<'_>) -> Vec<String>>,
    pub companion_of: Option<Extract>,
    pub signature_until: Option<&'static [&'static str]>,
    pub parameters_with: Option<ListFn>,
    pub declared_type: Option<Extract>,
    pub written_return_type: Option<TextFn>,
    pub declared_type_with: Option<TextFn>,
    pub doc_with: Option<TextFn>,
    pub modifiers_with: Option<ModifiersFn>,
}

impl Default for MetadataRule {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataRule {
    pub fn new() -> Self {
        Self {
            super_types: None,
            return_type: None,
            type_annotation: None,
            receiver_type: None,
            decorators: None,
            companion_of: None,
            signature_until: None,
            parameters_with: None,
            declared_type: None,
            written_return_type: None,
            declared_type_with: None,
            doc_with: None,
            modifiers_with: None,
        }
    }

    pub fn signature_until(mut self, body: &'static [&'static str]) -> Self {
        self.signature_until = Some(body);
        self
    }
    pub fn parameters_with(mut self, f: ListFn) -> Self {
        self.parameters_with = Some(f);
        self
    }
    pub fn declared_type(mut self, extract: Extract) -> Self {
        self.declared_type = Some(extract);
        self
    }
    pub fn declared_type_with(mut self, f: TextFn) -> Self {
        self.declared_type_with = Some(f);
        self
    }
    pub fn written_return_type(mut self, f: TextFn) -> Self {
        self.written_return_type = Some(f);
        self
    }
    pub fn doc_with(mut self, f: TextFn) -> Self {
        self.doc_with = Some(f);
        self
    }
    pub fn modifiers_with(mut self, f: ModifiersFn) -> Self {
        self.modifiers_with = Some(f);
        self
    }

    pub fn display(
        &self,
        rules: &DisplayRules,
        node: &N<'_>,
        name: &str,
        kind: crate::v2::types::DefKind,
        meta: &mut crate::v2::types::DefinitionMetadata,
    ) {
        use crate::v2::types::text;
        meta.signature = match rules.signature_with {
            Some(f) => f(node),
            None => rules.signature(node, self.signature_until),
        };
        meta.parameters = match self.parameters_with {
            Some(f) => text::parameters(f(node).iter().map(String::as_str)),
            None => rules.parameters(node),
        };
        let category = kind.category();
        meta.declared_return_type = match category {
            crate::v2::types::Category::Callable => {
                match self.written_return_type.or(rules.return_type_with) {
                    Some(f) => f(node),
                    None => rules.written(node, None, rules.return_type),
                }
                .and_then(|t| text::type_text(&t, text::RETURN_TYPE_CAP))
            }
            _ => None,
        };
        meta.declared_type = match category {
            crate::v2::types::Category::Value => match self.declared_type_with {
                Some(f) => f(node),
                None => rules.written(node, self.declared_type.as_ref(), rules.declared_type),
            }
            .and_then(|t| text::type_text(&t, text::DECLARED_TYPE_CAP)),
            _ => None,
        };
        meta.doc_comment = match self.doc_with {
            Some(f) => f(node),
            None => rules.doc_comment(node),
        };
        let (mut modifiers, visibility) = rules.modifiers(node);
        if let Some(f) = self.modifiers_with {
            modifiers.extend(f(node));
        }
        meta.modifiers = modifiers;
        meta.visibility = rules
            .visibility
            .map_or(visibility, |f| visibility.or(f(node, name)));
    }

    pub fn super_types(mut self, f: fn(&N<'_>) -> Vec<String>) -> Self {
        self.super_types = Some(f);
        self
    }
    pub fn return_type(mut self, extract: Extract) -> Self {
        self.return_type = Some(extract);
        self
    }
    pub fn type_annotation(mut self, extract: Extract) -> Self {
        self.type_annotation = Some(extract);
        self
    }
    pub fn receiver_type(mut self, extract: Extract) -> Self {
        self.receiver_type = Some(extract);
        self
    }
    pub fn decorators(mut self, f: fn(&N<'_>) -> Vec<String>) -> Self {
        self.decorators = Some(f);
        self
    }
    pub fn companion_of(mut self, extract: Extract) -> Self {
        self.companion_of = Some(extract);
        self
    }

    /// The `resolve` closure transforms bare type names into FQNs using tree context + import map.
    pub fn extract_metadata(
        &self,
        node: &N<'_>,
        resolve: impl Fn(String, &N<'_>) -> String,
    ) -> Option<Box<crate::v2::types::DefinitionMetadata>> {
        let super_types: Vec<String> = self
            .super_types
            .map(|f| f(node).into_iter().map(|s| resolve(s, node)).collect())
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve));
        // receiver_type uses bare apply (no import resolution) so it
        // stores the source-level type name. The lookup_by_receiver_type
        // matches against bare_type extracted from the chain's FQN.
        let receiver_type = self.receiver_type.as_ref().and_then(|e| e.apply(node));
        let decorators = self.decorators.map(|f| f(node)).unwrap_or_default();
        let companion_of = self.companion_of.as_ref().and_then(|e| e.apply(node));

        let has_data = !super_types.is_empty()
            || return_type.is_some()
            || type_annotation.is_some()
            || receiver_type.is_some()
            || !decorators.is_empty()
            || companion_of.is_some();

        if !has_data {
            return None;
        }

        Some(Box::new(crate::v2::types::DefinitionMetadata {
            super_types,
            return_type,
            type_annotation,
            receiver_type,
            decorators,
            companion_of,
            ..Default::default()
        }))
    }
}

pub fn metadata() -> MetadataRule {
    MetadataRule::new()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    Modifier(crate::v2::types::Modifier),
    Visibility(crate::v2::types::Visibility),
}

pub type VisibilityFn = fn(&N<'_>, &str) -> Option<crate::v2::types::Visibility>;
pub type TextFn = fn(&N<'_>) -> Option<String>;
pub type ListFn = fn(&N<'_>) -> Vec<String>;
pub type ModifiersFn = fn(&N<'_>) -> crate::v2::types::Modifiers;
pub type TestFn = fn(&N<'_>, &str) -> bool;

#[derive(Default)]
pub struct DisplayRules {
    pub body: &'static [&'static str],
    pub parameters: &'static [&'static str],
    pub return_type: &'static [&'static str],
    pub declared_type: &'static [&'static str],
    pub parameter_items: &'static [&'static str],
    pub doc_comments: &'static [&'static str],
    pub doc_prefixes: &'static [&'static str],
    pub doc_markers: &'static [&'static str],
    pub doc_hosts: &'static [&'static str],
    pub modifier_hosts: &'static [&'static str],
    pub tokens: &'static [(&'static str, Token)],
    pub visibility: Option<VisibilityFn>,
    pub doc_with: Option<TextFn>,
    pub return_type_with: Option<TextFn>,
    pub signature_with: Option<TextFn>,
}

const MAX_TOKEN_LEN: usize = 24;

impl DisplayRules {
    fn written(&self, node: &N<'_>, extract: Option<&Extract>, fields: &[&str]) -> Option<String> {
        match extract {
            Some(e) => e.apply(node),
            None => fields
                .iter()
                .find_map(|f| {
                    node.field(f)
                        .or_else(|| node.children().find(|c| c.kind().as_ref() == *f))
                })
                .map(|n| n.text().to_string()),
        }
    }

    pub fn signature(&self, node: &N<'_>, body: Option<&[&str]>) -> Option<String> {
        let body = body.unwrap_or(self.body);
        let start = node.range().start;
        let end = node
            .children()
            .flat_map(|c| {
                let grandchildren: Vec<N<'_>> = c.children().collect();
                std::iter::once(c).chain(grandchildren)
            })
            .filter(|c| body.contains(&c.kind().as_ref()))
            .map(|c| c.range().start)
            .chain(
                body.iter()
                    .filter_map(|f| node.field(f))
                    .map(|c| c.range().start),
            )
            .chain(
                node.children()
                    .filter_map(|c| body.iter().find_map(|f| c.field(f)))
                    .map(|c| c.range().start),
            )
            .min()
            .unwrap_or(node.range().end);
        let text = node.text();
        let slice = text.get(..end.saturating_sub(start).min(text.len()))?;
        crate::v2::types::text::signature(slice)
    }

    pub fn parameters(&self, node: &N<'_>) -> Vec<String> {
        let list = self.parameters.iter().find_map(|name| {
            node.field(name)
                .or_else(|| node.children().find(|c| c.kind().as_ref() == *name))
        });
        let items: Vec<String> = match list {
            Some(list) => list
                .children()
                .filter(|c| c.is_named() && !self.doc_comments.contains(&c.kind().as_ref()))
                .map(|c| c.text().to_string())
                .collect(),
            None => node
                .children()
                .filter(|c| self.parameter_items.contains(&c.kind().as_ref()))
                .map(|c| c.text().to_string())
                .collect(),
        };
        crate::v2::types::text::parameters(items.iter().map(String::as_str))
    }

    pub fn doc_comment(&self, node: &N<'_>) -> Option<String> {
        if let Some(f) = self.doc_with {
            return f(node);
        }
        if self.doc_comments.is_empty() {
            return None;
        }
        let mut host = node.clone();
        loop {
            if let Some(doc) = self.leading_comment(&host) {
                return Some(doc);
            }
            if host.prev_all().any(|p| p.is_named()) {
                return None;
            }
            match host.parent() {
                Some(parent) if self.doc_hosts.contains(&parent.kind().as_ref()) => host = parent,
                _ => return None,
            }
        }
    }

    fn leading_comment(&self, node: &N<'_>) -> Option<String> {
        let joined = leading_comments(node, self.doc_comments, self.doc_prefixes).join("\n");
        crate::v2::types::text::doc_paragraph(crate::v2::types::text::strip_comment_markers(
            &joined,
            self.doc_markers,
        ))
    }

    pub fn modifiers(
        &self,
        node: &N<'_>,
    ) -> (
        crate::v2::types::Modifiers,
        Option<crate::v2::types::Visibility>,
    ) {
        let mut modifiers = crate::v2::types::Modifiers::empty();
        let mut visibility = None;
        if self.tokens.is_empty() {
            return (modifiers, visibility);
        }
        let mut scan = |child: &N<'_>| {
            let range = child.range();
            if range.end - range.start > MAX_TOKEN_LEN {
                return;
            }
            let text = child.text();
            let text = text.trim();
            for (token, value) in self.tokens {
                if *token == text {
                    match value {
                        Token::Modifier(m) => modifiers.insert(*m),
                        Token::Visibility(v) => {
                            visibility.get_or_insert(*v);
                        }
                    }
                }
            }
        };
        for child in node.children() {
            if self.modifier_hosts.contains(&child.kind().as_ref()) {
                scan(&child);
                for inner in child.children() {
                    inner.children().for_each(|c| scan(&c));
                    scan(&inner);
                }
            } else if !child.is_named() {
                scan(&child);
            }
        }
        (modifiers, visibility)
    }
}

pub fn leading_comments(node: &N<'_>, kinds: &[&str], prefixes: &[&str]) -> Vec<String> {
    let mut texts: Vec<String> = Vec::new();
    let mut next_line = node.start_pos().line();
    let mut prev = node.prev().map(|p| trailing_comment_host(p, kinds));
    while let Some(sibling) = prev {
        if !kinds.contains(&sibling.kind().as_ref()) || sibling.end_pos().line() + 1 < next_line {
            break;
        }
        let text = sibling.text();
        if !prefixes.is_empty() && !prefixes.iter().any(|p| text.starts_with(p)) {
            break;
        }
        next_line = sibling.start_pos().line();
        texts.push(text.to_string());
        prev = sibling.prev();
    }
    texts.reverse();
    texts
}

fn trailing_comment_host<'r>(node: N<'r>, kinds: &[&str]) -> N<'r> {
    let mut current = node;
    while !kinds.contains(&current.kind().as_ref()) {
        match current.children().last() {
            Some(last) => current = last,
            None => break,
        }
    }
    current
}
