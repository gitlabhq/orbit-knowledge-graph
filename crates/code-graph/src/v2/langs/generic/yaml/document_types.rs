//! Each YAML config in `document_types/` declares how a named
//! document type matches files, which shapes become definitions, and
//! which keys become imports.

use super::config::{DOCUMENT_TYPES, DefinitionRule, DocumentType, KeyRule, Shape};
use super::{
    N, child_mapping, child_sequence, find_pair, is_pair, item_scalar, pair_key, pairs, scalar_text,
};
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, Fqn, ImportBindingKind, ImportMode, Range,
};
use std::collections::HashSet;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;

pub(super) const MAX_KEY_DEPTH: usize = 64;

fn node_range(node: &N<'_>) -> Range {
    crate::v2::dsl::utils::canonical_range(&crate::utils::node_to_range(node))
}

fn enclosing_pair<'a>(node: &N<'a>) -> Option<N<'a>> {
    let mut current = node.parent();
    while let Some(parent) = current {
        if is_pair(&parent) {
            return Some(parent);
        }
        current = parent.parent();
    }
    None
}

fn push_import(
    imports: &mut Vec<CanonicalImport>,
    import_type: &'static str,
    path: String,
    name: Option<String>,
    alias: Option<String>,
    range: Range,
) {
    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::SideEffect,
        mode: ImportMode::Declarative,
        path,
        name,
        alias,
        scope_fqn: None,
        range,
        is_type_only: false,
        wildcard: false,
    });
}

fn value_list(value: Option<&N<'_>>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    if let Some(single) = scalar_text(value) {
        return vec![single];
    }
    child_sequence(value)
        .map(|sequence| {
            sequence
                .children()
                .filter_map(|item| item_scalar(&item))
                .collect()
        })
        .unwrap_or_default()
}

fn emit_mapping(rule: &'static KeyRule, mapping: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    let range = node_range(mapping);
    for form in &rule.mapping_forms {
        let mut path: Option<String> = None;
        let mut names: Vec<String> = Vec::new();
        let mut alias: Option<String> = None;

        for pair in pairs(mapping) {
            let Some(key) = pair_key(&pair) else { continue };
            let value = pair.field("value");
            if key == form.path_key {
                path = value.as_ref().and_then(scalar_text);
            } else if form.name_keys.contains(&key) {
                names.extend(value_list(value.as_ref()));
            } else if form.alias_key.as_deref() == Some(key.as_str()) {
                alias = value.as_ref().and_then(scalar_text);
            }
        }

        let Some(mut path) = path else { continue };
        if let Some(sep) = form.version_split
            && let Some((base, version)) = path.rsplit_once(sep)
        {
            alias = Some(version.to_string());
            path = base.to_string();
        }
        if names.is_empty() {
            push_import(imports, &form.import_type, path, None, alias, range);
        } else {
            for name in names {
                push_import(
                    imports,
                    &form.import_type,
                    path.clone(),
                    Some(name),
                    alias.clone(),
                    range,
                );
            }
        }
    }
}

fn push_definition(
    defs: &mut Vec<CanonicalDefinition>,
    rule: &'static DefinitionRule,
    parts: &[&str],
    node: &N<'_>,
    sep: &'static str,
) {
    defs.push(CanonicalDefinition {
        definition_type: &rule.definition_type,
        kind: DefKind::Other,
        name: parts[parts.len() - 1].to_string(),
        fqn: Fqn::from_parts(parts, sep),
        range: node_range(node),
        is_top_level: parts.len() == 1,
        metadata: None,
    });
}

fn emit_shape(
    rule: &'static DefinitionRule,
    key: &str,
    pair: &N<'_>,
    defs: &mut Vec<CanonicalDefinition>,
    sep: &'static str,
) {
    let value = pair.field("value");
    match &rule.shape {
        Shape::RootKeys(_) => push_definition(defs, rule, &[key], pair, sep),
        Shape::ChildrenOf(_) => {
            for child in value
                .as_ref()
                .and_then(child_mapping)
                .iter()
                .flat_map(pairs)
            {
                if let Some(name) = pair_key(&child) {
                    push_definition(defs, rule, &[key, &name], &child, sep);
                }
            }
        }
        Shape::ItemsOf(_) => {
            for item in value
                .as_ref()
                .and_then(child_sequence)
                .iter()
                .flat_map(N::children)
            {
                if let Some(name) = item_scalar(&item) {
                    push_definition(defs, rule, &[key, &name], &item, sep);
                }
            }
        }
        Shape::ValueOf(path) => {
            let mut current = Some(pair.clone());
            for segment in path.split('.').skip(1) {
                current = current
                    .and_then(|c| c.field("value"))
                    .as_ref()
                    .and_then(child_mapping)
                    .and_then(|mapping| find_pair(&mapping, segment));
            }
            if let Some(name) = current
                .and_then(|c| c.field("value"))
                .as_ref()
                .and_then(scalar_text)
            {
                let document = pair.find(Ancestor, Kind("document"));
                push_definition(defs, rule, &[&name], document.as_ref().unwrap_or(pair), sep);
            }
        }
        Shape::AllKeys(_) => emit_key_tree(
            rule,
            pair,
            &mut vec![key.to_string()],
            &mut HashSet::new(),
            defs,
            sep,
        ),
    }
}

fn emit_key_tree(
    rule: &'static DefinitionRule,
    pair: &N<'_>,
    path: &mut Vec<String>,
    seen: &mut HashSet<String>,
    defs: &mut Vec<CanonicalDefinition>,
    sep: &'static str,
) {
    if path.len() > MAX_KEY_DEPTH
        || stacker::remaining_stack().unwrap_or(usize::MAX) < crate::utils::MINIMUM_STACK_REMAINING
    {
        return;
    }
    let parts: Vec<&str> = path.iter().map(String::as_str).collect();
    if seen.insert(parts.join(sep)) {
        push_definition(defs, rule, &parts, pair, sep);
    }
    let Some(value) = pair.field("value") else {
        return;
    };
    let nested: Vec<N<'_>> = if let Some(mapping) = child_mapping(&value) {
        pairs(&mapping).collect()
    } else if let Some(sequence) = child_sequence(&value) {
        sequence
            .children()
            .filter_map(|item| child_mapping(&item))
            .flat_map(|mapping| pairs(&mapping).collect::<Vec<_>>())
            .collect()
    } else {
        return;
    };
    for child in &nested {
        if let Some(name) = pair_key(child) {
            path.push(name);
            emit_key_tree(rule, child, path, seen, defs, sep);
            path.pop();
        }
    }
}

pub(super) fn extract_definitions(
    node: &N<'_>,
    file_path: &str,
    defs: &mut Vec<CanonicalDefinition>,
    _scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if !is_pair(node) || enclosing_pair(node).is_some() {
        return false;
    }
    let Some(key) = pair_key(node) else {
        return false;
    };
    let document_types: &'static [DocumentType] = &DOCUMENT_TYPES;
    for doc_type in document_types {
        let mut claiming = doc_type
            .definitions
            .iter()
            .filter(|rule| rule.shape.claims(doc_type, &key))
            .peekable();
        if claiming.peek().is_none() || !doc_type.matcher.matches(node, file_path) {
            continue;
        }
        for rule in claiming {
            emit_shape(rule, &key, node, defs, sep);
        }
        break;
    }
    false
}

fn emit_value(rule: &'static KeyRule, node: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    if let Some(scalar) = item_scalar(node) {
        if let Some(scalar_type) = &rule.scalar_type {
            push_import(imports, scalar_type, scalar, None, None, node_range(node));
        }
    } else if let Some(mapping) = child_mapping(node) {
        emit_mapping(rule, &mapping, imports);
    }
}

fn extract_with_rule(
    rule: &'static KeyRule,
    node: &N<'_>,
    imports: &mut Vec<CanonicalImport>,
) -> bool {
    let applies = match enclosing_pair(node) {
        Some(parent) => pair_key(&parent).is_some_and(|key| rule.also_under.contains(&key)),
        None => true,
    };
    if !applies {
        return false;
    }
    let Some(value) = node.field("value") else {
        return true;
    };
    if let Some(sequence) = child_sequence(&value) {
        for item in sequence.children() {
            emit_value(rule, &item, imports);
        }
    } else {
        emit_value(rule, &value, imports);
    }
    true
}

pub(super) fn extract_imports(
    node: &N<'_>,
    file_path: &str,
    imports: &mut Vec<CanonicalImport>,
) -> bool {
    if node.kind().as_ref() != "block_mapping_pair" {
        return false;
    }
    let Some(key) = pair_key(node) else {
        return false;
    };
    let document_types: &'static [DocumentType] = &DOCUMENT_TYPES;
    document_types.iter().any(|doc_type| {
        doc_type
            .imports
            .iter()
            .filter(|rule| rule.key == key)
            .any(|rule| {
                doc_type.matcher.matches(node, file_path) && extract_with_rule(rule, node, imports)
            })
    })
}
