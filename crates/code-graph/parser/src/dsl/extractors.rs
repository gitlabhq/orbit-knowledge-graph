use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// How to extract a name from a tree-sitter node.
pub enum Extract {
    /// Look for a `name` field, then fall back to first identifier child.
    Default,
    /// Always returns None. Used when the parent node has no path/name to extract.
    None,
    /// Extract the node's own text directly (for leaf nodes like identifiers).
    Text,
    /// Extract text from a named field (e.g. `node.field("name")`).
    Field(&'static str),
    /// Extract text from the first child of this node kind.
    ChildOfKind(&'static str),
    /// Follow a chain of fields and extract the final node's text.
    FieldChain(&'static [&'static str]),
    /// C-style: follow `declarator` -> `declarator` chain to find the name.
    Declarator,
}

impl Extract {
    pub fn extract_name(&self, node: &N<'_>) -> Option<String> {
        match self {
            Extract::Default => default_name(node),
            Extract::None => None,
            Extract::Text => Some(node.text().to_string()),
            Extract::Field(name) => node.field(name).map(|n| n.text().to_string()),
            Extract::ChildOfKind(kind) => node
                .children()
                .find(|c| c.kind().as_ref() == *kind)
                .map(|n| n.text().to_string()),
            Extract::FieldChain(fields) => {
                let mut current = node.clone();
                for f in *fields {
                    current = current.field(f)?;
                }
                Some(current.text().to_string())
            }
            Extract::Declarator => declarator_name(node),
        }
    }
}

fn default_name(node: &N<'_>) -> Option<String> {
    if let Some(name_node) = node.field("name") {
        return Some(name_node.text().to_string());
    }
    for child in node.children() {
        if child.is_named() && is_identifier_kind(&child.kind()) {
            return Some(child.text().to_string());
        }
    }
    None
}

fn is_identifier_kind(kind: &str) -> bool {
    matches!(
        kind,
        "identifier"
            | "type_identifier"
            | "simple_identifier"
            | "scoped_identifier"
            | "name"
            | "field_identifier"
            | "property_identifier"
    )
}

fn declarator_name(node: &N<'_>) -> Option<String> {
    let declarator = node.field("declarator")?;
    if let Some(inner) = declarator.field("declarator") {
        return Some(inner.text().to_string());
    }
    if is_identifier_kind(&declarator.kind()) {
        return Some(declarator.text().to_string());
    }
    if let Some(name) = declarator.field("name") {
        return Some(name.text().to_string());
    }
    None
}

pub fn field(name: &'static str) -> Extract {
    Extract::Field(name)
}

pub fn field_chain(fields: &'static [&'static str]) -> Extract {
    Extract::FieldChain(fields)
}

pub fn child_of_kind(kind: &'static str) -> Extract {
    Extract::ChildOfKind(kind)
}

pub fn declarator() -> Extract {
    Extract::Declarator
}

// ── Metadata extraction ─────────────────────────────────────────

/// How to extract a list of strings from a tree-sitter node.
/// Used for super_types, decorators, etc.
pub enum ExtractList {
    /// Extract all children of a specific kind from a named field.
    /// e.g. `ChildrenOfField("interfaces", &["type_identifier", "generic_type"])`
    /// → finds `node.field("interfaces")`, collects text of all children matching the kinds.
    ChildrenOfField(&'static str, &'static [&'static str]),
    /// Extract text of all children matching these kinds directly on the node.
    ChildrenOfKind(&'static [&'static str]),
    /// Extract text from a named field, then split on a separator.
    FieldSplit(&'static str, &'static str),
    /// Walk ancestors looking for decorator/annotation nodes.
    Decorators(&'static str),
    /// Custom function for complex extraction.
    Fn(fn(&N<'_>) -> Vec<String>),
}

impl ExtractList {
    pub fn extract(&self, node: &N<'_>) -> Vec<String> {
        match self {
            ExtractList::ChildrenOfField(field_name, kinds) => {
                let Some(field_node) = node.field(field_name) else {
                    return vec![];
                };
                field_node
                    .children()
                    .filter(|c| {
                        let k = c.kind();
                        kinds.iter().any(|&target| target == k.as_ref())
                    })
                    .map(|c| c.text().to_string())
                    .collect()
            }
            ExtractList::ChildrenOfKind(kinds) => node
                .children()
                .filter(|c| {
                    let k = c.kind();
                    kinds.iter().any(|&target| target == k.as_ref())
                })
                .map(|c| c.text().to_string())
                .collect(),
            ExtractList::FieldSplit(field_name, sep) => {
                let Some(field_node) = node.field(field_name) else {
                    return vec![];
                };
                field_node
                    .text()
                    .split(sep)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            ExtractList::Decorators(decorator_kind) => {
                if let Some(parent) = node.parent()
                    && parent.kind() == "decorated_definition"
                {
                    parent
                        .children()
                        .filter(|c| c.kind().as_ref() == *decorator_kind)
                        .map(|c| c.text().trim_start_matches('@').trim().to_string())
                        .collect()
                } else {
                    vec![]
                }
            }
            ExtractList::Fn(f) => f(node),
        }
    }
}

/// Declarative metadata extraction rules for a scope definition.
///
/// Each field is optional — only populated fields produce metadata.
pub struct MetadataRule {
    /// How to extract super types (extends, implements).
    pub super_types: Option<ExtractList>,
    /// How to extract the return type.
    pub return_type: Option<Extract>,
    /// How to extract the type annotation.
    pub type_annotation: Option<Extract>,
    /// How to extract the receiver type (Kotlin extension functions).
    pub receiver_type: Option<Extract>,
    /// How to extract decorators/annotations.
    pub decorators: Option<ExtractList>,
    /// How to extract companion_of.
    pub companion_of: Option<Extract>,
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
        }
    }

    pub fn super_types(mut self, extract: ExtractList) -> Self {
        self.super_types = Some(extract);
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

    pub fn decorators(mut self, extract: ExtractList) -> Self {
        self.decorators = Some(extract);
        self
    }

    pub fn companion_of(mut self, extract: Extract) -> Self {
        self.companion_of = Some(extract);
        self
    }

    /// Extract metadata from a node. Type names in return_type, type_annotation,
    /// receiver_type, and super_types are resolved against the file's imports
    /// to produce full FQNs where possible.
    pub fn extract_metadata(
        &self,
        node: &N<'_>,
        import_map: &rustc_hash::FxHashMap<String, String>,
        sep: &'static str,
    ) -> Option<Box<code_graph_types::DefinitionMetadata>> {
        use code_graph_types::IStr;
        let super_types: Vec<IStr> = self
            .super_types
            .as_ref()
            .map(|e| {
                e.extract(node)
                    .into_iter()
                    .map(|s| IStr::from(resolve_type_via_map(&s, import_map, sep).as_str()))
                    .collect()
            })
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| IStr::from(resolve_type_via_map(&s, import_map, sep).as_str()));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| IStr::from(resolve_type_via_map(&s, import_map, sep).as_str()));
        let receiver_type = self
            .receiver_type
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| IStr::from(resolve_type_via_map(&s, import_map, sep).as_str()));
        let decorators: Vec<IStr> = self
            .decorators
            .as_ref()
            .map(|e| {
                e.extract(node)
                    .into_iter()
                    .map(|s| IStr::from(s.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        let companion_of = self
            .companion_of
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| IStr::from(s.as_str()));

        let has_data = !super_types.is_empty()
            || return_type.is_some()
            || type_annotation.is_some()
            || receiver_type.is_some()
            || !decorators.is_empty()
            || companion_of.is_some();

        if !has_data {
            return None;
        }

        Some(Box::new(code_graph_types::DefinitionMetadata {
            super_types,
            return_type,
            type_annotation,
            receiver_type,
            decorators,
            companion_of,
        }))
    }
}

pub fn metadata() -> MetadataRule {
    MetadataRule::new()
}

/// Resolve a bare type name to a full FQN using the pre-built import map.
/// O(1) hashmap lookup instead of linear scan.
pub fn resolve_type_via_map(
    bare_name: &str,
    import_map: &rustc_hash::FxHashMap<String, String>,
    _sep: &str,
) -> String {
    import_map
        .get(bare_name)
        .cloned()
        .unwrap_or_else(|| bare_name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::LanguageExt;

    #[test]
    fn test_default_extract() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(
            Extract::Default.extract_name(&func),
            Some("foo".to_string())
        );
    }

    #[test]
    fn test_field_extract() {
        let root = SupportLang::Python.ast_grep("def bar(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").extract_name(&func), Some("bar".to_string()));
    }
}
