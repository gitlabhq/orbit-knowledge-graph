use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// How to extract a name from a tree-sitter node.
pub struct Extract {
    strategy: ExtractStrategy,
    /// Optional inner descent: if the extracted node has a child of
    /// `container_kind`, find the first child of `target_kind` inside it
    /// and use that instead. Falls back to the node's text if not found.
    inner: Option<(&'static str, &'static str)>,
    /// Optional chained extract applied to the node found by this extract.
    then: Option<Box<Extract>>,
}

enum ExtractStrategy {
    Default,
    None,
    Text,
    Field(&'static str),
    ChildOfKind(&'static str),
    FieldChain(&'static [&'static str]),
}

impl Extract {
    /// Navigate to a node, then delegate to a chained extract.
    fn navigate_to_node<'a>(&self, node: &N<'a>) -> Option<N<'a>> {
        match &self.strategy {
            ExtractStrategy::Default | ExtractStrategy::None | ExtractStrategy::Text => {
                Some(node.clone())
            }
            ExtractStrategy::Field(name) => node.field(name),
            ExtractStrategy::ChildOfKind(kind) => node.find(Child, Kind(kind)),
            ExtractStrategy::FieldChain(fields) => node.field_chain(fields),
        }
    }

    pub fn extract_name(&self, node: &N<'_>) -> Option<String> {
        if let Some(next) = &self.then {
            let navigated = self.navigate_to_node(node)?;
            return next.extract_name(&navigated);
        }
        match &self.strategy {
            ExtractStrategy::Default => default_name(node),
            ExtractStrategy::None => None,
            ExtractStrategy::Text => Some(node.text().to_string()),
            ExtractStrategy::Field(name) => {
                node.field(name).map(|n| self.extract_inner_or_text(&n))
            }
            ExtractStrategy::ChildOfKind(kind) => node
                .find(Child, Kind(kind))
                .map(|n| self.extract_inner_or_text(&n)),
            ExtractStrategy::FieldChain(fields) => {
                let current = node.field_chain(fields)?;
                Some(self.extract_inner_or_text(&current))
            }
        }
    }

    fn extract_inner_or_text(&self, node: &N<'_>) -> String {
        if let Some((container_kind, target_kind)) = self.inner
            && let Some(container) = node.find(Child, Kind(container_kind))
            && let Some(inner) = container.find(Descendant, Kind(target_kind))
        {
            return inner.text().to_string();
        }
        node.text().to_string()
    }

    /// Add an inner descent: if the result node has a child of
    /// `container_kind`, take the first `target_kind` inside it.
    ///
    /// ```ignore
    /// field("type").inner("type_arguments", "type_identifier")
    /// // List<UserService> → navigate type_arguments → UserService
    /// ```
    pub fn inner(mut self, container_kind: &'static str, target_kind: &'static str) -> Self {
        self.inner = Some((container_kind, target_kind));
        self
    }

    /// Chain another extract step on the result of this one.
    ///
    /// ```ignore
    /// field("receiver").then(child_of_kind("parameter_declaration").then(field("type")))
    /// ```
    pub fn then(mut self, next: Extract) -> Self {
        self.then = Some(Box::new(next));
        self
    }
}

// ── Constructors ────────────────────────────────────────────────

pub fn field(name: &'static str) -> Extract {
    Extract {
        strategy: ExtractStrategy::Field(name),
        inner: None,
        then: None,
    }
}

pub fn field_chain(fields: &'static [&'static str]) -> Extract {
    Extract {
        strategy: ExtractStrategy::FieldChain(fields),
        inner: None,
        then: None,
    }
}

pub fn child_of_kind(kind: &'static str) -> Extract {
    Extract {
        strategy: ExtractStrategy::ChildOfKind(kind),
        inner: None,
        then: None,
    }
}

pub fn text() -> Extract {
    Extract {
        strategy: ExtractStrategy::Text,
        inner: None,
        then: None,
    }
}

pub fn no_extract() -> Extract {
    Extract {
        strategy: ExtractStrategy::None,
        inner: None,
        then: None,
    }
}

pub fn default_extract() -> Extract {
    Extract {
        strategy: ExtractStrategy::Default,
        inner: None,
        then: None,
    }
}

// ── Helpers ─────────────────────────────────────────────────────

pub fn default_name(node: &N<'_>) -> Option<String> {
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
                    .children_matching(AnyKind(kinds))
                    .map(|c| c.text().to_string())
                    .collect()
            }
            ExtractList::ChildrenOfKind(kinds) => node
                .children_matching(AnyKind(kinds))
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
                if let Some(parent) = node.find(Parent, Kind("decorated_definition")) {
                    parent
                        .children_matching(Kind(decorator_kind))
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
    ) -> Option<Box<crate::v2::types::DefinitionMetadata>> {
        let super_types: Vec<String> = self
            .super_types
            .as_ref()
            .map(|e| {
                e.extract(node)
                    .into_iter()
                    .map(|s| resolve_type_via_map(&s, import_map, sep))
                    .collect()
            })
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let receiver_type = self
            .receiver_type
            .as_ref()
            .and_then(|e| e.extract_name(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let decorators = self
            .decorators
            .as_ref()
            .map(|e| e.extract(node))
            .unwrap_or_default();
        let companion_of = self
            .companion_of
            .as_ref()
            .and_then(|e| e.extract_name(node));

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
            default_extract().extract_name(&func),
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
