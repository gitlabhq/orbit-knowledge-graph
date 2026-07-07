use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::Ontology;

/// How much of the graph an entry (or a derived drift) invalidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scope {
    /// Full rebuild.
    #[serde(rename = "*")]
    All,
    /// SDLC-sourced tables. Pairs with an optional `entities:` subset.
    #[serde(rename = "sdlc")]
    Sdlc,
    /// Code-graph tables.
    #[serde(rename = "code")]
    Code,
}

/// A scope paired with the SDLC entities that narrow it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeDeclaration {
    pub scope: Scope,
    /// Empty means the whole scope; only `sdlc` declarations carry entities.
    pub entities: BTreeSet<String>,
}

impl ScopeDeclaration {
    #[must_use]
    pub fn covers(&self, required: &Self) -> bool {
        match (self.scope, required.scope) {
            (Scope::All, _) => true,
            (_, Scope::All) => false,
            (Scope::Code, Scope::Code) => true,
            (Scope::Code, Scope::Sdlc) | (Scope::Sdlc, Scope::Code) => false,
            (Scope::Sdlc, Scope::Sdlc) => {
                if self.entities.is_empty() {
                    true
                } else {
                    !required.entities.is_empty() && required.entities.is_subset(&self.entities)
                }
            }
        }
    }

    /// The widest of the two declarations; mixing `code` and `sdlc` widens to `"*"`.
    #[must_use]
    pub fn widen(&self, other: &Self) -> Self {
        match (self.scope, other.scope) {
            (Scope::All, _) | (_, Scope::All) => Self::all(),
            (Scope::Code, Scope::Code) => Self::code(),
            (Scope::Code, Scope::Sdlc) | (Scope::Sdlc, Scope::Code) => Self::all(),
            (Scope::Sdlc, Scope::Sdlc) => {
                if self.entities.is_empty() || other.entities.is_empty() {
                    Self::sdlc(BTreeSet::new())
                } else {
                    Self::sdlc(self.entities.union(&other.entities).cloned().collect())
                }
            }
        }
    }

    #[must_use]
    fn all() -> Self {
        Self {
            scope: Scope::All,
            entities: BTreeSet::new(),
        }
    }

    #[must_use]
    fn code() -> Self {
        Self {
            scope: Scope::Code,
            entities: BTreeSet::new(),
        }
    }

    #[must_use]
    fn sdlc(entities: BTreeSet<String>) -> Self {
        Self {
            scope: Scope::Sdlc,
            entities,
        }
    }
}

impl std::fmt::Display for ScopeDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.scope {
            Scope::All => write!(f, "scope \"*\""),
            Scope::Code => write!(f, "scope code"),
            Scope::Sdlc if self.entities.is_empty() => write!(f, "scope sdlc"),
            Scope::Sdlc => {
                let list = self.entities.iter().cloned().collect::<Vec<_>>().join(", ");
                write!(f, "scope sdlc, entities [{list}]")
            }
        }
    }
}

/// The resolved set of tables a migration must invalidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationScope {
    pub sdlc: SdlcScope,
    pub code: bool,
}

impl InvalidationScope {
    #[must_use]
    pub fn none() -> Self {
        Self {
            sdlc: SdlcScope::None,
            code: false,
        }
    }

    #[must_use]
    pub fn full() -> Self {
        Self {
            sdlc: SdlcScope::All,
            code: true,
        }
    }

    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        Self {
            sdlc: self.sdlc.union(&other.sdlc),
            code: self.code || other.code,
        }
    }
}

/// SDLC side of an [`InvalidationScope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdlcScope {
    None,
    All,
    Entities(BTreeSet<String>),
}

impl SdlcScope {
    #[must_use]
    fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (SdlcScope::All, _) | (_, SdlcScope::All) => SdlcScope::All,
            (SdlcScope::None, s) | (s, SdlcScope::None) => s.clone(),
            (SdlcScope::Entities(a), SdlcScope::Entities(b)) => {
                SdlcScope::Entities(a.union(b).cloned().collect())
            }
        }
    }
}

/// The minimal [`ScopeDeclaration`] a fingerprint diff justifies, or `None` if
/// nothing changed. Fail-safe: anything unmapped widens to [`Scope::All`].
#[must_use]
pub fn derive_scope(
    ontology: &Ontology,
    source_contents: &BTreeMap<String, String>,
    changed_sources: &BTreeSet<String>,
    changed_tables: &BTreeSet<String>,
) -> Option<ScopeDeclaration> {
    if changed_sources.is_empty() && changed_tables.is_empty() {
        return None;
    }

    let mut sdlc_entities: BTreeSet<String> = BTreeSet::new();
    let mut code_changed = false;

    for path in changed_sources {
        if path == "schema.yaml" || path == "reference.yaml" {
            return Some(ScopeDeclaration::all());
        }
        if path.starts_with("nodes/") {
            match source_contents
                .get(path)
                .map(String::as_str)
                .and_then(parse_node_scope)
            {
                Some(NodeScope::Sdlc(entity)) => {
                    sdlc_entities.insert(entity);
                }
                Some(NodeScope::Code) => code_changed = true,
                None => return Some(ScopeDeclaration::all()),
            }
        } else if path.starts_with("edges/") {
            match edge_kind_for_path(ontology, path) {
                Some(relationship_kind) => {
                    sdlc_entities.insert(relationship_kind);
                }
                None => return Some(ScopeDeclaration::all()),
            }
        } else if path.starts_with("derived/") {
            match source_contents
                .get(path)
                .map(String::as_str)
                .and_then(parse_derived_name)
            {
                Some(entity) => {
                    sdlc_entities.insert(entity);
                }
                None => return Some(ScopeDeclaration::all()),
            }
        } else {
            return Some(ScopeDeclaration::all());
        }
    }

    for table in changed_tables {
        if !table_owned_by_scope(ontology, table, &sdlc_entities, code_changed) {
            return Some(ScopeDeclaration::all());
        }
    }

    if !sdlc_entities.is_empty() && code_changed {
        return Some(ScopeDeclaration::all());
    }
    if code_changed {
        return Some(ScopeDeclaration::code());
    }
    Some(ScopeDeclaration::sdlc(sdlc_entities))
}

/// Names accepted in an entry's `entities:` list: etl-bearing nodes, derived
/// entities, and relationship kinds.
#[must_use]
pub fn sdlc_entity_names(ontology: &Ontology) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for node in ontology.nodes() {
        if node.etl.is_some() {
            names.insert(node.name.clone());
        }
    }
    for derived in ontology.derived_entities() {
        names.insert(derived.name.clone());
    }
    for kind in ontology.edge_names() {
        names.insert(kind.to_string());
    }
    names
}

enum NodeScope {
    Sdlc(String),
    Code,
}

#[derive(Deserialize)]
struct NodeScopeYaml {
    node_type: String,
    #[serde(default)]
    etl: Option<serde_yaml::Value>,
}

fn parse_node_scope(content: &str) -> Option<NodeScope> {
    let parsed: NodeScopeYaml = serde_yaml::from_str(content).ok()?;
    if parsed.etl.is_some() {
        Some(NodeScope::Sdlc(parsed.node_type))
    } else {
        Some(NodeScope::Code)
    }
}

#[derive(Deserialize)]
struct DerivedNameYaml {
    name: String,
}

fn parse_derived_name(content: &str) -> Option<String> {
    let parsed: DerivedNameYaml = serde_yaml::from_str(content).ok()?;
    Some(parsed.name)
}

/// `edges/<stem>.yaml` → the relationship kind, when it resolves in the ontology.
fn edge_kind_for_path(ontology: &Ontology, path: &str) -> Option<String> {
    let stem = path
        .strip_prefix("edges/")?
        .strip_suffix(".yaml")?
        .to_uppercase();
    ontology.has_edge(&stem).then_some(stem)
}

/// Whether `table`'s DDL change is explained by the scope collected so far.
fn table_owned_by_scope(
    ontology: &Ontology,
    table: &str,
    sdlc_entities: &BTreeSet<String>,
    code_changed: bool,
) -> bool {
    for entity in sdlc_entities {
        if let Some(node) = ontology.get_node(entity)
            && node.destination_table == table
        {
            return true;
        }
        if ontology.has_edge(entity) && ontology.edge_table_for_relationship(entity) == table {
            return true;
        }
    }
    code_changed && is_code_table(ontology, table)
}

fn is_code_table(ontology: &Ontology, table: &str) -> bool {
    ontology
        .nodes()
        .any(|n| n.etl.is_none() && n.destination_table == table)
        || table == "gl_code_edge"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entities(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn scope_covers_star_covers_all() {
        assert!(ScopeDeclaration::all().covers(&ScopeDeclaration::sdlc(entities(&["Note"]))));
        assert!(ScopeDeclaration::all().covers(&ScopeDeclaration::code()));
    }

    #[test]
    fn scope_covers_sdlc_subset_rules() {
        let any_sdlc = ScopeDeclaration::sdlc(BTreeSet::new());
        assert!(any_sdlc.covers(&ScopeDeclaration::sdlc(entities(&["Note"]))));
        assert!(
            ScopeDeclaration::sdlc(entities(&["Note", "Issue"]))
                .covers(&ScopeDeclaration::sdlc(entities(&["Note"])))
        );
        assert!(
            !ScopeDeclaration::sdlc(entities(&["Note"]))
                .covers(&ScopeDeclaration::sdlc(entities(&["Issue"])))
        );
        assert!(!ScopeDeclaration::sdlc(entities(&["Note"])).covers(&any_sdlc));
    }

    #[test]
    fn scope_covers_code_and_sdlc_are_disjoint() {
        assert!(!ScopeDeclaration::code().covers(&ScopeDeclaration::sdlc(entities(&["Note"]))));
        assert!(!ScopeDeclaration::sdlc(BTreeSet::new()).covers(&ScopeDeclaration::code()));
        assert!(ScopeDeclaration::code().covers(&ScopeDeclaration::code()));
    }

    #[test]
    fn widen_mixes_code_and_sdlc_to_all() {
        let widened = ScopeDeclaration::sdlc(entities(&["Note"])).widen(&ScopeDeclaration::code());
        assert_eq!(widened, ScopeDeclaration::all());
    }

    #[test]
    fn widen_unions_sdlc_entities() {
        let widened = ScopeDeclaration::sdlc(entities(&["Note"]))
            .widen(&ScopeDeclaration::sdlc(entities(&["Issue"])));
        assert_eq!(
            widened,
            ScopeDeclaration::sdlc(entities(&["Issue", "Note"]))
        );
    }

    #[test]
    fn widen_empty_entities_absorbs() {
        let widened = ScopeDeclaration::sdlc(entities(&["Note"]))
            .widen(&ScopeDeclaration::sdlc(BTreeSet::new()));
        assert_eq!(widened, ScopeDeclaration::sdlc(BTreeSet::new()));
    }

    #[test]
    fn derive_scope_none_when_no_drift() {
        let ontology = Ontology::new();
        assert!(
            derive_scope(
                &ontology,
                &BTreeMap::new(),
                &BTreeSet::new(),
                &BTreeSet::new()
            )
            .is_none()
        );
    }

    #[test]
    fn derive_scope_schema_yaml_is_full() {
        let ontology = Ontology::new();
        let scope = derive_scope(
            &ontology,
            &BTreeMap::new(),
            &entities(&["schema.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::all()));
    }

    #[test]
    fn derive_scope_etl_node_is_sdlc_entity() {
        let ontology = Ontology::new();
        let embedded = BTreeMap::from([(
            "nodes/core/note.yaml".to_string(),
            "node_type: Note\netl:\n  type: table\n".to_string(),
        )]);
        let scope = derive_scope(
            &ontology,
            &embedded,
            &entities(&["nodes/core/note.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::sdlc(entities(&["Note"]))));
    }

    #[test]
    fn derive_scope_etl_less_node_is_code() {
        let ontology = Ontology::new();
        let embedded = BTreeMap::from([(
            "nodes/source_code/file.yaml".to_string(),
            "node_type: File\n".to_string(),
        )]);
        let scope = derive_scope(
            &ontology,
            &embedded,
            &entities(&["nodes/source_code/file.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::code()));
    }

    #[test]
    fn derive_scope_mixed_code_and_sdlc_is_full() {
        let ontology = Ontology::new();
        let embedded = BTreeMap::from([
            (
                "nodes/core/note.yaml".to_string(),
                "node_type: Note\netl:\n  type: table\n".to_string(),
            ),
            (
                "nodes/source_code/file.yaml".to_string(),
                "node_type: File\n".to_string(),
            ),
        ]);
        let scope = derive_scope(
            &ontology,
            &embedded,
            &entities(&["nodes/core/note.yaml", "nodes/source_code/file.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::all()));
    }

    #[test]
    fn derive_scope_unowned_ddl_table_is_full() {
        let ontology = Ontology::new().with_nodes(["Note"]);
        let embedded = BTreeMap::from([(
            "nodes/core/note.yaml".to_string(),
            "node_type: Note\netl:\n  type: table\n".to_string(),
        )]);
        let scope = derive_scope(
            &ontology,
            &embedded,
            &entities(&["nodes/core/note.yaml"]),
            &entities(&["checkpoint"]),
        );
        assert_eq!(scope, Some(ScopeDeclaration::all()));
    }

    #[test]
    fn derive_scope_node_owns_its_ddl_table() {
        let ontology = Ontology::new().with_nodes(["Note"]);
        let embedded = BTreeMap::from([(
            "nodes/core/note.yaml".to_string(),
            "node_type: Note\netl:\n  type: table\n".to_string(),
        )]);
        let scope = derive_scope(
            &ontology,
            &embedded,
            &entities(&["nodes/core/note.yaml"]),
            &entities(&["gl_note"]),
        );
        assert_eq!(scope, Some(ScopeDeclaration::sdlc(entities(&["Note"]))));
    }

    #[test]
    fn derive_scope_edge_resolves_to_relationship_kind() {
        let ontology = Ontology::new().with_edges(["HAS_NOTE"]);
        let scope = derive_scope(
            &ontology,
            &BTreeMap::new(),
            &entities(&["edges/has_note.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::sdlc(entities(&["HAS_NOTE"]))));
    }

    #[test]
    fn derive_scope_unknown_edge_is_full() {
        let ontology = Ontology::new();
        let scope = derive_scope(
            &ontology,
            &BTreeMap::new(),
            &entities(&["edges/ghost.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(ScopeDeclaration::all()));
    }
}
