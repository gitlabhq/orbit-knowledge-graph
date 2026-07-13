use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{Ontology, loading::ONTOLOGY_SCHEMA_FILE};

/// The ledger YAML `scope:` value; an `entities:` subset narrows `Sdlc` only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerScope {
    /// Full rebuild.
    #[serde(rename = "*")]
    All,
    /// SDLC-sourced tables. Pairs with an optional `entities:` subset.
    #[serde(rename = "sdlc")]
    Sdlc,
    /// Code-graph tables.
    #[serde(rename = "code")]
    Code,
    /// Re-index nothing; the source text changed but the produced output is
    /// certified byte-identical. Requires a `note:` justifying output-neutrality.
    #[serde(rename = "none")]
    None,
}

/// What a migration invalidates and must re-index. Empty `Sdlc` set = the whole SDLC domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationScope {
    Full,
    Code,
    Sdlc(BTreeSet<String>),
    /// Re-index nothing: clone every table unchanged and advance the version.
    None,
}

impl MigrationScope {
    #[must_use]
    pub fn covers_scope_of(&self, required: &Self) -> bool {
        match (self, required) {
            (Self::Full, _) => true,
            // Anything covers "re-index nothing"; `None` itself covers only `None`.
            (_, Self::None) => true,
            (Self::None, _) => false,
            (_, Self::Full) => false,
            (Self::Code, Self::Code) => true,
            (Self::Code, Self::Sdlc(_)) | (Self::Sdlc(_), Self::Code) => false,
            (Self::Sdlc(covering), Self::Sdlc(required)) => {
                if covering.is_empty() {
                    true
                } else {
                    !required.is_empty() && required.is_subset(covering)
                }
            }
        }
    }

    /// The widest of the two scopes; mixing `code` and `sdlc` widens to `"*"`.
    #[must_use]
    pub fn widened_with(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full, _) | (_, Self::Full) => Self::Full,
            (Self::None, other) => other.clone(),
            (s, Self::None) => s.clone(),
            (Self::Code, Self::Code) => Self::Code,
            (Self::Code, Self::Sdlc(_)) | (Self::Sdlc(_), Self::Code) => Self::Full,
            (Self::Sdlc(left), Self::Sdlc(right)) => {
                if left.is_empty() || right.is_empty() {
                    Self::Sdlc(BTreeSet::new())
                } else {
                    Self::Sdlc(left.union(right).cloned().collect())
                }
            }
        }
    }
}

impl std::fmt::Display for MigrationScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "scope \"*\""),
            Self::None => write!(f, "scope none"),
            Self::Code => write!(f, "scope code"),
            Self::Sdlc(entities) if entities.is_empty() => write!(f, "scope sdlc"),
            Self::Sdlc(entities) => {
                let list = entities.iter().cloned().collect::<Vec<_>>().join(", ");
                write!(f, "scope sdlc, entities [{list}]")
            }
        }
    }
}

/// The minimal [`MigrationScope`] a fingerprint diff justifies, or `None` if
/// nothing changed. Fail-safe: anything unmapped widens to [`MigrationScope::Full`].
#[must_use]
pub fn derive_scope(
    ontology: &Ontology,
    source_contents: &BTreeMap<String, String>,
    changed_sources: &BTreeSet<String>,
    changed_tables: &BTreeSet<String>,
) -> Option<MigrationScope> {
    if changed_sources.is_empty() && changed_tables.is_empty() {
        return None;
    }

    let mut sdlc_entities: BTreeSet<String> = BTreeSet::new();
    let mut code_changed = false;

    for path in changed_sources {
        if path == ONTOLOGY_SCHEMA_FILE || path == "reference.yaml" {
            return Some(MigrationScope::Full);
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
                None => return Some(MigrationScope::Full),
            }
        } else if path.starts_with("edges/") {
            match edge_kind_for_path(ontology, path) {
                Some(kind)
                    if is_code_table(ontology, ontology.edge_table_for_relationship(&kind)) =>
                {
                    code_changed = true;
                }
                Some(kind) => {
                    sdlc_entities.insert(kind);
                }
                None => return Some(MigrationScope::Full),
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
                None => return Some(MigrationScope::Full),
            }
        } else {
            return Some(MigrationScope::Full);
        }
    }

    for table in changed_tables {
        if !table_owned_by_scope(ontology, table, &sdlc_entities, code_changed) {
            return Some(MigrationScope::Full);
        }
    }

    if !sdlc_entities.is_empty() && code_changed {
        return Some(MigrationScope::Full);
    }
    if code_changed {
        return Some(MigrationScope::Code);
    }
    Some(MigrationScope::Sdlc(sdlc_entities))
}

/// Names accepted in an entry's `entities:` list: etl-bearing nodes, derived
/// entities, and SDLC-routed relationship kinds (code edges are excluded).
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
        if !is_code_table(ontology, ontology.edge_table_for_relationship(kind)) {
            names.insert(kind.to_string());
        }
    }
    names
}

#[must_use]
pub fn code_entity_names(ontology: &Ontology) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for node in ontology.nodes() {
        if node.etl.is_none() {
            names.insert(node.name.clone());
        }
    }
    for kind in ontology.edge_names() {
        if is_code_table(ontology, ontology.edge_table_for_relationship(kind)) {
            names.insert(kind.to_string());
        }
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
        assert!(MigrationScope::Full.covers_scope_of(&MigrationScope::Sdlc(entities(&["Note"]))));
        assert!(MigrationScope::Full.covers_scope_of(&MigrationScope::Code));
    }

    #[test]
    fn scope_covers_sdlc_subset_rules() {
        let any_sdlc = MigrationScope::Sdlc(BTreeSet::new());
        assert!(any_sdlc.covers_scope_of(&MigrationScope::Sdlc(entities(&["Note"]))));
        assert!(
            MigrationScope::Sdlc(entities(&["Note", "Issue"]))
                .covers_scope_of(&MigrationScope::Sdlc(entities(&["Note"])))
        );
        assert!(
            !MigrationScope::Sdlc(entities(&["Note"]))
                .covers_scope_of(&MigrationScope::Sdlc(entities(&["Issue"])))
        );
        assert!(!MigrationScope::Sdlc(entities(&["Note"])).covers_scope_of(&any_sdlc));
    }

    #[test]
    fn scope_covers_code_and_sdlc_are_disjoint() {
        assert!(!MigrationScope::Code.covers_scope_of(&MigrationScope::Sdlc(entities(&["Note"]))));
        assert!(!MigrationScope::Sdlc(BTreeSet::new()).covers_scope_of(&MigrationScope::Code));
        assert!(MigrationScope::Code.covers_scope_of(&MigrationScope::Code));
    }

    #[test]
    fn scope_covers_none_is_covered_by_everything() {
        assert!(MigrationScope::Full.covers_scope_of(&MigrationScope::None));
        assert!(MigrationScope::Code.covers_scope_of(&MigrationScope::None));
        assert!(MigrationScope::Sdlc(entities(&["Note"])).covers_scope_of(&MigrationScope::None));
        assert!(MigrationScope::None.covers_scope_of(&MigrationScope::None));
    }

    #[test]
    fn scope_covers_none_covers_nothing_but_none() {
        assert!(!MigrationScope::None.covers_scope_of(&MigrationScope::Full));
        assert!(!MigrationScope::None.covers_scope_of(&MigrationScope::Code));
        assert!(!MigrationScope::None.covers_scope_of(&MigrationScope::Sdlc(entities(&["Note"]))));
    }

    #[test]
    fn widen_none_is_identity() {
        let sdlc = MigrationScope::Sdlc(entities(&["Note"]));
        assert_eq!(MigrationScope::None.widened_with(&sdlc), sdlc);
        assert_eq!(sdlc.widened_with(&MigrationScope::None), sdlc);
        assert_eq!(
            MigrationScope::None.widened_with(&MigrationScope::None),
            MigrationScope::None
        );
        assert_eq!(
            MigrationScope::None.widened_with(&MigrationScope::Full),
            MigrationScope::Full
        );
    }

    #[test]
    fn widen_mixes_code_and_sdlc_to_all() {
        let widened = MigrationScope::Sdlc(entities(&["Note"])).widened_with(&MigrationScope::Code);
        assert_eq!(widened, MigrationScope::Full);
    }

    #[test]
    fn widen_unions_sdlc_entities() {
        let widened = MigrationScope::Sdlc(entities(&["Note"]))
            .widened_with(&MigrationScope::Sdlc(entities(&["Issue"])));
        assert_eq!(widened, MigrationScope::Sdlc(entities(&["Issue", "Note"])));
    }

    #[test]
    fn widen_empty_entities_absorbs() {
        let widened = MigrationScope::Sdlc(entities(&["Note"]))
            .widened_with(&MigrationScope::Sdlc(BTreeSet::new()));
        assert_eq!(widened, MigrationScope::Sdlc(BTreeSet::new()));
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
        assert_eq!(scope, Some(MigrationScope::Full));
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
        assert_eq!(scope, Some(MigrationScope::Sdlc(entities(&["Note"]))));
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
        assert_eq!(scope, Some(MigrationScope::Code));
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
        assert_eq!(scope, Some(MigrationScope::Full));
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
        assert_eq!(scope, Some(MigrationScope::Full));
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
        assert_eq!(scope, Some(MigrationScope::Sdlc(entities(&["Note"]))));
    }

    // HAS_NOTE is FK-derived: no edge `etl:` block, but routed to the default
    // SDLC edge table, so it must derive an sdlc entity — not code.
    #[test]
    fn derive_scope_edge_resolves_to_relationship_kind() {
        let ontology = Ontology::new().with_edges(["HAS_NOTE"]);
        let scope = derive_scope(
            &ontology,
            &BTreeMap::new(),
            &entities(&["edges/has_note.yaml"]),
            &BTreeSet::new(),
        );
        assert_eq!(scope, Some(MigrationScope::Sdlc(entities(&["HAS_NOTE"]))));
    }

    // calls/defines/extends/imports/on_branch route to gl_code_edge, so an edge
    // change there must derive `code`, never a bogus sdlc entity.
    #[test]
    fn derive_scope_code_edge_is_code() {
        let ontology = Ontology::new()
            .with_edges(["CALLS"])
            .with_edge_table("gl_code_edge")
            .with_edge_for_table("CALLS", "gl_code_edge");
        let scope = derive_scope(
            &ontology,
            &BTreeMap::new(),
            &entities(&["edges/calls.yaml"]),
            &entities(&["gl_code_edge"]),
        );
        assert_eq!(scope, Some(MigrationScope::Code));
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
        assert_eq!(scope, Some(MigrationScope::Full));
    }
}
