use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Fingerprints, sha256_hex};
use crate::{DenormalizedProperty, NodeRefKind, Ontology, Pipeline};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeRows {
    #[default]
    Changed,
    Unchanged,
}

impl EdgeRows {
    #[must_use]
    pub fn is_changed(&self) -> bool {
        *self == Self::Changed
    }

    #[must_use]
    pub fn covers(self, required: Self) -> bool {
        self.is_changed() || !required.is_changed()
    }

    #[must_use]
    pub fn widened_with(self, other: Self) -> Self {
        if self.is_changed() || other.is_changed() {
            Self::Changed
        } else {
            Self::Unchanged
        }
    }
}

#[must_use]
pub fn hash_pipeline_edge_rows(ontology: &Ontology) -> BTreeMap<String, String> {
    ontology
        .iter_pipelines_by_owner()
        .map(|(owner, pipeline)| {
            let inputs = collect_edge_row_inputs(ontology, owner, pipeline);
            (pipeline.name.clone(), sha256_hex(&inputs.to_string()))
        })
        .collect()
}

#[must_use]
pub fn derive_edge_rows(
    ontology: &Ontology,
    current: &Fingerprints,
    base: &Fingerprints,
) -> EdgeRows {
    let (changed_sources, changed_tables) = current.get_versioned_diff_keys_between(base);
    let edge_tables = ontology.edge_tables();

    let lowering_code_only = changed_sources.is_empty() && changed_tables.is_empty();
    let base_has_no_edge_row_hashes = base.edge_rows.is_empty();
    let edge_table_ddl_changed = changed_tables
        .iter()
        .any(|table| edge_tables.contains(&table.as_str()));
    let pipeline_edge_rows_changed = base
        .edge_rows
        .iter()
        .any(|(pipeline, hash)| current.edge_rows.get(pipeline) != Some(hash));

    if lowering_code_only
        || base_has_no_edge_row_hashes
        || edge_table_ddl_changed
        || pipeline_edge_rows_changed
    {
        EdgeRows::Changed
    } else {
        EdgeRows::Unchanged
    }
}

fn collect_edge_row_inputs(ontology: &Ontology, owner: &str, pipeline: &Pipeline) -> Value {
    let emitted_kinds = ontology.relationship_kinds_emitted_by(owner);

    let routes: Vec<[&str; 4]> = emitted_kinds
        .iter()
        .flat_map(|kind| ontology.get_edge(kind).unwrap_or_default())
        .map(|edge| {
            [
                &edge.relationship_kind,
                &edge.source_kind,
                &edge.target_kind,
                &edge.destination_table,
            ]
            .map(String::as_str)
        })
        .collect();

    let tags: Vec<&DenormalizedProperty> = ontology
        .denormalized_properties()
        .iter()
        .filter(|property| emitted_kinds.contains(&property.relationship_kind))
        .collect();

    let owner_fields = list_fields_read_by_edge_mappings(pipeline)
        .into_iter()
        .filter_map(|name| describe_read_field(ontology, owner, name));
    let tag_fields = tags.iter().filter_map(|property| {
        describe_read_field(ontology, &property.node_kind, &property.property_name)
    });
    let read_fields: Vec<Value> = owner_fields.chain(tag_fields).collect();

    json!({
        "pipeline": pipeline,
        "routes": routes,
        "tags": tags,
        "read_fields": read_fields,
    })
}

fn list_fields_read_by_edge_mappings(pipeline: &Pipeline) -> BTreeSet<&str> {
    let mut names = BTreeSet::from(["traversal_path"]);
    for mapping in pipeline.transform.edges() {
        names.extend(mapping.array_field.as_deref());
        for node_ref in [&mapping.source, &mapping.target] {
            names.insert(node_ref.field.as_str());
            names.extend(node_ref.property_inputs.values().map(String::as_str));
            if let NodeRefKind::Derived { column, .. } = &node_ref.kind {
                names.insert(column.as_str());
            }
        }
    }
    names
}

fn describe_read_field(ontology: &Ontology, node: &str, name: &str) -> Option<Value> {
    let field = ontology
        .get_node(node)?
        .fields
        .iter()
        .find(|field| field.name == name)?;
    Some(json!({
        "node": node,
        "name": field.name,
        "source": field.source,
        "data_type": field.data_type,
        "nullable": field.nullable,
        "enum_values": field.enum_values,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::OntologyArchive;
    use crate::migrations::embedded_sources;

    const NOTE_YAML: &str = "nodes/core/note.yaml";

    fn hash_after_edit(path: &str, from: &str, to: &str) -> BTreeMap<String, String> {
        let mut sources = embedded_sources();
        let content = sources.get_mut(path).expect("source exists");
        assert!(content.contains(from), "{path} must contain {from:?}");
        *content = content.replacen(from, to, 1);
        let ontology = OntologyArchive::from_sources(1, &sources)
            .expect("archive builds")
            .load_ontology()
            .expect("edited ontology loads");
        hash_pipeline_edge_rows(&ontology)
    }

    fn list_changed_pipelines(edited: &BTreeMap<String, String>) -> BTreeSet<String> {
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let fingerprints = |edge_rows: BTreeMap<String, String>| Fingerprints {
            sources: BTreeMap::new(),
            ddl: BTreeMap::new(),
            edge_rows,
            auxiliary_schema: BTreeMap::new(),
        };
        fingerprints(edited.clone())
            .get_edge_row_diff_keys_between(&fingerprints(hash_pipeline_edge_rows(&ontology)))
    }

    fn fingerprints(
        source: &str,
        ddl: &[(&str, &str)],
        edge_rows: &[(&str, &str)],
    ) -> Fingerprints {
        let to_map = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(key, hash)| (key.to_string(), hash.to_string()))
                .collect()
        };
        Fingerprints {
            sources: BTreeMap::from([(NOTE_YAML.to_string(), source.to_string())]),
            ddl: to_map(ddl),
            edge_rows: to_map(edge_rows),
            auxiliary_schema: BTreeMap::new(),
        }
    }

    #[test]
    fn node_storage_change_keeps_every_edge_row_hash() {
        let edited = hash_after_edit(
            NOTE_YAML,
            "{name: idx_note, column: note, type: text, granularity: 1}",
            "{name: idx_note, column: note, type: text, granularity: 2}",
        );
        assert_eq!(list_changed_pipelines(&edited), BTreeSet::new());
    }

    #[test]
    fn edge_mapping_change_changes_only_its_pipeline_hash() {
        let edited = hash_after_edit(NOTE_YAML, "field: author_id", "field: project_id");
        assert_eq!(
            list_changed_pipelines(&edited),
            BTreeSet::from(["Note".to_string()])
        );
    }

    #[test]
    fn tag_source_change_changes_the_pipelines_that_emit_a_tagged_kind() {
        let edited = hash_after_edit(
            "nodes/plan/work_item.yaml",
            "source: state_id",
            "source: state_code",
        );
        let changed = list_changed_pipelines(&edited);
        assert!(
            changed.contains("Note"),
            "HAS_NOTE carries WorkItem state: {changed:?}"
        );
        assert!(
            !changed.contains("User"),
            "User emits no WorkItem kind: {changed:?}"
        );
    }

    #[test]
    fn node_only_change_keeps_edge_rows_unchanged() {
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let base = fingerprints("old", &[("gl_note", "old")], &[("Note", "edges")]);
        let current = fingerprints("new", &[("gl_note", "new")], &[("Note", "edges")]);
        assert_eq!(
            derive_edge_rows(&ontology, &current, &base),
            EdgeRows::Unchanged
        );

        let with_new_pipeline = fingerprints(
            "new",
            &[("gl_note", "new")],
            &[("Note", "edges"), ("AgentSession", "edges")],
        );
        assert_eq!(
            derive_edge_rows(&ontology, &with_new_pipeline, &base),
            EdgeRows::Unchanged
        );
    }

    #[test]
    fn any_edge_row_risk_marks_edge_rows_changed() {
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let base = fingerprints("old", &[("gl_edge", "old")], &[("Note", "edges")]);
        let cases = [
            ("lowering code only", base.clone()),
            (
                "edge table DDL",
                fingerprints("new", &[("gl_edge", "new")], &[("Note", "edges")]),
            ),
            (
                "pipeline edge rows",
                fingerprints("new", &[("gl_edge", "old")], &[("Note", "new")]),
            ),
            (
                "removed pipeline",
                fingerprints("new", &[("gl_edge", "old")], &[]),
            ),
        ];
        for (risk, current) in cases {
            assert_eq!(
                derive_edge_rows(&ontology, &current, &base),
                EdgeRows::Changed,
                "{risk}"
            );
        }

        let base_without_hashes = fingerprints("old", &[("gl_edge", "old")], &[]);
        let current = fingerprints("new", &[("gl_edge", "old")], &[("Note", "edges")]);
        assert_eq!(
            derive_edge_rows(&ontology, &current, &base_without_hashes),
            EdgeRows::Changed,
            "a snapshot from before edge row hashes cannot vouch for the edges"
        );
    }

    #[test]
    fn only_changed_covers_changed() {
        assert!(EdgeRows::Changed.covers(EdgeRows::Unchanged));
        assert!(EdgeRows::Unchanged.covers(EdgeRows::Unchanged));
        assert!(!EdgeRows::Unchanged.covers(EdgeRows::Changed));
        assert_eq!(
            EdgeRows::Unchanged.widened_with(EdgeRows::Changed),
            EdgeRows::Changed
        );
    }
}
