//! Translating validated packages into ontology sources and merging them with
//! the bundled core model.
//!
//! The loader never learns what a package means. It turns each entity into a
//! node source, each relationship into an edge source, registers them under a
//! single `extensions` domain in a copy of the core `schema.yaml`, and hands the
//! merged source set to [`ontology::archive::OntologyArchive::from_sources`]. The
//! resulting [`ontology::Ontology`] carries the core entities and the extension
//! entities in one model, which the indexer and the query compiler already read.

use std::collections::{BTreeMap, BTreeSet};

use ontology::Ontology;
use ontology::archive::OntologyArchive;
use serde_json::{Value, json};

use crate::package::{Package, PropertyType, Relationship};

const EXTENSION_DOMAIN: &str = "extensions";
const SCHEMA_SOURCE: &str = "schema.yaml";
const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("extension load rules failed:\n  - {}", .0.join("\n  - "))]
    Rules(Vec<String>),
    #[error("core schema.yaml is missing from the bundled sources")]
    MissingCoreSchema,
    #[error("core schema.yaml is not valid YAML: {0}")]
    CoreSchema(String),
    #[error("ontology failed to load the merged model: {0}")]
    Archive(#[from] ontology::archive::ArchiveError),
}

/// Translates and merges the packages into one ontology for the given core
/// schema version. Cross-package load rules run first; a violation rejects the
/// whole set with every error, because a partial model would be inconsistent.
///
/// The core entities always stay in the model. Dropping them (the optional E9
/// `ontology.core: false`) is not supported here: the bundled core `settings`
/// (denormalization, `local_db`, statistics, dictionaries, materialized views)
/// reference core nodes and tables, so the loader rejects a model with the core
/// entities removed. Exposing only extension entities belongs in the query and
/// schema layer, not in the model load.
pub fn load_merged_ontology(
    schema_version: u32,
    packages: &[Package],
) -> Result<Ontology, LoaderError> {
    let mut sources = ontology::migrations::embedded_sources();
    merge_packages_into(&mut sources, packages)?;
    let archive = OntologyArchive::from_sources(schema_version, &sources)?;
    Ok(archive.load_ontology()?)
}

/// Translates the packages into node and edge sources and registers them in the
/// `schema.yaml` of `sources`. Used by [`load_merged_ontology`]; exposed for the
/// storage layer, which merges the same way before deriving extension tables.
pub fn merge_packages_into(
    sources: &mut BTreeMap<String, String>,
    packages: &[Package],
) -> Result<(), LoaderError> {
    let known_entities = known_entity_names(sources, packages)?;
    check_cross_package_rules(packages, &known_entities)?;

    let mut schema = parse_schema(sources)?;
    for package in packages {
        register_package(sources, &mut schema, package);
    }
    sources.insert(
        SCHEMA_SOURCE.to_string(),
        orbit_utils::yaml::to_string(&schema)
            .map_err(|e| LoaderError::CoreSchema(e.to_string()))?,
    );
    Ok(())
}

/// The `gl_ext_<package>_<entity>_<digest8>` graph table of an entity. The digest
/// makes the name change when the document changes, so a new version reindexes
/// into a fresh table. The `gl_` prefix satisfies the ontology's `table_prefix`.
pub fn extension_table_name(package_name: &str, entity: &str, digest: &str) -> String {
    let digest8: String = digest.chars().take(8).collect();
    format!(
        "gl_ext_{}_{}_{}",
        package_name.replace('.', "_"),
        to_snake_case(entity),
        digest8
    )
}

fn known_entity_names(
    sources: &BTreeMap<String, String>,
    packages: &[Package],
) -> Result<BTreeSet<String>, LoaderError> {
    let mut names = core_entity_names(sources)?;
    for package in packages {
        for entity in package.document.graph.entities.keys() {
            names.insert(entity.clone());
        }
    }
    Ok(names)
}

fn check_cross_package_rules(
    packages: &[Package],
    known_entities: &BTreeSet<String>,
) -> Result<(), LoaderError> {
    let mut errors = Vec::new();
    let mut owners: BTreeMap<&str, &str> = BTreeMap::new();

    for package in packages {
        for entity in package.document.graph.entities.keys() {
            if let Some(previous) = owners.insert(entity, package.name()) {
                errors.push(format!(
                    "entity {entity} is declared by both {previous} and {}",
                    package.name()
                ));
            }
        }
    }

    for package in packages {
        for (name, relationship) in &package.document.graph.relationships {
            for variant in relationship.parsed_variants() {
                for endpoint in [&variant.source, &variant.target] {
                    if !known_entities.contains(endpoint) {
                        errors.push(format!(
                            "{}: relationship {name} points at unknown entity {endpoint}",
                            package.name()
                        ));
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(LoaderError::Rules(errors))
    }
}

fn register_package(sources: &mut BTreeMap<String, String>, schema: &mut Value, package: &Package) {
    let pkg = package.name();
    let mut nodes = serde_json::Map::new();

    for entity in package.document.graph.entities.keys() {
        let path = format!(
            "nodes/{EXTENSION_DOMAIN}/{}/{}.yaml",
            pkg.replace('.', "_"),
            to_snake_case(entity)
        );
        sources.insert(path.clone(), node_source(package, entity));
        nodes.insert(entity.clone(), Value::String(path));
    }

    schema["domains"][EXTENSION_DOMAIN] = json!({
        "description": format!("Extension entities served by provider package {pkg}."),
        "nodes": nodes,
    });

    for (name, relationship) in &package.document.graph.relationships {
        let path = format!("edges/{EXTENSION_DOMAIN}/{}.yaml", name.to_lowercase());
        sources.insert(path.clone(), edge_source(package, name, relationship));
        schema["edges"][name] = Value::String(path);
    }
}

fn node_source(package: &Package, entity: &str) -> String {
    let document = &package.document;
    let entity_def = &document.graph.entities[entity];
    let storage = &document.storage.entities[entity];
    let source = &document.sources.entities[entity];
    let table = extension_table_name(package.name(), entity, &package.digest);

    let mut properties = serde_json::Map::new();
    for (name, property) in &entity_def.properties {
        let mut field = json!({
            "type": property.property_type.ontology_type(),
            "source": name,
            "nullable": name != crate::package::ID_PROPERTY,
        });
        if name == crate::package::ID_PROPERTY {
            field["mutable"] = json!(false);
            field["traversal_path_lookup"] =
                json!({ "kind": "id", "source_table": table, "key_column": "id" });
        }
        properties.insert(name.clone(), field);
    }
    properties.insert(
        TRAVERSAL_PATH_COLUMN.to_string(),
        json!({ "type": "string", "source": TRAVERSAL_PATH_COLUMN, "nullable": false, "filterable": false, "hidden": true }),
    );

    let mut columns: Vec<Value> = storage
        .columns
        .iter()
        .map(|(name, ch_type)| json!({ "name": name, "type": ch_type }))
        .collect();
    columns.push(json!({ "name": TRAVERSAL_PATH_COLUMN, "type": "String", "default": "'0/'" }));

    let node = json!({
        "node_type": entity,
        "domain": EXTENSION_DOMAIN,
        "description": format!("Extension entity {entity} from package {}.", package.name()),
        "label": entity_def.label,
        "destination_table": table,
        "redaction": {
            "resource_type": format!("{}_{}", package.name().replace('.', "_"), to_snake_case(entity)),
            "id_column": "id",
            "ability": "read_extension_entity",
        },
        "properties": properties,
        "indexer": { "reindex": source.tables },
        "pipelines": [{
            "name": entity,
            "extract": {
                "type": "clickhouse",
                "tables": source.tables,
                "order_by": source.order_by,
                "query": "generated",
            },
            "transform": { "type": "datafusion" },
        }],
        "storage": {
            "primary_key": storage.sort_key,
            "columns": columns,
        },
    });

    orbit_utils::yaml::to_string(&node).expect("generated node yaml serializes")
}

fn edge_source(package: &Package, name: &str, relationship: &Relationship) -> String {
    let variants: Vec<Value> = relationship
        .parsed_variants()
        .into_iter()
        .map(|variant| match &relationship.fk {
            Some(fk) => json!({
                "from_node": { "type": variant.source, "id": "id" },
                "to_node": { "type": variant.target, "id": "id" },
                "fk_column": fk.column,
                "scope": "prune_to_source",
            }),
            None => json!({
                "from_node": { "type": variant.source, "id": "id" },
                "to_node": { "type": variant.target, "id": "id" },
                "scope": "prune_to_source",
            }),
        })
        .collect();

    let edge = match &relationship.table {
        Some(table) => json!({
            "description": format!("Extension relationship {name} from package {}.", package.name()),
            "variants": variants,
            "indexer": { "reindex": [table] },
            "pipelines": [{
                "name": format!("{name}_{table}"),
                "extract": {
                    "type": "clickhouse",
                    "tables": [table],
                    "fields": ["source_id", "target_id"],
                    "order_by": ["traversal_path", "source_id", "target_id"],
                    "query": "generated",
                },
                "transform": {
                    "type": "datafusion",
                    "edges": relationship.parsed_variants().into_iter().map(|variant| json!({
                        "from": { "field": "source_id", "kind": variant.source },
                        "to": { "field": "target_id", "kind": variant.target },
                        "label": name,
                    })).collect::<Vec<_>>(),
                },
            }],
        }),
        None => json!({
            "description": format!("Extension relationship {name} from package {}.", package.name()),
            "variants": variants,
        }),
    };

    orbit_utils::yaml::to_string(&edge).expect("generated edge yaml serializes")
}

fn parse_schema(sources: &BTreeMap<String, String>) -> Result<Value, LoaderError> {
    let raw = sources
        .get(SCHEMA_SOURCE)
        .ok_or(LoaderError::MissingCoreSchema)?;
    orbit_utils::yaml::from_str(raw).map_err(|e| LoaderError::CoreSchema(e.to_string()))
}

fn core_entity_names(sources: &BTreeMap<String, String>) -> Result<BTreeSet<String>, LoaderError> {
    let schema = parse_schema(sources)?;
    let mut names = BTreeSet::new();
    if let Some(domains) = schema.get("domains").and_then(Value::as_object) {
        for domain in domains.values() {
            for key in ["nodes", "derived"] {
                if let Some(entities) = domain.get(key).and_then(Value::as_object) {
                    names.extend(entities.keys().cloned());
                }
            }
        }
    }
    Ok(names)
}

impl PropertyType {
    fn ontology_type(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::String => "string",
            Self::Bool => "boolean",
            Self::Float => "float",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
        }
    }
}

fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (index, ch) in name.char_indices() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::short_digest;

    const WIDGETS: &str = include_str!("../tests/fixtures/example.widgets.yaml");

    fn widgets_package() -> Package {
        Package::parse(WIDGETS, short_digest(WIDGETS)).expect("fixture parses")
    }

    fn schema_version() -> u32 {
        orbit_versions::VERSIONS.schema
    }

    #[test]
    fn table_name_carries_the_digest() {
        let name = extension_table_name("example.widgets", "Widget", "abcdef1234567890");
        assert_eq!(name, "gl_ext_example_widgets_widget_abcdef12");
    }

    #[test]
    fn merged_model_holds_core_and_extension_entities() {
        let ontology = load_merged_ontology(schema_version(), &[widgets_package()])
            .expect("merged model loads");

        let node_names: BTreeSet<&str> = ontology.nodes().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains("Widget"), "extension entity present");
        assert!(node_names.contains("Gadget"), "extension entity present");
        assert!(node_names.contains("Project"), "core entity still present");
    }

    #[test]
    fn extension_node_targets_the_digest_table() {
        let package = widgets_package();
        let ontology = load_merged_ontology(schema_version(), std::slice::from_ref(&package))
            .expect("merged model loads");
        let widget = ontology
            .nodes()
            .find(|n| n.name == "Widget")
            .expect("Widget node");
        assert_eq!(
            widget.destination_table,
            extension_table_name("example.widgets", "Widget", &package.digest)
        );
    }

    #[test]
    fn rejects_an_entity_declared_twice() {
        let other = Package::parse(
            &WIDGETS.replace("example.widgets", "example.more"),
            "beef".into(),
        )
        .expect("second package parses");
        let err = load_merged_ontology(schema_version(), &[widgets_package(), other]).unwrap_err();
        assert!(matches!(err, LoaderError::Rules(_)), "got {err:?}");
    }
}
