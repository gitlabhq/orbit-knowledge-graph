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

use crate::live::{LIVE_SERVICE, LiveCatalog, LiveField};
use crate::package::{Package, PropertyType, RelationshipVariant};

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
    check_live_rules(packages)?;

    let live_fields = LiveCatalog::from_packages(packages).fields_by_entity();
    let mut schema = parse_schema(sources)?;
    register_nodes(sources, &mut schema, packages, &live_fields);
    register_relationships(sources, &mut schema, packages);
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

    let mut triples: BTreeMap<(&str, &str, &str), &str> = BTreeMap::new();
    for package in packages {
        for (name, relationship) in &package.document.graph.relationships {
            for variant in &relationship.variants {
                for endpoint in [&variant.from, &variant.to] {
                    if !known_entities.contains(endpoint) {
                        errors.push(format!(
                            "{}: relationship {name} points at unknown entity {endpoint}",
                            package.name()
                        ));
                    }
                }
                let triple = (name.as_str(), variant.from.as_str(), variant.to.as_str());
                if let Some(previous) = triples.insert(triple, package.name()) {
                    errors.push(format!(
                        "relationship {name} variant {} -> {} is declared by both {previous} and {}",
                        variant.from,
                        variant.to,
                        package.name()
                    ));
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

/// Live rules that need the whole package set: the target entity exists, the
/// live properties do not collide with stored ones or with each other across
/// packages, and every key is a stored property of the entity. A live block may
/// target an entity another loaded package declares.
fn check_live_rules(packages: &[Package]) -> Result<(), LoaderError> {
    let mut stored: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for package in packages {
        for (entity, def) in &package.document.graph.entities {
            stored
                .entry(entity)
                .or_default()
                .extend(def.properties.keys().map(String::as_str));
        }
    }

    let mut errors = Vec::new();
    let mut live_owner: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for package in packages {
        let Some(live) = &package.document.live else {
            continue;
        };
        for (entity, live_entity) in &live.entities {
            let Some(stored_props) = stored.get(entity.as_str()) else {
                errors.push(format!(
                    "{}: live block targets entity {entity}, which no loaded package declares",
                    package.name()
                ));
                continue;
            };
            for property in live_entity.properties.keys() {
                if stored_props.contains(property.as_str()) {
                    errors.push(format!(
                        "{}: live property {entity}.{property} collides with a stored property",
                        package.name()
                    ));
                }
                if let Some(previous) =
                    live_owner.insert((entity.as_str(), property.as_str()), package.name())
                {
                    errors.push(format!(
                        "live property {entity}.{property} is declared by both {previous} and {}",
                        package.name()
                    ));
                }
            }
            for key in &live_entity.operation.keys {
                if !stored_props.contains(key.as_str()) {
                    errors.push(format!(
                        "{}: live operation {} key `{key}` is not a stored property of {entity}",
                        package.name(),
                        live_entity.operation.name
                    ));
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

/// Registers every package's entities as nodes under one `extensions` domain.
/// Entity names are unique across packages (a cross-package rule), so their node
/// files and domain entries never collide.
fn register_nodes(
    sources: &mut BTreeMap<String, String>,
    schema: &mut Value,
    packages: &[Package],
    live_fields: &BTreeMap<String, Vec<LiveField>>,
) {
    let no_fields = Vec::new();
    let mut nodes = serde_json::Map::new();
    for package in packages {
        for entity in package.document.graph.entities.keys() {
            let path = format!(
                "nodes/{EXTENSION_DOMAIN}/{}/{}.yaml",
                package.name().replace('.', "_"),
                to_snake_case(entity)
            );
            let fields = live_fields.get(entity).unwrap_or(&no_fields);
            sources.insert(path.clone(), node_source(package, entity, fields));
            nodes.insert(entity.clone(), Value::String(path));
        }
    }
    schema["domains"][EXTENSION_DOMAIN] = json!({
        "description": "Extension entities served by provider packages.",
        "nodes": nodes,
    });
}

/// Registers one edge per relationship name, merging the variants every package
/// contributes to that name into a single edge definition.
fn register_relationships(
    sources: &mut BTreeMap<String, String>,
    schema: &mut Value,
    packages: &[Package],
) {
    let mut by_name: BTreeMap<&str, Vec<&RelationshipVariant>> = BTreeMap::new();
    for package in packages {
        for (name, relationship) in &package.document.graph.relationships {
            by_name
                .entry(name)
                .or_default()
                .extend(&relationship.variants);
        }
    }

    for (name, variants) in by_name {
        let path = format!("edges/{EXTENSION_DOMAIN}/{}.yaml", name.to_lowercase());
        sources.insert(path.clone(), edge_source(name, &variants));
        schema["edges"][name] = Value::String(path);
    }
}

fn node_source(package: &Package, entity: &str, live_fields: &[LiveField]) -> String {
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
    for field in live_fields {
        properties.insert(field.property.clone(), live_field_source(field));
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

/// A live property, as an ontology virtual field. `filterable: false` makes a
/// `WHERE` on it a compile error, and the virtual source makes an `ORDER BY` on
/// it one too; the enrich stage resolves it from the operation named by `lookup`.
/// `depends_on` lists the key columns, so the compiler keeps selecting them even
/// when the live property itself is dropped from the SQL.
fn live_field_source(field: &LiveField) -> Value {
    json!({
        "type": field.property_type.ontology_type(),
        "nullable": true,
        "filterable": false,
        "virtual": {
            "service": LIVE_SERVICE,
            "lookup": field.operation_id,
            "depends_on": field.keys,
        },
    })
}

/// One ontology edge for a relationship name. Each fk variant becomes an
/// FK-derived edge variant on the source table's column; each table variant
/// becomes a variant plus a datalake pipeline reading its edge table. A name may
/// mix both kinds and span several packages' entities.
fn edge_source(name: &str, variants: &[&RelationshipVariant]) -> String {
    let variant_defs: Vec<Value> = variants
        .iter()
        .map(|variant| {
            let mut def = json!({
                "from_node": { "type": variant.from, "id": "id" },
                "to_node": { "type": variant.to, "id": "id" },
                "scope": "prune_to_source",
            });
            if let Some(fk) = &variant.fk {
                def["fk_column"] = json!(fk.column);
            }
            def
        })
        .collect();

    let table_variants: Vec<&&RelationshipVariant> = variants
        .iter()
        .filter(|variant| variant.table.is_some())
        .collect();

    let mut edge = json!({
        "description": format!("Extension relationship {name}."),
        "variants": variant_defs,
    });

    if !table_variants.is_empty() {
        let tables: Vec<&String> = table_variants
            .iter()
            .filter_map(|variant| variant.table.as_ref())
            .collect();
        let pipelines: Vec<Value> = table_variants
            .iter()
            .map(|variant| {
                let table = variant.table.as_ref().expect("table variant has a table");
                json!({
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
                        "edges": [{
                            "from": { "field": "source_id", "kind": variant.from },
                            "to": { "field": "target_id", "kind": variant.to },
                            "label": name,
                        }],
                    },
                })
            })
            .collect();
        edge["indexer"] = json!({ "reindex": tables });
        edge["pipelines"] = json!(pipelines);
    }

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
    const GADGETS: &str = include_str!("../tests/fixtures/example.gadgets.yaml");

    fn widgets_package() -> Package {
        Package::parse(WIDGETS, short_digest(WIDGETS)).expect("fixture parses")
    }

    fn gadgets_package() -> Package {
        Package::parse(GADGETS, short_digest(GADGETS)).expect("fixture parses")
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

    #[test]
    fn two_packages_add_variants_to_one_relationship() {
        let ontology =
            load_merged_ontology(schema_version(), &[widgets_package(), gadgets_package()])
                .expect("both packages merge and load");
        let node_names: BTreeSet<&str> = ontology.nodes().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains("Widget"), "widgets entity present");
        assert!(
            node_names.contains("Sprocket"),
            "gadgets entity present: the two packages merge into one model"
        );
    }

    fn widget_model() -> query_data_model::ClickHouseDataModel {
        let ontology = load_merged_ontology(schema_version(), &[widgets_package()])
            .expect("merged model loads");
        query_data_model::ClickHouseDataModel::derive(std::sync::Arc::new(ontology))
            .expect("data model derives")
    }

    #[test]
    fn live_properties_become_virtual_fields_keyed_by_their_operation() {
        use query_data_model::OrbitQueryModel;
        let model = widget_model();
        let widget = model.entity("Widget").expect("Widget entity").id;

        let source = model
            .virtual_source_for_entity_id(widget, "colour")
            .expect("colour is a live virtual field");
        assert_eq!(source.service, LIVE_SERVICE);
        assert_eq!(source.lookup, "example.widgets|widgets.get");
        assert_eq!(source.depends_on, vec!["name", "status"]);
        assert!(
            model.property_is_virtual(widget, "weight"),
            "weight is live too"
        );
    }

    #[test]
    fn a_live_property_is_not_filterable_so_a_where_is_a_compile_error() {
        use query_data_model::OrbitQueryModel;
        let model = widget_model();
        assert!(
            !model.property_is_filterable("Widget", "colour"),
            "a WHERE on a live property must be rejected at compile time"
        );
        assert!(
            model.property_is_filterable("Widget", "name"),
            "a stored property stays filterable"
        );
    }

    #[test]
    fn live_catalog_lists_every_operation_with_its_limits() {
        let catalog = crate::live::LiveCatalog::from_packages(&[widgets_package()]);
        let widgets_get = catalog
            .get("example.widgets|widgets.get")
            .expect("widgets.get present");
        assert_eq!(widgets_get.keys, vec!["name", "status"]);
        assert_eq!(widgets_get.max_keys, 200);

        let gadgets_get = catalog
            .get("example.widgets|gadgets.get")
            .expect("gadgets.get present");
        assert_eq!(gadgets_get.entity, "Gadget");
        assert_eq!(gadgets_get.max_keys, 50);
        assert_eq!(gadgets_get.timeout_ms, 1000);
    }

    #[test]
    fn rejects_a_live_property_that_collides_with_a_stored_one() {
        let yaml = WIDGETS.replace(
            "        colour: { type: string }",
            "        name: { type: string }",
        );
        let package = Package::parse(&yaml, "d".into()).expect("parses");
        let err = load_merged_ontology(schema_version(), &[package]).unwrap_err();
        match err {
            LoaderError::Rules(errors) => assert!(
                errors
                    .iter()
                    .any(|e| e.contains("collides with a stored property")),
                "got {errors:?}"
            ),
            other => panic!("expected rules error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_live_key_that_is_not_a_stored_property() {
        let yaml = WIDGETS.replace(
            "        keys: [name, status]",
            "        keys: [name, missing]",
        );
        let package = Package::parse(&yaml, "d".into()).expect("parses");
        let err = load_merged_ontology(schema_version(), &[package]).unwrap_err();
        match err {
            LoaderError::Rules(errors) => assert!(
                errors.iter().any(|e| e.contains("key `missing`")),
                "got {errors:?}"
            ),
            other => panic!("expected rules error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_live_block_for_an_unknown_entity() {
        const PKG: &str = r#"apiVersion: orbit.gitlab.com/extensions/v1alpha1
kind: Package
metadata: { name: example.lonely, version: 0.1.0 }
graph:
  entities:
    Lonely:
      label: name
      properties:
        id: { type: int64 }
        name: { type: string }
authorization:
  entities:
    Lonely: { scope: namespace }
storage:
  store: graph
  entities:
    Lonely: { columns: { id: Int64, name: String }, sort_key: [traversal_path, id] }
sources:
  source: data_lake
  entities:
    Lonely: { tables: [ex_lonely], order_by: [traversal_path, id] }
live:
  entities:
    Ghost:
      properties:
        aura: { type: string }
      operation: { name: ghost.get, keys: [id] }
"#;
        let package = Package::parse(PKG, "d".into()).expect("parses");
        let err = load_merged_ontology(schema_version(), &[package]).unwrap_err();
        match err {
            LoaderError::Rules(errors) => assert!(
                errors
                    .iter()
                    .any(|e| e.contains("no loaded package declares")),
                "got {errors:?}"
            ),
            other => panic!("expected rules error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_the_same_relationship_variant_triple_twice() {
        // example.extra adds its own entity but repeats widgets' LINKS Widget -> Gadget triple.
        const EXTRA: &str = r#"apiVersion: orbit.gitlab.com/extensions/v1alpha1
kind: Package
metadata: { name: example.extra, version: 0.1.0 }
graph:
  entities:
    Extra:
      label: code
      properties:
        id: { type: int64 }
        code: { type: string }
  relationships:
    LINKS:
      variants:
        - { from: Widget, to: Gadget, table: ex_extra_links }
authorization:
  entities:
    Extra: { scope: namespace }
storage:
  store: graph
  entities:
    Extra: { columns: { id: Int64, code: String }, sort_key: [traversal_path, id] }
sources:
  source: data_lake
  entities:
    Extra: { tables: [ex_extras], order_by: [traversal_path, id] }
"#;
        let extra = Package::parse(EXTRA, "beef".into()).expect("parses");
        let err = load_merged_ontology(schema_version(), &[widgets_package(), extra]).unwrap_err();
        match err {
            LoaderError::Rules(errors) => {
                assert!(
                    errors.iter().any(|e| e.contains("Widget -> Gadget")),
                    "got {errors:?}"
                );
            }
            other => panic!("expected a rules error, got {other:?}"),
        }
    }
}
