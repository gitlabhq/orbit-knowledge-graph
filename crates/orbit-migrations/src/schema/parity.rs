use std::collections::BTreeMap;

use ontology::Ontology;
use ontology::archive::OntologyArchive;
use query_data_model::implementations::clickhouse::schema::{self as physical, Schema};

use super::{DictionaryCredentials, GraphSchema};

const PREFIX: &str = "v1_";

fn ontologies() -> Vec<(String, Ontology)> {
    let overlays =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/seeds/overlays");
    let mut inputs = vec![
        ("embedded".to_string(), Ontology::load_embedded().unwrap()),
        (
            "embedded prefixed".to_string(),
            Ontology::load_embedded()
                .unwrap()
                .with_schema_version_prefix("v118_"),
        ),
    ];
    let mut entries: Vec<_> = std::fs::read_dir(&overlays)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        inputs.push((
            format!("overlay {}", path.display()),
            Ontology::load_embedded_with_overlay(&path).unwrap(),
        ));
    }
    for version in OntologyArchive::bundled_versions().unwrap() {
        let archive = OntologyArchive::bundled(version).unwrap().unwrap();
        inputs.push((
            format!("archive v{version}"),
            archive.load_ontology().unwrap(),
        ));
    }
    inputs
}

fn credentials() -> DictionaryCredentials {
    DictionaryCredentials {
        database: "graph".into(),
        user: "reader".into(),
        password: Some("secret".into()),
    }
}

fn physical_credentials() -> physical::DictionaryCredentials {
    physical::DictionaryCredentials {
        database: "graph".into(),
        user: "reader".into(),
        password: Some("secret".into()),
    }
}

fn old_statements(ontology: &Ontology, replicated: bool) -> BTreeMap<&'static str, Vec<String>> {
    let schema = GraphSchema::from_ontology_replicated(ontology, replicated).unwrap();
    let known: Vec<String> = schema.storage.table_names().map(String::from).collect();
    let versioned: Vec<String> = schema
        .table_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    BTreeMap::from([
        (
            "tables",
            schema
                .tables
                .iter()
                .map(|table| table.to_create_sql(PREFIX))
                .collect(),
        ),
        (
            "views",
            schema
                .views
                .iter()
                .map(|view| {
                    let view = view.clone();
                    let versioned_flag = view.versioned;
                    let sql = view
                        .with_schema_version_prefix(PREFIX, &versioned)
                        .to_create_sql()
                        .unwrap();
                    format!("{versioned_flag} {sql}")
                })
                .collect(),
        ),
        (
            "dictionaries",
            schema
                .dictionaries
                .iter()
                .map(|dictionary| {
                    dictionary
                        .clone()
                        .with_schema_version_prefix(PREFIX)
                        .to_create_sql(&credentials())
                })
                .collect(),
        ),
        (
            "unversioned",
            schema
                .unversioned_definitions
                .iter()
                .map(|definition| {
                    format!(
                        "{} {} {}",
                        definition.entity_type, definition.name, definition.create_statement
                    )
                })
                .collect(),
        ),
        (
            "refreshable",
            schema
                .refreshable_views
                .iter()
                .map(|view| {
                    format!(
                        "{} {} {} {} {}",
                        view.name, view.versioned, view.append_to, view.refresh, view.select_query
                    )
                })
                .collect(),
        ),
        (
            "graph tables",
            sorted(schema.storage.graph_tables().iter().map(|table| {
                format!(
                    "{} {} {}",
                    table.name, table.global, table.has_traversal_path
                )
            })),
        ),
        (
            "dependencies",
            vec![format!("{:?}", schema.storage.dependencies())],
        ),
        (
            "deployed",
            schema
                .storage
                .deployed_tables()
                .map(|table| table.name.clone())
                .collect(),
        ),
        ("names", known),
        (
            "join sources",
            distinct(
                schema
                    .storage
                    .joins()
                    .iter()
                    .flat_map(|join| join.sources.iter().map(|source| source.table.clone())),
            ),
        ),
    ])
}

fn new_statements(schema: &Schema, replicated: bool) -> BTreeMap<&'static str, Vec<String>> {
    let engine = |engine: physical::Engine| {
        if replicated {
            engine.replicated()
        } else {
            engine
        }
    };
    let known: Vec<String> = schema.table_names().map(String::from).collect();
    let versioned: Vec<String> = schema
        .versioned_tables()
        .map(|table| table.name.clone())
        .collect();
    let view = |view: &physical::MaterializedView, prefix: &str, known: &[String]| {
        let mut view = view.clone().with_schema_version_prefix(prefix, known);
        view.engine = view.engine.map(engine);
        view.to_create_sql()
    };
    let table = |table: &physical::Table, prefix: &str| {
        let mut table = table.clone();
        table.engine = engine(table.engine);
        table.to_create_sql(prefix)
    };
    let unversioned = schema
        .auxiliary_tables()
        .iter()
        .filter(|auxiliary| !auxiliary.versioned)
        .map(|auxiliary| {
            format!(
                "TABLE {} {}",
                auxiliary.table.name,
                table(&auxiliary.table, "")
            )
        })
        .chain(
            schema
                .views()
                .iter()
                .filter(|definition| !definition.versioned)
                .map(|definition| {
                    format!(
                        "MATERIALIZED VIEW {} {}",
                        definition.name,
                        view(definition, "", &known)
                    )
                }),
        )
        .collect();
    BTreeMap::from([
        (
            "tables",
            schema
                .versioned_tables()
                .map(|definition| table(definition, PREFIX))
                .collect(),
        ),
        (
            "views",
            schema
                .views()
                .iter()
                .map(|definition| {
                    format!(
                        "{} {}",
                        definition.versioned,
                        view(definition, PREFIX, &versioned)
                    )
                })
                .collect(),
        ),
        (
            "dictionaries",
            schema
                .dictionaries()
                .iter()
                .map(|dictionary| {
                    dictionary
                        .clone()
                        .with_schema_version_prefix(PREFIX)
                        .to_create_sql(&physical_credentials())
                })
                .collect(),
        ),
        ("unversioned", unversioned),
        (
            "refreshable",
            schema
                .refreshable_views()
                .iter()
                .map(|view| {
                    format!(
                        "{} {} {} {} {}",
                        view.name, view.versioned, view.append_to, view.refresh, view.select_query
                    )
                })
                .collect(),
        ),
        (
            "graph tables",
            sorted(
                schema.graph_tables().iter().map(|table| {
                    format!("{} {} {}", table.name, table.global, table.has_path_column)
                }),
            ),
        ),
        ("dependencies", vec![format!("{:?}", schema.dependencies())]),
        (
            "deployed",
            schema
                .tables()
                .iter()
                .map(|table| table.name.clone())
                .collect(),
        ),
        ("names", known),
        (
            "join sources",
            distinct(schema.join_sources().map(String::from)),
        ),
    ])
}

fn distinct(items: impl Iterator<Item = String>) -> Vec<String> {
    let mut items = sorted(items);
    items.dedup();
    items
}

fn sorted(items: impl Iterator<Item = String>) -> Vec<String> {
    let mut items: Vec<String> = items.collect();
    items.sort();
    items
}

#[test]
fn backend_schema_renders_the_old_layout_ddl_byte_for_byte() {
    let mut compared = 0;
    for (label, ontology) in ontologies() {
        let schema = Schema::derive(&ontology).unwrap_or_else(|error| panic!("{label}: {error}"));
        let old_layout = query_data_model::Relational::<
            query_data_model::implementations::clickhouse::layout::ClickHouse,
        >::derive(&ontology)
        .unwrap();
        let writers_old = sorted(
            old_layout
                .tables()
                .iter()
                .map(|table| format!("{} {:?}", table.name, old_layout.writers(&table.name))),
        );
        let writers_new = sorted(
            old_layout
                .tables()
                .iter()
                .map(|table| format!("{} {:?}", table.name, schema.writers(&table.name))),
        );
        assert_eq!(writers_new, writers_old, "{label}: writers");
        for replicated in [false, true] {
            let old = old_statements(&ontology, replicated);
            let new = new_statements(&schema, replicated);
            for (category, expected) in &old {
                let actual = &new[category];
                for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
                    assert_eq!(
                        actual, expected,
                        "{label} replicated={replicated}: {category}[{index}]"
                    );
                }
                assert_eq!(
                    actual.len(),
                    expected.len(),
                    "{label} replicated={replicated}: {category} count"
                );
                compared += expected.len();
            }
        }
    }
    assert!(compared > 0);
}
