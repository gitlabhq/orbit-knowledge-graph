use std::sync::Arc;

use ontology::TraversalPathKind;
use query_data_model::catalog::{RowFilter, Scope};
use query_data_model::from_ontology::{self, from_ontology, packages};
use query_data_model::implementations::clickhouse::{ClickHouse, versioned};
use query_data_model::spec::Fragment;
use query_data_model::{ClickHouseDataModel, Model, OrbitQueryModel, Packages};

fn fragments(packages: &Packages) -> Vec<Fragment> {
    packages
        .sources()
        .iter()
        .map(|(path, yaml)| Fragment::parse(yaml).unwrap_or_else(|error| panic!("{path}: {error}")))
        .collect()
}

fn model() -> Arc<Model> {
    Packages::embedded().load().unwrap()
}

#[test]
fn checked_in_packages_match_the_ontology() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let expected = from_ontology(&ontology).unwrap();
    let checked_in = fragments(&Packages::embedded());
    assert_eq!(checked_in.len(), expected.len());
    for (checked_in, expected) in checked_in.iter().zip(&expected) {
        assert_eq!(
            checked_in,
            expected,
            "config/model/{} drifted from config/ontology",
            expected.package()
        );
    }
}

#[test]
fn rendered_packages_parse_back_to_their_fragments() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    assert_eq!(
        fragments(&packages(&ontology).unwrap()),
        from_ontology(&ontology).unwrap()
    );
}

#[test]
fn versioned_packages_match_the_prefixed_ontology() {
    const PREFIX: &str = "v1_";
    let overlays = [
        "denorm_approved",
        "denorm_approved_project",
        "reordered_edges",
        "variant_routes",
    ];
    let ontologies =
        std::iter::once(ontology::Ontology::load_embedded().unwrap()).chain(overlays.map(|name| {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config/seeds/overlays")
                .join(name);
            ontology::Ontology::load_embedded_with_overlay(&path).unwrap()
        }));
    for ontology in ontologies {
        let fragments = versioned(from_ontology(&ontology).unwrap(), PREFIX).unwrap();
        let model = Arc::new(Model::load(&fragments).unwrap());
        let actual = ClickHouseDataModel::from_model(&model).unwrap();
        let expected =
            from_ontology::clickhouse(&ontology.with_schema_version_prefix(PREFIX)).unwrap();
        assert_eq!(routing_facts(&actual), routing_facts(&expected));
    }
}

fn routing_facts(model: &ClickHouseDataModel) -> Vec<String> {
    let graph = model.graph();
    let mut tables = vec![model.default_edge_table().to_string()];
    let mut facts = Vec::new();
    for entity in graph.entities() {
        let table = model.table_of(entity.id).map(str::to_string);
        tables.extend(table.clone());
        facts.push(format!("{} table {table:?}", entity.name));
        for kind in [TraversalPathKind::Id, TraversalPathKind::FullPath] {
            let lookup = model.traversal_path_lookup(&entity.name, kind);
            facts.push(format!("{} {kind:?} lookup {lookup:?}", entity.name));
        }
    }
    for relationship in graph.relationships() {
        let routed = model.relationship_tables(std::slice::from_ref(&relationship.name));
        tables.extend(routed.clone());
        facts.push(format!("{} tables {routed:?}", relationship.name));
        for variant in &relationship.variants {
            let variant = graph.variant(*variant);
            let (source, target) = (
                &graph.entity(variant.source).name,
                &graph.entity(variant.target).name,
            );
            let route = model
                .variant_route(&relationship.name, source, target)
                .map(|route| &route.table);
            facts.push(format!(
                "{} {source}->{target} {route:?}",
                relationship.name
            ));
        }
    }
    for join in model.materialized_joins() {
        facts.push(format!("join {} {:?}", join.table, join.sources));
    }
    for table in tables {
        facts.push(format!(
            "{table} copies {:?}",
            model.equivalent_layouts(&table)
        ));
        let columns: std::collections::BTreeSet<_> =
            model.table_columns(&table).into_iter().flatten().collect();
        facts.push(format!("{table} columns {columns:?}"));
    }
    facts
}

#[test]
fn graph_matches_ontology_fields() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let model = model();
    let graph = model.graph();
    for property in graph.properties() {
        let entity = &graph.entity(property.entity).name;
        let name = format!("{entity}.{}", property.name);
        let node = ontology.get_node(entity).unwrap();
        let Some(field) = node.fields.iter().find(|field| field.name == property.name) else {
            assert_eq!(property.name, "id", "{name}");
            continue;
        };
        assert_eq!(property.data_type, field.data_type, "{name}");
        assert_eq!(property.nullable, field.nullable, "{name}");
        assert_eq!(property.mutable, field.mutable, "{name}");
        assert_eq!(property.unique, field.unique, "{name}");
        assert_eq!(property.selectivity, field.selectivity, "{name}");
        assert_eq!(property.enum_values, field.enum_values, "{name}");
        assert_eq!(property.enum_type, field.enum_type, "{name}");
    }
}

#[test]
fn graph_store_entities_filter_on_their_own_path_column() {
    let model = model();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    for entity in model.graph().entities() {
        let guard = store.guard(entity.id).unwrap();
        let expected = match model.access().entity(entity.id).unwrap().scope {
            Scope::Global => RowFilter::None,
            Scope::Namespace => RowFilter::Path {
                column: "traversal_path".into(),
            },
        };
        assert_eq!(guard.filter, expected, "{}", entity.name);
    }
}
