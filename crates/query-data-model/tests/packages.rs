use std::sync::Arc;

use query_data_model::catalog::{RowFilter, Scope};
use query_data_model::from_ontology::{from_ontology, packages};
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::spec::Fragment;
use query_data_model::{Model, Packages};

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
