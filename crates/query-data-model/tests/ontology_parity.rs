use std::sync::Arc;

use query_data_model::catalog::{RowFilter, Scope};
use query_data_model::from_ontology::from_ontology;
use query_data_model::implementations::clickhouse::backend::IndexKind;
use query_data_model::implementations::clickhouse::layout::ClickHouse;
use query_data_model::implementations::duckdb::layout::DuckDb;
use query_data_model::spec::Fragment;
use query_data_model::{
    ClickHouseDataModel, DuckDbDataModel, Model, OrbitQueryModel, PropertyId, PropertyRealization,
    RelationalMapping,
};

fn models() -> (Model, ClickHouseDataModel, DuckDbDataModel) {
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let model = Model::load(&from_ontology(&ontology).unwrap()).unwrap();
    let remote = ClickHouseDataModel::derive(Arc::clone(&ontology)).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();
    (model, remote, local)
}

#[test]
fn converted_spec_round_trips_through_yaml() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    for fragment in from_ontology(&ontology).unwrap() {
        let yaml = fragment.to_yaml().unwrap();
        assert_eq!(
            Fragment::parse(&yaml).unwrap(),
            fragment,
            "{}",
            fragment.package()
        );
    }
}

#[test]
fn graph_matches_ontology_fields() {
    let (model, remote, _) = models();
    let graph = model.graph();
    let ontology = ontology::Ontology::load_embedded().unwrap();
    for property in graph.properties() {
        let entity = &graph.entity(property.entity).name;
        let name = format!("{entity}.{}", property.name);
        assert_eq!(
            property.mutable,
            remote.property_mutable(property.id),
            "{name}"
        );
        assert_eq!(
            property.unique,
            remote.property_unique(property.id),
            "{name}"
        );
        assert_eq!(
            !property.references.is_empty(),
            remote.backend().property_is_foreign_key(property.id),
            "{name}"
        );
        let field = ontology
            .get_node(entity)
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name == property.name);
        if let Some(field) = field {
            assert_eq!(property.enum_values, field.enum_values, "{name}");
            assert_eq!(property.enum_type, field.enum_type, "{name}");
        }
    }
}

#[test]
fn clickhouse_catalog_matches_clickhouse_bindings() {
    let (model, remote, _) = models();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let facts = catalog.backend("graph").unwrap();
    for entity in graph.entities() {
        let source = store.source(entity.id).unwrap();
        let layout = remote.backend().entity(entity.id).unwrap();
        assert_eq!(source.table, layout.table, "{}", entity.name);
        assert_eq!(
            facts.high_fan_out(entity.id),
            layout.high_fan_out,
            "{}",
            entity.name
        );
        let scoped = layout.has_traversal_path && !layout.global;
        assert_eq!(source.path_column.is_some(), scoped, "{}", entity.name);
        let sort_key = facts.relational.sort_key(entity.id);
        assert_eq!(
            sort_key,
            remote.table_sort_key(&source.table),
            "{}",
            entity.name
        );
        for property in &entity.properties {
            let name = format!("{}.{}", entity.name, graph.property(*property).name);
            assert_eq!(
                store.column(*property),
                remote.property_column(*property),
                "{name}"
            );
            if let Some(column) = facts.column(*property) {
                let text = column.indexes.contains(&IndexKind::Text);
                assert_eq!(text, remote.has_text_index(*property), "{name}");
                let selectivity = remote.property_selectivity(*property);
                assert_eq!(column.selectivity, selectivity, "{name}");
            }
        }
    }
    for variant in graph.variants() {
        let route = catalog
            .routes(variant.id)
            .find(|route| route.edge.is_some_and(|edge| edge.store == "graph"))
            .map(|route| {
                let key = route.foreign_key.map(|key| key.key);
                (route.edge.map(|edge| edge.table), key)
            });
        let expected = remote
            .backend()
            .variant_route(variant.id)
            .map(|route| (Some(route.table.as_str()), route.foreign_key));
        assert_eq!(route, expected, "{variant:?}");
    }
}

#[test]
fn lookups_match_virtual_properties() {
    let (model, remote, _) = models();
    let names = |ids: &[PropertyId]| -> Vec<String> {
        ids.iter()
            .map(|id| model.graph().property(*id).name.clone())
            .collect()
    };
    for property in model.graph().properties() {
        let lookup = model.lookup(property.id);
        let expected = remote.virtual_source_for_entity_id(property.entity, &property.name);
        assert_eq!(lookup.is_some(), expected.is_some(), "{}", property.name);
        if let (Some(lookup), Some(source)) = (lookup, expected) {
            assert_eq!(lookup.service, source.service);
            assert_eq!(lookup.operation, source.lookup);
            assert!(lookup.provides.contains(&property.id));
            assert_eq!(names(&lookup.keys), source.depends_on);
            assert_eq!(lookup.operators, source.allowed_ops);
            assert_eq!(lookup.disabled, source.disabled);
        }
    }
}

#[test]
fn duckdb_catalog_matches_duckdb_bindings() {
    let (model, _, local) = models();
    let catalog = model.physical::<DuckDb>("local").unwrap();
    let store = catalog.store("local").unwrap();
    for entity in model.graph().entities() {
        let table = store.source(entity.id).map(|source| source.table.as_str());
        assert_eq!(table, local.entity_table(&entity.name), "{}", entity.name);
        for property in &entity.properties {
            let name = format!("{}.{}", entity.name, model.graph().property(*property).name);
            let expected = match local.property_realization(*property) {
                Some(PropertyRealization::Stored { column }) => Some(column.as_str()),
                _ => None,
            };
            assert_eq!(store.column(*property), expected, "{name}");
        }
        if let Some(guard) = store.guard(entity.id) {
            assert_eq!(guard.filter, RowFilter::None, "{}", entity.name);
            assert!(!guard.redacted, "{}", entity.name);
        }
    }
    let edge = catalog
        .backend("local")
        .unwrap()
        .relational
        .default_edge_table();
    assert_eq!(
        edge.map(|edge| edge.name.as_str()),
        Some(local.default_edge_table())
    );
}

#[test]
fn authorization_matches_gitlab_policy() {
    let (model, remote, _) = models();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    for entity in graph.entities() {
        let access = model.access().entity(entity.id).unwrap();
        let expected = remote.query_authorization().entity(entity.id);
        let redaction = access
            .redaction
            .as_ref()
            .map(|r| (&r.resource, &r.ability, r.id, r.owner));
        let old = expected.map(|e| {
            let id = graph.property_id(entity.id, &e.id_column).unwrap();
            (&e.resource_type, &e.ability, id, e.owner_entity)
        });
        assert_eq!(redaction, old, "{}", entity.name);
        let guard = store.guard(entity.id).unwrap();
        if let Some(role) = guard.min_role {
            let table = &store.source(entity.id).unwrap().table;
            assert_eq!(
                role,
                remote.table_minimum_access_level(table),
                "{}",
                entity.name
            );
        }
        for property in &entity.properties {
            let field = &graph.property(*property).name;
            let name = format!("{}.{field}", entity.name);
            let policy = model.access().property(*property);
            assert_eq!(
                policy.hidden,
                remote.property_is_hidden(*property),
                "{name}"
            );
            assert_eq!(
                policy.admin_only,
                remote.property_is_admin_only(*property),
                "{name}"
            );
            let filterable = remote.property_is_filterable(&entity.name, field);
            assert_eq!(policy.filterable, filterable, "{name}");
            let like = remote.property_allows_like(&entity.name, field);
            assert_eq!(policy.like, like, "{name}");
        }
    }
    for variant in graph.variants() {
        let scope = model.access().variant_scope(variant.id);
        let expected = remote.query_authorization().variant_scope(variant.id);
        assert_eq!(scope, expected, "{variant:?}");
    }
}

#[test]
fn graph_store_entities_filter_on_their_own_path_column() {
    let (model, _, _) = models();
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
