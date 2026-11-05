use std::sync::Arc;

use query_data_model::catalog::{IndexKind, RowFilter, Scope};
use query_data_model::spec::{Fragment, from_ontology};
use query_data_model::{
    Catalog, ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel, PropertyId,
    PropertyRealization, RelationalMapping,
};

fn models() -> (Catalog, ClickHouseDataModel, DuckDbDataModel) {
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let catalog = Catalog::load(&from_ontology(&ontology).unwrap()).unwrap();
    let remote = ClickHouseDataModel::derive(Arc::clone(&ontology)).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();
    (catalog, remote, local)
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
    let (catalog, remote, _) = models();
    let graph = catalog.graph();
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
fn graph_store_matches_clickhouse_bindings() {
    let (catalog, remote, _) = models();
    let graph = catalog.graph();
    let store = catalog.store("graph").unwrap();
    for entity in graph.entities() {
        let source = store.source(entity.id).unwrap();
        let layout = remote.backend().entity(entity.id).unwrap();
        assert_eq!(source.table, layout.table, "{}", entity.name);
        assert_eq!(source.high_fan_out, layout.high_fan_out, "{}", entity.name);
        let scoped = layout.has_traversal_path && !layout.global;
        assert_eq!(source.path_column.is_some(), scoped, "{}", entity.name);
        for property in &entity.properties {
            let name = format!("{}.{}", entity.name, graph.property(*property).name);
            let column = store.column(*property);
            let expected = remote.property_column(*property);
            assert_eq!(
                column.map(|column| column.name.as_str()),
                expected,
                "{name}"
            );
            if let Some(column) = column {
                let text = column.indexes.contains(&IndexKind::Text);
                assert_eq!(text, remote.has_text_index(*property), "{name}");
                assert_eq!(
                    column.selectivity,
                    remote.property_selectivity(*property),
                    "{name}"
                );
            }
        }
    }
    for variant in graph.variants() {
        let route = store
            .route(variant.id)
            .map(|route| (route.table.as_str(), route.foreign_key));
        let expected = remote
            .backend()
            .variant_route(variant.id)
            .map(|route| (route.table.as_str(), route.foreign_key));
        assert_eq!(route, expected, "{variant:?}");
    }
}

#[test]
fn lookups_match_virtual_properties() {
    let (catalog, remote, _) = models();
    let names = |ids: &[PropertyId]| -> Vec<String> {
        ids.iter()
            .map(|id| catalog.graph().property(*id).name.clone())
            .collect()
    };
    for property in catalog.graph().properties() {
        let lookup = catalog
            .realizations(property.id)
            .find_map(|(store, _)| Some((store, store.lookup(property.id)?)));
        let expected = remote.virtual_source_for_entity_id(property.entity, &property.name);
        assert_eq!(lookup.is_some(), expected.is_some(), "{}", property.name);
        if let (Some((store, lookup)), Some(source)) = (lookup, expected) {
            assert_eq!(store.name, source.service);
            assert_eq!(lookup.operation, source.lookup);
            assert!(lookup.provides.contains(&property.id));
            assert_eq!(names(&lookup.keys), source.depends_on);
            assert_eq!(lookup.operators, source.allowed_ops);
            assert_eq!(lookup.disabled, source.disabled);
        }
    }
}

#[test]
fn local_store_matches_duckdb_bindings() {
    let (catalog, _, local) = models();
    let store = catalog.store("local").unwrap();
    for entity in catalog.graph().entities() {
        let table = store.source(entity.id).map(|source| source.table.as_str());
        assert_eq!(table, local.entity_table(&entity.name), "{}", entity.name);
        for property in &entity.properties {
            let name = format!(
                "{}.{}",
                entity.name,
                catalog.graph().property(*property).name
            );
            let column = store.column(*property).map(|column| column.name.as_str());
            let expected = match local.property_realization(*property) {
                Some(PropertyRealization::Stored { column }) => Some(column.as_str()),
                _ => None,
            };
            assert_eq!(column, expected, "{name}");
        }
        if let Some(guard) = store.guard(entity.id) {
            assert_eq!(guard.filter, RowFilter::None, "{}", entity.name);
            assert!(!guard.redacted, "{}", entity.name);
        }
    }
}

#[test]
fn authorization_matches_gitlab_policy() {
    let (catalog, remote, _) = models();
    let graph = catalog.graph();
    let store = catalog.store("graph").unwrap();
    for entity in graph.entities() {
        let access = catalog.access().entity(entity.id).unwrap();
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
            let floor = remote.table_minimum_access_level(&store.source(entity.id).unwrap().table);
            assert_eq!(role, floor, "{}", entity.name);
        }
        for property in &entity.properties {
            let field = &graph.property(*property).name;
            let name = format!("{}.{field}", entity.name);
            let policy = catalog.access().property(*property);
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
            assert_eq!(
                policy.like,
                remote.property_allows_like(&entity.name, field),
                "{name}"
            );
        }
    }
    for variant in graph.variants() {
        let scope = catalog.access().variant_scope(variant.id);
        let expected = remote.query_authorization().variant_scope(variant.id);
        assert_eq!(scope, expected, "{variant:?}");
    }
}

#[test]
fn graph_store_entities_filter_on_their_own_path_column() {
    let (catalog, _, _) = models();
    let store = catalog.store("graph").unwrap();
    for entity in catalog.graph().entities() {
        let guard = store.guard(entity.id).unwrap();
        let expected = match catalog.access().entity(entity.id).unwrap().scope {
            Scope::Global => RowFilter::None,
            Scope::Namespace => RowFilter::Path {
                column: "traversal_path".into(),
            },
        };
        assert_eq!(guard.filter, expected, "{}", entity.name);
    }
}
