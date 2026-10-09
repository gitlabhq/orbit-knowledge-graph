#[test]
fn yaml_plan_shapes() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/plan_shape/fixtures");
    integration_testkit::plan_shape::run_dir(&directory, super::setup::embedded_ontology());
}

#[test]
fn reordered_layout_metadata_drives_plans_and_schema() {
    use query_data_model::{ClickHouseDataModel, QueryBackendCatalog};
    use std::sync::Arc;

    let base = super::setup::embedded_ontology();
    let overlay = Arc::new(integration_testkit::load_ontology_overlay(
        "reordered_edges",
    ));
    let model = ClickHouseDataModel::derive(overlay.clone()).unwrap();
    let layouts = model.backend().equivalent_layouts("gl_code_edge");
    assert_eq!(layouts, ["gl_code_edge_by_target"]);
    let storage =
        query_data_model::implementations::clickhouse::storage::StorageCatalog::derive(&overlay)
            .unwrap();
    let source = storage.table("gl_code_edge").unwrap();
    let copy = storage.table(layouts[0]).unwrap();
    assert_eq!(copy.columns.len(), source.columns.len());
    assert_eq!(copy.sort_key[1], "target_id");
    assert_eq!(
        storage.dependencies()[layouts[0]],
        std::collections::BTreeSet::from(["gl_code_edge".to_string()])
    );

    let query = r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"Definition"},{"id":"b","entity":"Definition","node_ids":[1]}],"relationships":[{"type":"CALLS","from":"a","to":"b"}]}"#;
    let context = super::setup::test_ctx();
    let compile = |ontology| {
        compiler::compile(query, compiler::Frontend::JsonDsl, ontology, &context)
            .unwrap()
            .base
            .render()
    };
    assert_ne!(compile(&base), compile(&overlay));
    assert!(compile(&overlay).contains(layouts[0]));

    let base_schema = orbit_migrations::schema::GraphSchema::from_ontology(&base);
    let overlay_schema = orbit_migrations::schema::GraphSchema::from_ontology(&overlay);
    assert_eq!(base_schema.tables.len() + 1, overlay_schema.tables.len());
    assert_eq!(base_schema.views.len() + 1, overlay_schema.views.len());
    assert!(
        overlay_schema
            .tables
            .iter()
            .any(|table| table.name == layouts[0])
    );
}

#[test]
fn catalog_routes_preserve_variant_endpoints_on_each_backend() {
    use query_data_model::{ClickHouseDataModel, DuckDbDataModel, QueryBackendCatalog};
    use std::sync::Arc;

    let ontology = Arc::new(integration_testkit::load_ontology_overlay("variant_routes"));
    let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let local = DuckDbDataModel::derive(ontology).unwrap();
    for (source, target, table) in [
        ("Definition", "Definition", "gl_code_edge"),
        ("File", "Definition", "gl_edge"),
        ("Definition", "ImportedSymbol", "gl_code_edge"),
        ("File", "ImportedSymbol", "gl_edge"),
    ] {
        let variant = remote
            .graph()
            .variant_named("CALLS", source, target)
            .unwrap();
        assert_eq!(
            remote.backend().variant_route(variant.id).unwrap().table,
            table
        );
        let local_variant = local
            .graph()
            .variant_named("CALLS", source, target)
            .unwrap();
        assert_eq!(
            local
                .backend()
                .variant_route(local_variant.id)
                .unwrap()
                .table,
            local.backend().default_edge_table()
        );
    }
}

#[test]
fn materialized_bindings_resolve_graph_identities_and_source_columns() {
    use query_data_model::ClickHouseDataModel;
    use std::sync::Arc;

    let ontology = Arc::new(integration_testkit::load_ontology_overlay(
        "denorm_approved",
    ));
    let model = ClickHouseDataModel::derive(ontology.clone()).unwrap();
    let storage =
        query_data_model::implementations::clickhouse::storage::StorageCatalog::derive(&ontology)
            .unwrap();
    assert!(!model.backend().materialized_joins().is_empty());
    for binding in model.backend().materialized_joins() {
        let table = storage.table(&binding.table).unwrap();
        let has_column = |name: &str| table.columns.iter().any(|column| column.name == name);
        for node in &binding.nodes {
            assert!(has_column(&node.identity_column));
            for (property, column) in &node.properties {
                assert!(
                    model
                        .graph()
                        .entity(node.entity)
                        .properties
                        .contains(property)
                );
                assert!(has_column(column));
            }
        }
        for relationship in &binding.relationships {
            let variant = model.graph().variant(relationship.variant);
            assert_eq!(
                variant.source,
                binding.nodes[relationship.source_slot].entity
            );
            assert_eq!(
                variant.target,
                binding.nodes[relationship.target_slot].entity
            );
            assert!(has_column(&relationship.source_id_column));
            assert!(has_column(&relationship.target_id_column));
        }
    }
}
