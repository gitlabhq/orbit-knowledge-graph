use ontology::FieldSource;
use query_data_model::from_ontology;
use query_data_model::implementations::clickhouse::schema::Schema;
use query_data_model::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel, Packages};

fn overlay_ontology(name: &str) -> ontology::Ontology {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/seeds/overlays")
        .join(name);
    ontology::Ontology::load_embedded_with_overlay(&path).unwrap()
}

#[test]
fn reordered_tables_retain_source_columns_and_dependencies() {
    let ontology = overlay_ontology("reordered_edges");
    let model = from_ontology::clickhouse(&ontology).unwrap();
    let backend = &model;
    assert_eq!(
        backend.equivalent_layouts("gl_code_edge"),
        ["gl_code_edge_by_target"]
    );
    let source = backend.table_columns("gl_code_edge").unwrap();
    let copy = backend.table_columns("gl_code_edge_by_target").unwrap();
    assert_eq!(copy, source);
    assert_eq!(
        backend.table_sort_key("gl_code_edge_by_target").unwrap()[1],
        "target_id"
    );
    let storage = from_ontology::schema(&ontology).unwrap();
    assert_eq!(
        storage.dependencies()["gl_code_edge_by_target"],
        std::collections::BTreeSet::from(["gl_code_edge".to_string()])
    );
}

#[test]
fn catalog_routes_preserve_variant_endpoints_on_each_backend() {
    let ontology = overlay_ontology("variant_routes");
    let remote = from_ontology::clickhouse(&ontology).unwrap();
    let local = from_ontology::duckdb(&ontology).unwrap();
    for (source, target, table) in [
        ("Definition", "Definition", "gl_code_edge"),
        ("File", "Definition", "gl_edge"),
        ("Definition", "ImportedSymbol", "gl_code_edge"),
        ("File", "ImportedSymbol", "gl_edge"),
    ] {
        let route = remote.variant_route("CALLS", source, target).unwrap();
        assert_eq!(route.table, table);
        let route = local.variant_route("CALLS", source, target).unwrap();
        assert_eq!(route.table, local.default_edge_table());
    }
}

#[test]
fn writer_catalog_tracks_exact_node_mapping_routes() {
    let storage = Schema::from_model(&Packages::embedded().load().unwrap()).unwrap();

    let code_writers = storage.writers("gl_code_edge").unwrap();
    assert!(code_writers.contains("Branch"));
    assert!(code_writers.contains("Directory"));
    assert!(!code_writers.contains("Group"));
    assert!(!code_writers.contains("Project"));

    let default_writers = storage.writers("gl_edge").unwrap();
    assert!(default_writers.contains("Group"));
    assert!(default_writers.contains("Project"));

    let ci_writers = storage.writers("gl_ci_edge").unwrap();
    assert!(ci_writers.contains("Pipeline"));
    assert!(!ci_writers.contains("Vulnerability"));

    let security_writers = storage.writers("gl_sec_edge").unwrap();
    assert!(security_writers.contains("Vulnerability"));
    assert!(!security_writers.contains("Pipeline"));
}

#[test]
fn materialized_bindings_resolve_graph_identities_and_source_columns() {
    let model = from_ontology::clickhouse(&overlay_ontology("denorm_approved")).unwrap();
    let approved = model
        .materialized_joins()
        .iter()
        .find(|binding| binding.table == "gl_denorm_approved")
        .unwrap();
    assert_eq!(approved.nodes.len(), 2);
    assert_eq!(approved.relationships.len(), 1);
    assert_eq!(approved.sources, ["gl_user", "gl_edge", "gl_merge_request"]);

    let approved_project_model =
        from_ontology::clickhouse(&overlay_ontology("denorm_approved_project")).unwrap();
    let approved_project = approved_project_model
        .materialized_joins()
        .iter()
        .find(|binding| binding.table == "gl_denorm_approved_project")
        .unwrap();
    assert_eq!(approved_project.nodes.len(), 3);
    assert_eq!(approved_project.relationships.len(), 2);
    assert_eq!(
        approved_project.sources,
        [
            "gl_user",
            "gl_edge",
            "gl_merge_request",
            "gl_edge",
            "gl_project"
        ]
    );

    for binding in model.materialized_joins() {
        let columns = model.table_columns(&binding.table).unwrap();
        let has_column = |name: &str| columns.contains(name);
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

#[test]
fn loads_remote_and_local_models_from_the_same_packages() {
    let remote = ClickHouseDataModel::embedded().unwrap();
    let local = DuckDbDataModel::embedded().unwrap();

    let definition = remote.graph().entity_id("Definition").unwrap();

    assert_eq!(
        remote.catalog().table_of(definition).unwrap(),
        "gl_definition"
    );
    assert_eq!(
        local.catalog().table_of(definition).unwrap(),
        "gl_definition"
    );
    assert_eq!(remote.relationship_table("CONTAINS"), None);
    assert_eq!(local.relationship_table("CONTAINS"), Some("gl_edge"));
}

#[test]
fn hides_traversal_path_in_remote_and_local_models() {
    let remote = ClickHouseDataModel::embedded().unwrap();
    let local = DuckDbDataModel::embedded().unwrap();
    let graph = remote.graph();
    let definition = graph.entity_id("Definition").unwrap();
    let property = graph.property_id(definition, "traversal_path").unwrap();

    assert!(remote.catalog().model().access().property(property).hidden);
    assert!(local.catalog().model().access().property(property).hidden);
}

#[test]
fn resolves_relationship_variants_and_foreign_keys_to_ids() {
    let model = ClickHouseDataModel::embedded().unwrap();
    let graph = model.graph();
    let relationship = graph.relationship_id("IN_PROJECT").unwrap();
    let source = graph.entity_id("Vulnerability").unwrap();
    let target = graph.entity_id("Project").unwrap();
    assert!(graph.variant_id(relationship, source, target).is_some());
    let foreign_key = model
        .foreign_key(&["IN_PROJECT".to_string()], "Vulnerability", "Project")
        .unwrap();

    assert_eq!(graph.property(foreign_key.property).name, "project_id");
}

#[test]
fn keeps_extraction_sources_separate_from_query_columns() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let model = ClickHouseDataModel::embedded().unwrap();
    let entity = model.graph().entity_id("MergeRequest").unwrap();
    let property = model.graph().property_id(entity, "project_id").unwrap();
    let table = model.catalog().table_of(entity).unwrap();

    let source = &ontology
        .get_node("MergeRequest")
        .unwrap()
        .fields
        .iter()
        .find(|field| field.name == "project_id")
        .unwrap()
        .source;
    assert!(matches!(source, FieldSource::DatabaseColumn(source) if source == "target_project_id"));
    assert_eq!(model.catalog().column(property), Some("project_id"));
    let backend = &model;
    assert!(backend.table_column_type(table, "project_id").is_some());
    assert!(
        backend
            .table_column_type(table, "target_project_id")
            .is_none()
    );
}

#[test]
fn exposes_storage_columns_as_query_identifiers() {
    let model = ClickHouseDataModel::embedded().unwrap();
    let entity = model.graph().entity_id("Job").unwrap();
    let property = model.graph().property_id(entity, "when").unwrap();
    let table = model.catalog().table_of(entity).unwrap();

    assert_eq!(model.catalog().column(property), Some("when"));
    let columns = model.table_columns(table).unwrap();
    assert!(columns.contains("when"));
    assert!(!columns.contains("`when`"));
}

#[test]
fn foreign_keys_identify_the_endpoint_and_referenced_key() {
    use query_data_model::Endpoint;

    let model = ClickHouseDataModel::embedded().unwrap();
    for (relationship, source, target, holder, property) in [
        (
            "AUTHORED",
            "User",
            "MergeRequest",
            Endpoint::Target,
            "author_id",
        ),
        (
            "IN_PROJECT",
            "MergeRequest",
            "Project",
            Endpoint::Source,
            "project_id",
        ),
        (
            "AUTO_CANCELED_BY",
            "Pipeline",
            "Pipeline",
            Endpoint::Source,
            "auto_canceled_by_id",
        ),
    ] {
        let key = model
            .foreign_key(&[relationship.into()], source, target)
            .unwrap();
        assert_eq!(key.holder, holder, "{relationship}");
        assert_eq!(model.graph().property(key.property).name, property);
        let referenced_entity = match holder {
            Endpoint::Source => target,
            Endpoint::Target => source,
        };
        let referenced = model.graph().property(key.referenced_key);
        assert_eq!(referenced.name, "id");
        assert_eq!(
            referenced.entity,
            model.graph().entity_id(referenced_entity).unwrap()
        );
    }
    assert!(
        model
            .foreign_key(
                &["AUTHORED".into(), "APPROVED".into()],
                "User",
                "MergeRequest",
            )
            .is_none()
    );
}

#[test]
fn property_realization_reports_backend_availability() {
    let remote = ClickHouseDataModel::embedded().unwrap();
    let local = DuckDbDataModel::embedded().unwrap();
    let remote_entity = remote.graph().entity_id("MergeRequest").unwrap();
    let local_entity = local.graph().entity_id("MergeRequest").unwrap();
    let remote_property = remote.graph().property_id(remote_entity, "title").unwrap();
    let local_property = local.graph().property_id(local_entity, "title").unwrap();
    assert_eq!(remote.catalog().column(remote_property), Some("title"));
    assert!(!local.catalog().readable(local_property));

    let file = remote.graph().entity_id("File").unwrap();
    let content = remote.graph().property_id(file, "content").unwrap();
    assert!(remote.catalog().lookup(content).is_some());
    assert!(remote.catalog().column(content).is_none());
}
