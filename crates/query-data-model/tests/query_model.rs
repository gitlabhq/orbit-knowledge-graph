use std::sync::Arc;

use query_data_model::catalog::sources::Writer;
use query_data_model::implementations::clickhouse::schema::Schema;
use query_data_model::testkit::overlay;
use query_data_model::{Catalog, ClickHouse, DuckDb, Model, Packages};

fn overlay_model(name: &str) -> Arc<Model> {
    overlay(name).deployment("server").unwrap()
}

#[test]
fn reordered_tables_retain_source_columns_and_dependencies() {
    let overlay = overlay_model("reordered_edges");
    let model = Catalog::<ClickHouse>::from_model(&overlay).unwrap();
    let catalog = &model;
    assert_eq!(
        catalog.copies_of("gl_code_edge"),
        ["gl_code_edge_by_target"]
    );
    let source = &catalog.physical_table("gl_code_edge").unwrap().columns;
    let copy = catalog.physical_table("gl_code_edge_by_target").unwrap();
    assert_eq!(&copy.columns, source);
    assert_eq!(copy.sort_key[1], "target_id");
    let storage = Schema::from_model(&overlay).unwrap();
    assert_eq!(
        storage.dependencies()["gl_code_edge_by_target"],
        std::collections::BTreeSet::from(["gl_code_edge".to_string()])
    );
}

#[test]
fn catalog_routes_preserve_variant_endpoints_on_each_backend() {
    let remote = Catalog::<ClickHouse>::from_model(&overlay_model("variant_routes")).unwrap();
    let local = overlay("variant_routes").deployment("local").unwrap();
    let local = Catalog::<DuckDb>::from_model(&local).unwrap();
    for (source, target, table) in [
        ("Definition", "Definition", "gl_code_edge"),
        ("File", "Definition", "gl_edge"),
        ("Definition", "ImportedSymbol", "gl_code_edge"),
        ("File", "ImportedSymbol", "gl_edge"),
    ] {
        let variant = remote.names().variant("CALLS", source, target).unwrap();
        assert_eq!(remote.edge_table(variant), Some(table));
        assert_eq!(local.edge_table(variant), local.shared_edge_table());
    }
}

#[test]
fn writer_catalog_tracks_exact_node_mapping_routes() {
    let storage = Schema::from_model(&Packages::embedded().deployment("server").unwrap()).unwrap();

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
    let model = Catalog::<ClickHouse>::from_model(&overlay_model("denorm_approved")).unwrap();
    let approved = model
        .joins()
        .find(|binding| binding.table == "gl_denorm_approved")
        .unwrap();
    assert_eq!(approved.nodes.len(), 2);
    assert_eq!(approved.relationships.len(), 1);
    assert_eq!(approved.sources, ["gl_user", "gl_edge", "gl_merge_request"]);

    let approved_project_model =
        Catalog::<ClickHouse>::from_model(&overlay_model("denorm_approved_project")).unwrap();
    let approved_project = approved_project_model
        .joins()
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

    for binding in model.joins() {
        let table = model.physical_table(&binding.table).unwrap();
        let has_column = |name: &str| table.has_column(name);
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
    let remote = Catalog::<ClickHouse>::embedded("server").unwrap();
    let local = Catalog::<DuckDb>::embedded("local").unwrap();

    let definition = remote.names().entity("Definition").unwrap();

    assert_eq!(remote.table_of(definition).unwrap(), "gl_definition");
    assert_eq!(local.table_of(definition).unwrap(), "gl_definition");
    let contains = remote.names().relationship("CONTAINS").unwrap();
    assert!(remote.edge_tables(&[contains]).len() > 1);
    assert_eq!(local.edge_tables(&[contains]), ["gl_edge"]);
}

#[test]
fn hides_traversal_path_in_remote_and_local_models() {
    let remote = Catalog::<ClickHouse>::embedded("server").unwrap();
    let local = Catalog::<DuckDb>::embedded("local").unwrap();
    let names = remote.names();
    let definition = names.entity("Definition").unwrap();
    let property = names.property(definition, "traversal_path").unwrap();

    assert!(remote.model().access().property(property).hidden);
    assert!(local.model().access().property(property).hidden);
}

#[test]
fn resolves_relationship_variants_and_foreign_keys_to_ids() {
    let model = Catalog::<ClickHouse>::embedded("server").unwrap();
    let graph = model.graph();
    let names = model.names();
    let relationship = names.relationship("IN_PROJECT").unwrap();
    let source = names.entity("Vulnerability").unwrap();
    let target = names.entity("Project").unwrap();
    assert!(graph.variant_id(relationship, source, target).is_some());
    let foreign_key = model
        .shared_foreign_key(&[relationship], source, target)
        .unwrap();

    assert_eq!(graph.property(foreign_key.property).name, "project_id");
}

#[test]
fn keeps_extraction_sources_separate_from_query_columns() {
    let model = Catalog::<ClickHouse>::embedded("server").unwrap();
    let entity = model.names().entity("MergeRequest").unwrap();
    let property = model.names().property(entity, "project_id").unwrap();
    let table = model.table_of(entity).unwrap();

    let pipeline = model
        .model()
        .sources()
        .pipelines()
        .iter()
        .find(|pipeline| pipeline.writer == Writer::Entity(entity))
        .unwrap();
    let source = pipeline
        .columns
        .iter()
        .find(|column| column.property == property)
        .unwrap();
    assert_eq!(source.column, "target_project_id");
    assert_eq!(model.column(property), Some("project_id"));
    let table = model.physical_table(table).unwrap();
    assert!(table.has_column("project_id"));
    assert!(!table.has_column("target_project_id"));
}

#[test]
fn exposes_storage_columns_as_query_identifiers() {
    let model = Catalog::<ClickHouse>::embedded("server").unwrap();
    let entity = model.names().entity("Job").unwrap();
    let property = model.names().property(entity, "when").unwrap();
    let table = model.table_of(entity).unwrap();

    assert_eq!(model.column(property), Some("when"));
    let table = model.physical_table(table).unwrap();
    assert!(table.has_column("when"));
    assert!(!table.has_column("`when`"));
}

#[test]
fn foreign_keys_identify_the_endpoint_and_referenced_key() {
    use query_data_model::Endpoint;

    let model = Catalog::<ClickHouse>::embedded("server").unwrap();
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
        let names = model.names();
        let ids = [names.relationship(relationship).unwrap()];
        let (source, target) = (names.entity(source).unwrap(), names.entity(target).unwrap());
        let key = model.shared_foreign_key(&ids, source, target).unwrap();
        assert_eq!(key.holder, holder, "{relationship}");
        assert_eq!(model.graph().property(key.property).name, property);
        let referenced = model.graph().property(key.referenced_key);
        assert_eq!(referenced.name, "id");
        assert_eq!(
            referenced.entity,
            match holder {
                Endpoint::Source => target,
                Endpoint::Target => source,
            }
        );
    }
    let names = model.names();
    let both = ["AUTHORED", "APPROVED"].map(|name| names.relationship(name).unwrap());
    let (user, request) = (
        names.entity("User").unwrap(),
        names.entity("MergeRequest").unwrap(),
    );
    assert!(model.shared_foreign_key(&both, user, request).is_none());
}

#[test]
fn property_realization_reports_backend_availability() {
    let remote = Catalog::<ClickHouse>::embedded("server").unwrap();
    let local = Catalog::<DuckDb>::embedded("local").unwrap();
    let remote_entity = remote.names().entity("MergeRequest").unwrap();
    let local_entity = local.names().entity("MergeRequest").unwrap();
    let remote_property = remote.names().property(remote_entity, "title").unwrap();
    let local_property = local.names().property(local_entity, "title").unwrap();
    assert_eq!(remote.column(remote_property), Some("title"));
    assert!(!local.readable(local_property));

    let file = remote.names().entity("File").unwrap();
    let content = remote.names().property(file, "content").unwrap();
    assert!(remote.lookup(content).is_some());
    assert!(remote.column(content).is_none());
}
