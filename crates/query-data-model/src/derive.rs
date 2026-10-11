use crate::implementations::{clickhouse, duckdb};
use crate::{
    ClickHouseDataModel, DataModelError, DuckDbDataModel, GitLabAuthzCatalog, GraphCatalog,
    Relational, TrustedLocalCatalog,
};
use std::sync::Arc;

impl ClickHouseDataModel {
    pub fn derive(ontology: Arc<ontology::Ontology>) -> Result<Self, DataModelError> {
        let graph = GraphCatalog::derive(&ontology)?;
        let schema = Relational::<clickhouse::layout::ClickHouse>::derive(&ontology)?;
        let authorization = GitLabAuthzCatalog::from_ontology(&ontology, &graph)?;
        Self::build(graph, schema, authorization)
    }
}

impl DuckDbDataModel {
    pub fn derive(ontology: Arc<ontology::Ontology>) -> Result<Self, DataModelError> {
        let graph = GraphCatalog::derive(&ontology)?;
        let schema = Relational::<duckdb::layout::DuckDb>::derive(&ontology);
        let authorization = TrustedLocalCatalog::from_ontology(&ontology, &graph)?;
        Self::build(graph, schema, authorization)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel, PropertyRealization,
        RelationalMapping,
    };
    use ontology::FieldSource;

    #[test]
    fn constructs_a_document_model_without_relational_or_gitlab_contracts() {
        use crate::{DataModel, DataModelError, GraphCatalog, Mapping};
        use std::collections::HashMap;

        struct Documents {
            collections: Vec<String>,
        }
        struct DocumentMapping(HashMap<crate::PropertyId, Vec<String>>);
        impl Mapping<Documents> for DocumentMapping {
            fn derive(graph: &GraphCatalog, schema: &Documents) -> Result<Self, DataModelError> {
                assert_eq!(schema.collections, ["records"]);
                let property = graph.property_named("Record", "key").unwrap().id;
                Ok(Self(HashMap::from([(
                    property,
                    vec!["metadata".into(), "key".into()],
                )])))
            }
        }

        let mut graph = GraphCatalog::new();
        let entity = graph.add_entity("Record".into()).unwrap();
        let key = graph
            .add_property(entity, "key".into(), ontology::DataType::Uuid)
            .unwrap();
        let related = graph
            .add_relationship("RELATED".into(), &[(entity, entity)])
            .unwrap();
        let model = DataModel::<_, DocumentMapping, _>::build(
            graph,
            Documents {
                collections: vec!["records".into()],
            },
            (),
        )
        .unwrap();

        assert_eq!(model.layout().schema().collections, ["records"]);
        assert_eq!(model.backend().0[&key], ["metadata", "key"]);
        assert!(model.graph().property_id(entity, "id").is_none());
        assert!(model.graph().variant_id(related, entity, entity).is_some());
        assert!(model.graph().property_id(entity, "key").is_some());
    }

    #[test]
    fn rejects_a_schema_whose_entities_are_missing_from_the_graph() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let schema =
            crate::Relational::<crate::implementations::clickhouse::layout::ClickHouse>::derive(
                &ontology,
            )
            .unwrap();
        let result = crate::DataModel::<_, crate::implementations::ClickHouseMapping, _>::build(
            crate::GraphCatalog::new(),
            schema,
            (),
        );
        assert!(matches!(
            result,
            Err(crate::DataModelError::UnknownReference { kind: "entity", .. })
        ));
    }

    #[test]
    fn derives_remote_and_local_models_from_the_same_ontology() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let remote = ClickHouseDataModel::derive(Arc::clone(&ontology)).unwrap();
        let local = DuckDbDataModel::derive(ontology).unwrap();

        let definition = remote.graph().entity_id("Definition").unwrap();
        let contains = remote.graph().relationship_id("CONTAINS").unwrap();

        assert_eq!(
            remote.backend().entity(definition).unwrap().table,
            "gl_definition"
        );
        assert_eq!(
            local.backend().entity(definition).unwrap().table,
            "gl_definition"
        );
        assert_eq!(
            remote.backend().relationship_table(contains),
            Some("gl_edge")
        );
        assert_eq!(
            local.backend().relationship_table(contains),
            Some("gl_edge")
        );
    }

    #[test]
    fn hides_traversal_path_only_in_the_remote_model() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let remote = ClickHouseDataModel::derive(Arc::clone(&ontology)).unwrap();
        let local = DuckDbDataModel::derive(ontology).unwrap();
        let property = remote.property("Definition", "traversal_path").unwrap().id;

        assert!(remote.property_is_hidden(property));
        assert!(!local.property_is_hidden(property));
    }

    #[test]
    fn resolves_relationship_variants_and_foreign_keys_to_ids() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
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
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let model = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let entity = model.graph().entity_id("MergeRequest").unwrap();
        let property = model.graph().property_id(entity, "project_id").unwrap();
        let table = model.backend().table_for_entity(entity).unwrap();

        let source = &ontology
            .get_node("MergeRequest")
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name == "project_id")
            .unwrap()
            .source;
        assert!(
            matches!(source, FieldSource::DatabaseColumn(source) if source == "target_project_id")
        );
        assert!(matches!(
            model.backend().property_realization(property),
            Some(PropertyRealization::Stored { column }) if column == "project_id"
        ));
        assert_eq!(
            model.backend().property_column(property),
            Some("project_id")
        );
        assert!(table.column_types.contains_key("project_id"));
        assert!(!table.column_types.contains_key("target_project_id"));
    }

    #[test]
    fn exposes_storage_columns_as_query_identifiers() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let entity = model.graph().entity_id("Job").unwrap();
        let property = model.graph().property_id(entity, "when").unwrap();
        let table = model.backend().table_for_entity(entity).unwrap();

        assert_eq!(model.backend().property_column(property), Some("when"));
        assert!(table.columns.contains("when"));
        assert!(!table.columns.contains("`when`"));
    }

    #[test]
    fn foreign_keys_identify_the_endpoint_and_referenced_key() {
        use crate::Endpoint;

        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
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
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let local = DuckDbDataModel::derive(ontology).unwrap();
        let remote_entity = remote.graph().entity_id("MergeRequest").unwrap();
        let local_entity = local.graph().entity_id("MergeRequest").unwrap();
        let remote_property = remote.graph().property_id(remote_entity, "title").unwrap();
        let local_property = local.graph().property_id(local_entity, "title").unwrap();
        assert!(matches!(remote.property_realization(remote_property),
            Some(PropertyRealization::Stored { column }) if column == "title"));
        assert!(local.property_realization(local_property).is_none());
        assert!(local.property_column(local_property).is_none());

        let file = remote.graph().entity_id("File").unwrap();
        let content = remote.graph().property_id(file, "content").unwrap();
        assert!(matches!(
            remote.property_realization(content),
            Some(PropertyRealization::Virtual(_))
        ));
        assert!(remote.property_column(content).is_none());
    }
}
