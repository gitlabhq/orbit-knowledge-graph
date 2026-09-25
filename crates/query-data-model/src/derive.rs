#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{ClickHouseDataModel, DuckDbDataModel, PropertyRealization};
    use ontology::FieldSource;

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
            remote.backend().relationship(contains).unwrap().table,
            "gl_edge"
        );
        assert_eq!(
            local.backend().relationship_table(contains),
            Some("gl_edge")
        );
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
        let variant = graph.variant_id(relationship, source, target).unwrap();
        let foreign_key = model
            .backend()
            .variant(variant)
            .unwrap()
            .foreign_key
            .unwrap();

        assert_eq!(graph.property(foreign_key).name, "project_id");
    }

    #[test]
    fn keeps_extraction_sources_separate_from_query_columns() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let entity = model.graph().entity_id("MergeRequest").unwrap();
        let property = model.graph().property_id(entity, "project_id").unwrap();
        let table = model.backend().table_for_entity(entity).unwrap();

        let source = &model
            .ontology()
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
            model.graph().property(property).realization,
            PropertyRealization::Stored
        ));
        assert_eq!(
            model.backend().property_column(property),
            Some("project_id")
        );
        assert!(table.column_types.contains_key("project_id"));
        assert!(!table.column_types.contains_key("target_project_id"));
    }
}
