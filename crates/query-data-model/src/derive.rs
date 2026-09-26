#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        ClickHouseDataModel, DenormalizedDirection, DuckDbDataModel, PropertyRealization,
        QueryDataModel,
    };
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
            remote.backend().relationship_table(contains),
            Some("gl_edge")
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
        let public = model
            .foreign_key(&["IN_PROJECT".to_string()], "Vulnerability", "Project")
            .unwrap();
        assert_eq!(public.holder, source);
        assert_eq!(public.property, foreign_key);
        assert_eq!(model.foreign_key_column(&public), Some("project_id"));
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
    fn preserves_text_index_tokenizers() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let definition = model.graph().entity_id("Definition").unwrap();
        let name = model.graph().property_id(definition, "name").unwrap();
        let file_path = model.graph().property_id(definition, "file_path").unwrap();

        let name_index = model.text_index(name).unwrap();
        assert_eq!(name_index.name, "idx_name");
        assert_eq!(name_index.index_type, "text(tokenizer = splitByNonAlpha)");
        assert_eq!(name_index.granularity, 1);
        assert_eq!(name_index.tokenizer, "tokenizer = splitByNonAlpha");
        assert_eq!(
            model.text_index(file_path).unwrap().tokenizer,
            "tokenizer = splitByString(['/'])"
        );
    }

    #[test]
    fn resolves_denormalized_properties_to_ids() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let graph = model.graph();
        let entity = graph.entity_id("MergeRequest").unwrap();
        let property = graph.property_id(entity, "state").unwrap();
        let reviewer = graph.relationship_id("REVIEWER").unwrap();
        let layout = model
            .denormalized_property("MergeRequest", "state", DenormalizedDirection::Target)
            .unwrap();

        assert_eq!(layout.entity, entity);
        assert_eq!(layout.property, property);
        assert!(layout.carries(reviewer));
        assert_eq!(layout.edge_column, "target_tags");
        assert_eq!(layout.tag_key, "state");
    }

    #[test]
    fn exposes_denormalized_join_access_paths() {
        let ontology = ontology::Ontology::load_embedded()
            .unwrap()
            .with_denormalized_join(
                "reviewer_project",
                &[
                    ("REVIEWER", "User", "MergeRequest", false),
                    ("IN_PROJECT", "MergeRequest", "Project", true),
                ],
            );
        let model = ClickHouseDataModel::derive(Arc::new(ontology)).unwrap();
        let graph = model.graph();
        let join = model
            .denormalized_joins()
            .iter()
            .find(|join| join.name == "reviewer_project")
            .unwrap();
        let merge_request = graph.entity_id("MergeRequest").unwrap();
        let title = graph.property_id(merge_request, "title").unwrap();
        let reviewer = graph
            .variant_named("REVIEWER", "User", "MergeRequest")
            .unwrap();
        let in_project = graph
            .variant_named("IN_PROJECT", "MergeRequest", "Project")
            .unwrap();

        assert_eq!(join.table, "gl_denorm_reviewer_project");
        assert_eq!(
            join.hops.iter().map(|hop| hop.variant).collect::<Vec<_>>(),
            [reviewer.id, in_project.id]
        );
        assert_eq!(join.property_column(2, title), Some("t2_title"));
        assert!(join.hops[0].edge_table.is_some());
        assert!(join.hops[1].edge_table.is_none());
        assert!(!join.path_columns.is_empty());
    }
}
