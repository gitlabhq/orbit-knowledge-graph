#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        ClickHouseDataModel, DuckDbDataModel, PropertyRealization, QueryBackendCatalog,
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
            remote
                .backend()
                .storage()
                .table(remote.backend().entity(definition).unwrap().table)
                .name,
            "gl_definition"
        );
        assert_eq!(
            local
                .backend()
                .storage()
                .table(local.backend().entity(definition).unwrap().table)
                .name,
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
        for backend in [remote.backend().storage(), local.backend().storage()] {
            assert!(backend.table_id("gl_definition").is_some());
        }
        assert_eq!(
            remote.backend().entity_table_id(definition),
            remote.backend().storage().table_id("gl_definition")
        );
        assert_eq!(
            local.backend().entity_table_id(definition),
            local.backend().storage().table_id("gl_definition")
        );
        assert_eq!(
            remote.backend().relationship_table_id(contains),
            remote.backend().storage().table_id("gl_edge")
        );
        assert_eq!(
            local.backend().relationship_table_id(contains),
            Some(local.backend().default_edge_table_id())
        );
    }

    #[test]
    fn stored_schemas_preserve_backend_columns_and_row_semantics() {
        use crate::storage::{LocalType, RowSemantics, StorageType};

        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let local = DuckDbDataModel::derive(ontology.clone()).unwrap();
        for name in ontology.local_entity_names() {
            let entity = local.graph().entity_id(name).unwrap();
            let table_name = local.query_backend().entity_table(entity).unwrap();
            let local_table = local.table(table_name).unwrap();
            let remote_table = remote.table(table_name).unwrap();
            assert_eq!(local_table.row_semantics, RowSemantics::Current);
            assert!(local_table.column("_version").is_none());
            assert!(local_table.column("_deleted").is_none());
            assert_eq!(
                local_table.column("id").unwrap().data_type,
                StorageType::DuckDb(LocalType::Int64)
            );
            assert_eq!(
                remote_table.column("_version").unwrap().clickhouse_type(),
                "DateTime64(6, 'UTC')"
            );
            assert_eq!(
                remote_table.column("_deleted").unwrap().clickhouse_type(),
                "Bool"
            );
            assert_eq!(local_table.sort_key, remote_table.sort_key);
        }
        let local_edge = local.table(local.default_edge_table()).unwrap();
        assert_eq!(local_edge.columns.len(), 6);
        assert!(local_edge.column("source_tags").is_none());
        assert!(
            remote
                .table(remote.default_edge_table())
                .unwrap()
                .column("source_tags")
                .is_some()
        );
        let note = remote.table("gl_note").unwrap();
        assert_eq!(
            note.column("created_at").unwrap().clickhouse_type(),
            "Nullable(DateTime64(0, 'UTC'))"
        );
    }

    #[test]
    fn local_schema_excludes_columns_and_their_sort_keys_together() {
        use crate::storage::{LocalType, StorageType, TableLayout};

        let ontology = ontology::Ontology::load_embedded().unwrap();
        let node = ontology.get_node("Definition").unwrap();
        let table = TableLayout::local_node(node, &["branch".into()]).unwrap();
        assert!(table.column("branch").is_none());
        assert_eq!(
            table
                .sort_columns()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["traversal_path", "project_id", "id"]
        );
        assert_eq!(
            table.column("id").unwrap().data_type,
            StorageType::DuckDb(LocalType::Int64)
        );
        assert!(table.column("_version").is_none());
    }

    #[test]
    fn stored_keys_and_paths_must_reference_declared_columns() {
        use crate::storage::{RowSemantics, TableLayout, remote_node_columns};

        let ontology = ontology::Ontology::load_embedded().unwrap();
        let node = ontology.get_node("Project").unwrap();
        let invalid = TableLayout::new(
            "projects",
            remote_node_columns(node),
            &["missing".into()],
            RowSemantics::Current,
        );
        assert!(
            invalid
                .unwrap_err()
                .to_string()
                .contains("projects.missing")
        );
        let mut table = TableLayout::new(
            "projects",
            remote_node_columns(node),
            &node.sort_key,
            RowSemantics::Current,
        )
        .unwrap();
        let project = crate::ClickHouseDataModel::derive(Arc::new(ontology))
            .unwrap()
            .graph()
            .entity_id("Project")
            .unwrap();
        table
            .add_path_column("traversal_path", Some(project))
            .unwrap();
        let path = &table.path_columns[0];
        assert_eq!(table.columns[path.column.index()].name, "traversal_path");
        assert!(
            table
                .add_path_column("missing", None)
                .unwrap_err()
                .to_string()
                .contains("projects.missing")
        );
        assert_eq!(table.path_columns.len(), 1);
    }

    #[test]
    fn storage_references_follow_table_ownership_and_declaration_order() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let first = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let second = ClickHouseDataModel::derive(ontology).unwrap();
        let first = first.backend().storage();
        let second = second.backend().storage();
        for table in first.tables() {
            let table_id = first.table_id(&table.name).unwrap();
            assert_eq!(Some(table_id), second.table_id(&table.name));
            for (index, column) in table.columns.iter().enumerate() {
                let name = column.name.trim_matches('`');
                let reference = first.column_ref(table_id, name).unwrap();
                assert_eq!(reference.table, table_id);
                assert_eq!(reference.column.index(), index);
                assert_eq!(first.column(reference), column);
                assert_eq!(Some(reference), second.column_ref(table_id, name));
            }
            assert!(first.column_ref(table_id, "missing_column").is_none());
        }
        let project = first.table_id("gl_project").unwrap();
        let user = first.table_id("gl_user").unwrap();
        assert_ne!(
            first.column_ref(project, "id"),
            first.column_ref(user, "id")
        );
    }

    #[test]
    fn prefixed_catalog_keeps_storage_columns_and_path_provenance() {
        let ontology = Arc::new(
            ontology::Ontology::load_embedded()
                .unwrap()
                .with_schema_version_prefix("v123_"),
        );
        let model = ClickHouseDataModel::derive(ontology).unwrap();
        let project = model.graph().entity_id("Project").unwrap();
        let table = model.backend().table_for_entity(project).unwrap();
        assert_eq!(table.name, "v123_gl_project");
        assert_eq!(
            table
                .sort_columns()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["traversal_path", "id"]
        );
        assert_eq!(table.path_columns[0].entity, Some(project));
        assert_eq!(
            table.column("_version").unwrap().clickhouse_type(),
            "DateTime64(6, 'UTC')"
        );
        assert!(model.table("gl_project").is_none());
    }

    #[test]
    fn bundled_archives_resolve_their_own_stored_properties() {
        use ontology::archive::OntologyArchive;

        let versions = OntologyArchive::bundled_versions().unwrap();
        assert!(!versions.is_empty());
        for version in versions {
            let ontology = OntologyArchive::bundled(version)
                .unwrap()
                .unwrap()
                .load_ontology()
                .unwrap()
                .with_schema_version_prefix(&format!("v{version}_"));
            let model = ClickHouseDataModel::derive(Arc::new(ontology))
                .unwrap_or_else(|error| panic!("archive {version}: {error}"));
            for property in model.graph().properties() {
                if let Some(PropertyRealization::Stored { column }) =
                    model.property_realization(property.id)
                {
                    assert!(!model.backend().storage().column(*column).name.is_empty());
                    assert!(
                        model
                            .backend()
                            .storage()
                            .table(column.table)
                            .name
                            .starts_with(&format!("v{version}_"))
                    );
                }
            }
        }
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
            model.backend().property_realization(property),
            Some(PropertyRealization::Stored { column }) if model.backend().storage().column(*column).name == "project_id"
        ));
        assert_eq!(
            model.backend().property_column(property),
            Some("project_id")
        );
        assert!(table.column("project_id").unwrap().query_type.is_some());
        assert!(table.column("target_project_id").is_none());
    }

    #[test]
    fn stored_realizations_bind_destination_columns_on_both_backends() {
        let ontology = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Record"])
                .with_fields("Record", [("owner_id", ontology::DataType::Int)])
                .with_storage_columns("Record", [("owner_id", "Int64")])
                .modify_field("Record", "owner_id", |field| {
                    field.source = FieldSource::DatabaseColumn("extracted_owner".into())
                })
                .unwrap(),
        );
        let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let local = DuckDbDataModel::derive(ontology).unwrap();
        let assert_binding =
            |realization: &PropertyRealization, storage: &crate::storage::StorageCatalog| {
                let PropertyRealization::Stored { column } = realization else {
                    panic!("stored property")
                };
                assert_eq!(storage.table(column.table).name, "gl_record");
                assert_eq!(storage.column(*column).name, "owner_id");
                assert!(
                    storage
                        .column_ref(column.table, "extracted_owner")
                        .is_none()
                );
            };
        let property = remote
            .graph()
            .property_named("Record", "owner_id")
            .unwrap()
            .id;
        assert_binding(
            remote.property_realization(property).unwrap(),
            remote.backend().storage(),
        );
        let property = local
            .graph()
            .property_named("Record", "owner_id")
            .unwrap()
            .id;
        assert_binding(
            local.property_realization(property).unwrap(),
            local.backend().storage(),
        );
    }

    #[test]
    fn missing_stored_property_fails_model_derivation() {
        let ontology = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Record"])
                .with_fields("Record", [("missing", ontology::DataType::String)]),
        );
        for error in [
            ClickHouseDataModel::derive(ontology.clone()).err(),
            DuckDbDataModel::derive(ontology).err(),
        ] {
            assert!(error.unwrap().to_string().contains("gl_record.missing"));
        }
    }

    #[test]
    fn traversal_lookup_binds_its_actual_stored_key() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let model = ClickHouseDataModel::derive(ontology).unwrap();
        let project = model.graph().entity_id("Project").unwrap();
        let lookup = model
            .backend()
            .traversal_path_lookup(project, ontology::TraversalPathKind::FullPath)
            .unwrap();
        assert_eq!(
            model.backend().storage().table(lookup.key.table).name,
            "gl_project"
        );
        assert_eq!(
            model.backend().storage().column(lookup.key).name,
            "full_path"
        );
        let invalid = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Project"])
                .with_traversal_path_lookup("Project", "missing_key"),
        );
        let error = ClickHouseDataModel::derive(invalid).err().unwrap();
        assert!(error.to_string().contains("gl_project.missing_key"));
    }

    #[test]
    fn redaction_columns_resolve_on_the_protected_entity_table() {
        let model =
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap();
        let file = model.graph().entity_id("File").unwrap();
        let column = model.redaction_column(file).unwrap();
        assert_eq!(
            model.backend().storage().table(column.table).name,
            "gl_file"
        );
        assert_eq!(model.backend().storage().column(column).name, "project_id");

        let invalid = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["File"])
                .with_redaction("File", "project", "missing_key"),
        );
        let error = ClickHouseDataModel::derive(invalid).err().unwrap();
        assert!(error.to_string().contains("gl_file.missing_key"));
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
        assert!(table.column("when").is_some());
        assert!(table.column("`when`").is_none());
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
            Some(PropertyRealization::Stored { column }) if remote.backend().storage().column(*column).name == "title"));
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
