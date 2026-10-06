#[cfg(test)]
mod tests {
    use crate::implementations::duckdb::storage::DuckDbColumn;
    use crate::storage::{StorageCatalog, StoredColumn, TableLayout};
    use crate::{
        ClickHouseDataModel, DuckDbDataModel, PropertyRealization, QueryBackendCatalog,
        QueryDataModel,
    };
    use std::sync::Arc;

    #[test]
    fn storage_references_are_generic_and_validate_ownership() {
        let columns = || {
            vec![
                StoredColumn::new("id", (8u8, false)),
                StoredColumn::new("version", (8, true)),
            ]
        };
        let table = || {
            TableLayout::new("records", columns(), &["id".into()])
                .unwrap()
                .versioned("version", None)
                .unwrap()
        };
        let storage = StorageCatalog::new([table()]).unwrap();
        let repeated = StorageCatalog::new([table()]).unwrap();
        let reference = storage.resolve_column("records", "id").unwrap();
        assert_eq!(reference, repeated.resolve_column("records", "id").unwrap());
        assert_eq!(storage.column(reference).storage(), &(8, false));
        assert_eq!(
            storage.table(reference.table).sort_key(),
            &[reference.column]
        );
        assert!(storage.resolve_column("records", "missing").is_err());
        assert!(TableLayout::new("invalid", columns(), &["missing".into()]).is_err());
        assert!(StorageCatalog::new([table(), table()]).is_err());
        assert!(
            TableLayout::new(
                "duplicate",
                vec![StoredColumn::new("id", ()), StoredColumn::new("id", ())],
                &[]
            )
            .is_err()
        );
    }

    fn assert_destination_binding<M: QueryDataModel>(model: &M) {
        let property = model
            .graph()
            .property_named("Record", "owner_id")
            .unwrap()
            .id;
        let PropertyRealization::Stored { column } = model.property_realization(property).unwrap()
        else {
            panic!("stored property")
        };
        assert_eq!(
            model.query_backend().storage().column(*column).name(),
            "owner_id"
        );
        assert_eq!(
            model.query_backend().storage().table(column.table).name(),
            "gl_record"
        );
    }

    #[test]
    fn stored_realizations_use_destination_names_and_reject_missing_columns() {
        let ontology = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Record"])
                .with_fields("Record", [("owner_id", ontology::DataType::Int)])
                .with_storage_columns("Record", [("owner_id", "Int64")])
                .modify_field("Record", "owner_id", |field| {
                    field.source = ontology::FieldSource::DatabaseColumn("extracted_owner".into())
                })
                .unwrap(),
        );
        assert_destination_binding(&ClickHouseDataModel::derive(ontology.clone()).unwrap());
        assert_destination_binding(&DuckDbDataModel::derive(ontology).unwrap());
        let invalid = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Record"])
                .with_fields("Record", [("missing", ontology::DataType::String)]),
        );
        for error in [
            ClickHouseDataModel::derive(invalid.clone()).err(),
            DuckDbDataModel::derive(invalid).err(),
        ] {
            assert!(error.unwrap().to_string().contains("gl_record.missing"));
        }
    }

    #[test]
    fn local_exclusions_remove_columns_and_sort_keys_together() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let local = DuckDbDataModel::derive(ontology.clone()).unwrap();
        let remote = ClickHouseDataModel::derive(ontology.clone()).unwrap();
        assert!(
            local
                .property_column_named("MergeRequest", "title")
                .is_none()
        );
        assert!(remote.property_column_named("File", "content").is_none());
        assert_eq!(
            remote.property_column_named("MergeRequest", "project_id"),
            Some("project_id")
        );
        assert!(remote.table("gl_job").unwrap().column("when").is_some());
        let table = TableLayout::<DuckDbColumn>::local_node(
            ontology.get_node("Definition").unwrap(),
            &["branch".into()],
        )
        .unwrap();
        assert!(table.column("branch").is_none());
        assert_eq!(
            table
                .sort_columns()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            ["traversal_path", "project_id", "id"]
        );
    }

    #[test]
    fn archives_resolve_properties_lookup_keys_and_redaction_in_their_own_schema() {
        use ontology::archive::OntologyArchive;
        let versions = OntologyArchive::bundled_versions().unwrap();
        assert!(!versions.is_empty());
        for version in versions {
            let prefix = format!("v{version}_");
            let ontology = OntologyArchive::bundled(version)
                .unwrap()
                .unwrap()
                .load_ontology()
                .unwrap()
                .with_schema_version_prefix(&prefix);
            let model = ClickHouseDataModel::derive(Arc::new(ontology)).unwrap();
            for property in model.graph().properties() {
                if let Some(PropertyRealization::Stored { column }) =
                    model.property_realization(property.id)
                {
                    assert!(!model.backend().storage().column(*column).name().is_empty());
                    assert!(
                        model
                            .backend()
                            .storage()
                            .table(column.table)
                            .name()
                            .starts_with(&prefix)
                    );
                }
            }
            let project = model.graph().entity_id("Project").unwrap();
            let lookup = model
                .backend()
                .traversal_path_lookup(project, ontology::TraversalPathKind::FullPath)
                .unwrap();
            assert_eq!(
                model.backend().storage().column(lookup.key).name(),
                "full_path"
            );
            let file = model.graph().entity_id("File").unwrap();
            let column = model.redaction_column(file).unwrap();
            assert_eq!(
                model.backend().storage().column(column).name(),
                "project_id"
            );
        }
        let invalid = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["Project"])
                .with_traversal_path_lookup("Project", "missing"),
        );
        assert!(
            ClickHouseDataModel::derive(invalid)
                .err()
                .unwrap()
                .to_string()
                .contains("gl_project.missing")
        );
        let invalid = Arc::new(
            ontology::Ontology::new()
                .with_nodes(["File"])
                .with_redaction("File", "project", "missing"),
        );
        assert!(
            ClickHouseDataModel::derive(invalid)
                .err()
                .unwrap()
                .to_string()
                .contains("gl_file.missing")
        );
    }

    #[test]
    fn foreign_keys_keep_semantic_endpoint_and_referenced_property() {
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
            assert_eq!(key.holder, holder);
            assert_eq!(model.graph().property(key.property).name, property);
            let target = match holder {
                Endpoint::Source => target,
                Endpoint::Target => source,
            };
            let referenced = model.graph().property(key.referenced_key);
            assert_eq!(referenced.name, "id");
            assert_eq!(referenced.entity, model.graph().entity_id(target).unwrap());
        }
        assert!(
            model
                .foreign_key(
                    &["AUTHORED".into(), "APPROVED".into()],
                    "User",
                    "MergeRequest"
                )
                .is_none()
        );
    }
}
