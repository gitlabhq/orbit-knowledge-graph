use query_data_model::bindings::{BindingError, ExportOrigin, QueryBindings, RelationSource};
use query_data_model::storage::{StorageCatalog, StoredColumn, TableLayout};

fn catalog() -> StorageCatalog<()> {
    StorageCatalog::new([
        TableLayout::new("projects", vec![StoredColumn::new("id", ())], &[]).unwrap(),
    ])
    .unwrap()
}

#[test]
fn scans_and_derived_outputs_have_distinct_scoped_bindings() {
    let storage = catalog();
    let stored = storage.resolve_column("projects", "id").unwrap();
    let mut bindings = QueryBindings::new();
    let root = bindings.root();
    let left = bindings.scan(&storage, root, stored.table).unwrap();
    let right = bindings.scan(&storage, root, stored.table).unwrap();
    let left_id = bindings.stored_column(root, left, stored).unwrap();
    let right_id = bindings.stored_column(root, right, stored).unwrap();
    assert_ne!(left_id, right_id);
    assert_eq!(
        bindings.origin(left_id.export()).unwrap(),
        ExportOrigin::Stored(stored)
    );
    assert_eq!(
        bindings.column(root, right, left_id.export()),
        Err(BindingError::MissingExport)
    );

    let child = bindings.scope(root).unwrap();
    let scan = bindings.scan(&storage, child, stored.table).unwrap();
    let inner = bindings.stored_column(child, scan, stored).unwrap();
    let projected = bindings.project(child).unwrap();
    let derived = bindings.derived(root, child).unwrap();
    assert!(bindings.column(root, derived, projected).is_ok());
    assert_eq!(
        bindings.column(root, derived, inner.export()),
        Err(BindingError::MissingExport)
    );
    assert_eq!(
        bindings.column(root, scan, inner.export()),
        Err(BindingError::OutsideScope)
    );
    assert_eq!(bindings.project(child), Err(BindingError::OutsideScope));
    let foreign = QueryBindings::new();
    assert_eq!(
        bindings.scan(&storage, foreign.root(), stored.table),
        Err(BindingError::ForeignQuery)
    );
}

#[test]
fn definitions_share_outputs_and_unions_bind_positions() {
    let mut bindings = QueryBindings::new();
    let root = bindings.root();
    let body = bindings.scope(root).unwrap();
    let output = bindings.project(body).unwrap();
    let definition = bindings.define(root, body).unwrap();
    let left = bindings.reference(root, definition).unwrap();
    let right = bindings.reference(root, definition).unwrap();
    assert_ne!(
        bindings.column(root, left, output).unwrap(),
        bindings.column(root, right, output).unwrap()
    );
    assert!(bindings.reference(body, definition).is_ok());

    let nested = bindings.scope(body).unwrap();
    let private = bindings.define(body, nested).unwrap();
    assert_eq!(
        bindings.reference(root, private),
        Err(BindingError::OutsideScope)
    );
    let second = bindings.scope(root).unwrap();
    assert_eq!(
        bindings.union(root, &[body, second]),
        Err(BindingError::UnionShape)
    );
    let second_output = bindings.project(second).unwrap();
    let union = bindings.union(root, &[body, second]).unwrap();
    assert_eq!(
        bindings.source(union).unwrap(),
        &RelationSource::Union(vec![body, second])
    );
    assert_eq!(bindings.outputs(second).unwrap(), &[second_output]);
    assert_eq!(bindings.exports(union).unwrap(), &[output]);
    assert!(bindings.column(root, union, output).is_ok());
    assert_eq!(
        bindings.column(root, union, second_output),
        Err(BindingError::MissingExport)
    );
}
