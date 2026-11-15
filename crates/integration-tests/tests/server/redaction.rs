use crate::common::{MockRedactionService, load_ontology, run_redaction};
use orbit_server::redaction::QueryResult;
use query_engine::compiler::build_entity_auth;

/// A row with a NULL entity ID is unverifiable and must fail closed (it can
/// arise from outer joins or data inconsistencies).
#[test]
fn fail_closed_null_id_denies_row() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::compiler::ResultContext;
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    let ontology = load_ontology();

    let batch = make_batch(vec![
        (
            "_orbit_u_id",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn Array>,
        ),
        (
            "_orbit_u_type",
            Arc::new(StringArray::from(vec!["User", "User", "User"])) as Arc<dyn Array>,
        ),
        (
            "_orbit_p_id",
            Arc::new(Int64Array::from(vec![100, 200, 300])) as Arc<dyn Array>,
        ),
        (
            "_orbit_p_type",
            Arc::new(StringArray::from(vec!["Project", "Project", "Project"])) as Arc<dyn Array>,
        ),
    ]);

    let mut ctx = ResultContext::new();
    ctx.add_node("u", "User");
    ctx.add_node("p", "Project");
    for (entity, config) in build_entity_auth(&ontology) {
        ctx.add_entity_auth(entity, config);
    }

    let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 3]);
    mock_service.allow("project", &[100, 200, 300]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "row with NULL user ID must be redacted");
    assert_eq!(result.authorized_count(), 2);
    assert!(
        result.rows()[0].is_authorized(),
        "user 1 should be authorized"
    );
    assert!(
        !result.rows()[1].is_authorized(),
        "NULL ID row must be denied (fail-closed)"
    );
    assert!(
        result.rows()[2].is_authorized(),
        "user 3 should be authorized"
    );
}

#[test]
fn fail_closed_null_type_denies_row() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::compiler::ResultContext;
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    let ontology = load_ontology();

    let batch = make_batch(vec![
        (
            "_orbit_u_id",
            Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
        ),
        (
            "_orbit_u_type",
            Arc::new(StringArray::from(vec![Some("User"), None])) as Arc<dyn Array>,
        ),
    ]);

    let mut ctx = ResultContext::new();
    ctx.add_node("u", "User");
    for (entity, config) in build_entity_auth(&ontology) {
        ctx.add_entity_auth(entity, config);
    }

    let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "row with NULL type must be redacted");
    assert!(result.rows()[0].is_authorized());
    assert!(
        !result.rows()[1].is_authorized(),
        "NULL type row must be denied (fail-closed)"
    );
}
