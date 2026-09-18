use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, HydrationPlan, ParameterizedQuery, QueryType, ResultContext};
use formatters::{
    FormatName, GraphFormatter, RAW_OUTPUT_FORMAT_VERSION, ResultFormatter, ToonFormatter,
    column_value_to_json,
};
use orbit_utils::arrow::ColumnValue;
use orbit_utils::toon::encode;
use serde_json::json;
use shared::PipelineOutput;
use types::QueryResult;

#[test]
fn graph_payload_keeps_ids_properties_and_float_wire_tokens() {
    let batch = RecordBatch::try_from_iter([
        (
            "_gkg_p_id",
            Arc::new(Int64Array::from(vec![1, i64::MAX])) as ArrayRef,
        ),
        (
            "_gkg_p_type",
            Arc::new(StringArray::from(vec!["Project", "Project"])) as ArrayRef,
        ),
        (
            "p_score",
            Arc::new(Float64Array::from(vec![2_f64.powi(63), 2_f64.powi(64)])) as ArrayRef,
        ),
        (
            "p_name",
            Arc::new(StringArray::from(vec!["世界", "true"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut context = ResultContext::new().with_query_type(QueryType::Traversal);
    context.add_node("p", "Project");
    let result = QueryResult::from_batches(&[batch], &context);
    let output = PipelineOutput {
        row_count: result.authorized_count(),
        redacted_count: 0,
        query_type: "traversal".into(),
        raw_query_strings: vec![],
        compiled: Arc::new(CompiledQueryContext {
            query_type: QueryType::Traversal,
            base: ParameterizedQuery {
                sql: String::new(),
                params: HashMap::new(),
                result_context: context.clone(),
                query_config: Default::default(),
                dialect: Default::default(),
            },
            hydration: HydrationPlan::None,
            input: serde_json::from_value(json!({
                "query_type": "traversal", "nodes": [{"id": "p", "entity": "Project"}], "limit": 10
            }))
            .unwrap(),
        }),
        query_result: result,
        result_context: context,
        execution_log: vec![],
        pagination: None,
    };
    let raw = GraphFormatter.format(&output);
    let raw_version = RAW_OUTPUT_FORMAT_VERSION.to_string();
    assert_eq!(raw["format_version"], raw_version);
    assert_eq!(raw["nodes"][1]["id"], i64::MAX.to_string());
    assert_eq!(raw["nodes"][0]["score"], json!(2_f64.powi(63)));
    let (text, version, name) = ToonFormatter.format_stamped(&output);
    assert_eq!(name, FormatName::Toon);
    assert_eq!(version, "2.0.0");
    let expected = [
        format!(
            concat!(
                "format_version: {}\nquery_type: traversal\nnodes[2]{{type,id,score,name}}:\n",
                "  Project,\"1\",9223372036854776000,世界\n",
                "  Project,\"9223372036854775807\",18446744073709552000,\"true\"\nedges: []"
            ),
            raw_version
        ),
        format!(
            concat!(
                "format_version: {}\nquery_type: traversal\nnodes[2]{{type,id,name,score}}:\n",
                "  Project,\"1\",世界,9223372036854776000\n",
                "  Project,\"9223372036854775807\",\"true\",18446744073709552000\nedges: []"
            ),
            raw_version
        ),
    ];
    let text = text.as_str().unwrap();
    assert!(expected.iter().any(|expected| expected == text), "{text}");
}

#[test]
fn column_value_numeric_and_keyword_properties_encode_without_corruption() {
    let value = json!({
        "false": false, "null": null, "true": true,
        "min": column_value_to_json(&ColumnValue::Int64(i64::MIN)),
        "finite": column_value_to_json(&ColumnValue::Float64(2_f64.powi(64))),
        "nonfinite": column_value_to_json(&ColumnValue::Float64(f64::INFINITY)),
    });
    assert_eq!(
        encode(&value).unwrap(),
        concat!(
            "false: false\nnull: null\ntrue: true\nmin: -9223372036854775808\n",
            "finite: 18446744073709552000\nnonfinite: null"
        )
    );
}
