use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, HydrationPlan, ParameterizedQuery, QueryType, ResultContext};
use formatters::{
    FormatName, GraphFormatter, RAW_OUTPUT_FORMAT_VERSION, ResultFormatter,
    TOON_OUTPUT_FORMAT_VERSION, ToonFormatter, column_value_to_json,
};
use orbit_utils::arrow::ColumnValue;
use orbit_utils::toon::encode;
use serde_json::{Value, json};
use shared::PipelineOutput;
use types::QueryResult;

fn pipeline_output(batch: RecordBatch, context: ResultContext, input: Value) -> PipelineOutput {
    let query_type = context.query_type.unwrap();
    let result = QueryResult::from_batches(&[batch], &context);
    PipelineOutput {
        row_count: result.authorized_count(),
        redacted_count: 0,
        query_type: query_type.to_string(),
        raw_query_strings: vec![],
        compiled: Arc::new(CompiledQueryContext {
            query_type,
            base: ParameterizedQuery {
                sql: String::new(),
                params: HashMap::new(),
                result_context: context.clone(),
                query_config: Default::default(),
                dialect: Default::default(),
            },
            hydration: HydrationPlan::None,
            input: serde_json::from_value(input).unwrap(),
        }),
        query_result: result,
        result_context: context,
        execution_log: vec![],
        pagination: None,
    }
}

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
    let output = pipeline_output(
        batch,
        context,
        json!({
            "query_type": "traversal", "nodes": [{"id": "p", "entity": "Project"}], "limit": 10
        }),
    );
    let raw = GraphFormatter.format(&output);
    let raw_version = RAW_OUTPUT_FORMAT_VERSION.to_string();
    assert_eq!(raw["format_version"], raw_version);
    assert_eq!(raw["nodes"][1]["id"], i64::MAX.to_string());
    assert_eq!(raw["nodes"][0]["score"], json!(2_f64.powi(63)));
    assert_eq!(
        raw["nodes"][0],
        json!({"id": "1", "name": "世界", "score": 2_f64.powi(63), "type": "Project"})
    );
    assert_eq!(
        serde_json::to_string(&raw["nodes"][0]["score"]).unwrap(),
        "9.223372036854776e+18"
    );
    let (text, version, name) = ToonFormatter.format_stamped(&output);
    assert_eq!(name, FormatName::Toon);
    assert_eq!(version, TOON_OUTPUT_FORMAT_VERSION.to_string());
    let expected = format!(
        concat!(
            "format_version: {}\nquery_type: traversal\nnodes[2]{{type,id,name,score}}:\n",
            "  Project,\"1\",世界,9223372036854776000\n",
            "  Project,\"9223372036854775807\",\"true\",18446744073709552000\nedges: []"
        ),
        raw_version
    );
    assert_eq!(text.as_str().unwrap(), expected);
    assert_eq!(GraphFormatter.format(&output), raw);
}

#[test]
fn aggregation_maps_are_sorted_only_for_toon_and_rows_keep_their_order() {
    let batch = RecordBatch::try_from_iter([
        (
            "_gkg_p_id",
            Arc::new(Int64Array::from(vec![9, 2])) as ArrayRef,
        ),
        (
            "_gkg_p_type",
            Arc::new(StringArray::from(vec!["Project", "Project"])) as ArrayRef,
        ),
        (
            "p_zebra",
            Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
        ),
        (
            "p_alpha",
            Arc::new(StringArray::from(vec!["z", "a"])) as ArrayRef,
        ),
        ("total", Arc::new(Int64Array::from(vec![7, 5])) as ArrayRef),
    ])
    .unwrap();
    let mut context = ResultContext::new().with_query_type(QueryType::Aggregation);
    context.add_node("p", "Project");
    let output = pipeline_output(
        batch,
        context,
        json!({
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project"}],
            "group_by": ["p"],
            "aggregations": [{"count": "p", "as": "total"}],
            "limit": 10
        }),
    );
    let raw = GraphFormatter.format(&output);
    let text = ToonFormatter.format(&output);
    assert!(
        text.as_str().unwrap().contains(concat!(
            "rows[2]{p{id,properties{alpha,zebra},type},total}:\n",
            "  \"9\",z,3,Project,7\n",
            "  \"2\",a,4,Project,5"
        )),
        "{text}"
    );
    assert_eq!(GraphFormatter.format(&output), raw);
}

#[test]
fn column_value_numeric_and_keyword_properties_encode_without_corruption() {
    let mut value = json!({
        "false": false, "null": null, "true": true,
        "min": column_value_to_json(&ColumnValue::Int64(i64::MIN)),
        "finite": column_value_to_json(&ColumnValue::Float64(2_f64.powi(64))),
        "nonfinite": column_value_to_json(&ColumnValue::Float64(f64::INFINITY)),
    });
    value.sort_all_objects();
    assert_eq!(
        encode(&value).unwrap(),
        concat!(
            "false: false\nfinite: 18446744073709552000\nmin: -9223372036854775808\n",
            "nonfinite: null\nnull: null\ntrue: true"
        )
    );
}
