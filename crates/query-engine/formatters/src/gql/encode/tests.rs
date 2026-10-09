use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use compiler::{
    CompiledQueryContext, HydrationPlan, ParameterizedQuery, ResultContext, edge_kinds_column,
    neighbor_id_column, neighbor_is_outgoing_column, neighbor_type_column, path_column,
    relationship_type_column,
};
use orbit_utils::arrow::ColumnValue;
use serde_json::{Map, Value, json};
use shared::{PaginationMeta, PipelineOutput};
use types::QueryResult;

use super::{encode, literal, node_literal, render};

fn output(
    input: Value,
    context: &[(&str, &str)],
    columns: Vec<(String, ArrayRef)>,
    pagination: Option<PaginationMeta>,
) -> PipelineOutput {
    let input: compiler::Input = serde_json::from_value(input).unwrap();
    let mut result_context = ResultContext::new().with_query_type(input.query_type);
    for (alias, entity) in context {
        result_context.add_node(alias, entity);
    }
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect();
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let query_result = QueryResult::from_batches(&[batch], &result_context);
    PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: 0,
        query_type: input.query_type.to_string(),
        raw_query_strings: vec![],
        compiled: Arc::new(CompiledQueryContext {
            query_type: input.query_type,
            base: ParameterizedQuery {
                sql: String::new(),
                params: HashMap::new(),
                result_context: result_context.clone(),
                query_config: Default::default(),
                dialect: Default::default(),
            },
            hydration: HydrationPlan::None,
            input,
            pagination: Default::default(),
            has_virtual_columns: false,
        }),
        query_result,
        result_context,
        execution_log: vec![],
        pagination,
    }
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn column(name: &str, array: ArrayRef) -> (String, ArrayRef) {
    (name.to_string(), array)
}

fn hydrate(output: &mut PipelineOutput, key: &str, value: &str) {
    for row in output.query_result.rows_mut() {
        for node in row.dynamic_nodes_mut() {
            if node.entity_type == "User" {
                node.properties
                    .insert(key.into(), ColumnValue::String(value.into()));
            }
        }
    }
}

fn cells(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn renders_a_padded_table_with_a_row_count() {
    let table = render(
        &cells(&["u", "g"]),
        &[
            cells(&["(:User {id: 1})", "(:Group {id: 22})"]),
            cells(&["NULL", "(:Group {id: 3})"]),
        ],
        None,
    );
    assert_eq!(
        table,
        "+-------------------------------------+\n\
         | u               | g                 |\n\
         +-------------------------------------+\n\
         | (:User {id: 1}) | (:Group {id: 22}) |\n\
         | NULL            | (:Group {id: 3})  |\n\
         +-------------------------------------+\n\
         \n\
         2 rows\n"
    );
}

#[test]
fn empty_results_keep_the_header_and_report_more_pages() {
    let page = PaginationMeta {
        has_more: true,
        truncated: true,
        next_cursor: Some("abc".into()),
    };
    assert_eq!(
        render(&cells(&["n"]), &[], Some(&page)),
        "+---+\n| n |\n+---+\n+---+\n\n0 rows, more available\nnext_cursor: \"abc\"\n"
    );
    assert!(render(&cells(&["n"]), &[cells(&["1"])], None).ends_with("\n1 row\n"));
}

#[test]
fn values_use_cypher_shell_literals() {
    let mut properties = Map::new();
    for (key, value) in [
        ("username", json!("zoë \"z\"")),
        ("confidential", json!(false)),
        ("score", json!(2.0)),
        ("milestone", json!(null)),
        ("labels", json!(["bug", "p1"])),
    ] {
        properties.insert(key.into(), value);
    }
    assert_eq!(
        node_literal("User", 7, &properties),
        "(:User {id: 7, username: \"zoë \\\"z\\\"\", confidential: FALSE, labels: [\"bug\", \"p1\"], score: 2.0})"
    );
    assert_eq!(literal(&json!(null), "x"), "NULL");
    assert_eq!(literal(&json!(true), "x"), "TRUE");
    assert!(literal(&json!("x".repeat(250)), "description").ends_with("...\""));
}

#[test]
fn traversal_keeps_every_result_row_and_reports_the_next_page() {
    let page = PaginationMeta {
        has_more: true,
        truncated: true,
        next_cursor: Some("abc".into()),
    };
    let output = output(
        json!({"query_type": "traversal", "nodes": [{"id": "u", "entity": "User"}, {"id": "g", "entity": "Group"}]}),
        &[("u", "User"), ("g", "Group")],
        vec![
            column("_gkg_u_id", ints(&[1, 1])),
            column("_gkg_u_type", strings(&["User", "User"])),
            column("u_username", strings(&["root", "root"])),
            column("_gkg_g_id", ints(&[22, 22])),
            column("_gkg_g_type", strings(&["Group", "Group"])),
        ],
        Some(page),
    );
    assert_eq!(
        encode(&output),
        "+-------------------------------------------------------+\n\
         | u                                 | g                 |\n\
         +-------------------------------------------------------+\n\
         | (:User {id: 1, username: \"root\"}) | (:Group {id: 22}) |\n\
         | (:User {id: 1, username: \"root\"}) | (:Group {id: 22}) |\n\
         +-------------------------------------------------------+\n\
         \n\
         2 rows, more available\n\
         next_cursor: \"abc\"\n"
    );
}

#[test]
fn node_groups_render_as_nodes() {
    let output = output(
        json!({"query_type": "aggregation",
               "nodes": [{"id": "g", "entity": "Group"}, {"id": "u", "entity": "User"}],
               "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
               "group_by": ["g"], "aggregations": [{"count": "u", "as": "user_count"}]}),
        &[("g", "Group")],
        vec![
            column("_gkg_g_id", ints(&[22])),
            column("_gkg_g_type", strings(&["Group"])),
            column("g_name", strings(&["Toolbox"])),
            column("user_count", ints(&[3])),
        ],
        None,
    );
    let table = encode(&output);
    assert!(
        table.contains("| g                                  | user_count |"),
        "{table}"
    );
    assert!(
        table.contains("| (:Group {id: 22, name: \"Toolbox\"}) | 3          |"),
        "{table}"
    );
}

#[test]
fn neighbors_follow_the_stored_edge_direction() {
    let mut output = output(
        json!({"query_type": "neighbors", "nodes": [{"id": "c", "entity": "Group", "node_ids": [22]}],
               "neighbors": {"direction": "both"}}),
        &[("c", "Group")],
        vec![
            column("_gkg_c_id", ints(&[22, 22])),
            column("_gkg_c_type", strings(&["Group", "Group"])),
            column(neighbor_id_column(), ints(&[1, 2])),
            column(neighbor_type_column(), strings(&["User", "Project"])),
            column(
                relationship_type_column(),
                strings(&["MEMBER_OF", "CONTAINS"]),
            ),
            column(neighbor_is_outgoing_column(), ints(&[0, 1])),
        ],
        None,
    );
    hydrate(&mut output, "username", "root");
    let table = encode(&output);
    assert!(
        table.contains("(:Group {id: 22})<-[:MEMBER_OF]-(:User {id: 1, username: \"root\"})"),
        "{table}"
    );
    assert!(
        table.contains("(:Group {id: 22})-[:CONTAINS]->(:Project {id: 2})"),
        "{table}"
    );
    assert!(table.ends_with("\n2 rows\n"), "{table}");
}

#[test]
fn path_finding_prints_each_distinct_path_once() {
    let ids = Int64Array::from(vec![1, 2, 3, 1, 2, 3, 1, 4, 3]);
    let labels = StringArray::from(vec![
        "User", "Group", "Project", "User", "Group", "Project", "User", "Group", "Project",
    ]);
    let nodes = StructArray::new(
        vec![
            Arc::new(Field::new("1", DataType::Int64, false)),
            Arc::new(Field::new("2", DataType::Utf8, false)),
        ]
        .into(),
        vec![Arc::new(ids) as _, Arc::new(labels) as _],
        None,
    );
    let node_field = Arc::new(Field::new("item", nodes.data_type().clone(), true));
    let paths = ListArray::new(
        node_field,
        OffsetBuffer::new(vec![0i32, 3, 6, 9].into()),
        Arc::new(nodes),
        None,
    );
    let kinds = StringArray::from(vec![
        "MEMBER_OF",
        "CONTAINS",
        "MEMBER_OF",
        "CONTAINS",
        "MEMBER_OF",
        "CONTAINS",
    ]);
    let kinds = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 2, 4, 6].into()),
        Arc::new(kinds),
        None,
    );
    let mut output = output(
        json!({"query_type": "path_finding",
               "nodes": [{"id": "u", "entity": "User"}, {"id": "p", "entity": "Project"}],
               "path": {"type": "shortest", "from": "u", "to": "p", "max_depth": 2}}),
        &[],
        vec![
            (path_column().to_string(), Arc::new(paths) as ArrayRef),
            (edge_kinds_column().to_string(), Arc::new(kinds) as ArrayRef),
        ],
        None,
    );
    hydrate(&mut output, "username", "root");
    let table = encode(&output);
    assert!(table.contains("| (:User {id: 1, username: \"root\"})-[:MEMBER_OF]->(:Group {id: 2})-[:CONTAINS]->(:Project {id: 3}) |"), "{table}");
    assert!(table.contains("(:Group {id: 4})"), "{table}");
    assert!(table.ends_with("\n2 rows\n"), "{table}");
}

#[test]
fn truncated_text_reports_its_length_and_long_cells_skip_padding() {
    let mut properties = Map::new();
    properties.insert("description".into(), json!("x".repeat(250)));
    assert!(node_literal("Issue", 1, &properties).ends_with("description_len: 250})"));

    let long = "y".repeat(200);
    let table = render(&cells(&["n"]), &[cells(&[&long]), cells(&["short"])], None);
    let short = table.lines().find(|line| line.contains("short")).unwrap();
    assert_eq!(short.chars().count(), 124, "{table}");
    assert!(
        table.starts_with(&format!("+{}+\n", "-".repeat(122))),
        "{table}"
    );
}
