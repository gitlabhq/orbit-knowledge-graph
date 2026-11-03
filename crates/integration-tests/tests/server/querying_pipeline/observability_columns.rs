use std::sync::Arc;

use integration_testkit::seeded_resolver::SeededColumnResolver;
use orbit_arrow::ColumnValue;
use query_engine::compiler::VirtualColumnRequest;
use query_engine::shared::content::{
    ColumnResolverRegistry, PropertyMap, PropertyRow, ResolverContext, resolve_virtual_columns,
};

const SERVICE: &str = "observability";

fn registry() -> ColumnResolverRegistry {
    let mut registry = ColumnResolverRegistry::new();
    registry.register(SERVICE, Arc::new(SeededColumnResolver::from_seed_file()));
    registry
}

fn request(column_name: &str, lookup: &str) -> VirtualColumnRequest {
    VirtualColumnRequest {
        column_name: column_name.into(),
        service: SERVICE.into(),
        lookup: lookup.into(),
    }
}

fn property_map(entity: &str, ids: &[i64]) -> PropertyMap {
    let mut map = PropertyMap::new();
    for &id in ids {
        let mut props = PropertyRow::new();
        props.insert("id".into(), ColumnValue::Int64(id));
        props.insert(
            "traversal_path".into(),
            ColumnValue::String("9970/12345/".into()),
        );
        map.insert((entity.into(), id), props);
    }
    map
}

async fn resolve(entity: &str, ids: &[i64], requests: &[VirtualColumnRequest]) -> PropertyMap {
    let mut map = property_map(entity, ids);
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![(entity, requests)];

    resolve_virtual_columns(
        &registry(),
        &ResolverContext::default(),
        &specs,
        &mut map,
        100,
    )
    .await
    .expect("resolution should succeed");

    map
}

fn column<'a>(map: &'a PropertyMap, entity: &str, id: i64, name: &str) -> Option<&'a ColumnValue> {
    map.get(&(entity.to_string(), id))?.get(name)
}

#[tokio::test]
async fn resolves_a_pipeline_trace_id() {
    let map = resolve(
        "Pipeline",
        &[3000],
        &[request("observability_trace_id", "pipeline_trace_id")],
    )
    .await;

    assert_eq!(
        column(&map, "Pipeline", 3000, "observability_trace_id"),
        Some(&ColumnValue::String(
            "4bf92f3577b34da6a3ce929d0e0e4736".into()
        ))
    );
}

#[tokio::test]
async fn resolves_several_pipeline_columns_in_one_pass() {
    let map = resolve(
        "Pipeline",
        &[3000],
        &[
            request("observability_trace_id", "pipeline_trace_id"),
            request(
                "observability_trace_duration_nano",
                "pipeline_trace_duration",
            ),
            request(
                "observability_trace_error_count",
                "pipeline_trace_error_count",
            ),
        ],
    )
    .await;

    assert!(column(&map, "Pipeline", 3000, "observability_trace_id").is_some());
    assert!(column(&map, "Pipeline", 3000, "observability_trace_duration_nano").is_some());
    assert!(column(&map, "Pipeline", 3000, "observability_trace_error_count").is_some());
}

#[tokio::test]
async fn a_pipeline_without_a_trace_resolves_to_null() {
    let map = resolve(
        "Pipeline",
        &[3001],
        &[request("observability_trace_id", "pipeline_trace_id")],
    )
    .await;

    assert!(column(&map, "Pipeline", 3001, "observability_trace_id").is_none());
}

#[tokio::test]
async fn an_unseeded_pipeline_resolves_to_null() {
    let map = resolve(
        "Pipeline",
        &[9999],
        &[request("observability_trace_id", "pipeline_trace_id")],
    )
    .await;

    assert!(column(&map, "Pipeline", 9999, "observability_trace_id").is_none());
}

/// Each row keeps its own value: a resolved row must not leak into an
/// unresolved one sharing the batch.
#[tokio::test]
async fn rows_in_one_batch_do_not_share_values() {
    let map = resolve(
        "Pipeline",
        &[3000, 3001, 9999],
        &[request("observability_trace_id", "pipeline_trace_id")],
    )
    .await;

    assert!(column(&map, "Pipeline", 3000, "observability_trace_id").is_some());
    assert!(column(&map, "Pipeline", 3001, "observability_trace_id").is_none());
    assert!(column(&map, "Pipeline", 9999, "observability_trace_id").is_none());
}

#[tokio::test]
async fn resolves_a_job_span() {
    let map = resolve(
        "Job",
        &[4000],
        &[request("observability_span_id", "job_span_id")],
    )
    .await;

    assert_eq!(
        column(&map, "Job", 4000, "observability_span_id"),
        Some(&ColumnValue::String("00f067aa0ba902b7".into()))
    );
}

#[tokio::test]
async fn resolves_a_job_log_tail_across_several_lines() {
    let map = resolve(
        "Job",
        &[4000],
        &[request("observability_log_tail", "job_log_tail")],
    )
    .await;

    let Some(ColumnValue::String(tail)) = column(&map, "Job", 4000, "observability_log_tail")
    else {
        panic!("expected a log tail");
    };
    assert!(tail.contains("bundle exec rspec"));
    assert_eq!(tail.lines().count(), 3);
}

#[tokio::test]
async fn an_unknown_lookup_resolves_to_null() {
    let map = resolve(
        "Pipeline",
        &[3000],
        &[request("observability_trace_id", "not_a_lookup")],
    )
    .await;

    assert!(column(&map, "Pipeline", 3000, "observability_trace_id").is_none());
}
