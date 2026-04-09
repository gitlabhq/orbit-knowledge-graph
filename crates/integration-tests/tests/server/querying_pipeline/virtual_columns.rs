//! Tests for virtual column resolution dispatch logic.
//!
//! Exercises `resolve_virtual_columns` with `MockColumnResolver` -- no
//! ClickHouse or Gitaly needed.

use std::sync::Arc;

use gkg_utils::arrow::ColumnValue;
use query_engine::compiler::VirtualColumnRequest;
use query_engine::shared::content::{
    ColumnResolverRegistry, PropertyMap, PropertyRow, ResolverContext, resolve_virtual_columns,
};

use super::common::MockColumnResolver;

fn test_registry() -> ColumnResolverRegistry {
    test_registry_with_batch_size(100)
}

fn test_registry_with_batch_size(max_batch_size: usize) -> ColumnResolverRegistry {
    let mut registry = ColumnResolverRegistry::new().with_max_batch_size(max_batch_size);
    registry.register("gitaly", Arc::new(MockColumnResolver));
    registry
}

fn file_property_map() -> PropertyMap {
    let mut props = PropertyRow::new();
    props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
    let mut map = PropertyMap::new();
    map.insert(("File".into(), 1), props.clone());
    map.insert(("File".into(), 2), props);
    map
}

#[tokio::test]
async fn skips_when_no_virtual_columns() {
    let registry = test_registry();
    let rctx = ResolverContext::default();
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &[])];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    resolve_virtual_columns(&registry, &rctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}

#[tokio::test]
async fn merges_results_into_property_map() {
    let registry = test_registry();
    let rctx = ResolverContext::default();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    resolve_virtual_columns(&registry, &rctx, &specs, &mut map)
        .await
        .unwrap();

    for props in map.values() {
        assert_eq!(
            props.get("content"),
            Some(&ColumnValue::String("mock:blob_content".into()))
        );
    }
}

#[tokio::test]
async fn errors_for_unknown_service() {
    let registry = test_registry();
    let rctx = ResolverContext::default();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "unknown_service".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    let err = resolve_virtual_columns(&registry, &rctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(
        matches!(&err, query_engine::pipeline::PipelineError::ContentResolution(msg) if msg.contains("unknown_service"))
    );
}

#[tokio::test]
async fn skips_unmatched_entity_type() {
    let registry = test_registry();
    let rctx = ResolverContext::default();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("Definition", &vcrs)];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    resolve_virtual_columns(&registry, &rctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}

#[tokio::test]
async fn errors_when_batch_size_exceeded() {
    let registry = test_registry_with_batch_size(1);
    let rctx = ResolverContext::default();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map(); // 2 File entries, limit is 1

    let err = resolve_virtual_columns(&registry, &rctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(
        matches!(&err, query_engine::pipeline::PipelineError::ContentResolution(msg) if msg.contains("batch size"))
    );
}
