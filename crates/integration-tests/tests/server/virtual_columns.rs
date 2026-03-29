//! Tests for virtual column resolution dispatch logic.
//!
//! Exercises `resolve_virtual_columns` with `MockVirtualService` — no
//! ClickHouse or Gitaly needed. These will extend to cover the real
//! Gitaly service once it's wired up.

use std::collections::HashMap;
use std::sync::Arc;

use gkg_server::content::{MockVirtualService, PropertyRow, VirtualServiceRegistry};
use gkg_server::pipeline::HydrationStage;
use gkg_utils::arrow::ColumnValue;
use ontology::Ontology;
use query_engine::compiler::{SecurityContext, VirtualColumnRequest};
use query_engine::pipeline::{PipelineError, QueryPipelineContext, TypeMap};

fn test_ctx() -> QueryPipelineContext {
    let mut registry = VirtualServiceRegistry::new();
    registry.register("gitaly", Arc::new(MockVirtualService));

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(registry);

    QueryPipelineContext {
        query_json: String::new(),
        compiled: None,
        ontology: Arc::new(Ontology::new()),
        security_context: Some(SecurityContext::new(1, vec!["1/2/".into()]).unwrap()),
        server_extensions,
        phases: TypeMap::default(),
    }
}

type PropertyMap = HashMap<(String, i64), PropertyRow>;

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
    let ctx = test_ctx();
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &[])];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}

#[tokio::test]
async fn merges_results_into_property_map() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    for (_, props) in &map {
        assert_eq!(
            props.get("content"),
            Some(&ColumnValue::String("mock:blob_content".into()))
        );
    }
}

#[tokio::test]
async fn errors_without_registry() {
    let ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: None,
        ontology: Arc::new(Ontology::new()),
        security_context: Some(SecurityContext::new(1, vec!["1/2/".into()]).unwrap()),
        server_extensions: TypeMap::default(),
        phases: TypeMap::default(),
    };
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    let err = HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(matches!(err, PipelineError::ContentResolution(_)));
}

#[tokio::test]
async fn errors_for_unknown_service() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "unknown_service".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("File", &vcrs)];
    let mut map = file_property_map();

    let err = HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap_err();

    assert!(
        matches!(&err, PipelineError::ContentResolution(msg) if msg.contains("unknown_service"))
    );
}

#[tokio::test]
async fn skips_unmatched_entity_type() {
    let ctx = test_ctx();
    let vcrs = [VirtualColumnRequest {
        column_name: "content".into(),
        service: "gitaly".into(),
        lookup: "blob_content".into(),
    }];
    let specs: Vec<(&str, &[VirtualColumnRequest])> = vec![("Definition", &vcrs)];
    let mut map = file_property_map();
    let original_len = map.values().next().unwrap().len();

    HydrationStage::resolve_virtual_columns(&ctx, &specs, &mut map)
        .await
        .unwrap();

    assert_eq!(map.values().next().unwrap().len(), original_len);
}
