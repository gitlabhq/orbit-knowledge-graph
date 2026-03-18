//! Shared test utilities for code indexing module tests.

use bytes::Bytes;
use prost::Message;
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

pub struct EventBuilder {
    columns: Vec<(&'static str, Value)>,
    operation: i32,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            operation: 2,
        }
    }

    pub fn with_operation(mut self, operation: i32) -> Self {
        self.operation = operation;
        self
    }

    pub fn with_i64(mut self, name: &'static str, val: i64) -> Self {
        self.columns.push((
            name,
            Value {
                value: Some(value::Value::Int64Value(val)),
            },
        ));
        self
    }

    pub fn with_string(mut self, name: &'static str, val: &str) -> Self {
        self.columns.push((
            name,
            Value {
                value: Some(value::Value::StringValue(val.to_string())),
            },
        ));
        self
    }

    pub fn build(self) -> (Vec<String>, ReplicationEvent) {
        let column_names: Vec<String> = self.columns.iter().map(|(n, _)| n.to_string()).collect();
        let event_columns: Vec<Column> = self
            .columns
            .into_iter()
            .enumerate()
            .map(|(idx, (_, value))| Column {
                column_index: idx as u32,
                value: Some(value),
            })
            .collect();

        let event = ReplicationEvent {
            operation: self.operation,
            columns: event_columns,
        };

        (column_names, event)
    }
}

pub fn build_replication_events(events: Vec<(Vec<String>, ReplicationEvent)>) -> Bytes {
    build_replication_events_for_table("p_knowledge_graph_code_indexing_tasks", events)
}

pub fn build_replication_events_for_table(
    table: &str,
    events: Vec<(Vec<String>, ReplicationEvent)>,
) -> Bytes {
    let (columns, events): (Vec<_>, Vec<_>) = events.into_iter().unzip();
    let column_names = columns.into_iter().next().unwrap_or_default();

    let payload = LogicalReplicationEvents {
        event: 1,
        table: table.to_string(),
        schema: "public".to_string(),
        application_identifier: "test".to_string(),
        events,
        columns: column_names,
        version_hash: 0,
    };

    let encoded = payload.encode_to_vec();
    let compressed = zstd::encode_all(encoded.as_slice(), 0).expect("compression failed");
    Bytes::from(compressed)
}

pub fn code_indexing_task_columns(
    id: i64,
    project_id: i64,
    ref_name: &str,
    commit_sha: &str,
    traversal_path: &str,
) -> EventBuilder {
    EventBuilder::new()
        .with_i64("id", id)
        .with_i64("project_id", project_id)
        .with_string("ref", ref_name)
        .with_string("commit_sha", commit_sha)
        .with_string("traversal_path", traversal_path)
}
