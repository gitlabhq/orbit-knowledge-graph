//! Shared test utilities for code indexing module tests.

use bytes::Bytes;
use prost::Message;
use siphon_proto::replication_event::Column;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

use super::config::siphon_actions;

pub struct EventBuilder {
    columns: Vec<(&'static str, Value)>,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self { columns: vec![] }
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

    pub fn with_i32(mut self, name: &'static str, val: i32) -> Self {
        self.columns.push((
            name,
            Value {
                value: Some(value::Value::Int16Value(val)),
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
            operation: 2,
            columns: event_columns,
        };

        (column_names, event)
    }
}

pub fn build_replication_events(events: Vec<(Vec<String>, ReplicationEvent)>) -> Bytes {
    let (columns, events): (Vec<_>, Vec<_>) = events.into_iter().unzip();
    let column_names = columns.into_iter().next().unwrap_or_default();

    let payload = LogicalReplicationEvents {
        event: 1,
        table: "events".to_string(),
        schema: "public".to_string(),
        application_identifier: "test".to_string(),
        events,
        columns: column_names,
    };

    let encoded = payload.encode_to_vec();
    let compressed = zstd::encode_all(encoded.as_slice(), 0).expect("compression failed");
    Bytes::from(compressed)
}

pub fn push_event_columns(id: i64, project_id: i64, author_id: i64) -> EventBuilder {
    EventBuilder::new()
        .with_i64("id", id)
        .with_i32("action", siphon_actions::PUSH_EVENT)
        .with_i64("project_id", project_id)
        .with_i64("author_id", author_id)
        .with_string("created_at", "2024-01-15T10:00:00Z")
}

pub fn push_payload_columns(
    event_id: i64,
    project_id: Option<i64>,
    ref_name: &str,
    commit: &str,
) -> EventBuilder {
    use super::config::{siphon_actions, siphon_ref_types};

    let mut builder = EventBuilder::new()
        .with_i64("event_id", event_id)
        .with_i32("ref_type", siphon_ref_types::BRANCH)
        .with_i32("action", siphon_actions::PUSHED)
        .with_string("ref", ref_name)
        .with_string("commit_to", commit);

    if let Some(id) = project_id {
        builder = builder.with_i64("project_id", id);
    }

    builder
}
