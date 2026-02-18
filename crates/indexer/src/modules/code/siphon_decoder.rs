//! Helper functions for decoding Siphon protobuf types.

use prost::Message;
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

fn decompress(payload: &[u8]) -> Result<Vec<u8>, String> {
    zstd::decode_all(payload).map_err(|e| format!("failed to decompress zstd payload: {e}"))
}

pub fn decode_logical_replication_events(
    payload: &[u8],
) -> Result<LogicalReplicationEvents, String> {
    let decompressed = decompress(payload)?;
    LogicalReplicationEvents::decode(decompressed.as_slice())
        .map_err(|e| format!("failed to decode LogicalReplicationEvents: {e}"))
}

pub fn get_column_index(events: &LogicalReplicationEvents, column_name: &str) -> Option<usize> {
    events.columns.iter().position(|c| c == column_name)
}

pub fn get_column_value(event: &ReplicationEvent, column_index: usize) -> Option<&Value> {
    event
        .columns
        .iter()
        .find(|c| c.column_index as usize == column_index)
        .and_then(|c| c.value.as_ref())
}

pub fn extract_i64(value: &Value) -> Option<i64> {
    match &value.value {
        Some(value::Value::Int64Value(v)) => Some(*v),
        _ => None,
    }
}

pub fn extract_i32(value: &Value) -> Option<i32> {
    match &value.value {
        Some(value::Value::Int16Value(v)) => Some(*v),
        Some(value::Value::Int64Value(v)) => Some(*v as i32),
        _ => None,
    }
}

pub struct ColumnExtractor<'a> {
    events: &'a LogicalReplicationEvents,
}

impl<'a> ColumnExtractor<'a> {
    pub fn new(events: &'a LogicalReplicationEvents) -> Self {
        Self { events }
    }

    pub fn get_i64(&self, event: &ReplicationEvent, column_name: &str) -> Option<i64> {
        let index = get_column_index(self.events, column_name)?;
        let value = get_column_value(event, index)?;
        extract_i64(value)
    }

    pub fn get_i32(&self, event: &ReplicationEvent, column_name: &str) -> Option<i32> {
        let index = get_column_index(self.events, column_name)?;
        let value = get_column_value(event, index)?;
        extract_i32(value)
    }

    pub fn get_string(&self, event: &'a ReplicationEvent, column_name: &str) -> Option<&'a str> {
        let index = get_column_index(self.events, column_name)?;
        let value = get_column_value(event, index)?;
        match &value.value {
            Some(value::Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use siphon_proto::replication_event::Column;

    fn compress(data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 0).expect("compression failed")
    }

    fn create_test_events(
        columns: Vec<&str>,
        event_columns: Vec<(u32, Value)>,
    ) -> LogicalReplicationEvents {
        let event = ReplicationEvent {
            operation: 2,
            columns: event_columns
                .into_iter()
                .map(|(idx, value)| Column {
                    column_index: idx,
                    value: Some(value),
                })
                .collect(),
        };

        LogicalReplicationEvents {
            event: 1,
            table: "events".to_string(),
            schema: "public".to_string(),
            application_identifier: "test".to_string(),
            events: vec![event],
            columns: columns.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn decode_and_extract_i64() {
        let events = create_test_events(
            vec!["id", "project_id"],
            vec![
                (
                    0,
                    Value {
                        value: Some(value::Value::Int64Value(123)),
                    },
                ),
                (
                    1,
                    Value {
                        value: Some(value::Value::Int64Value(456)),
                    },
                ),
            ],
        );

        let encoded = compress(&events.encode_to_vec());
        let decoded = decode_logical_replication_events(&encoded).unwrap();

        let extractor = ColumnExtractor::new(&decoded);
        let event = &decoded.events[0];

        assert_eq!(extractor.get_i64(event, "id"), Some(123));
        assert_eq!(extractor.get_i64(event, "project_id"), Some(456));
        assert_eq!(extractor.get_i64(event, "nonexistent"), None);
    }

    #[test]
    fn decode_and_extract_string() {
        let events = create_test_events(
            vec!["name", "ref"],
            vec![
                (
                    0,
                    Value {
                        value: Some(value::Value::StringValue("test".to_string())),
                    },
                ),
                (
                    1,
                    Value {
                        value: Some(value::Value::StringValue("refs/heads/main".to_string())),
                    },
                ),
            ],
        );

        let encoded = compress(&events.encode_to_vec());
        let decoded = decode_logical_replication_events(&encoded).unwrap();

        let extractor = ColumnExtractor::new(&decoded);
        let event = &decoded.events[0];

        assert_eq!(extractor.get_string(event, "name"), Some("test"));
        assert_eq!(extractor.get_string(event, "ref"), Some("refs/heads/main"));
    }
}
