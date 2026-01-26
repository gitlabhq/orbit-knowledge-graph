use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use etl_engine::entities::{DataType, Entity, Field};
use etl_engine::module::{Handler, HandlerContext, HandlerError, Module};
use etl_engine::types::{Envelope, Topic};
use tracing::info;

const ENTITY_NAME: &str = "placeholder_events";

fn placeholder_entity() -> Entity {
    Entity::Node {
        name: ENTITY_NAME.to_string(),
        fields: vec![
            Field {
                name: "id".into(),
                data_type: DataType::String,
                nullable: false,
                default: None,
            },
            Field {
                name: "payload".into(),
                data_type: DataType::String,
                nullable: false,
                default: None,
            },
            Field {
                name: "attempt".into(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
            },
        ],
        primary_keys: vec!["id".to_string()],
    }
}

pub struct PlaceholderModule;

impl Module for PlaceholderModule {
    fn name(&self) -> &str {
        "placeholder"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(PlaceholderHandler)]
    }

    fn entities(&self) -> Vec<Entity> {
        vec![placeholder_entity()]
    }
}

struct PlaceholderHandler;

#[async_trait]
impl Handler for PlaceholderHandler {
    fn name(&self) -> &str {
        "placeholder-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new("placeholder-stream", "placeholder.events")
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        info!(message_id = %message.id.0, "processing placeholder message");

        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Utf8, false),
            ArrowField::new("payload", ArrowDataType::Utf8, false),
            ArrowField::new("attempt", ArrowDataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![message.id.0.as_ref()])),
                Arc::new(StringArray::from(vec![
                    String::from_utf8_lossy(&message.payload).as_ref(),
                ])),
                Arc::new(Int64Array::from(vec![message.attempt as i64])),
            ],
        )
        .map_err(|e| HandlerError::Processing(e.to_string()))?;

        let writer = context
            .destination
            .new_batch_writer(&placeholder_entity())
            .await
            .map_err(|e| HandlerError::Processing(e.to_string()))?;
        writer
            .write_batch(&[batch])
            .await
            .map_err(|e| HandlerError::Processing(e.to_string()))?;

        Ok(())
    }
}
