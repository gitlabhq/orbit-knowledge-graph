//! NATS handler for mailbox messages.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Topic};
use tracing::{debug, warn};

use crate::handler::{ArrowConverter, DeduplicationStore};
use crate::storage::{PluginStore, TraversalPathResolver};
use crate::types::{MailboxMessage, NodePayload, NodeReference};
use crate::validation::MessageValidator;

pub const MAILBOX_STREAM: &str = "mailbox-stream";
pub const MAILBOX_SUBJECT: &str = "mailbox.messages";

pub struct MailboxHandler {
    plugin_store: Arc<PluginStore>,
    traversal_resolver: Arc<TraversalPathResolver>,
}

impl MailboxHandler {
    pub fn new(
        plugin_store: Arc<PluginStore>,
        traversal_resolver: Arc<TraversalPathResolver>,
    ) -> Self {
        Self {
            plugin_store,
            traversal_resolver,
        }
    }
}

#[async_trait]
impl Handler for MailboxHandler {
    fn name(&self) -> &str {
        "mailbox-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new(MAILBOX_STREAM, MAILBOX_SUBJECT)
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        debug!(message_id = %message.id.0.to_string(), "received mailbox message");
        let mailbox_message: MailboxMessage = message.to_event().map_err(|e| {
            HandlerError::Processing(format!("failed to deserialize message: {}", e))
        })?;

        debug!(message_id = %mailbox_message.message_id, "deserialized mailbox message");
        let dedup = DeduplicationStore::new(context.nats.clone());

        debug!(message_id = %mailbox_message.message_id, "checking for duplicates");
        if dedup
            .is_duplicate(&mailbox_message.message_id)
            .await
            .unwrap_or(false)
        {
            debug!(
                message_id = %mailbox_message.message_id,
                "skipping duplicate message"
            );
            return Ok(());
        }

        debug!(message_id = %mailbox_message.message_id, "getting plugin");
        let plugin = self
            .plugin_store
            .get(&mailbox_message.plugin_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to get plugin: {}", e)))?
            .ok_or_else(|| {
                HandlerError::Processing(format!("plugin not found: {}", mailbox_message.plugin_id))
            })?;

        debug!(message_id = %mailbox_message.message_id, "validating message");
        MessageValidator::validate(&mailbox_message, &plugin)
            .map_err(|e| HandlerError::Processing(format!("validation failed: {}", e)))?;

        debug!(message_id = %mailbox_message.message_id, "resolving traversal path");
        let traversal_path = self
            .traversal_resolver
            .resolve(plugin.namespace_id)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to resolve traversal path: {}", e))
            })?;

        debug!(message_id = %mailbox_message.message_id, "grouping nodes by kind");
        let nodes_by_kind = group_nodes_by_kind(&mailbox_message.nodes);

        for (node_kind, nodes) in nodes_by_kind {
            debug!(message_id = %mailbox_message.message_id, "getting node definition for kind: {}", node_kind);
            let node_definition = plugin.schema.get_node(&node_kind).ok_or_else(|| {
                HandlerError::Processing(format!("unknown node kind: {}", node_kind))
            })?;

            debug!(message_id = %mailbox_message.message_id, "building node batch for kind: {}", node_kind);
            let batch =
                ArrowConverter::build_node_batch(&plugin, node_definition, &nodes, &traversal_path)
                    .map_err(|e| {
                        HandlerError::Processing(format!("failed to build batch: {}", e))
                    })?;

            debug!(message_id = %mailbox_message.message_id, "writing nodes to table");
            if batch.num_rows() > 0 {
                debug!(message_id = %mailbox_message.message_id, "creating writer for table");
                let table_name = plugin.table_name_for_node(&node_kind);
                let writer = context
                    .destination
                    .new_batch_writer(&table_name)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("failed to create writer: {}", e))
                    })?;

                writer.write_batch(&[batch]).await.map_err(|e| {
                    HandlerError::Processing(format!("failed to write nodes: {}", e))
                })?;

                debug!(
                    table = %table_name,
                    count = nodes.len(),
                    "wrote plugin nodes"
                );
            }
        }

        debug!(message_id = %mailbox_message.message_id, "processing node deletions");
        if !mailbox_message.delete_nodes.is_empty() {
            self.process_node_deletions(&context, &mailbox_message, &plugin, &traversal_path)
                .await?;
        }

        debug!(message_id = %mailbox_message.message_id, "processing edges");
        if !mailbox_message.edges.is_empty() {
            self.process_edges(&context, &mailbox_message, &plugin, &traversal_path)
                .await?;
        }

        debug!(message_id = %mailbox_message.message_id, "processing edge deletions");
        if !mailbox_message.delete_edges.is_empty() {
            self.process_edge_deletions(&context, &mailbox_message, &plugin, &traversal_path)
                .await?;
        }

        if let Err(e) = dedup.mark_processed(&mailbox_message.message_id).await {
            warn!(
                message_id = %mailbox_message.message_id,
                error = %e.to_string(),
                "failed to mark message as processed"
            );
        }

        Ok(())
    }
}

impl MailboxHandler {
    async fn process_edges(
        &self,
        context: &HandlerContext,
        message: &MailboxMessage,
        plugin: &crate::types::Plugin,
        traversal_path: &str,
    ) -> Result<(), HandlerError> {
        use crate::handler::id_generator::{generate_edge_id, generate_node_id};
        use arrow::array::{
            BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use chrono::Utc;

        let edge_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new(
                "_version",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("_deleted", DataType::Boolean, false),
        ]);

        let edges = &message.edges;
        let capacity = edges.len();

        let mut id_builder = Int64Builder::with_capacity(capacity);
        let mut rel_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut source_id_builder = Int64Builder::with_capacity(capacity);
        let mut source_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut target_id_builder = Int64Builder::with_capacity(capacity);
        let mut target_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut path_builder =
            StringBuilder::with_capacity(capacity, traversal_path.len() * capacity);
        let mut version_builder =
            TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC");
        let mut deleted_builder = BooleanBuilder::with_capacity(capacity);

        let now_micros = Utc::now().timestamp_micros();

        for edge in edges {
            let edge_id = generate_edge_id(
                &plugin.plugin_id,
                plugin.namespace_id,
                &edge.relationship_kind,
                &edge.external_id,
            );

            let source_id = generate_node_id(
                &plugin.plugin_id,
                plugin.namespace_id,
                &edge.source.node_kind,
                &edge.source.external_id,
            );

            let target_id = if edge
                .target
                .node_kind
                .starts_with(&format!("{}_", plugin.plugin_id.replace('-', "_")))
            {
                generate_node_id(
                    &plugin.plugin_id,
                    plugin.namespace_id,
                    &edge.target.node_kind,
                    &edge.target.external_id,
                )
            } else {
                edge.target.external_id.parse::<i64>().map_err(|_| {
                    HandlerError::Processing(format!(
                        "system node external_id '{}' must be a valid int64",
                        edge.target.external_id
                    ))
                })?
            };

            id_builder.append_value(edge_id);
            rel_kind_builder.append_value(&edge.relationship_kind);
            source_id_builder.append_value(source_id);
            source_kind_builder.append_value(&edge.source.node_kind);
            target_id_builder.append_value(target_id);
            target_kind_builder.append_value(&edge.target.node_kind);
            path_builder.append_value(traversal_path);
            version_builder.append_value(now_micros);
            deleted_builder.append_value(false);
        }

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(edge_schema),
            vec![
                std::sync::Arc::new(id_builder.finish()),
                std::sync::Arc::new(rel_kind_builder.finish()),
                std::sync::Arc::new(source_id_builder.finish()),
                std::sync::Arc::new(source_kind_builder.finish()),
                std::sync::Arc::new(target_id_builder.finish()),
                std::sync::Arc::new(target_kind_builder.finish()),
                std::sync::Arc::new(path_builder.finish()),
                std::sync::Arc::new(version_builder.finish()),
                std::sync::Arc::new(deleted_builder.finish()),
            ],
        )
        .map_err(|e| HandlerError::Processing(format!("failed to build edge batch: {}", e)))?;

        let writer = context
            .destination
            .new_batch_writer("gl_edges")
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create edge writer: {}", e))
            })?;

        writer
            .write_batch(&[batch])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write edges: {}", e)))?;

        debug!(count = edges.len(), "wrote plugin edges");

        Ok(())
    }

    async fn process_node_deletions(
        &self,
        context: &HandlerContext,
        message: &MailboxMessage,
        plugin: &crate::types::Plugin,
        traversal_path: &str,
    ) -> Result<(), HandlerError> {
        let delete_refs_by_kind = group_delete_refs_by_kind(&message.delete_nodes);

        for (node_kind, delete_refs) in delete_refs_by_kind {
            let node_definition = plugin.schema.get_node(&node_kind).ok_or_else(|| {
                HandlerError::Processing(format!("unknown node kind for deletion: {}", node_kind))
            })?;

            let batch = ArrowConverter::build_node_deletion_batch(
                plugin,
                node_definition,
                &delete_refs,
                traversal_path,
            )
            .map_err(|e| {
                HandlerError::Processing(format!("failed to build deletion batch: {}", e))
            })?;

            if batch.num_rows() > 0 {
                let table_name = plugin.table_name_for_node(&node_kind);
                let writer = context
                    .destination
                    .new_batch_writer(&table_name)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("failed to create writer: {}", e))
                    })?;

                writer.write_batch(&[batch]).await.map_err(|e| {
                    HandlerError::Processing(format!("failed to write node deletions: {}", e))
                })?;

                debug!(
                    table = %table_name,
                    count = delete_refs.len(),
                    "deleted plugin nodes"
                );
            }
        }

        Ok(())
    }

    async fn process_edge_deletions(
        &self,
        context: &HandlerContext,
        message: &MailboxMessage,
        plugin: &crate::types::Plugin,
        traversal_path: &str,
    ) -> Result<(), HandlerError> {
        use crate::handler::id_generator::generate_edge_id;
        use arrow::array::{
            BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use chrono::Utc;

        let edge_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new(
                "_version",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("_deleted", DataType::Boolean, false),
        ]);

        let delete_edges = &message.delete_edges;
        let capacity = delete_edges.len();

        let mut id_builder = Int64Builder::with_capacity(capacity);
        let mut rel_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut source_id_builder = Int64Builder::with_capacity(capacity);
        let mut source_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut target_id_builder = Int64Builder::with_capacity(capacity);
        let mut target_kind_builder = StringBuilder::with_capacity(capacity, capacity * 32);
        let mut path_builder =
            StringBuilder::with_capacity(capacity, traversal_path.len() * capacity);
        let mut version_builder =
            TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC");
        let mut deleted_builder = BooleanBuilder::with_capacity(capacity);

        let now_micros = Utc::now().timestamp_micros();

        for edge_ref in delete_edges {
            let edge_id = generate_edge_id(
                &plugin.plugin_id,
                plugin.namespace_id,
                &edge_ref.relationship_kind,
                &edge_ref.external_id,
            );

            id_builder.append_value(edge_id);
            rel_kind_builder.append_value(&edge_ref.relationship_kind);
            source_id_builder.append_value(0);
            source_kind_builder.append_value("");
            target_id_builder.append_value(0);
            target_kind_builder.append_value("");
            path_builder.append_value(traversal_path);
            version_builder.append_value(now_micros);
            deleted_builder.append_value(true);
        }

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(edge_schema),
            vec![
                std::sync::Arc::new(id_builder.finish()),
                std::sync::Arc::new(rel_kind_builder.finish()),
                std::sync::Arc::new(source_id_builder.finish()),
                std::sync::Arc::new(source_kind_builder.finish()),
                std::sync::Arc::new(target_id_builder.finish()),
                std::sync::Arc::new(target_kind_builder.finish()),
                std::sync::Arc::new(path_builder.finish()),
                std::sync::Arc::new(version_builder.finish()),
                std::sync::Arc::new(deleted_builder.finish()),
            ],
        )
        .map_err(|e| {
            HandlerError::Processing(format!("failed to build edge deletion batch: {}", e))
        })?;

        let writer = context
            .destination
            .new_batch_writer("gl_edges")
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create edge writer: {}", e))
            })?;

        writer.write_batch(&[batch]).await.map_err(|e| {
            HandlerError::Processing(format!("failed to write edge deletions: {}", e))
        })?;

        debug!(count = delete_edges.len(), "deleted plugin edges");

        Ok(())
    }
}

fn group_nodes_by_kind(nodes: &[NodePayload]) -> HashMap<String, Vec<NodePayload>> {
    let mut grouped: HashMap<String, Vec<NodePayload>> = HashMap::new();

    for node in nodes {
        grouped
            .entry(node.node_kind.clone())
            .or_default()
            .push(node.clone());
    }

    grouped
}

fn group_delete_refs_by_kind(refs: &[NodeReference]) -> HashMap<String, Vec<NodeReference>> {
    let mut grouped: HashMap<String, Vec<NodeReference>> = HashMap::new();

    for node_ref in refs {
        grouped
            .entry(node_ref.node_kind.clone())
            .or_default()
            .push(node_ref.clone());
    }

    grouped
}
