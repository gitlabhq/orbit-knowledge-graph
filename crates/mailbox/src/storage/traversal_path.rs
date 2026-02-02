//! Traversal path resolution for plugin nodes.

use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;

use crate::error::MailboxError;

pub struct TraversalPathResolver {
    client: Arc<ArrowClickHouseClient>,
}

impl TraversalPathResolver {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn resolve(&self, namespace_id: i64) -> Result<String, MailboxError> {
        let sql = format!(
            r#"SELECT traversal_path
            FROM gl_groups FINAL
            WHERE id = {}
            LIMIT 1"#,
            namespace_id,
        );

        let batches = self.client.query_arrow(&sql).await.map_err(|e| {
            MailboxError::storage(format!("failed to query traversal_path: {}", e))
        })?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Err(MailboxError::validation(format!(
                "namespace {} not found in gl_groups",
                namespace_id
            )));
        }

        use arrow::array::{Array, StringArray};
        let traversal_path = batches[0]
            .column_by_name("traversal_path")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .map(|a| a.value(0).to_string())
            .ok_or_else(|| MailboxError::storage("missing traversal_path column"))?;

        Ok(traversal_path)
    }
}
