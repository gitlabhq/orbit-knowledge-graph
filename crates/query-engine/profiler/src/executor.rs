use clickhouse_client::ArrowClickHouseClient;
use shared::PipelineOutput;

use crate::config::ProfilingConfig;

pub async fn enrich_output(
    client: &ArrowClickHouseClient,
    output: &mut PipelineOutput,
    config: &ProfilingConfig,
) {
    for exec in &mut output.execution_log {
        let rendered = &exec.rendered_sql;
        if config.explain {
            exec.explain_plan = client.explain_plan(rendered).await.ok();
            exec.explain_pipeline = client.explain_pipeline(rendered).await.ok();
        }
        if config.query_log
            && !exec.query_id.is_empty()
            && let Ok(Some(entry)) = client.fetch_query_log_by_id(&exec.query_id).await
        {
            exec.query_log = Some(serde_json::to_value(&entry).unwrap_or_default());
        }
        if config.processors
            && !exec.query_id.is_empty()
            && let Ok(profiles) = client.fetch_processors_profile(&exec.query_id).await
            && !profiles.is_empty()
        {
            exec.processors = Some(serde_json::to_value(&profiles).unwrap_or_default());
        }
    }
}
