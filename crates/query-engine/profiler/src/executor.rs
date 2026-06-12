use clickhouse_client::ArrowClickHouseClient;
use shared::PipelineOutput;

use gkg_server_config::ProfilingConfig;

pub async fn enrich_output(
    client: &ArrowClickHouseClient,
    output: &mut PipelineOutput,
    config: &ProfilingConfig,
) {
    for exec in &mut output.execution_log {
        let rendered = &exec.rendered_sql;
        if config.explain {
            exec.explain_plan = match client.explain_plan(rendered).await {
                Ok(plan) => Some(plan),
                Err(e) => {
                    eprintln!("EXPLAIN PLAN failed: {e}");
                    None
                }
            };
            exec.explain_pipeline = match client.explain_pipeline(rendered).await {
                Ok(pipeline) => Some(pipeline),
                Err(e) => {
                    eprintln!("EXPLAIN PIPELINE failed: {e}");
                    None
                }
            };
        }
    }
}
