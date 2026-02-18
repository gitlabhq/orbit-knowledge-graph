mod clickhouse;
pub mod config;
mod continuous;
mod data_generation;
mod domain;
mod report;
mod seeding;
mod state;

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::info;

use crate::clickhouse::ClickHouseWriter;
use crate::config::SimulatorConfig;
use crate::data_generation::SchemaRegistry;
use crate::domain::foundation;
use crate::domain::layout::ProjectEntityLayout;
use crate::report::MetricsCollector;
use crate::seeding::pipeline;
use crate::seeding::state_builder;

pub async fn run(config: &SimulatorConfig, skip_seeding: bool) -> Result<()> {
    let mut collector = MetricsCollector::new(config.metrics.enabled);
    collector.start_system_sampling();

    let datalake = config.datalake.build_client();
    let registry = Arc::new(
        SchemaRegistry::from_clickhouse(&datalake, &config.datalake.database)
            .await
            .context("failed to build schema registry")?,
    );

    let state = if skip_seeding {
        info!(dir = %config.state.dir, "loading saved state (seeding skipped)");
        state::HierarchyState::load(Path::new(&config.state.dir))?
    } else {
        let writer = Arc::new(ClickHouseWriter::new(
            &config.datalake,
            config.generation.batch_size,
        ));
        pipeline::truncate_stage_tables(&writer, &registry).await?;

        let foundation_start = Instant::now();
        let foundation = foundation::build_foundation(&config.generation);
        collector.record_phase("foundation build", foundation_start.elapsed());
        info!(
            users = foundation.users.len(),
            groups = foundation.groups.len(),
            projects = foundation.projects.len(),
            "foundation ready"
        );

        let layout = ProjectEntityLayout::from(&config.generation.per_project);
        let watermark_micros = Utc::now().timestamp_micros();
        pipeline::run_stages(
            &writer,
            &registry,
            config,
            &foundation,
            layout,
            watermark_micros,
            &mut collector,
        )
        .await?;

        let state = state_builder::build_state_for_continuous(&foundation, layout);
        state.save(Path::new(&config.state.dir))?;
        info!(dir = %config.state.dir, "saved state");
        state
    };

    if config.continuous.enabled {
        continuous::run_continuous(config, Arc::clone(&registry), state).await?;
    }

    collector.stop_system_sampling();
    if config.metrics.enabled {
        if let Err(error) = report::write_report_json(&collector, &config.metrics.output_path) {
            tracing::warn!(%error, path = %config.metrics.output_path, "failed to write report");
        }
        report::print_summary(&collector);
    }

    info!("data generation complete");
    Ok(())
}
