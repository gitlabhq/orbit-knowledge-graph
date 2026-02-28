use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::clickhouse::ClickHouseWriter;
use crate::config::SimulatorConfig;
use crate::data_generation::SchemaRegistry;
use crate::data_generation::fake_values::SiphonFakeValueGenerator;
use crate::data_generation::row_builder::DirectBatchBuilder;
use crate::dispatch::run_dispatch_indexing;
use crate::seeding::catalog;
use crate::state::GraphState;
use synthetic_graph::state::ContinuousResult;

/// Separate seed from initial generation to produce different sequences in continuous mode.
const CONTINUOUS_SEED: u64 = 99;

pub async fn run_continuous(
    config: &SimulatorConfig,
    registry: Arc<SchemaRegistry>,
    state: GraphState,
) -> Result<()> {
    let state = Arc::new(RwLock::new(state));
    let mut generator = ContinuousGenerator::new(config.clone(), registry, Arc::clone(&state));
    let result = generator.run().await?;
    info!(
        cycles = result.cycles_completed,
        inserts = result.total_inserts,
        updates = result.total_updates,
        deletes = result.total_deletes,
        "continuous mode complete"
    );
    let final_state = state.read().await;
    final_state.save(Path::new(&config.state.dir))?;
    Ok(())
}

struct ProjectRef {
    project_id: i64,
    traversal_path: String,
    namespace_id: i64,
}

struct ContinuousGenerator {
    config: SimulatorConfig,
    seeder: ClickHouseWriter,
        registry: Arc<SchemaRegistry>,
        state: Arc<RwLock<GraphState>>,
    rng: SmallRng,
    value_generator: SiphonFakeValueGenerator,
}

enum DmlKind {
    Insert,
    Update,
    Delete,
}

impl ContinuousGenerator {
    fn new(
        config: SimulatorConfig,
        registry: Arc<SchemaRegistry>,
    state: Arc<RwLock<GraphState>>,
    ) -> Self {
        Self {
            seeder: ClickHouseWriter::new(&config.datalake, 10_000),
            registry,
            rng: SmallRng::seed_from_u64(CONTINUOUS_SEED),
            value_generator: SiphonFakeValueGenerator::with_seed(CONTINUOUS_SEED),
            config,
            state,
        }
    }

    async fn run(&mut self) -> Result<ContinuousResult> {
        let mut result = ContinuousResult::default();
        let cycle_count = if self.config.continuous.cycles == 0 {
            usize::MAX
        } else {
            self.config.continuous.cycles
        };

        for cycle in 0..cycle_count {
            let cycle_watermark = Utc::now();
            info!(cycle, watermark = %cycle_watermark, "starting continuous cycle");

            let inserts = self
                .execute_operations(DmlKind::Insert, cycle_watermark)
                .await?;
            let updates = self
                .execute_operations(DmlKind::Update, cycle_watermark)
                .await?;
            let deletes = self
                .execute_operations(DmlKind::Delete, cycle_watermark)
                .await?;

            result.total_inserts += inserts;
            result.total_updates += updates;
            result.total_deletes += deletes;
            result.cycles_completed += 1;

            info!(cycle, inserts, updates, deletes, "cycle complete");

            if self.config.continuous.dispatch_indexing
                && let Err(error) = run_dispatch_indexing(&self.config).await
            {
                warn!(cycle, %error, "dispatch indexing failed, continuing");
            }

            tokio::time::sleep(std::time::Duration::from_secs(
                self.config.continuous.cycle_interval_secs,
            ))
            .await;
        }

        Ok(result)
    }

    async fn execute_operations(
        &mut self,
        kind: DmlKind,
        watermark: DateTime<Utc>,
    ) -> Result<usize> {
        let watermark_micros = watermark.timestamp_micros();
        let mut rows_written = 0;

        let operations_map = match kind {
            DmlKind::Insert => &self.config.continuous.inserts_per_cycle,
            DmlKind::Update => &self.config.continuous.updates_per_cycle,
            DmlKind::Delete => &self.config.continuous.deletes_per_cycle,
        };
        let entity_operations: Vec<(String, usize)> = operations_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        let mut next_entity_id = {
            let state = self.state.read().await;
            state.metadata.next_entity_id
        };

        for (entity_type, count) in &entity_operations {
            let table_name = match catalog::table_for_entity_type(entity_type) {
                Some(name) => name,
                None => {
                    tracing::warn!(
                        entity_type = entity_type.as_str(),
                        "no table mapping found, skipping"
                    );
                    continue;
                }
            };
            let schema = match self.registry.schema_for_table(table_name) {
                Some(s) => s,
                None => {
                    tracing::warn!(
                        entity_type = entity_type.as_str(),
                        table_name,
                        "no schema found, skipping"
                    );
                    continue;
                }
            };

            let table_overrides = self
                .config
                .generation
                .field_overrides
                .get(entity_type.as_str());

            let state = self.state.read().await;
            let project_indices = project_entry_indices(&state);
            let mut builder =
                DirectBatchBuilder::new(schema, *count, watermark_micros, table_overrides);

            let rows_built = match kind {
                DmlKind::Insert => build_insert_rows(
                    entity_type,
                    *count,
                    &state,
                    &project_indices,
                    &mut self.rng,
                    &mut builder,
                    &mut self.value_generator,
                    &mut next_entity_id,
                ),
                DmlKind::Update => build_existing_entity_rows(
                    entity_type,
                    *count,
                    &state,
                    &project_indices,
                    &mut self.rng,
                    &mut builder,
                    &mut self.value_generator,
                    false,
                ),
                DmlKind::Delete => build_existing_entity_rows(
                    entity_type,
                    *count,
                    &state,
                    &project_indices,
                    &mut self.rng,
                    &mut builder,
                    &mut self.value_generator,
                    true,
                ),
            };
            drop(state);

            if rows_built == 0 {
                continue;
            }

            let batch = builder.finish()?;
            self.seeder
                .insert_batches(table_name, std::slice::from_ref(&batch))
                .await?;

            rows_written += rows_built;
        }

        if matches!(kind, DmlKind::Insert) {
            let mut state = self.state.write().await;
            state.metadata.next_entity_id = next_entity_id;

            for (entity_type, count) in &entity_operations {
                if let Some(range) = state.metadata.entity_ranges.get_mut(entity_type.as_str()) {
                    range.count += count;
                }
            }
        }

        Ok(rows_written)
    }
}

fn set_entity_fields(
    entity_type: &str,
    project: &ProjectRef,
    builder: &mut DirectBatchBuilder<'_>,
    entity_id: i64,
    state: &GraphState,
    rng: &mut SmallRng,
) {
    match entity_type {
        "MergeRequest" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("target_project_id", project.project_id);
            builder.set_int64("source_project_id", project.project_id);
            builder.set_int64("iid", entity_id);
            builder.set_int64("author_id", random_user_id(state, rng));
        }
        "WorkItem" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("namespace_id", project.namespace_id);
            builder.set_int64("iid", entity_id);
            builder.set_int64("author_id", random_user_id(state, rng));
        }
        "Pipeline" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("project_id", project.project_id);
        }
        "Note" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_string("noteable_type", "MergeRequest");
            builder.set_int64("noteable_id", random_entity_id(state, rng, "MergeRequest"));
            builder.set_int64("author_id", random_user_id(state, rng));
        }
        "Vulnerability" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("project_id", project.project_id);
            builder.set_int64("author_id", random_user_id(state, rng));
        }
        "Label" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("project_id", project.project_id);
            builder.set_int64("organization_id", random_organization_id(state, rng));
        }
        "Milestone" => {
            builder.set_string("traversal_path", &project.traversal_path);
            builder.set_int64("project_id", project.project_id);
            builder.set_int64("iid", entity_id);
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn build_insert_rows(
    entity_type: &str,
    count: usize,
    state: &GraphState,
    project_indices: &[usize],
    rng: &mut SmallRng,
    builder: &mut DirectBatchBuilder<'_>,
    value_generator: &mut SiphonFakeValueGenerator,
    next_entity_id: &mut i64,
) -> usize {
    for _ in 0..count {
        let entity_id = *next_entity_id;
        *next_entity_id += 1;

        builder.set_int64("id", entity_id);

        if let Some(project) = pick_random_project(state, project_indices, rng) {
            set_entity_fields(entity_type, &project, builder, entity_id, state, rng);
        }

        builder.fill_unset_fields(value_generator);
    }

    count
}

#[allow(clippy::too_many_arguments)]
fn build_existing_entity_rows(
    entity_type: &str,
    count: usize,
    state: &GraphState,
    project_indices: &[usize],
    rng: &mut SmallRng,
    builder: &mut DirectBatchBuilder<'_>,
    value_generator: &mut SiphonFakeValueGenerator,
    mark_deleted: bool,
) -> usize {
    let range = match state.metadata.entity_ranges.get(entity_type) {
        Some(r) if r.count > 0 => r,
        _ => return 0,
    };

    let actual_count = count.min(range.count);

    for _ in 0..actual_count {
        let entity_id = range.sample(rng);
        builder.set_int64("id", entity_id);

        if mark_deleted {
            if builder.column_index("_siphon_deleted").is_some() {
                builder.set_bool("_siphon_deleted", true);
            }
            if builder.column_index("deleted").is_some() {
                builder.set_bool("deleted", true);
            }
        }

        if let Some(project) = pick_random_project(state, project_indices, rng) {
            set_entity_fields(entity_type, &project, builder, entity_id, state, rng);
        }

        builder.fill_unset_fields(value_generator);
    }

    actual_count
}

fn pick_random_project(
    state: &GraphState,
    project_indices: &[usize],
    rng: &mut SmallRng,
) -> Option<ProjectRef> {
    if project_indices.is_empty() {
        return None;
    }

    let index = project_indices[rng.gen_range(0..project_indices.len())];
    let entry = &state.path_entries[index];
    Some(ProjectRef {
        project_id: entry.id,
        traversal_path: entry.traversal_path.clone(),
        namespace_id: entry.namespace_id.unwrap_or(0),
    })
}

fn project_entry_indices(state: &GraphState) -> Vec<usize> {
    state
        .path_entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.entity_type == "Project")
        .map(|(i, _)| i)
        .collect()
}

fn random_user_id(state: &GraphState, rng: &mut SmallRng) -> i64 {
    random_entity_id(state, rng, "User")
}

fn random_entity_id(state: &GraphState, rng: &mut SmallRng, entity_type: &str) -> i64 {
    match state.metadata.entity_ranges.get(entity_type) {
        Some(range) if range.count > 0 => range.sample(rng),
        _ => 1,
    }
}

fn random_organization_id(state: &GraphState, rng: &mut SmallRng) -> i64 {
    if state.metadata.enabled_namespaces.is_empty() {
        return 1;
    }
    let index = rng.gen_range(0..state.metadata.enabled_namespaces.len());
    state.metadata.enabled_namespaces[index].organization_id
}
