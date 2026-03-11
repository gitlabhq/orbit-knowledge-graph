use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use tracing::{debug, info};

use crate::clickhouse::ClickHouseWriter;
use crate::config::SimulatorConfig;
use crate::data_generation::SchemaRegistry;
use crate::data_generation::row_builder::DirectBatchBuilder;
use crate::domain::foundation::Foundation;
use crate::domain::layout::{
    ProjectEntityLayout, map_child_to_parent_index, synthetic_row_id, table_base_id,
};
use crate::report::MetricsCollector;
use crate::seeding::catalog;

#[derive(Clone, Copy)]
struct TablePlan {
    name: &'static str,
    kind: TablePlanKind,
}

#[derive(Clone, Copy)]
enum TablePlanKind {
    Foundation(FoundationTableSpec),
    Project(ProjectTableSpec),
}

#[derive(Clone, Copy)]
struct FoundationTableSpec {
    write_rows: FoundationWriterFn,
}

type FoundationWriterFn = fn(&TableWriteContext<'_>, Arc<Schema>, &TableSink<'_>) -> Result<usize>;

#[derive(Clone, Copy)]
struct ProjectTableSpec {
    table_name: &'static str,
    row_field_writer: Option<ProjectRowFieldWriterFn>,
}

type ProjectRowFieldWriterFn = fn(&ProjectLinkResolver<'_>, &mut RowAppender<'_, '_>, usize, usize);

struct TableWriteContext<'a> {
    foundation: &'a Foundation,
    layout: ProjectEntityLayout,
    watermark_micros: i64,
    batch_size: usize,
    seed: u64,
    field_overrides: Option<&'a HashMap<String, Vec<serde_json::Value>>>,
}

struct TableSink<'a> {
    table_name: &'static str,
    sender: &'a SyncSender<(&'static str, RecordBatch)>,
}

impl<'a> TableSink<'a> {
    fn send(&self, batch: RecordBatch) -> Result<()> {
        self.sender.send((self.table_name, batch))?;
        Ok(())
    }
}

struct FieldResolver {
    id: Option<usize>,
    project_id: Option<usize>,
    namespace_id: Option<usize>,
    traversal_path: Option<usize>,
}

impl FieldResolver {
    fn from_builder(builder: &DirectBatchBuilder<'_>) -> Self {
        Self {
            id: builder.column_index("id"),
            project_id: builder.column_index("project_id"),
            namespace_id: builder.column_index("namespace_id"),
            traversal_path: builder.column_index("traversal_path"),
        }
    }
}

struct RowAppender<'a, 'b> {
    fields: &'a HashSet<String>,
    builder: &'b mut DirectBatchBuilder<'a>,
}

impl<'a, 'b> RowAppender<'a, 'b> {
    fn new(fields: &'a HashSet<String>, builder: &'b mut DirectBatchBuilder<'a>) -> Self {
        Self { fields, builder }
    }

    fn i64(&mut self, name: &str, value: i64) {
        if self.fields.contains(name) {
            self.builder.set_int64(name, value);
        }
    }

    fn str(&mut self, name: &str, value: &str) {
        if self.fields.contains(name) {
            self.builder.set_string(name, value);
        }
    }
}

pub async fn truncate_stage_tables(
    writer: &ClickHouseWriter,
    registry: &SchemaRegistry,
) -> Result<()> {
    let stage_tables: HashSet<&str> = catalog::stage_definitions()
        .iter()
        .flat_map(|stage| stage.tables.iter().copied())
        .collect();
    for (table_name, _) in registry.seedable_tables() {
        if stage_tables.contains(table_name) {
            writer.truncate_table(table_name).await?;
        }
    }
    Ok(())
}

pub async fn run_stages(
    writer: &Arc<ClickHouseWriter>,
    registry: &SchemaRegistry,
    config: &SimulatorConfig,
    foundation: &Foundation,
    layout: ProjectEntityLayout,
    watermark_micros: i64,
    collector: &mut MetricsCollector,
) -> Result<()> {
    let table_plans = table_catalog()?;
    for stage in catalog::stage_definitions() {
        let start = Instant::now();
        let rows_by_table = seed_stage_tables(
            writer,
            registry,
            config,
            foundation,
            layout,
            watermark_micros,
            &stage.tables,
            &table_plans,
        )
        .await?;
        for (table_name, rows) in &rows_by_table {
            if *rows > 0 {
                collector.record_rows(table_name, *rows);
            }
        }
        collector.record_phase(stage.name, start.elapsed());
        info!(
            name = stage.name,
            rows = rows_by_table.values().sum::<usize>(),
            "stage complete"
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_stage_tables(
    writer: &Arc<ClickHouseWriter>,
    registry: &SchemaRegistry,
    config: &SimulatorConfig,
    foundation: &Foundation,
    layout: ProjectEntityLayout,
    watermark_micros: i64,
    stage_tables: &[&'static str],
    catalog: &HashMap<&'static str, TablePlan>,
) -> Result<HashMap<String, usize>> {
    let channel_bound = 16;
    let (batch_sender, batch_receiver) =
        mpsc::sync_channel::<(&'static str, RecordBatch)>(channel_bound);
    let insert_writer = Arc::clone(writer);

    let insert_handle = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Handle::current();
        let mut pending = Vec::new();
        while let Ok((table_name, batch)) = batch_receiver.recv() {
            let writer = Arc::clone(&insert_writer);
            pending.push(runtime.spawn(async move {
                debug!(table = %table_name, rows = batch.num_rows(), "inserting batch");
                writer.insert_batches(table_name, &[batch]).await
            }));
        }
        runtime.block_on(async {
            for task in pending {
                task.await??;
            }
            Ok::<(), anyhow::Error>(())
        })
    });

    let field_overrides = &config.generation.field_overrides;
    let batch_size = config.generation.batch_size;
    let base_seed = config.generation.seed;
    let mut rows_by_table = HashMap::new();
    std::thread::scope(|scope| -> Result<()> {
        let mut producers = Vec::new();

        for (table_offset, table_name) in stage_tables.iter().enumerate() {
            let Some(schema) = registry.schema_for_table(table_name) else {
                continue;
            };
            let plan = catalog
                .get(table_name)
                .copied()
                .with_context(|| format!("missing table plan for stage table {table_name}"))?;
            let sender = batch_sender.clone();
            let table_overrides = catalog::entity_type_for_table(table_name)
                .and_then(|entity| field_overrides.get(entity));
            let seed = base_seed ^ (table_offset as u64);

            producers.push(scope.spawn(move || {
                let ctx = TableWriteContext {
                    foundation,
                    layout,
                    watermark_micros,
                    batch_size,
                    seed,
                    field_overrides: table_overrides,
                };
                let sink = TableSink {
                    table_name: plan.name,
                    sender: &sender,
                };
                let rows = write_table(plan, &ctx, schema, &sink, seed)?;
                Ok::<(String, usize), anyhow::Error>((plan.name.to_string(), rows))
            }));
        }

        drop(batch_sender);
        for producer in producers {
            let (table_name, rows) = producer
                .join()
                .map_err(|_| anyhow::anyhow!("generation thread panicked"))??;
            rows_by_table.insert(table_name, rows);
        }
        Ok(())
    })?;

    insert_handle.await??;
    Ok(rows_by_table)
}

fn write_table(
    plan: TablePlan,
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
    seed: u64,
) -> Result<usize> {
    let result = match plan.kind {
        TablePlanKind::Foundation(spec) => (spec.write_rows)(ctx, schema, sink),
        TablePlanKind::Project(spec) => write_project_table(spec, ctx, schema, sink),
    };
    result.with_context(|| format!("failed generating table {} (seed={seed})", plan.name))
}

fn write_foundation_users(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    let organization_id = 1_i64;
    stream_rows_from_slice(ctx, schema, &ctx.foundation.users, sink, |user, row| {
        row.i64("id", user.id);
        row.i64("organization_id", organization_id);
    })
}

fn write_foundation_namespaces(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_from_slice(ctx, schema, &ctx.foundation.groups, sink, |group, row| {
        row.i64("id", group.namespace_id);
        row.i64("organization_id", group.organization_id);
        row.str("type", "Group");
        if let Some(parent) = group.parent_namespace_id {
            row.i64("parent_id", parent);
        }
    })
}

fn write_foundation_namespace_details(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_from_slice(ctx, schema, &ctx.foundation.groups, sink, |group, row| {
        row.i64("namespace_id", group.namespace_id);
    })
}

fn write_foundation_namespace_traversal_paths(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_from_slice(ctx, schema, &ctx.foundation.groups, sink, |group, row| {
        row.i64("id", group.namespace_id);
        row.str("traversal_path", &group.traversal_path);
    })
}

fn write_foundation_projects(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_from_slice(
        ctx,
        schema,
        &ctx.foundation.projects,
        sink,
        |project, row| {
            row.i64("id", project.id);
            row.i64("namespace_id", project.parent_namespace_id);
            row.i64("project_namespace_id", project.namespace_id);
            row.i64("organization_id", project.organization_id);
        },
    )
}

fn write_foundation_project_namespace_traversal_paths(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_from_slice(
        ctx,
        schema,
        &ctx.foundation.projects,
        sink,
        |project, row| {
            row.i64("id", project.id);
            row.str("traversal_path", &project.traversal_path);
        },
    )
}

fn write_foundation_enabled_namespaces(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    stream_rows_by_index(
        ctx,
        schema,
        ctx.foundation.root_group_namespace_ids.len(),
        sink,
        |index, row| {
            let row_id = (index + 1) as i64;
            let root_namespace_id = ctx.foundation.root_group_namespace_ids[index];
            row.i64("id", row_id);
            row.i64("root_namespace_id", root_namespace_id);
        },
    )
}

fn write_project_table(
    spec: ProjectTableSpec,
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    sink: &TableSink<'_>,
) -> Result<usize> {
    let rows_per_project = catalog::project_rows_per_table(&ctx.layout, spec.table_name);
    let project_count = ctx.foundation.projects.len();
    if rows_per_project == 0 || project_count == 0 {
        return Ok(0);
    }

    let table_id_base = table_base_id(spec.table_name, ctx.foundation, ctx.layout);
    let total_rows = rows_per_project * project_count;
    let fields = schema_field_names(&schema);
    let links = ProjectLinkResolver::new(ctx.foundation, ctx.layout);
    let mut fake =
        crate::data_generation::fake_values::SiphonFakeValueGenerator::with_seed(ctx.seed);

    let mut rows_written = 0usize;
    let mut rows_in_batch = 0usize;
    let mut builder = DirectBatchBuilder::new(
        Arc::clone(&schema),
        ctx.batch_size.min(total_rows).max(1),
        ctx.watermark_micros,
        ctx.field_overrides,
    );
    let resolved = FieldResolver::from_builder(&builder);

    for project_index in 0..project_count {
        let project = &ctx.foundation.projects[project_index];
        for entity_index in 0..rows_per_project {
            let row_id =
                synthetic_row_id(table_id_base, rows_per_project, project_index, entity_index);

            if let Some(index) = resolved.id {
                builder.set_int64_by_index(index, row_id);
            }
            if let Some(index) = resolved.project_id {
                builder.set_int64_by_index(index, project.id);
            }
            if let Some(index) = resolved.namespace_id {
                builder.set_int64_by_index(index, project.namespace_id);
            }
            if let Some(index) = resolved.traversal_path {
                builder.set_string_by_index(index, &project.traversal_path);
            }

            if let Some(row_field_writer) = spec.row_field_writer {
                let mut row = RowAppender::new(&fields, &mut builder);
                row_field_writer(&links, &mut row, project_index, entity_index);
            }

            builder.fill_unset_fields(&mut fake);
            rows_in_batch += 1;
            if rows_in_batch >= ctx.batch_size {
                let remaining = total_rows.saturating_sub(rows_written + rows_in_batch);
                let next_capacity = ctx.batch_size.min(remaining).max(1);
                sink.send(builder.finish_and_reset(next_capacity)?)?;
                rows_written += rows_in_batch;
                rows_in_batch = 0;
            }
        }
    }

    if rows_in_batch > 0 {
        sink.send(builder.finish()?)?;
        rows_written += rows_in_batch;
    }

    Ok(rows_written)
}

struct ProjectLinkResolver<'a> {
    foundation: &'a Foundation,
    layout: ProjectEntityLayout,
    base_merge_requests: i64,
    base_work_items: i64,
    base_pipelines: i64,
    base_stages: i64,
    base_diffs: i64,
    base_builds: i64,
    base_scans: i64,
    base_vulnerabilities: i64,
    base_scanners: i64,
    base_identifiers: i64,
    base_occurrences: i64,
}

impl<'a> ProjectLinkResolver<'a> {
    fn new(foundation: &'a Foundation, layout: ProjectEntityLayout) -> Self {
        Self {
            foundation,
            layout,
            base_merge_requests: table_base_id("merge_requests", foundation, layout),
            base_work_items: table_base_id("hierarchy_work_items", foundation, layout),
            base_pipelines: table_base_id("siphon_p_ci_pipelines", foundation, layout),
            base_stages: table_base_id("siphon_p_ci_stages", foundation, layout),
            base_diffs: table_base_id("siphon_merge_request_diffs", foundation, layout),
            base_builds: table_base_id("siphon_p_ci_builds", foundation, layout),
            base_scans: table_base_id("siphon_security_scans", foundation, layout),
            base_vulnerabilities: table_base_id("siphon_vulnerabilities", foundation, layout),
            base_scanners: table_base_id("siphon_vulnerability_scanners", foundation, layout),
            base_identifiers: table_base_id("siphon_vulnerability_identifiers", foundation, layout),
            base_occurrences: table_base_id("siphon_vulnerability_occurrences", foundation, layout),
        }
    }

    fn row_id(
        &self,
        base: i64,
        rows_per_project: usize,
        project_index: usize,
        entity_index: usize,
    ) -> i64 {
        synthetic_row_id(base, rows_per_project, project_index, entity_index)
    }
}

fn write_merge_request_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    let project = &links.foundation.projects[project_index];
    row.i64("target_project_id", project.id);
    row.i64("source_project_id", project.id);
    row.i64("author_id", random_user_id(links.foundation, entity_index));
    row.i64("iid", entity_index as i64 + 1);
}

fn write_work_item_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    _project_index: usize,
    entity_index: usize,
) {
    row.i64(
        "author_id",
        random_user_id(links.foundation, entity_index + 11),
    );
    row.i64("iid", entity_index as i64 + 1);
}

fn write_note_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    let rows_per_project = links.layout.notes;
    let mr_count = links.layout.merge_requests;
    let wi_count = links.layout.work_items;
    if mr_count + wi_count > 0 {
        let mr_notes = rows_per_project * mr_count / (mr_count + wi_count);
        if entity_index < mr_notes && mr_count > 0 {
            let idx = entity_index % mr_count;
            let noteable_id = links.row_id(links.base_merge_requests, mr_count, project_index, idx);
            row.str("noteable_type", "MergeRequest");
            row.i64("noteable_id", noteable_id);
        } else if wi_count > 0 {
            let idx = (entity_index.saturating_sub(mr_notes)) % wi_count;
            let noteable_id = links.row_id(links.base_work_items, wi_count, project_index, idx);
            row.str("noteable_type", "WorkItem");
            row.i64("noteable_id", noteable_id);
        }
    }
    row.i64(
        "author_id",
        random_user_id(links.foundation, entity_index + 17),
    );
}

fn write_merge_request_diff_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.merge_requests > 0 {
        let parent = links.row_id(
            links.base_merge_requests,
            links.layout.merge_requests,
            project_index,
            entity_index % links.layout.merge_requests,
        );
        row.i64("merge_request_id", parent);
    }
}

fn write_merge_request_diff_file_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.merge_request_diffs > 0 {
        let parent = links.row_id(
            links.base_diffs,
            links.layout.merge_request_diffs,
            project_index,
            entity_index % links.layout.merge_request_diffs,
        );
        row.i64("merge_request_diff_id", parent);
    }
    row.i64("relative_order", entity_index as i64);
}

fn write_stage_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.pipelines > 0 {
        let pipeline_index =
            map_child_to_parent_index(entity_index, links.layout.stages, links.layout.pipelines);
        let pipeline_id = links.row_id(
            links.base_pipelines,
            links.layout.pipelines,
            project_index,
            pipeline_index,
        );
        row.i64("pipeline_id", pipeline_id);
    }
}

fn write_build_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.stages > 0 {
        let stage_index =
            map_child_to_parent_index(entity_index, links.layout.jobs, links.layout.stages);
        let stage_id = links.row_id(
            links.base_stages,
            links.layout.stages,
            project_index,
            stage_index,
        );
        row.i64("stage_id", stage_id);
    }
}

fn write_security_scan_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.pipelines > 0 {
        let pipeline_index = map_child_to_parent_index(
            entity_index,
            links.layout.security_scans,
            links.layout.pipelines,
        );
        let pipeline_id = links.row_id(
            links.base_pipelines,
            links.layout.pipelines,
            project_index,
            pipeline_index,
        );
        row.i64("pipeline_id", pipeline_id);
    }
    if links.layout.jobs > 0 {
        let build_index =
            map_child_to_parent_index(entity_index, links.layout.security_scans, links.layout.jobs);
        let build_id = links.row_id(
            links.base_builds,
            links.layout.jobs,
            project_index,
            build_index,
        );
        row.i64("build_id", build_id);
    }
}

fn write_security_finding_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.security_scans > 0 {
        let scan_index = map_child_to_parent_index(
            entity_index,
            links.layout.security_findings,
            links.layout.security_scans,
        );
        let scan_id = links.row_id(
            links.base_scans,
            links.layout.security_scans,
            project_index,
            scan_index,
        );
        row.i64("scan_id", scan_id);
    }
    if links.layout.vulnerabilities > 0 {
        let scanner_id = links.row_id(
            links.base_scanners,
            links.layout.vulnerabilities,
            project_index,
            entity_index % links.layout.vulnerabilities,
        );
        row.i64("scanner_id", scanner_id);
    }
}

fn write_vulnerability_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    _project_index: usize,
    entity_index: usize,
) {
    row.i64(
        "author_id",
        random_user_id(links.foundation, entity_index + 29),
    );
}

fn write_vulnerability_occurrence_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.vulnerabilities > 0 {
        let index = entity_index % links.layout.vulnerabilities;
        let scanner_id = links.row_id(
            links.base_scanners,
            links.layout.vulnerabilities,
            project_index,
            index,
        );
        let identifier_id = links.row_id(
            links.base_identifiers,
            links.layout.vulnerabilities,
            project_index,
            index,
        );
        let vulnerability_id = links.row_id(
            links.base_vulnerabilities,
            links.layout.vulnerabilities,
            project_index,
            index,
        );
        row.i64("scanner_id", scanner_id);
        row.i64("primary_identifier_id", identifier_id);
        row.i64("vulnerability_id", vulnerability_id);
    }
}

fn write_vulnerability_merge_request_link_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.vulnerabilities > 0 {
        let vulnerability_id = links.row_id(
            links.base_vulnerabilities,
            links.layout.vulnerabilities,
            project_index,
            entity_index % links.layout.vulnerabilities,
        );
        row.i64("vulnerability_id", vulnerability_id);
    }
    if links.layout.merge_requests > 0 {
        let merge_request_id = links.row_id(
            links.base_merge_requests,
            links.layout.merge_requests,
            project_index,
            entity_index % links.layout.merge_requests,
        );
        row.i64("merge_request_id", merge_request_id);
    }
}

fn write_merge_request_closing_issue_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.merge_requests > 0 {
        let merge_request_id = links.row_id(
            links.base_merge_requests,
            links.layout.merge_requests,
            project_index,
            entity_index % links.layout.merge_requests,
        );
        row.i64("merge_request_id", merge_request_id);
    }
    if links.layout.work_items > 0 {
        let issue_id = links.row_id(
            links.base_work_items,
            links.layout.work_items,
            project_index,
            entity_index % links.layout.work_items,
        );
        row.i64("issue_id", issue_id);
    }
}

fn write_work_item_parent_link_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.work_items > 1 {
        let parent_id = links.row_id(
            links.base_work_items,
            links.layout.work_items,
            project_index,
            0,
        );
        let child_id = links.row_id(
            links.base_work_items,
            links.layout.work_items,
            project_index,
            (entity_index + 1).min(links.layout.work_items - 1),
        );
        row.i64("work_item_parent_id", parent_id);
        row.i64("work_item_id", child_id);
    }
}

fn write_issue_link_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.work_items > 1 {
        let source_id = links.row_id(
            links.base_work_items,
            links.layout.work_items,
            project_index,
            entity_index,
        );
        let target_id = links.row_id(
            links.base_work_items,
            links.layout.work_items,
            project_index,
            (entity_index + 1).min(links.layout.work_items - 1),
        );
        row.i64("source_id", source_id);
        row.i64("target_id", target_id);
    }
}

fn write_vulnerability_occurrence_identifier_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    if links.layout.vulnerabilities > 0 {
        let index = entity_index % links.layout.vulnerabilities;
        let occurrence_id = links.row_id(
            links.base_occurrences,
            links.layout.vulnerabilities,
            project_index,
            index,
        );
        let identifier_id = links.row_id(
            links.base_identifiers,
            links.layout.vulnerabilities,
            project_index,
            index,
        );
        row.i64("occurrence_id", occurrence_id);
        row.i64("identifier_id", identifier_id);
    }
}

fn write_member_fields(
    links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    project_index: usize,
    entity_index: usize,
) {
    let project_id = links.foundation.projects[project_index].id;
    row.i64("source_id", project_id);
    row.str("source_type", "Project");
    row.i64(
        "user_id",
        random_user_id(links.foundation, entity_index + 41),
    );
}

fn write_milestone_fields(
    _links: &ProjectLinkResolver<'_>,
    row: &mut RowAppender<'_, '_>,
    _project_index: usize,
    entity_index: usize,
) {
    row.i64("iid", entity_index as i64 + 1);
}

// Write one row per item from an existing collection (users/groups/projects).
fn stream_rows_from_slice<T>(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    rows: &[T],
    sink: &TableSink<'_>,
    mut write_row: impl FnMut(&T, &mut RowAppender<'_, '_>),
) -> Result<usize> {
    let total_rows = rows.len();
    if total_rows == 0 {
        return Ok(0);
    }

    let fields = schema_field_names(&schema);
    let mut fake =
        crate::data_generation::fake_values::SiphonFakeValueGenerator::with_seed(ctx.seed);
    let mut start = 0usize;
    while start < total_rows {
        let chunk_rows = ctx.batch_size.min(total_rows - start).max(1);
        let end = start + chunk_rows;
        let mut builder = DirectBatchBuilder::new(
            Arc::clone(&schema),
            chunk_rows,
            ctx.watermark_micros,
            ctx.field_overrides,
        );
        for item in &rows[start..end] {
            let mut row = RowAppender::new(&fields, &mut builder);
            write_row(item, &mut row);
            builder.fill_unset_fields(&mut fake);
        }
        sink.send(builder.finish()?)?;
        start = end;
    }
    Ok(total_rows)
}

// Write rows by index when there is no source collection to iterate.
fn stream_rows_by_index(
    ctx: &TableWriteContext<'_>,
    schema: Arc<Schema>,
    total_rows: usize,
    sink: &TableSink<'_>,
    mut write_row: impl FnMut(usize, &mut RowAppender<'_, '_>),
) -> Result<usize> {
    if total_rows == 0 {
        return Ok(0);
    }

    let fields = schema_field_names(&schema);
    let mut fake = crate::data_generation::fake_values::SiphonFakeValueGenerator::with_seed(42);
    let mut start = 0usize;
    while start < total_rows {
        let chunk_rows = ctx.batch_size.min(total_rows - start).max(1);
        let end = start + chunk_rows;
        let mut builder = DirectBatchBuilder::new(
            Arc::clone(&schema),
            chunk_rows,
            ctx.watermark_micros,
            ctx.field_overrides,
        );
        for index in start..end {
            let mut row = RowAppender::new(&fields, &mut builder);
            write_row(index, &mut row);
            builder.fill_unset_fields(&mut fake);
        }
        sink.send(builder.finish()?)?;
        start = end;
    }
    Ok(total_rows)
}

fn schema_field_names(schema: &Schema) -> HashSet<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn random_user_id(foundation: &Foundation, index: usize) -> i64 {
    if foundation.users.is_empty() {
        return 1;
    }
    foundation.users[index % foundation.users.len()].id
}

fn foundation_writer_for_table(table_name: &str) -> Option<FoundationWriterFn> {
    match table_name {
        "siphon_users" => Some(write_foundation_users),
        "siphon_namespaces" => Some(write_foundation_namespaces),
        "siphon_namespace_details" => Some(write_foundation_namespace_details),
        "namespace_traversal_paths" => Some(write_foundation_namespace_traversal_paths),
        "siphon_projects" => Some(write_foundation_projects),
        "project_namespace_traversal_paths" => {
            Some(write_foundation_project_namespace_traversal_paths)
        }
        "siphon_knowledge_graph_enabled_namespaces" => Some(write_foundation_enabled_namespaces),
        _ => None,
    }
}

fn project_row_writer_for_table(table_name: &str) -> Option<ProjectRowFieldWriterFn> {
    match table_name {
        "merge_requests" => Some(write_merge_request_fields),
        "hierarchy_work_items" => Some(write_work_item_fields),
        "siphon_issues" => Some(write_work_item_fields),
        "siphon_vulnerabilities" => Some(write_vulnerability_fields),
        "siphon_vulnerability_occurrences" => Some(write_vulnerability_occurrence_fields),
        "siphon_notes" => Some(write_note_fields),
        "siphon_merge_request_diffs" => Some(write_merge_request_diff_fields),
        "siphon_p_ci_stages" => Some(write_stage_fields),
        "siphon_p_ci_builds" => Some(write_build_fields),
        "siphon_security_scans" => Some(write_security_scan_fields),
        "siphon_security_findings" => Some(write_security_finding_fields),
        "siphon_merge_request_diff_files" => Some(write_merge_request_diff_file_fields),
        "siphon_vulnerability_merge_request_links" => {
            Some(write_vulnerability_merge_request_link_fields)
        }
        "siphon_merge_requests_closing_issues" => Some(write_merge_request_closing_issue_fields),
        "siphon_work_item_parent_links" => Some(write_work_item_parent_link_fields),
        "siphon_issue_links" => Some(write_issue_link_fields),
        "siphon_vulnerability_occurrence_identifiers" => {
            Some(write_vulnerability_occurrence_identifier_fields)
        }
        "siphon_milestones" => Some(write_milestone_fields),
        "siphon_members" => Some(write_member_fields),
        _ => None,
    }
}

fn table_catalog() -> Result<HashMap<&'static str, TablePlan>> {
    let mut plans = HashMap::new();
    for spec in catalog::all_table_specs() {
        let kind = match spec.scope {
            catalog::TableScope::Foundation => {
                let write_rows =
                    foundation_writer_for_table(spec.table_name).with_context(|| {
                        format!(
                            "missing foundation writer for configured table {}",
                            spec.table_name
                        )
                    })?;
                TablePlanKind::Foundation(FoundationTableSpec { write_rows })
            }
            catalog::TableScope::Project => TablePlanKind::Project(ProjectTableSpec {
                table_name: spec.table_name,
                row_field_writer: project_row_writer_for_table(spec.table_name),
            }),
        };
        plans.insert(
            spec.table_name,
            TablePlan {
                name: spec.table_name,
                kind,
            },
        );
    }
    Ok(plans)
}
