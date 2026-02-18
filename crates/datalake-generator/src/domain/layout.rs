use crate::config::PerProjectConfig;
use crate::domain::foundation::Foundation;
use crate::seeding::catalog;

#[derive(Copy, Clone)]
pub struct ProjectEntityLayout {
    pub merge_requests: usize,
    pub merge_request_diffs: usize,
    pub merge_request_diff_files: usize,
    pub work_items: usize,
    pub pipelines: usize,
    pub vulnerabilities: usize,
    pub notes: usize,
    pub stages: usize,
    pub jobs: usize,
    pub security_scans: usize,
    pub security_findings: usize,
    pub milestones: usize,
    pub labels: usize,
    pub members: usize,
}

impl From<&PerProjectConfig> for ProjectEntityLayout {
    fn from(config: &PerProjectConfig) -> Self {
        Self {
            merge_requests: config.merge_requests,
            merge_request_diffs: config.merge_request_diffs,
            merge_request_diff_files: config.merge_request_diff_files,
            work_items: config.work_items,
            pipelines: config.pipelines,
            vulnerabilities: config.vulnerabilities,
            notes: config.notes,
            stages: config.stages,
            jobs: config.jobs,
            security_scans: config.security_scans,
            security_findings: config.security_findings,
            milestones: config.milestones,
            labels: config.labels,
            members: config.members,
        }
    }
}

impl ProjectEntityLayout {
    pub fn max_rows_per_project(&self) -> usize {
        [
            self.merge_requests,
            self.merge_request_diffs,
            self.merge_request_diff_files,
            self.work_items,
            self.pipelines,
            self.vulnerabilities,
            self.notes,
            self.stages,
            self.jobs,
            self.security_scans,
            self.security_findings,
            self.milestones,
            self.labels,
            self.members,
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }
}

pub fn table_base_id(
    table_name: &str,
    foundation: &Foundation,
    layout: ProjectEntityLayout,
) -> i64 {
    let project_count = foundation.projects.len();
    let max_rows_per_project = layout.max_rows_per_project().max(1);
    let block_size = (project_count * max_rows_per_project + 1) as i64;
    let table_position = catalog::project_table_position(table_name).unwrap_or(0) as i64;
    foundation.next_entity_id + table_position * block_size
}

pub fn synthetic_row_id(
    table_id_base: i64,
    rows_per_project: usize,
    project_index: usize,
    entity_index: usize,
) -> i64 {
    table_id_base + (project_index * rows_per_project + entity_index) as i64
}

pub fn map_child_to_parent_index(
    child_index: usize,
    child_count: usize,
    parent_count: usize,
) -> usize {
    if child_count == 0 || parent_count == 0 {
        return 0;
    }
    // Spread children across parents in a stable, repeatable way.
    let mapped = child_index.saturating_mul(parent_count) / child_count.max(1);
    mapped.min(parent_count.saturating_sub(1))
}
