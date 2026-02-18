use crate::domain::layout::ProjectEntityLayout;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TableScope {
    Foundation,
    Project,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SeedStage {
    Foundation,
    Primary,
    Secondary,
    Leaf,
}

impl SeedStage {
    pub fn name(self) -> &'static str {
        match self {
            SeedStage::Foundation => "stage 1 foundation",
            SeedStage::Primary => "stage 2 primary entities",
            SeedStage::Secondary => "stage 3 secondary entities",
            SeedStage::Leaf => "stage 4 leaf entities",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProjectRowSource {
    MergeRequests,
    WorkItems,
    Pipelines,
    Vulnerabilities,
    Notes,
    MergeRequestDiffs,
    Stages,
    Jobs,
    SecurityScans,
    SecurityFindings,
    MergeRequestDiffFiles,
    WorkItemLinks,
    Milestones,
    Labels,
    Members,
}

#[derive(Clone, Copy)]
pub struct TableSpec {
    pub table_name: &'static str,
    pub scope: TableScope,
    pub stage: SeedStage,
    pub entity_type: Option<&'static str>,
    pub state_range_entity: Option<&'static str>,
    pub project_row_source: Option<ProjectRowSource>,
    pub project_order: Option<usize>,
    pub preferred_entity_table: bool,
}

pub struct StageDefinition {
    pub name: &'static str,
    pub tables: Vec<&'static str>,
}

pub const TABLE_SPECS: &[TableSpec] = &[
    TableSpec {
        table_name: "siphon_users",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("User"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_namespaces",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("Group"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_namespace_details",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("Group"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: false,
    },
    TableSpec {
        table_name: "namespace_traversal_paths",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("Group"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: false,
    },
    TableSpec {
        table_name: "siphon_projects",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("Project"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "project_namespace_traversal_paths",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("Project"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: false,
    },
    TableSpec {
        table_name: "siphon_knowledge_graph_enabled_namespaces",
        scope: TableScope::Foundation,
        stage: SeedStage::Foundation,
        entity_type: Some("EnabledNamespace"),
        state_range_entity: None,
        project_row_source: None,
        project_order: None,
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "hierarchy_merge_requests",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("MergeRequest"),
        state_range_entity: Some("MergeRequest"),
        project_row_source: Some(ProjectRowSource::MergeRequests),
        project_order: Some(0),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "hierarchy_work_items",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("WorkItem"),
        state_range_entity: Some("WorkItem"),
        project_row_source: Some(ProjectRowSource::WorkItems),
        project_order: Some(1),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_issues",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("WorkItem"),
        state_range_entity: None,
        project_row_source: Some(ProjectRowSource::WorkItems),
        project_order: Some(2),
        preferred_entity_table: false,
    },
    TableSpec {
        table_name: "siphon_p_ci_pipelines",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("Pipeline"),
        state_range_entity: Some("Pipeline"),
        project_row_source: Some(ProjectRowSource::Pipelines),
        project_order: Some(3),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerabilities",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("Vulnerability"),
        state_range_entity: Some("Vulnerability"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(4),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerability_scanners",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("VulnerabilityScanner"),
        state_range_entity: Some("VulnerabilityScanner"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(5),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerability_identifiers",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("VulnerabilityIdentifier"),
        state_range_entity: Some("VulnerabilityIdentifier"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(6),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerability_occurrences",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("VulnerabilityOccurrence"),
        state_range_entity: Some("VulnerabilityOccurrence"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(7),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_notes",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("Note"),
        state_range_entity: Some("Note"),
        project_row_source: Some(ProjectRowSource::Notes),
        project_order: Some(8),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_merge_request_diffs",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("MergeRequestDiff"),
        state_range_entity: Some("MergeRequestDiff"),
        project_row_source: Some(ProjectRowSource::MergeRequestDiffs),
        project_order: Some(9),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_p_ci_stages",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("Stage"),
        state_range_entity: Some("Stage"),
        project_row_source: Some(ProjectRowSource::Stages),
        project_order: Some(10),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_p_ci_builds",
        scope: TableScope::Project,
        stage: SeedStage::Leaf,
        entity_type: Some("Job"),
        state_range_entity: Some("Job"),
        project_row_source: Some(ProjectRowSource::Jobs),
        project_order: Some(11),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_security_scans",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("SecurityScan"),
        state_range_entity: Some("SecurityScan"),
        project_row_source: Some(ProjectRowSource::SecurityScans),
        project_order: Some(12),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_security_findings",
        scope: TableScope::Project,
        stage: SeedStage::Leaf,
        entity_type: Some("SecurityFinding"),
        state_range_entity: Some("SecurityFinding"),
        project_row_source: Some(ProjectRowSource::SecurityFindings),
        project_order: Some(13),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_merge_request_diff_files",
        scope: TableScope::Project,
        stage: SeedStage::Leaf,
        entity_type: Some("MergeRequestDiffFile"),
        state_range_entity: Some("MergeRequestDiffFile"),
        project_row_source: Some(ProjectRowSource::MergeRequestDiffFiles),
        project_order: Some(14),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerability_merge_request_links",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("VulnerabilityMergeRequestLink"),
        state_range_entity: Some("VulnerabilityMergeRequestLink"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(15),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_merge_requests_closing_issues",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("MergeRequestClosingIssue"),
        state_range_entity: Some("MergeRequestClosingIssue"),
        project_row_source: Some(ProjectRowSource::MergeRequests),
        project_order: Some(16),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_work_item_parent_links",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("WorkItemParentLink"),
        state_range_entity: Some("WorkItemParentLink"),
        project_row_source: Some(ProjectRowSource::WorkItemLinks),
        project_order: Some(17),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_issue_links",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("IssueLink"),
        state_range_entity: Some("IssueLink"),
        project_row_source: Some(ProjectRowSource::WorkItemLinks),
        project_order: Some(18),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_vulnerability_occurrence_identifiers",
        scope: TableScope::Project,
        stage: SeedStage::Secondary,
        entity_type: Some("VulnerabilityOccurrenceIdentifier"),
        state_range_entity: Some("VulnerabilityOccurrenceIdentifier"),
        project_row_source: Some(ProjectRowSource::Vulnerabilities),
        project_order: Some(19),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_milestones",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("Milestone"),
        state_range_entity: Some("Milestone"),
        project_row_source: Some(ProjectRowSource::Milestones),
        project_order: Some(20),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_labels",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("Label"),
        state_range_entity: Some("Label"),
        project_row_source: Some(ProjectRowSource::Labels),
        project_order: Some(21),
        preferred_entity_table: true,
    },
    TableSpec {
        table_name: "siphon_members",
        scope: TableScope::Project,
        stage: SeedStage::Primary,
        entity_type: Some("Member"),
        state_range_entity: Some("Member"),
        project_row_source: Some(ProjectRowSource::Members),
        project_order: Some(22),
        preferred_entity_table: true,
    },
];

pub fn all_table_specs() -> &'static [TableSpec] {
    TABLE_SPECS
}

pub fn seeding_table_names() -> Vec<&'static str> {
    TABLE_SPECS.iter().map(|spec| spec.table_name).collect()
}

pub fn stage_definitions() -> Vec<StageDefinition> {
    [
        SeedStage::Foundation,
        SeedStage::Primary,
        SeedStage::Secondary,
        SeedStage::Leaf,
    ]
    .into_iter()
    .map(|stage| StageDefinition {
        name: stage.name(),
        tables: TABLE_SPECS
            .iter()
            .filter(|spec| spec.stage == stage)
            .map(|spec| spec.table_name)
            .collect(),
    })
    .collect()
}

pub fn project_table_names_in_order() -> Vec<&'static str> {
    let mut ordered: Vec<(usize, &'static str)> = TABLE_SPECS
        .iter()
        .filter_map(|spec| {
            spec.project_order
                .map(|position| (position, spec.table_name))
        })
        .collect();
    ordered.sort_by_key(|(position, _)| *position);
    ordered.into_iter().map(|(_, table)| table).collect()
}

pub fn project_table_position(table_name: &str) -> Option<usize> {
    TABLE_SPECS
        .iter()
        .find(|spec| spec.table_name == table_name)
        .and_then(|spec| spec.project_order)
}

pub fn entity_type_for_table(table_name: &str) -> Option<&'static str> {
    TABLE_SPECS
        .iter()
        .find(|spec| spec.table_name == table_name)
        .and_then(|spec| spec.entity_type)
}

pub fn table_for_entity_type(entity_type: &str) -> Option<&'static str> {
    TABLE_SPECS
        .iter()
        .find(|spec| spec.entity_type == Some(entity_type) && spec.preferred_entity_table)
        .map(|spec| spec.table_name)
}

pub fn project_rows_per_table(layout: &ProjectEntityLayout, table_name: &str) -> usize {
    let Some(source) = TABLE_SPECS
        .iter()
        .find(|spec| spec.table_name == table_name)
        .and_then(|spec| spec.project_row_source)
    else {
        return 0;
    };

    match source {
        ProjectRowSource::MergeRequests => layout.merge_requests,
        ProjectRowSource::WorkItems => layout.work_items,
        ProjectRowSource::Pipelines => layout.pipelines,
        ProjectRowSource::Vulnerabilities => layout.vulnerabilities,
        ProjectRowSource::Notes => layout.notes,
        ProjectRowSource::MergeRequestDiffs => layout.merge_request_diffs,
        ProjectRowSource::Stages => layout.stages,
        ProjectRowSource::Jobs => layout.jobs,
        ProjectRowSource::SecurityScans => layout.security_scans,
        ProjectRowSource::SecurityFindings => layout.security_findings,
        ProjectRowSource::MergeRequestDiffFiles => layout.merge_request_diff_files,
        ProjectRowSource::WorkItemLinks => layout.work_items.saturating_sub(1),
        ProjectRowSource::Milestones => layout.milestones,
        ProjectRowSource::Labels => layout.labels,
        ProjectRowSource::Members => layout.members,
    }
}

pub fn state_range_definitions() -> Vec<(&'static str, &'static str)> {
    TABLE_SPECS
        .iter()
        .filter_map(|spec| {
            spec.state_range_entity
                .map(|entity| (entity, spec.table_name))
        })
        .collect()
}
