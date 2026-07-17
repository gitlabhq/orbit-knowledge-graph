//! Pipeline identity derived from ETL declarations.

use std::collections::BTreeSet;

use crate::Ontology;
use crate::etl::EtlScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineDescriptor {
    pub name: String,
    pub scope: EtlScope,
    pub reindex_targets: BTreeSet<String>,
}

impl Ontology {
    pub fn pipeline_descriptors(&self) -> Vec<PipelineDescriptor> {
        let mut descriptors = Vec::new();
        for node in self.nodes() {
            for pipeline in &node.pipelines {
                descriptors.push(self.pipeline_descriptor(
                    pipeline.name.clone(),
                    pipeline.scope,
                    &node.name,
                ));
            }
        }
        for (kind, pipeline) in self.edge_etl_configs() {
            descriptors.push(self.pipeline_descriptor(pipeline.name.clone(), pipeline.scope, kind));
        }
        for derived in self.derived_entities() {
            for pipeline in &derived.pipelines {
                descriptors.push(self.pipeline_descriptor(
                    pipeline.name.clone(),
                    pipeline.scope,
                    &derived.name,
                ));
            }
        }
        descriptors
    }

    fn pipeline_descriptor(
        &self,
        name: String,
        scope: EtlScope,
        entity: &str,
    ) -> PipelineDescriptor {
        let mut reindex_targets = self.relationship_kinds_emitted_by(entity);
        reindex_targets.insert(entity.to_string());
        PipelineDescriptor {
            name,
            scope,
            reindex_targets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptors() -> Vec<PipelineDescriptor> {
        Ontology::load_embedded()
            .expect("should load ontology")
            .pipeline_descriptors()
    }

    fn find(descriptors: &[PipelineDescriptor], name: &str) -> PipelineDescriptor {
        descriptors
            .iter()
            .find(|d| d.name == name)
            .unwrap_or_else(|| panic!("descriptor '{name}' should exist"))
            .clone()
    }

    #[test]
    fn node_pipeline_emits_itself_and_its_fk_edge_kinds() {
        let all = descriptors();
        let note = find(&all, "Note");
        assert_eq!(note.scope, EtlScope::Namespaced);
        assert!(note.reindex_targets.contains("Note"));
        assert!(note.reindex_targets.contains("HAS_NOTE"));
    }

    #[test]
    fn global_node_pipeline_carries_global_scope() {
        let user = find(&descriptors(), "User");
        assert_eq!(user.scope, EtlScope::Global);
    }

    #[test]
    fn derived_entity_pipeline_emits_its_declared_kinds() {
        let system_note = find(&descriptors(), "SystemNote");
        assert!(system_note.reindex_targets.contains("SystemNote"));
        assert!(system_note.reindex_targets.contains("MENTIONS"));
    }

    #[test]
    fn shared_source_edge_etls_produce_distinct_target_suffixed_names() {
        let all = descriptors();
        let reopened: Vec<&PipelineDescriptor> = all
            .iter()
            .filter(|d| d.reindex_targets.contains("REOPENED"))
            .collect();
        assert_eq!(
            reopened.len(),
            2,
            "REOPENED has an MR-side and a WorkItem-side ETL"
        );
        let names: BTreeSet<&str> = reopened.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "REOPENED_siphon_resource_state_events_MergeRequest",
                "REOPENED_siphon_resource_state_events_WorkItem",
            ])
        );
    }

    #[test]
    fn pipeline_names_are_unique() {
        let all = descriptors();
        let unique: BTreeSet<&str> = all.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(unique.len(), all.len());
    }

    #[test]
    fn composed_pipeline_names_are_exactly_the_known_set() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let composed: BTreeSet<String> = ontology
            .edge_etl_configs()
            .map(|(_, pipeline)| pipeline.name.clone())
            .collect();
        assert_eq!(
            composed,
            [
                "APPROVED_siphon_approvals_MergeRequest",
                "ASSIGNED_siphon_issue_assignees_WorkItem",
                "ASSIGNED_siphon_merge_request_assignees_MergeRequest",
                "BUILT_BY_siphon_packages_build_infos_Pipeline",
                "BUILT_BY_siphon_packages_package_file_build_infos_Pipeline",
                "CHILD_OF_siphon_ci_sources_pipelines_Pipeline",
                "CLOSES_siphon_merge_requests_closing_issues_WorkItem",
                "CONTAINS_siphon_work_item_parent_links_WorkItem",
                "DECLARES_DEPENDENCY_siphon_packages_dependency_links_Dependency",
                "DEPLOYED_TO_siphon_deployment_merge_requests_Deployment",
                "FIXES_siphon_vulnerability_merge_request_links_Vulnerability",
                "HAS_IDENTIFIER_siphon_vulnerability_occurrence_identifiers_VulnerabilityIdentifier",
                "HAS_LABEL_siphon_label_links_Label",
                "HAS_VULNERABILITY_siphon_sbom_occurrences_vulnerabilities_Vulnerability",
                "MEMBER_OF_siphon_members",
                "RELATED_TO_siphon_issue_links_WorkItem",
                "REOPENED_siphon_resource_state_events_MergeRequest",
                "REOPENED_siphon_resource_state_events_WorkItem",
                "REVIEWER_siphon_merge_request_reviewers_MergeRequest",
                "TRIGGERS_PIPELINE_siphon_ci_sources_pipelines_Pipeline",
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
            "standalone-edge pipeline names are persisted in checkpoint keys; \
             a changed entry here invalidates that pipeline's live cursors"
        );
    }
}
