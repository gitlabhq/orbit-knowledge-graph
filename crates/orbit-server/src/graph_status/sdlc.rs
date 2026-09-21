use std::collections::HashMap;

use indexer::modules::sdlc::jobs::NAMESPACE_DATA;
use jobs::{JobLedger, JobRun, JobState};
use ontology::{EtlScope, Ontology};
use orbit_utils::traversal_path::TraversalPath;
use tracing::warn;

use super::{state_priority, status_with_state, unknown_status};
use crate::proto::{IndexingState, IndexingStatus};

const SANITIZED_INDEXING_ERROR: &str = "Something went wrong during indexing.";

pub struct SdlcIndexingState {
    pub aggregate: IndexingStatus,
    pub node_states: HashMap<String, IndexingState>,
}

pub async fn get_sdlc_indexing_state(
    ledger: &JobLedger,
    ontology: &Ontology,
    traversal_path: &TraversalPath,
) -> SdlcIndexingState {
    let runs = match ledger.latest_runs(traversal_path, &NAMESPACE_DATA).await {
        Ok(runs) => runs,
        Err(error) => {
            warn!(%error, %traversal_path, "failed to read namespace-data job runs");
            return SdlcIndexingState {
                aggregate: unknown_status(),
                node_states: HashMap::new(),
            };
        }
    };

    let pipelines = runs_per_pipeline(ontology, runs);
    let pipeline_states = pipelines
        .iter()
        .map(|(name, run)| (name.clone(), state_of(run.as_ref())))
        .collect();

    SdlcIndexingState {
        aggregate: aggregate_status(&pipelines),
        node_states: resolve_node_states(ontology, &pipeline_states),
    }
}

fn runs_per_pipeline(ontology: &Ontology, runs: Vec<JobRun>) -> Vec<(String, Option<JobRun>)> {
    let mut by_plan: HashMap<String, JobRun> = HashMap::new();
    for run in runs {
        let worse_than_current = by_plan.get(&run.key).is_none_or(|current| {
            state_priority(state_of(Some(&run))) > state_priority(state_of(Some(current)))
        });
        if worse_than_current {
            by_plan.insert(run.key.clone(), run);
        }
    }
    namespaced_pipeline_names(ontology)
        .into_iter()
        .map(|name| {
            let run = by_plan.remove(&name);
            (name, run)
        })
        .collect()
}

fn aggregate_status(pipelines: &[(String, Option<JobRun>)]) -> IndexingStatus {
    let worst = pipelines
        .iter()
        .map(|(_, run)| run.as_ref())
        .max_by_key(|run| state_priority(state_of(*run)));

    match worst.flatten() {
        Some(run) => indexing_status_from_run(run),
        None => status_with_state(IndexingState::NotIndexed),
    }
}

fn state_of(run: Option<&JobRun>) -> IndexingState {
    let Some(run) = run else {
        return IndexingState::NotIndexed;
    };
    match run.state {
        JobState::Succeeded | JobState::Skipped => IndexingState::Indexed,
        JobState::Failed => IndexingState::Error,
        JobState::Deferred => IndexingState::Backfilling,
        JobState::Pending | JobState::Queued | JobState::Running | JobState::Retrying => {
            if run.completed_at.is_some() {
                IndexingState::Indexing
            } else {
                IndexingState::Backfilling
            }
        }
    }
}

fn indexing_status_from_run(run: &JobRun) -> IndexingStatus {
    let duration_ms = run
        .completed_at
        .map(|completed| {
            completed
                .signed_duration_since(run.started_at)
                .num_milliseconds()
        })
        .map(|millis| u64::try_from(millis).unwrap_or(0));
    let succeeded = run.state == JobState::Succeeded;

    IndexingStatus {
        state: state_of(Some(run)).into(),
        last_started_at: Some(run.started_at.to_rfc3339()),
        last_completed_at: run.completed_at.map(|at| at.to_rfc3339()),
        last_duration_ms: duration_ms,
        last_error: (run.state == JobState::Failed).then(|| SANITIZED_INDEXING_ERROR.to_string()),
        last_rows_read: succeeded.then_some(run.rows_read),
        last_rows_written: succeeded.then_some(run.rows_written),
    }
}

pub(super) fn namespaced_pipeline_names(ontology: &Ontology) -> Vec<String> {
    ontology
        .pipeline_descriptors()
        .into_iter()
        .filter(|descriptor| descriptor.scope == EtlScope::Namespaced)
        .map(|descriptor| descriptor.name)
        .collect()
}

pub(super) fn resolve_node_states(
    ontology: &Ontology,
    pipeline_states: &HashMap<String, IndexingState>,
) -> HashMap<String, IndexingState> {
    ontology
        .nodes()
        .filter(|node| !node.pipelines.is_empty())
        .filter_map(|node| {
            node.pipelines
                .iter()
                .filter(|pipeline| pipeline.scope == EtlScope::Namespaced)
                .filter_map(|pipeline| pipeline_states.get(&pipeline.name).copied())
                .max_by_key(|state| state_priority(*state))
                .map(|state| (node.name.clone(), state))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::sync::Arc;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn run(state: JobState, started_ago_s: i64, completed_ago_s: Option<i64>) -> JobRun {
        let now = Utc::now();
        JobRun {
            namespace_id: 100,
            traversal_path: "1/100/".into(),
            key: "MergeRequest".into(),
            state,
            reason: (state == JobState::Failed).then(|| "scan failure".to_string()),
            rows_read: 0,
            rows_written: 0,
            started_at: now - Duration::seconds(started_ago_s),
            completed_at: completed_ago_s.map(|s| now - Duration::seconds(s)),
        }
    }

    fn pipelines(runs: Vec<(&str, Option<JobRun>)>) -> Vec<(String, Option<JobRun>)> {
        runs.into_iter()
            .map(|(name, run)| (name.to_string(), run))
            .collect()
    }

    #[test]
    fn state_of_maps_job_states_to_indexing_states() {
        let cases = [
            (run(JobState::Running, 0, None), IndexingState::Backfilling),
            (run(JobState::Running, 0, Some(60)), IndexingState::Indexing),
            (run(JobState::Deferred, 0, None), IndexingState::Backfilling),
            (
                run(JobState::Succeeded, 30, Some(25)),
                IndexingState::Indexed,
            ),
            (run(JobState::Failed, 30, Some(29)), IndexingState::Error),
        ];
        for (input, expected) in cases {
            assert_eq!(state_of(Some(&input)), expected, "{:?}", input.state);
        }
        assert_eq!(state_of(None), IndexingState::NotIndexed);
    }

    #[test]
    fn aggregate_not_indexed_when_no_pipeline_has_run() {
        let status = aggregate_status(&pipelines(vec![("MergeRequest", None), ("Issue", None)]));
        assert_eq!(status.state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_missing_pipeline_wins_over_indexed() {
        let status = aggregate_status(&pipelines(vec![
            ("MergeRequest", Some(run(JobState::Succeeded, 30, Some(25)))),
            ("Issue", None),
        ]));
        assert_eq!(status.state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_error_wins_over_indexed_and_indexing() {
        let status = aggregate_status(&pipelines(vec![
            ("MergeRequest", Some(run(JobState::Succeeded, 30, Some(25)))),
            ("Issue", Some(run(JobState::Running, 0, Some(60)))),
            ("Project", Some(run(JobState::Failed, 30, Some(29)))),
        ]));
        assert_eq!(status.state, IndexingState::Error as i32);
        assert_eq!(status.last_error.as_deref(), Some(SANITIZED_INDEXING_ERROR));
    }

    #[test]
    fn indexing_status_never_leaks_the_raw_failure_reason() {
        let mut failed = run(JobState::Failed, 30, Some(29));
        failed.reason = Some("Code: 999. DB::NetException: peer 192.0.2.1:55555".into());

        let status = indexing_status_from_run(&failed);

        assert_eq!(status.last_error.as_deref(), Some(SANITIZED_INDEXING_ERROR));
    }

    #[test]
    fn indexing_status_reports_rows_and_duration_only_for_a_success() {
        let mut succeeded = run(JobState::Succeeded, 30, Some(25));
        succeeded.rows_read = 307;
        succeeded.rows_written = 465;
        let status = indexing_status_from_run(&succeeded);
        assert_eq!(status.last_rows_read, Some(307));
        assert_eq!(status.last_rows_written, Some(465));
        assert_eq!(status.last_duration_ms, Some(5000));
        assert!(status.last_error.is_none());

        let failed = indexing_status_from_run(&run(JobState::Failed, 30, Some(29)));
        assert_eq!(failed.last_rows_read, None);
        assert_eq!(failed.last_rows_written, None);
    }

    #[test]
    fn resolve_node_states_reads_worst_namespaced_pipeline() {
        let ontology = test_ontology();
        let states = HashMap::from([
            ("MergeRequest".to_string(), IndexingState::Error),
            ("WorkItem".to_string(), IndexingState::Indexed),
        ]);

        let resolved = resolve_node_states(&ontology, &states);

        assert_eq!(resolved.get("MergeRequest"), Some(&IndexingState::Error));
        assert_eq!(resolved.get("WorkItem"), Some(&IndexingState::Indexed));
    }

    #[test]
    fn resolve_node_states_omits_pipelineless_and_global_only_nodes() {
        let ontology = test_ontology();
        let resolved = resolve_node_states(&ontology, &HashMap::new());

        assert!(!resolved.contains_key("Definition"));
        assert!(!resolved.contains_key("User"));
    }

    #[test]
    fn namespaced_pipeline_names_include_edge_and_derived_pipelines() {
        let names = namespaced_pipeline_names(&test_ontology());

        assert!(names.contains(&"MergeRequest".to_string()));
        assert!(names.contains(&"MEMBER_OF_siphon_members".to_string()));
        assert!(names.contains(&"SystemNote".to_string()));
        assert!(!names.contains(&"User".to_string()));
    }
}
