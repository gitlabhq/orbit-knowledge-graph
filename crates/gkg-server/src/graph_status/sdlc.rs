use std::collections::HashMap;

use futures::stream::{FuturesUnordered, StreamExt};
use indexer::indexing_status::{IndexingProgress, IndexingStatusStore};
use ontology::{EtlScope, Ontology};
use tracing::warn;

use super::{state_priority, status_with_state, unknown_status};
use crate::proto::{IndexingState, IndexingStatus};

const SANITIZED_INDEXING_ERROR: &str = "Something went wrong during indexing.";

pub struct SdlcIndexingState {
    pub aggregate: Option<IndexingStatus>,
    pub node_states: HashMap<String, IndexingState>,
}

pub async fn get_sdlc_indexing_state(
    store: Option<&IndexingStatusStore>,
    ontology: &Ontology,
    traversal_path: &str,
) -> SdlcIndexingState {
    let Some(store) = store else {
        return SdlcIndexingState {
            aggregate: None,
            node_states: HashMap::new(),
        };
    };

    let reads = fetch_pipeline_progress(store, ontology, traversal_path).await;

    if reads.pipelines.is_empty() && reads.read_errors > 0 {
        return SdlcIndexingState {
            aggregate: Some(unknown_status()),
            node_states: HashMap::new(),
        };
    }

    let pipeline_states = pipeline_states(&reads);
    SdlcIndexingState {
        aggregate: Some(aggregate_status(&reads)),
        node_states: resolve_node_states(ontology, &pipeline_states),
    }
}

struct PipelineReads {
    pipelines: Vec<PipelineProgress>,
    legacy: Option<IndexingProgress>,
    read_errors: usize,
}

struct PipelineProgress {
    name: String,
    state: IndexingState,
    progress: Option<IndexingProgress>,
}

async fn fetch_pipeline_progress(
    store: &IndexingStatusStore,
    ontology: &Ontology,
    traversal_path: &str,
) -> PipelineReads {
    let names = namespaced_pipeline_names(ontology);

    let mut futures = FuturesUnordered::new();
    for name in &names {
        futures.push(async move { (name.as_str(), store.get_entity(traversal_path, name).await) });
    }

    let mut pipelines = Vec::new();
    let mut read_errors = 0usize;
    while let Some((name, result)) = futures.next().await {
        match result {
            Ok(progress) => pipelines.push(PipelineProgress {
                name: name.to_string(),
                state: progress
                    .as_ref()
                    .map_or(IndexingState::NotIndexed, derive_state),
                progress,
            }),
            Err(error) => {
                read_errors += 1;
                warn!(%error, traversal_path, pipeline = name, "failed to read pipeline indexing progress");
            }
        }
    }

    let legacy = match store.get(traversal_path).await {
        Ok(progress) => progress,
        Err(error) => {
            read_errors += 1;
            warn!(%error, traversal_path, "failed to read indexing progress from NATS KV");
            None
        }
    };

    PipelineReads {
        pipelines,
        legacy,
        read_errors,
    }
}

// Rollout fallback: nothing per-pipeline has been written yet → the legacy single-key state
// stands in for every known pipeline so pre-MR deployments keep annotating nodes.
fn pipeline_states(reads: &PipelineReads) -> HashMap<String, IndexingState> {
    if reads.pipelines.iter().all(|p| p.progress.is_none()) {
        let legacy_state = reads
            .legacy
            .as_ref()
            .map_or(IndexingState::NotIndexed, derive_state);
        return reads
            .pipelines
            .iter()
            .map(|p| (p.name.clone(), legacy_state))
            .collect();
    }

    reads
        .pipelines
        .iter()
        .map(|p| (p.name.clone(), p.state))
        .collect()
}

fn aggregate_status(reads: &PipelineReads) -> IndexingStatus {
    let any_present = reads.pipelines.iter().any(|p| p.progress.is_some());
    if !any_present {
        return match &reads.legacy {
            None => status_with_state(IndexingState::NotIndexed),
            Some(progress) => indexing_status_from_progress(derive_state(progress), progress),
        };
    }

    let (worst_state, worst_progress) = reads
        .pipelines
        .iter()
        .map(|p| (p.state, p.progress.as_ref()))
        .chain(reads.legacy.as_ref().map(|p| (derive_state(p), Some(p))))
        .max_by_key(|(state, _)| state_priority(*state))
        .unwrap_or((IndexingState::NotIndexed, None));

    match worst_progress {
        Some(progress) => indexing_status_from_progress(worst_state, progress),
        None => status_with_state(worst_state),
    }
}

fn derive_state(progress: &IndexingProgress) -> IndexingState {
    match progress.last_completed_at {
        None => IndexingState::Backfilling,
        Some(completed) if progress.last_started_at > completed => IndexingState::Indexing,
        Some(_) if progress.last_error.is_some() => IndexingState::Error,
        Some(_) => IndexingState::Indexed,
    }
}

fn indexing_status_from_progress(state: IndexingState, p: &IndexingProgress) -> IndexingStatus {
    IndexingStatus {
        state: state.into(),
        last_started_at: Some(p.last_started_at.to_rfc3339()),
        last_completed_at: p.last_completed_at.map(|t| t.to_rfc3339()),
        last_duration_ms: p.last_duration_ms,
        last_error: p
            .last_error
            .as_ref()
            .map(|_| SANITIZED_INDEXING_ERROR.to_string()),
        last_rows_read: p.last_rows_read,
        last_rows_written: p.last_rows_written,
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

    fn completed_progress(error: Option<&str>) -> IndexingProgress {
        let started = Utc::now() - Duration::seconds(30);
        IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(5)),
            last_duration_ms: Some(5000),
            last_error: error.map(String::from),
            last_rows_read: None,
            last_rows_written: None,
        }
    }

    fn reads(
        pipelines: Vec<(&str, Option<IndexingProgress>)>,
        legacy: Option<IndexingProgress>,
    ) -> PipelineReads {
        PipelineReads {
            pipelines: pipelines
                .into_iter()
                .map(|(name, progress)| PipelineProgress {
                    name: name.to_string(),
                    state: progress
                        .as_ref()
                        .map_or(IndexingState::NotIndexed, derive_state),
                    progress,
                })
                .collect(),
            legacy,
            read_errors: 0,
        }
    }

    #[test]
    fn derive_state_backfilling_when_started_but_not_completed() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Backfilling);
    }

    #[test]
    fn derive_state_indexed_when_completed_successfully() {
        let started = Utc::now();
        let progress = IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(5)),
            last_duration_ms: Some(5000),
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Indexed);
    }

    #[test]
    fn derive_state_indexed_when_started_equals_completed() {
        let now = Utc::now();
        let progress = IndexingProgress {
            last_started_at: now,
            last_completed_at: Some(now),
            last_duration_ms: Some(0),
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Indexed);
    }

    #[test]
    fn derive_state_error_when_completed_with_error() {
        let started = Utc::now();
        let progress = IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(1)),
            last_duration_ms: Some(1000),
            last_error: Some("deadline exceeded".to_string()),
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Error);
    }

    #[test]
    fn derive_state_backfilling_when_error_but_not_completed() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: Some("connection reset".to_string()),
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Backfilling);
    }

    #[test]
    fn derive_state_indexing_when_started_after_completion() {
        let completed = Utc::now() - Duration::seconds(60);
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: Some(completed),
            last_duration_ms: Some(5000),
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        assert_eq!(derive_state(&progress), IndexingState::Indexing);
    }

    #[test]
    fn aggregate_falls_back_to_legacy_when_no_entity_keys_present() {
        let r = reads(
            vec![("MergeRequest", None), ("Issue", None)],
            Some(completed_progress(None)),
        );
        assert_eq!(aggregate_status(&r).state, IndexingState::Indexed as i32);
    }

    #[test]
    fn aggregate_not_indexed_when_no_entity_keys_and_no_legacy() {
        let r = reads(vec![("MergeRequest", None), ("Issue", None)], None);
        assert_eq!(aggregate_status(&r).state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_missing_entity_key_wins_over_indexed() {
        let r = reads(
            vec![
                ("MergeRequest", Some(completed_progress(None))),
                ("Issue", None),
            ],
            None,
        );
        assert_eq!(aggregate_status(&r).state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_error_wins_over_indexed_and_indexing() {
        let in_flight = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: Some(Utc::now() - Duration::seconds(60)),
            last_duration_ms: Some(5000),
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        let r = reads(
            vec![
                ("MergeRequest", Some(completed_progress(None))),
                ("Issue", Some(in_flight)),
                ("Project", Some(completed_progress(Some("scan failure")))),
            ],
            None,
        );
        let aggregate = aggregate_status(&r);
        assert_eq!(aggregate.state, IndexingState::Error as i32);
        assert_eq!(
            aggregate.last_error.as_deref(),
            Some(SANITIZED_INDEXING_ERROR)
        );
    }

    #[test]
    fn aggregate_legacy_folds_into_worst_state() {
        let legacy = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        let r = reads(
            vec![("MergeRequest", Some(completed_progress(None)))],
            Some(legacy),
        );
        assert_eq!(
            aggregate_status(&r).state,
            IndexingState::Backfilling as i32
        );
    }

    #[test]
    fn pipeline_states_reports_per_pipeline() {
        let r = reads(
            vec![
                ("MergeRequest", Some(completed_progress(None))),
                (
                    "MEMBER_OF_siphon_members",
                    Some(completed_progress(Some("scan failure"))),
                ),
                ("Issue", None),
            ],
            None,
        );
        let states = pipeline_states(&r);

        assert_eq!(states.get("MergeRequest"), Some(&IndexingState::Indexed));
        assert_eq!(
            states.get("MEMBER_OF_siphon_members"),
            Some(&IndexingState::Error)
        );
        assert_eq!(states.get("Issue"), Some(&IndexingState::NotIndexed));
        assert_eq!(aggregate_status(&r).state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn pipeline_states_fallback_assigns_legacy_state_to_every_pipeline() {
        let r = reads(
            vec![("MergeRequest", None), ("Issue", None)],
            Some(completed_progress(None)),
        );
        let states = pipeline_states(&r);

        assert_eq!(aggregate_status(&r).state, IndexingState::Indexed as i32);
        assert_eq!(states.get("MergeRequest"), Some(&IndexingState::Indexed));
        assert_eq!(states.get("Issue"), Some(&IndexingState::Indexed));
    }

    #[test]
    fn indexing_status_replaces_raw_error_with_generic_message() {
        let raw = "processing failed: failed to finish write for example_internal_table: failed \
             to write batch: bad response: Code: 999. DB::NetException: Timeout exceeded while \
             reading from socket (peer: 192.0.2.1:55555, local: 198.51.100.2:8124, 30000 ms). \
             (SOCKET_TIMEOUT) (version 0.0.0.0 (official build))";
        let status =
            indexing_status_from_progress(IndexingState::Error, &completed_progress(Some(raw)));

        assert_eq!(status.last_error.as_deref(), Some(SANITIZED_INDEXING_ERROR));
        for leaked in [
            "example_internal_table",
            "192.0.2.1",
            "8124",
            "0.0.0.0",
            "Code: 999",
        ] {
            assert!(
                !status.last_error.as_deref().unwrap().contains(leaked),
                "graph status leaked {leaked:?}"
            );
        }
    }

    #[test]
    fn indexing_status_keeps_error_absent_on_success() {
        let status =
            indexing_status_from_progress(IndexingState::Indexed, &completed_progress(None));
        assert!(status.last_error.is_none());
    }

    #[test]
    fn indexing_status_from_progress_carries_rows() {
        let mut progress = completed_progress(None);
        progress.last_rows_read = Some(307);
        progress.last_rows_written = Some(465);

        let status = indexing_status_from_progress(IndexingState::Indexed, &progress);

        assert_eq!(status.last_rows_read, Some(307));
        assert_eq!(status.last_rows_written, Some(465));

        let without_rows =
            indexing_status_from_progress(IndexingState::Indexed, &completed_progress(None));
        assert_eq!(without_rows.last_rows_read, None);
        assert_eq!(without_rows.last_rows_written, None);
    }

    fn find_node<'a>(ontology: &'a Ontology, name: &str) -> &'a ontology::NodeEntity {
        ontology
            .nodes()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("node {name} should exist"))
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
        let empty = HashMap::new();

        assert!(
            find_node(&ontology, "Definition").pipelines.is_empty(),
            "Definition is a code node handled by the code surface"
        );
        let resolved = resolve_node_states(&ontology, &empty);
        assert!(!resolved.contains_key("Definition"));
        assert!(
            !resolved.contains_key("User"),
            "global-only nodes report no per-entity state"
        );
    }

    #[test]
    fn namespaced_pipeline_names_include_edge_and_derived_pipelines() {
        let names = namespaced_pipeline_names(&test_ontology());

        assert!(names.contains(&"MergeRequest".to_string()));
        assert!(names.contains(&"MEMBER_OF_siphon_members".to_string()));
        assert!(names.contains(&"SystemNote".to_string()));
        assert!(
            !names.contains(&"User".to_string()),
            "User is global-scoped"
        );
    }
}
