use std::collections::HashMap;

use futures::stream::{FuturesUnordered, StreamExt};
use indexer::indexing_status::{IndexingProgress, IndexingStatusStore};
use ontology::{EtlScope, Ontology};
use orbit_utils::traversal_path::TraversalPath;
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
    traversal_path: &TraversalPath,
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
    traversal_path: &TraversalPath,
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
                warn!(%error, %traversal_path, pipeline = name, "failed to read pipeline indexing progress");
            }
        }
    }

    PipelineReads {
        pipelines,
        read_errors,
    }
}

fn pipeline_states(reads: &PipelineReads) -> HashMap<String, IndexingState> {
    reads
        .pipelines
        .iter()
        .map(|p| (p.name.clone(), p.state))
        .collect()
}

fn aggregate_status(reads: &PipelineReads) -> IndexingStatus {
    let (worst_state, worst_progress) = reads
        .pipelines
        .iter()
        .map(|p| (p.state, p.progress.as_ref()))
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

    fn progress(
        started_ago_s: i64,
        completed_ago_s: Option<i64>,
        error: Option<&str>,
    ) -> IndexingProgress {
        let now = Utc::now();
        IndexingProgress {
            last_started_at: now - Duration::seconds(started_ago_s),
            last_completed_at: completed_ago_s.map(|s| now - Duration::seconds(s)),
            last_duration_ms: None,
            last_error: error.map(String::from),
            last_rows_read: None,
            last_rows_written: None,
        }
    }

    fn completed_progress(error: Option<&str>) -> IndexingProgress {
        progress(30, Some(25), error)
    }

    fn reads(pipelines: Vec<(&str, Option<IndexingProgress>)>) -> PipelineReads {
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
            read_errors: 0,
        }
    }

    #[test]
    fn derive_state_maps_progress_shape_to_state() {
        let cases = [
            (progress(0, None, None), IndexingState::Backfilling),
            (progress(30, Some(25), None), IndexingState::Indexed),
            (progress(0, Some(0), None), IndexingState::Indexed),
            (
                progress(30, Some(29), Some("deadline exceeded")),
                IndexingState::Error,
            ),
            (
                progress(0, None, Some("connection reset")),
                IndexingState::Backfilling,
            ),
            (progress(0, Some(60), None), IndexingState::Indexing),
        ];
        for (i, (input, expected)) in cases.iter().enumerate() {
            assert_eq!(
                derive_state(input),
                *expected,
                "case {i} expected {expected:?}"
            );
        }
    }

    #[test]
    fn aggregate_not_indexed_when_no_pipeline_progress() {
        let r = reads(vec![("MergeRequest", None), ("Issue", None)]);
        assert_eq!(aggregate_status(&r).state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_missing_entity_key_wins_over_indexed() {
        let r = reads(vec![
            ("MergeRequest", Some(completed_progress(None))),
            ("Issue", None),
        ]);
        assert_eq!(aggregate_status(&r).state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_error_wins_over_indexed_and_indexing() {
        let r = reads(vec![
            ("MergeRequest", Some(completed_progress(None))),
            ("Issue", Some(progress(0, Some(60), None))),
            ("Project", Some(completed_progress(Some("scan failure")))),
        ]);
        let aggregate = aggregate_status(&r);
        assert_eq!(aggregate.state, IndexingState::Error as i32);
        assert_eq!(
            aggregate.last_error.as_deref(),
            Some(SANITIZED_INDEXING_ERROR)
        );
    }

    #[test]
    fn pipeline_states_reports_per_pipeline() {
        let r = reads(vec![
            ("MergeRequest", Some(completed_progress(None))),
            (
                "MEMBER_OF_siphon_members",
                Some(completed_progress(Some("scan failure"))),
            ),
            ("Issue", None),
        ]);
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
        let mut with_rows = completed_progress(None);
        with_rows.last_rows_read = Some(307);
        with_rows.last_rows_written = Some(465);

        let status = indexing_status_from_progress(IndexingState::Indexed, &with_rows);

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
