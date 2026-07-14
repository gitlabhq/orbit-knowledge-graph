mod datalake;
mod handler;
mod metrics;
pub(crate) mod observer;
mod partitioning;
mod pipeline;
mod plan;
mod transform;

use std::sync::Arc;

use ontology::EtlScope;

use crate::IndexerConfig;
use crate::analytics::IndexingAnalytics;
use crate::checkpoint::ClickHouseCheckpointStore;
use crate::clickhouse::ClickHouseConfigurationExt;

use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{
    GLOBAL_HANDLER_TOPIC, GlobalIndexingRequest, NAMESPACE_HANDLER_TOPIC, NamespaceIndexingRequest,
};
use crate::types::Event;
use datalake::{Datalake, DatalakeQuery};
use gkg_server_config::SubscriptionConfig;
use handler::entity::EntityHandler;
use metrics::SdlcMetrics;
use pipeline::Pipeline;
use tracing::info;

const SDLC_CONCURRENCY_GROUP: &str = "sdlc";

/// Default subscription policy shared by the SDLC dispatch topics
/// ([`GLOBAL_HANDLER_TOPIC`] and [`NAMESPACE_HANDLER_TOPIC`]). Both are
/// re-dispatched every scheduler cycle, so a transient failure is retried on the
/// next cycle rather than in-band — `max_attempts: 1`, no dead-letter.
pub fn sdlc_dispatch_topic_policy() -> SubscriptionConfig {
    SubscriptionConfig {
        concurrency_group: Some(SDLC_CONCURRENCY_GROUP.to_string()),
        max_attempts: Some(1),
        retry_interval_secs: Some(60),
        dead_letter_on_exhaustion: None,
        max_ack_pending: None,
    }
}

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
    writer: Arc<crate::clickhouse::ClickHouseWriter>,
    analytics: IndexingAnalytics,
) -> Result<(), HandlerInitError> {
    let entity_handler_config = config.engine.handlers.entity_handler.clone();

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(
        datalake_client,
        entity_handler_config.stream_block_size,
    ));
    let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();

    let inputs = plan::input::from_ontology(ontology);
    let partition_strategies = partitioning::build_strategies(
        &inputs,
        &entity_handler_config.partition_overrides,
        entity_handler_config.partition_min_rows,
    );
    let plans = plan::build_plans(
        ontology,
        entity_handler_config.datalake_batch_size,
        entity_handler_config.datalake_batch_size,
        &entity_handler_config.batch_size_overrides,
    );

    let mut transform_registry = transform::TransformRegistry::default();
    transform::system_notes::register(
        &mut transform_registry,
        Arc::clone(&datalake),
        ontology.edge_table(),
        entity_handler_config.system_notes_resolve_lookup_batch_size,
    );
    let transform_registry = Arc::new(transform_registry);

    let pipeline = Arc::new(
        Pipeline::new(
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            metrics.clone(),
            config.engine.datalake_retry.clone(),
        )
        .with_registry(Arc::clone(&transform_registry)),
    );

    let global_policy = sdlc_dispatch_topic_policy()
        .with_optional_override(config.engine.topics.get(GLOBAL_HANDLER_TOPIC));
    let global_subscription = GlobalIndexingRequest::subscription().with_config(&global_policy);
    let namespace_policy = sdlc_dispatch_topic_policy()
        .with_optional_override(config.engine.topics.get(NAMESPACE_HANDLER_TOPIC));
    let namespace_subscription =
        NamespaceIndexingRequest::subscription().with_config(&namespace_policy);

    let mut global_count = 0;
    let mut namespaced_count = 0;
    for plan in plans.global {
        if !transform_registry.is_registered(&plan.transform) {
            info!(entity = %plan.name, transform = ?plan.transform, "skipping handler: transform not registered");
            continue;
        }
        let strategy = partition_strategies.get(&plan.name).cloned();
        registry.register_handler(Box::new(EntityHandler::new(
            plan,
            EtlScope::Global,
            Arc::clone(&pipeline),
            Arc::clone(&writer),
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            metrics.clone(),
            global_subscription.clone(),
            strategy,
            analytics.clone(),
        )));
        global_count += 1;
    }
    for plan in plans.namespaced {
        if !transform_registry.is_registered(&plan.transform) {
            info!(entity = %plan.name, transform = ?plan.transform, "skipping handler: transform not registered");
            continue;
        }
        let strategy = partition_strategies.get(&plan.name).cloned();
        registry.register_handler(Box::new(EntityHandler::new(
            plan,
            EtlScope::Namespaced,
            Arc::clone(&pipeline),
            Arc::clone(&writer),
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            metrics.clone(),
            namespace_subscription.clone(),
            strategy,
            analytics.clone(),
        )));
        namespaced_count += 1;
    }

    info!(
        global_handlers = global_count,
        namespaced_handlers = namespaced_count,
        partitioned_entities = partition_strategies.len(),
        "registered SDLC entity handlers"
    );

    Ok(())
}

#[cfg(test)]
pub(crate) mod test_helpers;

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::Ontology;

    #[test]
    fn declared_dispatch_policy_does_not_retry() {
        let policy = sdlc_dispatch_topic_policy();
        assert_eq!(policy.max_attempts, Some(1));
        assert_eq!(policy.dead_letter_on_exhaustion, None);
        assert_eq!(policy.concurrency_group.as_deref(), Some("sdlc"));
    }

    #[test]
    fn build_plans_returns_global_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());
        let names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"User"));
    }

    #[test]
    fn build_plans_returns_namespaced_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());
        let names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"Group"));
        assert!(names.contains(&"Project"));
    }

    #[test]
    fn build_plans_wires_system_note_derived_entity_as_extract_only_plan() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());

        let system_note = plans
            .namespaced
            .iter()
            .find(|p| p.name == "SystemNote")
            .expect("SystemNote derived entity should produce a namespaced plan");

        assert!(
            matches!(&system_note.transform, plan::TransformSpec::Rust(name) if name == "system_notes"),
            "derived entities name a custom transform, not data_fusion: {:?}",
            system_note.transform
        );
        let template = &system_note.extract_template;

        // #830: the metadata join is bounded by the page CTE, not inlined
        // above the LIMIT. The _batch CTE contains only the base table scan;
        // siphon_system_note_metadata is read via an enrichment CTE scoped
        // to `note_id IN (SELECT DISTINCT id FROM _batch)`.
        assert!(
            template.contains("WITH _batch AS ("),
            "extract must wrap the base scan in a _batch CTE: {template}"
        );
        assert!(
            !template.contains("INNER JOIN siphon_system_note_metadata"),
            "metadata table must not be inlined above the LIMIT: {template}"
        );
        assert!(
            template.contains("note_id IN (SELECT DISTINCT id FROM _batch)"),
            "enrichment CTE must scope metadata to the page: {template}"
        );
        assert!(
            template.contains("_e0.action AS action"),
            "action column must be projected from the enrichment CTE: {template}"
        );
        assert!(
            template.contains("LEFT JOIN _e0"),
            "enrichment must LEFT JOIN back onto _batch: {template}"
        );
        assert!(template.contains("sn.system = true"));
        assert!(template.contains("snm._siphon_deleted = false"));
        assert!(
            template.contains("startsWith(snm.traversal_path, {traversal_path:String})"),
            "enrichment CTE must prune by traversal_path: {template}"
        );
        assert!(template.contains("ORDER BY traversal_path, id"));
        assert_eq!(system_note.watermark_column, "sn._siphon_watermark");
    }

    #[test]
    fn every_reindex_target_maps_to_a_dispatch_plan() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());

        let dispatch_targets: std::collections::BTreeSet<&str> =
            plans.namespaced.iter().map(|p| p.target.as_str()).collect();

        let orphans: Vec<String> = ontology
            .reindex_sources()
            .into_iter()
            .map(|source| source.target)
            .filter(|target| !dispatch_targets.contains(target.as_str()))
            .collect();

        assert!(
            orphans.is_empty(),
            "reindex_on targets without a namespaced dispatch plan would be silently \
             skipped by incremental dispatch: {orphans:?}"
        );
    }
}
