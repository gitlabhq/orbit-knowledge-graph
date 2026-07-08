mod datalake;
mod handler;
mod metrics;
pub(crate) mod observer;
mod partitioning;
mod pipeline;
mod plan;
mod transform;

use std::collections::BTreeSet;
use std::sync::Arc;

use ontology::EtlScope;
use ontology::migrations::{ScopeDeclaration, sdlc_entity_names};

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
use handler::entity::EntityHandler;
use metrics::SdlcMetrics;
use pipeline::Pipeline;
use tracing::{info, warn};

/// The dispatch-plan names for a set of re-index targets, split by scope. Plan
/// names are the segment after `ns.<id>.` / `global.` in a checkpoint key, so
/// checkpoint seeding and the completion gate use them to identify which keys
/// belong to invalidated plans. Names are independent of batch sizes.
#[derive(Debug)]
pub struct DispatchPlanNames {
    pub namespaced: Vec<String>,
    pub global: Vec<String>,
}

/// The dispatch plans a narrowed `sdlc` migration invalidates: the scope's
/// entities (or every SDLC entity when it names none), with each FK-derived
/// relationship kind expanded to the entities that emit it. A target that
/// resolves to no plan even after expansion is an orphan edge kind — declared
/// but produced by no dispatcher, so it carries no data and is dropped; it
/// contributes no checkpoint to seed out or gate on, which is correct.
pub fn invalidated_dispatch_plans(
    ontology: &ontology::Ontology,
    scope: &ScopeDeclaration,
) -> DispatchPlanNames {
    let requested = if scope.entities.is_empty() {
        sdlc_entity_names(ontology)
    } else {
        scope.entities.clone()
    };
    let dispatchable = expand_targets_to_emitting_entities(ontology, &requested);
    plan_names_for_targets(ontology, &dispatchable)
}

pub fn plan_names_for_targets(
    ontology: &ontology::Ontology,
    targets: &BTreeSet<String>,
) -> DispatchPlanNames {
    let plans = plan::build_plans(ontology, 1, 1, &Default::default());
    let matching = |scoped: Vec<plan::Plan>| -> Vec<String> {
        scoped
            .into_iter()
            .filter(|plan| targets.contains(&plan.target))
            .map(|plan| plan.name)
            .collect()
    };
    DispatchPlanNames {
        namespaced: matching(plans.namespaced),
        global: matching(plans.global),
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

    let mut global_subscription = GlobalIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(GLOBAL_HANDLER_TOPIC) {
        global_subscription = global_subscription.with_config(topic_config);
    }
    let mut namespace_subscription = NamespaceIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(NAMESPACE_HANDLER_TOPIC) {
        namespace_subscription = namespace_subscription.with_config(topic_config);
    }

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

/// Replaces each target that no dispatch plan produces directly (an FK-derived
/// relationship kind like `HAS_NOTE`, emitted only as a side effect of a node's
/// ETL) with the node/derived entities that emit it — re-running those plans
/// re-emits the edges, which supersede in the cloned edge table. Targets that
/// already have a plan pass through; orphan edge kinds with no emitter drop out.
fn expand_targets_to_emitting_entities(
    ontology: &ontology::Ontology,
    targets: &BTreeSet<String>,
) -> BTreeSet<String> {
    let plan_targets = dispatch_plan_targets(ontology);
    let mut dispatchable = BTreeSet::new();
    for target in targets {
        if plan_targets.contains(target) {
            dispatchable.insert(target.clone());
            continue;
        }
        let emitters = emitting_entities_with_plan(ontology, target, &plan_targets);
        if emitters.is_empty() {
            warn!(
                entity = %target,
                "invalidated target has no dispatch plan and no emitter — excluded from \
                 seeding and gating as an orphan (produces no rows). A build-time test pins \
                 the orphan set; a new orphan here means a real emitter mapping was missed"
            );
        }
        dispatchable.extend(emitters);
    }
    dispatchable
}

fn dispatch_plan_targets(ontology: &ontology::Ontology) -> BTreeSet<String> {
    let plans = plan::build_plans(ontology, 1, 1, &Default::default());
    plans
        .namespaced
        .into_iter()
        .chain(plans.global)
        .map(|plan| plan.target)
        .collect()
}

fn emitting_entities_with_plan(
    ontology: &ontology::Ontology,
    kind: &str,
    plan_targets: &BTreeSet<String>,
) -> BTreeSet<String> {
    ontology
        .nodes()
        .map(|node| node.name.clone())
        .chain(
            ontology
                .derived_entities()
                .map(|derived| derived.name.clone()),
        )
        .filter(|entity| {
            plan_targets.contains(entity)
                && ontology
                    .relationship_kinds_emitted_by(entity)
                    .contains(kind)
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod test_helpers;

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::Ontology;

    #[test]
    fn plan_names_for_targets_splits_by_scope() {
        let ontology = Ontology::load_embedded().expect("should load ontology");

        let note = plan_names_for_targets(&ontology, &BTreeSet::from(["Note".to_string()]));
        assert!(note.namespaced.contains(&"Note".to_string()));
        assert!(note.global.is_empty());

        let user = plan_names_for_targets(&ontology, &BTreeSet::from(["User".to_string()]));
        assert!(user.global.contains(&"User".to_string()));
        assert!(user.namespaced.is_empty());
    }

    #[test]
    fn plan_names_for_targets_ignores_unknown_targets() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let names = plan_names_for_targets(&ontology, &BTreeSet::from(["Ghost".to_string()]));
        assert!(names.namespaced.is_empty());
        assert!(names.global.is_empty());
    }

    fn sdlc_scope(entities: &[&str]) -> ScopeDeclaration {
        ScopeDeclaration {
            scope: ontology::migrations::Scope::Sdlc,
            entities: entities.iter().map(|s| s.to_string()).collect(),
        }
    }

    // HAS_NOTE is FK-derived: emitted by the Note node's ETL, with no dispatch
    // plan of its own. Invalidating it must re-index Note.
    #[test]
    fn invalidated_dispatch_plans_expands_fk_edge_kind_to_emitting_node() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = invalidated_dispatch_plans(&ontology, &sdlc_scope(&["HAS_NOTE"]));
        assert!(
            plans.namespaced.contains(&"Note".to_string()),
            "HAS_NOTE must expand to the Note plan: {:?}",
            plans.namespaced
        );
    }

    #[test]
    fn invalidated_dispatch_plans_whole_sdlc_covers_every_plan() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let whole = invalidated_dispatch_plans(&ontology, &sdlc_scope(&[]));
        let all = plan::build_plans(&ontology, 1, 1, &Default::default());
        assert_eq!(whole.namespaced.len(), all.namespaced.len());
        assert_eq!(whole.global.len(), all.global.len());
    }

    // A narrowed migration skips (with a warn) any invalidated target that
    // resolves to zero plans, treating it as an orphan with no rows to
    // re-index. That skip is only safe while the orphan set stays empty: an
    // entry here means the emitted-kinds mapping missed a real emitter (a
    // silently un-re-indexed change), so this pins the set at build time
    // instead of discovering it at runtime.
    #[test]
    fn orphan_sdlc_entities_are_exactly_the_known_set() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let orphans: BTreeSet<String> = sdlc_entity_names(&ontology)
            .into_iter()
            .filter(|entity| {
                expand_targets_to_emitting_entities(&ontology, &BTreeSet::from([entity.clone()]))
                    .is_empty()
            })
            .collect();
        assert_eq!(
            orphans,
            BTreeSet::new(),
            "declared edge kinds that no ETL produces; if this set changes, confirm the new \
             entry is a genuine orphan and not a missed emitter mapping"
        );
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
