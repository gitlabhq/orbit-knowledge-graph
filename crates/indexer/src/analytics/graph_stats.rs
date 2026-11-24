use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use labkit_events::StructuredEvent;
use orbit_analytics::{
    AnalyticsTracker, GitlabInstanceWatch, OrbitGraphStatsContext, OrbitNamespaceGraphStatsContext,
    orbit_graph_stats, orbit_namespace_graph_stats, validation,
};
use orbit_server_config::AnalyticsConfig;

use super::context::{build_common, saturating_i64};

const ORBIT_CATEGORY: &str = "orbit";
const ACTION_GRAPH_STATS: &str = "orbit_graph_stats";
const ACTION_NAMESPACE_GRAPH_STATS: &str = "orbit_namespace_graph_stats";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStats {
    pub graph_bytes: u64,
    pub entity_counts: BTreeMap<String, u64>,
    pub projects_indexed: u64,
    pub namespaces: Vec<NamespaceGraphStats>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceGraphStats {
    pub root_namespace_id: i64,
    pub graph_bytes: u64,
    pub entity_counts: BTreeMap<String, u64>,
    pub projects_indexed: u64,
}

#[derive(Clone)]
pub struct GraphStatsReporter {
    tracker: Arc<dyn AnalyticsTracker>,
    config: Arc<AnalyticsConfig>,
    instance: GitlabInstanceWatch,
}

impl GraphStatsReporter {
    pub fn new(
        tracker: Arc<dyn AnalyticsTracker>,
        config: Arc<AnalyticsConfig>,
        instance: GitlabInstanceWatch,
    ) -> Self {
        Self {
            tracker,
            config,
            instance,
        }
    }

    pub fn report(&self, stats: &GraphStats) -> Result<(), labkit_events::Error> {
        let mut events = vec![self.instance_event(stats)?];
        for namespace in &stats.namespaces {
            events.push(self.namespace_event(namespace)?);
        }
        for event in events {
            self.tracker.track(event);
        }
        Ok(())
    }

    fn instance_event(&self, stats: &GraphStats) -> Result<StructuredEvent, labkit_events::Error> {
        let common = build_common(&self.config, self.instance.identity(), None)?;
        let context = OrbitGraphStatsContext::new(orbit_graph_stats::OrbitGraphStats {
            graph_bytes: saturating_i64(stats.graph_bytes),
            entity_counts: typed_entity_counts(&stats.entity_counts)?,
            projects_indexed: saturating_i64(stats.projects_indexed),
            top_level_groups_indexed: saturating_i64(stats.namespaces.len() as u64),
        });
        StructuredEvent::builder(ORBIT_CATEGORY, ACTION_GRAPH_STATS)
            .context(common)
            .context(context)
            .build()
    }

    fn namespace_event(
        &self,
        namespace: &NamespaceGraphStats,
    ) -> Result<StructuredEvent, labkit_events::Error> {
        let common = build_common(
            &self.config,
            self.instance.identity(),
            Some(namespace.root_namespace_id),
        )?;
        let context = OrbitNamespaceGraphStatsContext::new(
            orbit_namespace_graph_stats::OrbitNamespaceGraphStats {
                graph_bytes: saturating_i64(namespace.graph_bytes),
                entity_counts: typed_entity_counts(&namespace.entity_counts)?,
                projects_indexed: saturating_i64(namespace.projects_indexed),
            },
        );
        StructuredEvent::builder(ORBIT_CATEGORY, ACTION_NAMESPACE_GRAPH_STATS)
            .context(common)
            .context(context)
            .build()
    }
}

fn typed_entity_counts<K>(
    counts: &BTreeMap<String, u64>,
) -> Result<HashMap<K, i64>, labkit_events::Error>
where
    K: FromStr + Eq + Hash,
    K::Err: std::fmt::Display,
{
    counts
        .iter()
        .map(|(node, count)| {
            let key = node.parse::<K>().map_err(validation("entity_counts"))?;
            Ok((key, saturating_i64(*count)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use orbit_analytics::{
        InMemoryAnalyticsTracker, ORBIT_COMMON_SCHEMA, ORBIT_GRAPH_STATS_SCHEMA,
        ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA, load_schema_json,
    };
    use orbit_server_config::{DeploymentConfig, DeploymentEnvironment, DeploymentKind};

    use super::*;

    fn config() -> Arc<AnalyticsConfig> {
        Arc::new(AnalyticsConfig {
            enabled: Some(true),
            collector_url: String::new(),
            local_collector_url: String::new(),
            deployment: DeploymentConfig {
                kind: DeploymentKind::SelfManaged,
                environment: DeploymentEnvironment::Production,
            },
        })
    }

    fn stats() -> GraphStats {
        GraphStats {
            graph_bytes: 437_763_723_476,
            entity_counts: BTreeMap::from([
                ("MergeRequest".to_string(), 8_238_413),
                ("Project".to_string(), 202_930),
            ]),
            projects_indexed: 207_587,
            namespaces: vec![
                namespace(9970, 40_000_000_000, 120_000),
                namespace(2769844, 9_000_000_000, 80_000),
            ],
        }
    }

    fn namespace(root_namespace_id: i64, graph_bytes: u64, projects: u64) -> NamespaceGraphStats {
        NamespaceGraphStats {
            root_namespace_id,
            graph_bytes,
            entity_counts: BTreeMap::from([("Project".to_string(), projects)]),
            projects_indexed: projects,
        }
    }

    fn context<'a>(event: &'a StructuredEvent, schema: &str) -> &'a serde_json::Value {
        &event
            .contexts()
            .iter()
            .find(|context| context.schema == schema)
            .unwrap_or_else(|| panic!("{schema} context attached"))
            .data
    }

    fn reporter(tracker: &Arc<InMemoryAnalyticsTracker>) -> GraphStatsReporter {
        GraphStatsReporter::new(tracker.clone(), config(), GitlabInstanceWatch::default())
    }

    #[test]
    fn report_sends_the_instance_event_that_matches_the_iglu_schema() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        reporter(&tracker).report(&stats()).unwrap();

        let events = tracker.drain();
        assert_eq!(events[0].action(), ACTION_GRAPH_STATS);
        let data = context(&events[0], ORBIT_GRAPH_STATS_SCHEMA);
        assert_eq!(data["graph_bytes"], 437_763_723_476_i64);
        assert_eq!(data["entity_counts"]["Project"], 202_930);
        assert_eq!(data["top_level_groups_indexed"], 2);
        assert!(
            context(&events[0], ORBIT_COMMON_SCHEMA)["root_namespace_ids"].is_null(),
            "the instance event names no group"
        );

        let validator = jsonschema::validator_for(&load_schema_json("orbit_graph_stats")).unwrap();
        assert!(validator.is_valid(data), "{data}");
    }

    #[test]
    fn report_sends_one_namespace_event_per_group_that_matches_the_iglu_schema() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        reporter(&tracker).report(&stats()).unwrap();

        let events = tracker.drain();
        assert_eq!(events.len(), 3, "one instance event and one per group");
        let validator =
            jsonschema::validator_for(&load_schema_json("orbit_namespace_graph_stats")).unwrap();
        for (event, root) in events[1..].iter().zip([9970, 2769844]) {
            assert_eq!(event.action(), ACTION_NAMESPACE_GRAPH_STATS);
            assert_eq!(
                context(event, ORBIT_COMMON_SCHEMA)["root_namespace_ids"],
                serde_json::json!([root])
            );
            let data = context(event, ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA);
            assert!(validator.is_valid(data), "{data}");
        }
        let data = context(&events[1], ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA);
        assert_eq!(data["graph_bytes"], 40_000_000_000_i64);
        assert_eq!(data["entity_counts"]["Project"], 120_000);
        assert_eq!(data["projects_indexed"], 120_000);
    }

    #[test]
    fn report_rejects_a_key_that_is_not_a_node_type_name() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut stats = stats();
        stats.entity_counts.insert("v118_gl_project".to_string(), 1);

        let error = reporter(&tracker).report(&stats).unwrap_err();

        assert!(error.to_string().contains("entity_counts"), "{error}");
        assert_eq!(tracker.count(), 0, "a rejected snapshot is not sent");
    }

    #[test]
    fn report_sends_nothing_when_one_group_is_rejected() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut stats = stats();
        stats.namespaces[1]
            .entity_counts
            .insert("v118_gl_project".to_string(), 1);

        let error = reporter(&tracker).report(&stats).unwrap_err();

        assert!(error.to_string().contains("entity_counts"), "{error}");
        assert_eq!(
            tracker.count(),
            0,
            "no event of a rejected snapshot is sent"
        );
    }
}
