use std::collections::BTreeMap;
use std::sync::Arc;

use labkit_events::StructuredEvent;
use orbit_analytics::{
    AnalyticsTracker, GitlabInstanceWatch, OrbitGraphStatsContext, orbit_graph_stats, validation,
};
use orbit_server_config::AnalyticsConfig;

use super::context::{build_common, saturating_i64};

const ORBIT_CATEGORY: &str = "orbit";
const ACTION_GRAPH_STATS: &str = "orbit_graph_stats";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStats {
    pub graph_bytes: u64,
    pub entity_counts: BTreeMap<String, u64>,
    pub projects_indexed: u64,
    pub top_level_groups_indexed: u64,
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
        let common = build_common(&self.config, self.instance.identity(), None)?;
        let entity_counts = stats
            .entity_counts
            .iter()
            .map(|(node, count)| {
                let key = node
                    .parse::<orbit_graph_stats::OrbitGraphStatsEntityCountsKey>()
                    .map_err(validation("entity_counts"))?;
                Ok((key, saturating_i64(*count)))
            })
            .collect::<Result<_, labkit_events::Error>>()?;
        let context = OrbitGraphStatsContext::new(orbit_graph_stats::OrbitGraphStats {
            graph_bytes: saturating_i64(stats.graph_bytes),
            entity_counts,
            projects_indexed: saturating_i64(stats.projects_indexed),
            top_level_groups_indexed: saturating_i64(stats.top_level_groups_indexed),
        });

        let event = StructuredEvent::builder(ORBIT_CATEGORY, ACTION_GRAPH_STATS)
            .context(common)
            .context(context)
            .build()?;
        self.tracker.track(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use orbit_analytics::{InMemoryAnalyticsTracker, ORBIT_GRAPH_STATS_SCHEMA, load_schema_json};
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
            top_level_groups_indexed: 2,
        }
    }

    fn reporter(tracker: &Arc<InMemoryAnalyticsTracker>) -> GraphStatsReporter {
        GraphStatsReporter::new(tracker.clone(), config(), GitlabInstanceWatch::default())
    }

    #[test]
    fn report_sends_one_event_that_matches_the_iglu_schema() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        reporter(&tracker).report(&stats()).unwrap();

        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action(), ACTION_GRAPH_STATS);
        let data = &events[0]
            .contexts()
            .iter()
            .find(|context| context.schema == *ORBIT_GRAPH_STATS_SCHEMA)
            .expect("orbit_graph_stats context attached")
            .data;
        assert_eq!(data["graph_bytes"], 437_763_723_476_i64);
        assert_eq!(data["entity_counts"]["Project"], 202_930);
        assert_eq!(data["top_level_groups_indexed"], 2);

        let validator = jsonschema::validator_for(&load_schema_json("orbit_graph_stats")).unwrap();
        assert!(validator.is_valid(data), "{data}");
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
}
