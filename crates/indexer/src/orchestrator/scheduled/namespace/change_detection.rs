use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::{Trigger, TriggerTraversalPath};

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::TaskError;

const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";
const ROOT_PATH_PATTERN: &str = "^[0-9]+/[0-9]+/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChangedNamespace {
    pub namespace_id: i64,
    pub traversal_path: String,
    pub target_keys: Vec<String>,
}

#[async_trait]
pub(super) trait NamespaceChangeDetector: Send + Sync {
    async fn changed_namespaces(
        &self,
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<ChangedNamespace>, TaskError>;
}

pub(super) struct NamespaceTriggerDetector {
    datalake: ArrowClickHouseClient,
    query: NamespaceTriggerQuery,
}

impl NamespaceTriggerDetector {
    pub(super) fn new(datalake: ArrowClickHouseClient, ontology: &ontology::Ontology) -> Self {
        Self {
            datalake,
            query: NamespaceTriggerQuery::from_ontology(ontology),
        }
    }
}

#[async_trait]
impl NamespaceChangeDetector for NamespaceTriggerDetector {
    async fn changed_namespaces(
        &self,
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<ChangedNamespace>, TaskError> {
        let lower = lower.format(TIMESTAMP_FORMAT).to_string();
        let upper = upper.format(TIMESTAMP_FORMAT).to_string();
        let batches = self
            .datalake
            .query(&self.query.sql)
            .param("lower", lower)
            .param("upper", upper)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;

        let namespace_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        let target_keys = String::extract_column(&batches, 2).map_err(TaskError::new)?;

        Ok(namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .zip(target_keys)
            .map(
                |((namespace_id, traversal_path), target_keys)| ChangedNamespace {
                    namespace_id,
                    traversal_path,
                    target_keys: parse_target_keys(&target_keys),
                },
            )
            .collect())
    }
}

#[derive(Debug, Clone)]
struct NamespaceTriggerQuery {
    sql: String,
}

impl NamespaceTriggerQuery {
    fn from_ontology(ontology: &ontology::Ontology) -> Self {
        Self::from_sources(collect_namespace_trigger_sources(ontology))
    }

    #[cfg(test)]
    fn new(triggers: Vec<Trigger>) -> Self {
        Self::from_sources(
            triggers
                .into_iter()
                .map(|trigger| NamespaceTriggerSource {
                    target_keys: target_keys("Test"),
                    trigger,
                })
                .collect(),
        )
    }

    fn from_sources(sources: Vec<NamespaceTriggerSource>) -> Self {
        Self {
            sql: render_trigger_query(&dedupe_trigger_sources(sources)),
        }
    }
}

#[derive(Debug, Clone)]
struct NamespaceTriggerSource {
    target_keys: BTreeSet<String>,
    trigger: Trigger,
}

fn collect_namespace_trigger_sources(ontology: &ontology::Ontology) -> Vec<NamespaceTriggerSource> {
    let node_triggers = ontology
        .nodes()
        .filter_map(|node| node.etl.as_ref().map(|etl| (node.name.as_str(), etl)))
        .flat_map(|(target_key, etl)| trigger_sources(target_key, etl.triggers()));
    let derived_triggers = ontology
        .derived_entities()
        .flat_map(|derived| trigger_sources(derived.name.as_str(), derived.etl.triggers()));
    let edge_triggers = ontology
        .edge_etl_configs()
        .flat_map(|(relationship_kind, config)| {
            trigger_sources(relationship_kind, config.triggers.as_slice())
        });

    node_triggers
        .chain(derived_triggers)
        .chain(edge_triggers)
        .chain(std::iter::once(enabled_namespace_trigger_source()))
        .collect()
}

fn enabled_namespace_trigger_source() -> NamespaceTriggerSource {
    NamespaceTriggerSource {
        target_keys: target_keys("*"),
        trigger: Trigger {
            table: ENABLED_NAMESPACE_TABLE.to_string(),
            watermark: ontology::siphon_watermark_column().to_string(),
            traversal_path: TriggerTraversalPath::Column("traversal_path".to_string()),
        },
    }
}

fn trigger_sources(target_key: &str, triggers: &[Trigger]) -> Vec<NamespaceTriggerSource> {
    triggers
        .iter()
        .cloned()
        .map(|trigger| NamespaceTriggerSource {
            target_keys: target_keys(target_key),
            trigger,
        })
        .collect()
}

fn render_trigger_query(sources: &[NamespaceTriggerSource]) -> String {
    let changed = if sources.is_empty() {
        "SELECT '' AS root_path, array('') AS target_keys WHERE false".to_string()
    } else {
        sources
            .iter()
            .map(render_trigger_branch)
            .collect::<Vec<_>>()
            .join("\nUNION ALL\n")
    };

    let deleted_column = ontology::siphon_deleted_column();

    format!(
        r#"WITH
  {{upper:String}} AS upper,
  {{lower:String}} AS lower,
  enabled AS (
    SELECT DISTINCT root_namespace_id, traversal_path
    FROM {ENABLED_NAMESPACE_TABLE}
    WHERE {deleted_column} = false AND traversal_path != ''
  ),
  changed AS (
{changed}
  )
SELECT
  enabled.root_namespace_id,
  enabled.traversal_path,
  arrayStringConcat(
    arraySort(arrayDistinct(arrayFlatten(groupArray(changed.target_keys)))),
    ','
  ) AS target_keys
FROM changed
INNER JOIN enabled ON changed.root_path = enabled.traversal_path
GROUP BY enabled.root_namespace_id, enabled.traversal_path"#
    )
}

fn render_trigger_branch(source: &NamespaceTriggerSource) -> String {
    let path_expression = match &source.trigger.traversal_path {
        TriggerTraversalPath::Column(column) => column.clone(),
        TriggerTraversalPath::Dictionary {
            dictionary,
            key_column,
        } => format!(
            "dictGetOrDefault('{dictionary}', 'traversal_path', toUInt64({key_column}), '0/')"
        ),
    };
    let predicates = trigger_predicates(&source.trigger);

    format!(
        "    SELECT {root_path} AS root_path, {target_keys} AS target_keys\n    FROM {table}\n    WHERE {predicates}\n    GROUP BY root_path",
        root_path = root_path_expression(&path_expression),
        target_keys = render_target_keys(&source.target_keys),
        table = source.trigger.table,
        predicates = predicates.join("\n      AND ")
    )
}

fn trigger_predicates(trigger: &Trigger) -> Vec<String> {
    let path_expression = match &trigger.traversal_path {
        TriggerTraversalPath::Column(column) => column.clone(),
        TriggerTraversalPath::Dictionary {
            dictionary,
            key_column,
        } => format!(
            "dictGetOrDefault('{dictionary}', 'traversal_path', toUInt64({key_column}), '0/')"
        ),
    };
    vec![
        format!("{} > lower", trigger.watermark),
        format!("{} <= upper", trigger.watermark),
        format!("match({path_expression}, '{ROOT_PATH_PATTERN}')"),
    ]
}

fn root_path_expression(path_expression: &str) -> String {
    format!(
        "concat(splitByChar('/', {path_expression})[1], '/', splitByChar('/', {path_expression})[2], '/')"
    )
}

fn dedupe_trigger_sources(sources: Vec<NamespaceTriggerSource>) -> Vec<NamespaceTriggerSource> {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut deduped: Vec<NamespaceTriggerSource> = Vec::new();

    for source in sources {
        let key = trigger_source_key(&source);
        if let Some(index) = seen.get(&key) {
            deduped[*index]
                .target_keys
                .extend(source.target_keys.iter().cloned());
        } else {
            seen.insert(key, deduped.len());
            deduped.push(source);
        }
    }

    deduped
}

fn trigger_source_key(source: &NamespaceTriggerSource) -> String {
    let trigger = &source.trigger;
    format!(
        "{}\u{1f}{}\u{1f}{:?}",
        trigger.table, trigger.watermark, trigger.traversal_path
    )
}

fn render_target_keys(target_keys: &BTreeSet<String>) -> String {
    let values = target_keys
        .iter()
        .map(|key| format!("'{}'", sql_string_literal_content(key)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("array({values})")
}

fn parse_target_keys(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter(|target_key| !target_key.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn target_keys(target_key: &str) -> BTreeSet<String> {
    BTreeSet::from([target_key.to_string()])
}

fn sql_string_literal_content(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_trigger(table: &str) -> Trigger {
        Trigger {
            table: table.to_string(),
            watermark: "_siphon_watermark".to_string(),
            traversal_path: TriggerTraversalPath::Column("traversal_path".to_string()),
        }
    }

    fn trigger_source(target_key: &str, trigger: Trigger) -> NamespaceTriggerSource {
        NamespaceTriggerSource {
            target_keys: target_keys(target_key),
            trigger,
        }
    }

    #[test]
    fn trigger_query_filters_enabled_namespaces() {
        let query = NamespaceTriggerQuery::new(vec![column_trigger("work_items")]);
        assert!(query.sql.contains(ENABLED_NAMESPACE_TABLE));
        assert!(query.sql.contains("_siphon_deleted = false"));
        assert!(query.sql.contains("traversal_path != ''"));
        assert!(query.sql.contains("INNER JOIN enabled"));
    }

    #[test]
    fn trigger_query_uses_watermark_bounds() {
        let query = NamespaceTriggerQuery::new(vec![column_trigger("work_items")]);
        assert!(query.sql.contains("_siphon_watermark > lower"));
        assert!(query.sql.contains("_siphon_watermark <= upper"));
    }

    #[test]
    fn trigger_query_extracts_root_path() {
        let query = NamespaceTriggerQuery::new(vec![column_trigger("work_items")]);
        assert!(query.sql.contains("splitByChar('/', traversal_path)[1]"));
        assert!(query.sql.contains("splitByChar('/', traversal_path)[2]"));
        assert!(query.sql.contains(ROOT_PATH_PATTERN));
    }

    #[test]
    fn trigger_query_renders_dictionary_lookup() {
        let query = NamespaceTriggerQuery::new(vec![Trigger {
            table: "siphon_projects".to_string(),
            watermark: "_siphon_watermark".to_string(),
            traversal_path: TriggerTraversalPath::Dictionary {
                dictionary: "project_traversal_paths_dict".to_string(),
                key_column: "id".to_string(),
            },
        }]);
        assert!(query.sql.contains(
            "dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', toUInt64(id), '0/')"
        ));
    }

    #[test]
    fn trigger_query_combines_sources_with_union_all() {
        let query = NamespaceTriggerQuery::new(vec![
            column_trigger("work_items"),
            column_trigger("siphon_notes"),
        ]);
        assert!(query.sql.contains("UNION ALL"));
    }

    #[test]
    fn duplicate_triggers_render_once() {
        let query = NamespaceTriggerQuery::new(vec![
            column_trigger("work_items"),
            column_trigger("work_items"),
        ]);
        assert_eq!(query.sql.matches("FROM work_items").count(), 1);
    }

    #[test]
    fn duplicate_tables_for_distinct_targets_share_one_branch() {
        let query = NamespaceTriggerQuery::from_sources(vec![
            trigger_source("Note", column_trigger("siphon_notes")),
            trigger_source("SystemNote", column_trigger("siphon_notes")),
        ]);

        assert_eq!(query.sql.matches("FROM siphon_notes").count(), 1);
        assert!(
            query
                .sql
                .contains("array('Note', 'SystemNote') AS target_keys")
        );
        assert!(query.sql.contains("GROUP BY enabled.root_namespace_id"));
    }

    #[test]
    fn ontology_triggers_include_enabled_namespaces() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let sources = collect_namespace_trigger_sources(&ontology);

        assert!(sources.iter().any(|source| {
            source.target_keys.contains("*") && source.trigger.table == ENABLED_NAMESPACE_TABLE
        }));
    }
}
