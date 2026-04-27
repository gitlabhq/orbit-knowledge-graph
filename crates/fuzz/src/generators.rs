//! Structured generators for fuzz testing.
//!
//! Generates JSON query strings that conform to (or partially conform to)
//! the GKG query DSL schema, allowing fuzzing to reach deeper compiler logic
//! beyond initial JSON parsing.

use bolero::generator::TypeGenerator;
use bolero::generator::bolero_generator::Driver;
use compiler::Ontology;
use serde_json::{Map, Value, json};
use std::sync::OnceLock;

fn ontology() -> &'static Ontology {
    static ONT: OnceLock<Ontology> = OnceLock::new();
    ONT.get_or_init(|| Ontology::load_embedded().expect("load embedded ontology"))
}

fn ontology_node_names() -> &'static Vec<String> {
    static NAMES: OnceLock<Vec<String>> = OnceLock::new();
    NAMES.get_or_init(|| ontology().node_names().map(String::from).collect())
}

fn ontology_edge_names() -> &'static Vec<String> {
    static NAMES: OnceLock<Vec<String>> = OnceLock::new();
    NAMES.get_or_init(|| ontology().edge_names().map(String::from).collect())
}

/// Pick a random item from a slice using the bolero driver.
fn pick<'a, T>(driver: &mut impl Driver, items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() {
        return None;
    }
    let idx: u8 = driver.produce()?;
    Some(&items[idx as usize % items.len()])
}

/// Pick either a valid ontology name or a random garbage string.
fn pick_entity_name(driver: &mut impl Driver) -> Option<String> {
    let use_valid: bool = driver.produce()?;
    if use_valid {
        pick(driver, ontology_node_names()).cloned()
    } else {
        let garbage: Vec<u8> = driver.produce()?;
        Some(String::from_utf8_lossy(&garbage[..garbage.len().min(20)]).into_owned())
    }
}

fn pick_edge_name(driver: &mut impl Driver) -> Option<String> {
    let use_valid: bool = driver.produce()?;
    if use_valid {
        pick(driver, ontology_edge_names()).cloned()
    } else {
        let garbage: Vec<u8> = driver.produce()?;
        Some(String::from_utf8_lossy(&garbage[..garbage.len().min(20)]).into_owned())
    }
}

const QUERY_TYPES: &[&str] = &["traversal", "aggregation", "path_finding", "neighbors"];
const FILTER_OPS: &[&str] = &[
    "eq",
    "gt",
    "lt",
    "gte",
    "lte",
    "in",
    "contains",
    "starts_with",
    "ends_with",
    "is_null",
    "is_not_null",
];
const DIRECTIONS: &[&str] = &["outgoing", "incoming", "both"];
const AGG_FUNCTIONS: &[&str] = &["count", "sum", "avg", "min", "max", "collect"];
const PATH_TYPES: &[&str] = &["shortest", "all_shortest", "any"];

fn gen_filter(driver: &mut impl Driver) -> Option<Value> {
    let use_op: bool = driver.produce()?;
    if use_op {
        let op = *pick(driver, FILTER_OPS)?;
        let val: i64 = driver.produce()?;
        Some(json!({"op": op, "value": val}))
    } else {
        let val: i64 = driver.produce()?;
        Some(json!(val))
    }
}

fn gen_node(driver: &mut impl Driver, id: &str) -> Option<Value> {
    let entity = pick_entity_name(driver)?;
    let mut node = Map::new();
    node.insert("id".into(), json!(id));
    node.insert("entity".into(), json!(entity));

    let has_columns: bool = driver.produce()?;
    if has_columns {
        let use_wildcard: bool = driver.produce()?;
        if use_wildcard {
            node.insert("columns".into(), json!("*"));
        } else {
            let cols = ["id", "title", "name", "created_at", "updated_at"];
            let n: usize = driver.produce()?;
            let selected: Vec<&str> = cols.iter().copied().take((n % cols.len()) + 1).collect();
            node.insert("columns".into(), json!(selected));
        }
    }

    let has_filters: bool = driver.produce()?;
    if has_filters {
        let mut filters = Map::new();
        let filter_count: u8 = driver.produce()?;
        for i in 0..(filter_count % 3) + 1 {
            let field = format!("field_{i}");
            if let Some(f) = gen_filter(driver) {
                filters.insert(field, f);
            }
        }
        node.insert("filters".into(), Value::Object(filters));
    }

    let has_node_ids: bool = driver.produce()?;
    if has_node_ids {
        let count: u8 = driver.produce()?;
        let ids: Vec<i64> = (0..(count % 5) + 1)
            .filter_map(|_| driver.produce())
            .collect();
        node.insert("node_ids".into(), json!(ids));
    }

    Some(Value::Object(node))
}

fn gen_relationship(driver: &mut impl Driver, from: &str, to: &str) -> Option<Value> {
    let edge_name = pick_edge_name(driver)?;
    let direction = *pick(driver, DIRECTIONS)?;

    let mut rel = Map::new();
    rel.insert("type".into(), json!(edge_name));
    rel.insert("from".into(), json!(from));
    rel.insert("to".into(), json!(to));
    rel.insert("direction".into(), json!(direction));

    let has_hops: bool = driver.produce()?;
    if has_hops {
        let min: u8 = driver.produce()?;
        let max: u8 = driver.produce()?;
        rel.insert("min_hops".into(), json!((min % 3) + 1));
        rel.insert("max_hops".into(), json!((max % 5) + 1));
    }

    Some(Value::Object(rel))
}

fn gen_aggregation(driver: &mut impl Driver, node_id: &str) -> Option<Value> {
    let func = *pick(driver, AGG_FUNCTIONS)?;
    let mut agg = Map::new();
    agg.insert("function".into(), json!(func));
    agg.insert("target".into(), json!(node_id));

    let has_group_by: bool = driver.produce()?;
    if has_group_by {
        agg.insert("group_by".into(), json!(node_id));
        agg.insert("property".into(), json!("name"));
    }

    Some(Value::Object(agg))
}

/// A generated JSON query string for the GKG query DSL.
#[derive(Debug, Clone)]
pub struct FuzzQuery {
    pub json: String,
}

impl TypeGenerator for FuzzQuery {
    fn generate<D: Driver>(driver: &mut D) -> Option<Self> {
        let query_type = *pick(driver, QUERY_TYPES)?;
        let mut query = Map::new();
        query.insert("query_type".into(), json!(query_type));

        match query_type {
            "traversal" | "aggregation" => {
                let node_count: u8 = driver.produce()?;
                let node_count = (node_count % 3) + 1;

                let node_ids: Vec<String> = (0..node_count).map(|i| format!("n{i}")).collect();

                let use_single_node: bool = driver.produce()?;
                if node_count == 1 || use_single_node {
                    if let Some(node) = gen_node(driver, &node_ids[0]) {
                        query.insert("node".into(), node);
                    }
                } else {
                    let nodes: Vec<Value> = node_ids
                        .iter()
                        .filter_map(|id| gen_node(driver, id))
                        .collect();
                    query.insert("nodes".into(), json!(nodes));
                }

                if node_count > 1 {
                    let rel_count: u8 = driver.produce()?;
                    let rels: Vec<Value> = (0..(rel_count % node_count).max(1))
                        .filter_map(|i| {
                            let from_idx = i as usize % node_ids.len();
                            let to_idx = (i as usize + 1) % node_ids.len();
                            gen_relationship(driver, &node_ids[from_idx], &node_ids[to_idx])
                        })
                        .collect();
                    if !rels.is_empty() {
                        query.insert("relationships".into(), json!(rels));
                    }
                }

                if query_type == "aggregation" {
                    let agg_count: u8 = driver.produce()?;
                    let aggs: Vec<Value> = (0..(agg_count % 3) + 1)
                        .filter_map(|_| {
                            let target = pick(driver, &node_ids)?;
                            gen_aggregation(driver, target)
                        })
                        .collect();
                    query.insert("aggregations".into(), json!(aggs));
                }
            }
            "path_finding" => {
                let from_node = gen_node(driver, "from")?;
                let to_node = gen_node(driver, "to")?;
                query.insert("nodes".into(), json!([from_node, to_node]));

                let path_type = *pick(driver, PATH_TYPES)?;
                let max_depth: u8 = driver.produce()?;
                let mut path = Map::new();
                path.insert("type".into(), json!(path_type));
                path.insert("from".into(), json!("from"));
                path.insert("to".into(), json!("to"));
                path.insert("max_depth".into(), json!((max_depth % 10) + 1));
                query.insert("path".into(), Value::Object(path));
            }
            "neighbors" => {
                let node = gen_node(driver, "center")?;
                query.insert("node".into(), node);

                let direction = *pick(driver, DIRECTIONS)?;
                let mut neighbors = Map::new();
                neighbors.insert("node".into(), json!("center"));
                neighbors.insert("direction".into(), json!(direction));

                let has_rel_types: bool = driver.produce()?;
                if has_rel_types {
                    let count: u8 = driver.produce()?;
                    let types: Vec<String> = (0..(count % 3) + 1)
                        .filter_map(|_| pick_edge_name(driver))
                        .collect();
                    neighbors.insert("rel_types".into(), json!(types));
                }

                query.insert("neighbors".into(), Value::Object(neighbors));
            }
            _ => {}
        }

        let has_limit: bool = driver.produce()?;
        if has_limit {
            let limit: u16 = driver.produce()?;
            query.insert("limit".into(), json!(limit % 1000));
        }

        let has_order: bool = driver.produce()?;
        if has_order {
            let node_ref = match query_type {
                "neighbors" => "center",
                "path_finding" => "from",
                _ => "n0",
            };
            let asc: bool = driver.produce()?;
            let dir = if asc { "ASC" } else { "DESC" };
            query.insert(
                "order_by".into(),
                json!({"node": node_ref, "property": "id", "direction": dir}),
            );
        }

        let json = serde_json::to_string(&Value::Object(query)).ok()?;
        Some(FuzzQuery { json })
    }
}
