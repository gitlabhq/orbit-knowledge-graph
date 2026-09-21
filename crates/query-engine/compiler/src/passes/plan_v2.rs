use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::DEFAULT_PRIMARY_KEY;
use ontology::Ontology;

use crate::error::Result;
use crate::input::*;

pub struct JoinGraph {
    by_kind: HashMap<String, JoinPath>,
}

#[derive(Clone)]
pub struct JoinPath {
    pub fk_column: Option<String>,
    pub scope_preserving: bool,
    pub edge_table: String,
}

pub enum HopStrategy {
    EdgeScan { table: String, dedup: bool },
    FkJoin { fk_column: String },
}

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut by_kind = HashMap::new();
        for edge in ontology.edges() {
            by_kind.entry(edge.relationship_kind.clone()).or_insert(JoinPath {
                fk_column: edge.fk_column.clone(),
                scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                edge_table: edge.destination_table.clone(),
            });
        }
        Self { by_kind }
    }

    pub fn resolve(&self, rel: &InputRelationship, default_table: &str, chain_len: usize) -> HopStrategy {
        if rel.hops.max == 1
            && !matches!(rel.direction, Direction::Both)
            && rel.filters.is_empty()
            && rel.scope_preserving
            && rel.types.iter().all(|t| self.is_fk_eligible(t))
        {
            let fk = self.by_kind[&rel.types[0]].fk_column.as_ref().unwrap();
            return HopStrategy::FkJoin { fk_column: fk.clone() };
        }

        HopStrategy::EdgeScan {
            table: self.edge_table(&rel.types, default_table),
            dedup: chain_len >= 2 && rel.hops.max == 1,
        }
    }

    fn is_fk_eligible(&self, kind: &str) -> bool {
        self.by_kind.get(kind).is_some_and(|jp| jp.fk_column.is_some() && jp.scope_preserving)
    }

    pub fn edge_table(&self, rel_types: &[String], default: &str) -> String {
        for t in rel_types {
            if let Some(jp) = self.by_kind.get(t) {
                return jp.edge_table.clone();
            }
        }
        default.to_string()
    }
}

pub enum PhysOp {
    Scan { table: String, alias: String, dedup: bool },
    Join { left: Box<PhysOp>, right: Box<PhysOp>, left_col: (String, String), right_col: (String, String) },
    Union { arms: Vec<PhysOp>, alias: String },
    Cte { name: String, body: Box<PhysOp>, consumer: Box<PhysOp> },
    TopN { input: Box<PhysOp>, limit: u32 },
    Aggregate { input: Box<PhysOp>, limit: u32 },
}

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

pub fn plan(input: &mut Input, ontology: &Ontology) -> Result<(PlanMetadata, PhysOp)> {
    if input.compiler.table_sort_keys.is_empty() {
        for node in ontology.nodes() {
            input.compiler.table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }

    let graph = JoinGraph::build(ontology);
    let limit = input.fetch_limit();

    let op = match input.query_type {
        QueryType::Traversal => PhysOp::TopN {
            input: Box::new(plan_chain(input, &graph)),
            limit,
        },
        QueryType::Aggregation => PhysOp::Aggregate {
            input: Box::new(plan_chain(input, &graph)),
            limit,
        },
        QueryType::Neighbors => plan_neighbors(input, &graph, limit),
        QueryType::PathFinding => plan_pathfinding(input, &graph, limit)?,
        QueryType::Hydration => plan_hydration(input, limit),
    };

    let meta = PlanMetadata {
        node_edge_mappings: compute_node_edge_mappings(input, &graph),
        hop_count: input.relationships.len(),
        phys_op: None,
    };

    Ok((meta, op))
}

fn compute_node_edge_mappings(input: &Input, graph: &JoinGraph) -> HashMap<String, (String, String)> {
    let mut m = HashMap::new();
    let det = &input.compiler.default_edge_table;
    let chain_len = input.relationships.len();
    for (i, rel) in input.relationships.iter().enumerate() {
        match graph.resolve(rel, det, chain_len) {
            HopStrategy::FkJoin { fk_column } => {
                let from_has_fk = input.nodes.iter()
                    .find(|n| n.id == rel.from)
                    .and_then(|n| n.table.as_deref())
                    .and_then(|t| input.compiler.table_columns.get(t))
                    .is_some_and(|cols| cols.contains(&fk_column));
                let (fk_alias, target_alias) = if from_has_fk {
                    (&rel.from, &rel.to)
                } else {
                    (&rel.to, &rel.from)
                };
                m.entry(fk_alias.clone())
                    .or_insert_with(|| (fk_alias.clone(), DEFAULT_PRIMARY_KEY.to_string()));
                m.entry(target_alias.clone())
                    .or_insert_with(|| (fk_alias.clone(), fk_column.clone()));
            }
            HopStrategy::EdgeScan { .. } => {
                let ea = format!("e{i}");
                let (sc, ec) = rel.direction.edge_columns();
                m.entry(rel.from.clone()).or_insert_with(|| (ea.clone(), sc.to_string()));
                m.entry(rel.to.clone()).or_insert_with(|| (ea.clone(), ec.to_string()));
            }
        }
    }
    m
}

fn plan_chain(input: &Input, graph: &JoinGraph) -> PhysOp {
    if input.relationships.is_empty() {
        let n = &input.nodes[0];
        return PhysOp::Scan {
            table: n.table.as_deref().unwrap_or("").to_string(),
            alias: n.id.clone(),
            dedup: true,
        };
    }

    let mut tree: Option<PhysOp> = None;
    let det = &input.compiler.default_edge_table;
    let chain_len = input.relationships.len();
    let mut fk_joined: HashSet<String> = HashSet::new();

    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        let (sc, ec) = rel.direction.edge_columns();

        match graph.resolve(rel, det, chain_len) {
            HopStrategy::FkJoin { fk_column } => {
                let from_node = input.nodes.iter().find(|n| n.id == rel.from);
                let to_node = input.nodes.iter().find(|n| n.id == rel.to);
                let (fk_alias, target_alias) = if from_node
                    .and_then(|n| n.table.as_deref())
                    .and_then(|t| input.compiler.table_columns.get(t))
                    .is_some_and(|cols| cols.contains(&fk_column))
                {
                    (&rel.from, &rel.to)
                } else {
                    (&rel.to, &rel.from)
                };
                let fk_node = input.nodes.iter().find(|n| &n.id == fk_alias);
                let tgt_node = input.nodes.iter().find(|n| &n.id == target_alias);

                if tree.is_none() {
                    if let Some(n) = fk_node {
                        tree = Some(PhysOp::Scan {
                            table: n.table.as_deref().unwrap_or("").to_string(),
                            alias: fk_alias.clone(),
                            dedup: true,
                        });
                        fk_joined.insert(fk_alias.clone());
                    }
                }
                if let Some(n) = tgt_node {
                    if !fk_joined.contains(target_alias) {
                        tree = Some(PhysOp::Join {
                            left: Box::new(tree.unwrap()),
                            right: Box::new(PhysOp::Scan {
                                table: n.table.as_deref().unwrap_or("").to_string(),
                                alias: target_alias.clone(),
                                dedup: true,
                            }),
                            left_col: (fk_alias.clone(), fk_column.clone()),
                            right_col: (target_alias.clone(), DEFAULT_PRIMARY_KEY.to_string()),
                        });
                        fk_joined.insert(target_alias.clone());
                    }
                }
            }
            HopStrategy::EdgeScan { table, dedup } => {
                let edge = PhysOp::Scan { table, alias: ea.clone(), dedup };

                tree = Some(match tree {
                    None => edge,
                    Some(prev) => {
                        let prev_col = if i > 0 {
                            let (_, pe) = input.relationships[i - 1].direction.edge_columns();
                            (format!("e{}", i - 1), pe.to_string())
                        } else {
                            (ea.clone(), sc.to_string())
                        };
                        PhysOp::Join {
                            left: Box::new(prev),
                            right: Box::new(edge),
                            left_col: prev_col,
                            right_col: (ea.clone(), sc.to_string()),
                        }
                    }
                });
            }
        }
    }

    let mut hydrated = fk_joined;
    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        let (sc, ec) = rel.direction.edge_columns();
        for (na, col) in [(&rel.from, sc), (&rel.to, ec)] {
            if !hydrated.insert(na.clone()) { continue; }
            let Some(n) = input.nodes.iter().find(|n| &n.id == na) else { continue; };
            if !needs_node_join(n, input) { continue; }
            tree = Some(PhysOp::Join {
                left: Box::new(tree.unwrap()),
                right: Box::new(PhysOp::Scan {
                    table: n.table.as_deref().unwrap_or("").to_string(),
                    alias: na.clone(),
                    dedup: true,
                }),
                left_col: (ea.clone(), col.to_string()),
                right_col: (na.clone(), ontology::constants::DEFAULT_PRIMARY_KEY.to_string()),
            });
        }
    }

    tree.unwrap()
}

fn needs_node_join(node: &InputNode, input: &Input) -> bool {
    let a = &node.id;
    !node.filters.is_empty()
        || !node.node_ids.is_empty()
        || node.id_range.is_some()
        || matches!(&node.columns, Some(ColumnSelection::List(c)) if !c.is_empty())
        || input.order_by.as_ref().is_some_and(|ob| ob.node == *a)
        || input.aggregation.group_by.iter().any(|g| g.node() == a.as_str())
        || input.aggregation.metrics.iter().any(|m| {
            m.expr.node() == a.as_str()
                && m.expr.property().is_some()
                && !matches!(m.expr.function(), AggFunction::Count)
        })
}

fn plan_neighbors(input: &Input, graph: &JoinGraph, limit: u32) -> PhysOp {
    let config = input.neighbors.as_ref().expect("neighbors config");
    let _center = &input.nodes[0];
    let det = &input.compiler.default_edge_table;
    let et = graph.edge_table(&config.rel_types, det);

    let arm = |_dir: Direction| -> PhysOp {
        PhysOp::Scan { table: et.clone(), alias: "e".to_string(), dedup: false }
    };

    let body = match config.direction {
        Direction::Both => PhysOp::Union {
            arms: vec![arm(Direction::Outgoing), arm(Direction::Incoming)],
            alias: "_neighbors".to_string(),
        },
        dir => arm(dir),
    };

    PhysOp::TopN { input: Box::new(body), limit }
}

fn plan_pathfinding(input: &Input, graph: &JoinGraph, limit: u32) -> Result<PhysOp> {
    let cfg = input.path.as_ref().expect("path config");
    let det = &input.compiler.default_edge_table;
    let et = graph.edge_table(&cfg.rel_types, det);
    let max_depth = cfg.max_depth;
    let fwd_depth = max_depth / 2 + max_depth % 2;
    let bwd_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

    let start = input.nodes.iter().find(|n| n.id == cfg.from).expect("start node");
    let end = input.nodes.iter().find(|n| n.id == cfg.to).expect("end node");

    let frontier = |depth: u32| -> PhysOp {
        let arms: Vec<PhysOp> = (1..=depth).map(|d| {
            let mut chain = PhysOp::Scan { table: et.clone(), alias: "e1".to_string(), dedup: false };
            for j in 2..=d {
                chain = PhysOp::Join {
                    left: Box::new(chain),
                    right: Box::new(PhysOp::Scan {
                        table: et.clone(),
                        alias: format!("e{j}"),
                        dedup: false,
                    }),
                    left_col: (format!("e{}", j - 1), ontology::constants::TARGET_ID_COLUMN.to_string()),
                    right_col: (format!("e{j}"), ontology::constants::SOURCE_ID_COLUMN.to_string()),
                };
            }
            chain
        }).collect();
        if arms.len() == 1 {
            arms.into_iter().next().unwrap()
        } else {
            PhysOp::Union { arms, alias: "_frontier".to_string() }
        }
    };

    let start_cte = PhysOp::Cte {
        name: "_start".to_string(),
        body: Box::new(PhysOp::Scan {
            table: start.table.as_deref().unwrap_or("").to_string(),
            alias: start.id.clone(),
            dedup: true,
        }),
        consumer: Box::new(PhysOp::Cte {
            name: "_end".to_string(),
            body: Box::new(PhysOp::Scan {
                table: end.table.as_deref().unwrap_or("").to_string(),
                alias: end.id.clone(),
                dedup: true,
            }),
            consumer: Box::new(PhysOp::Cte {
                name: "forward".to_string(),
                body: Box::new(frontier(fwd_depth)),
                consumer: Box::new(if bwd_depth > 0 {
                    PhysOp::Cte {
                        name: "backward".to_string(),
                        body: Box::new(frontier(bwd_depth)),
                        consumer: Box::new(PhysOp::Union {
                            arms: vec![
                                PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false },
                                PhysOp::Join {
                                    left: Box::new(PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false }),
                                    right: Box::new(PhysOp::Scan { table: "backward".to_string(), alias: "b".to_string(), dedup: false }),
                                    left_col: ("f".to_string(), "end_id".to_string()),
                                    right_col: ("b".to_string(), "end_id".to_string()),
                                },
                            ],
                            alias: "paths".to_string(),
                        }),
                    }
                } else {
                    PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false }
                }),
            }),
        }),
    };

    Ok(PhysOp::TopN { input: Box::new(start_cte), limit })
}

fn plan_hydration(input: &Input, limit: u32) -> PhysOp {
    let arms: Vec<PhysOp> = input.nodes.iter().map(|n| {
        PhysOp::Scan {
            table: n.table.as_deref().unwrap_or("").to_string(),
            alias: n.id.clone(),
            dedup: false,
        }
    }).collect();

    let body = if arms.len() == 1 {
        arms.into_iter().next().unwrap()
    } else {
        PhysOp::Union { arms, alias: "hydrate".to_string() }
    };

    PhysOp::TopN { input: Box::new(body), limit }
}
