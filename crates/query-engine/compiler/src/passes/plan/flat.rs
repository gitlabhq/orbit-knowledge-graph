use query_data_model::bindings::DefinitionId;
use query_data_model::{
    QueryBackendCatalog, QueryDataModel, bindings::RelationId, storage::StoredColumnRef,
};
use std::collections::{HashMap, HashSet};

use ontology::constants::{
    DEFAULT_PRIMARY_KEY, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};

use super::requirements::{OutputValue, Predicate, live};
use crate::constants::*;
use crate::error::{QueryError, Result};

use super::HydrationStrategy;
use super::context::PlanningContext;
use super::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};

struct FlatBuilder<'a, 'm, M: QueryDataModel + ?Sized> {
    facts: &'a mut PlanningContext<'m, M>,
    definitions: Vec<(DefinitionId, PhysicalPlan)>,
    filtered: HashMap<String, DefinitionId>,
    tagged: HashSet<(String, String)>,
}

pub(super) fn plan<M: QueryDataModel + ?Sized>(
    facts: &mut PlanningContext<'_, M>,
) -> Result<ExecutionPlan> {
    FlatBuilder {
        facts,
        definitions: Vec::new(),
        filtered: HashMap::new(),
        tagged: HashSet::new(),
    }
    .build()
}

impl<M: QueryDataModel + ?Sized> FlatBuilder<'_, '_, M> {
    fn build(mut self) -> Result<ExecutionPlan> {
        let mut source: Option<PhysicalSource> = None;
        let mut cascades = Vec::new();
        let mut edges = Vec::new();
        let mut paths = Vec::new();
        for index in 0..self.facts.hops.len() {
            let membership = self.filter_keys(index)?;
            let cascade = self.cascade(index, cascades.last().and_then(Option::as_ref))?;
            let (edge, path) = self.edge(index, membership, cascade.as_ref())?;
            let relation = edge.relation();
            let hop = &self.facts.hops[index];
            source = Some(match source {
                Some(previous) => {
                    let join = hop
                        .join_prev
                        .as_ref()
                        .expect("non-first hop must have join_prev");
                    previous.inner_join(
                        edge,
                        (
                            self.facts.column(edges[index - 1], &join.prev_col)?,
                            self.facts.column(relation, &join.curr_col)?,
                        ),
                    )
                }
                None => edge,
            });
            cascades.push(cascade);
            edges.push(relation);
            paths.push(path);
        }
        let mut plan = ExecutionPlan {
            source: source.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?,
            definitions: self.definitions,
            outputs: Vec::new(),
            bindings: HashMap::new(),
        };
        if !self.facts.aggregate() {
            for (index, relation) in edges.iter().enumerate() {
                let hop = &self.facts.hops[index];
                let multi_hop = hop.max_hops > 1;
                let edge = format!("e{index}");
                let prefix = if hop.max_hops > 1 {
                    format!("hop_{edge}")
                } else {
                    edge.clone()
                };
                plan.outputs.extend(
                    [
                        (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
                        (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
                        (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
                        (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
                        (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
                    ]
                    .into_iter()
                    .map(|(column, suffix)| {
                        self.facts.projection(
                            self.facts.bindings.root(),
                            OutputValue::Column(self.facts.column(*relation, column)?),
                            format!("{prefix}_{suffix}"),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
                );
                if multi_hop {
                    plan.outputs.push(self.facts.projection(
                        self.facts.bindings.root(),
                        OutputValue::Column(
                            paths[index].expect("multi-hop source declares a path"),
                        ),
                        format!("{prefix}_{PATH_NODES_COLUMN}"),
                    )?);
                }
            }
        }
        let mut visited = HashSet::new();
        for (index, cascade) in cascades.iter().enumerate() {
            let hop = &self.facts.hops[index];
            let (start, end) = hop.direction.edge_columns();
            for (alias, column) in [(hop.from_node.clone(), start), (hop.to_node.clone(), end)] {
                let Some(node) = self
                    .facts
                    .nodes
                    .get(&alias)
                    .filter(|_| visited.insert(alias.clone()))
                else {
                    continue;
                };
                let edge = &format!("e{index}");
                let joined = node.hydration != HydrationStrategy::Skip
                    && !(node.hydration == HydrationStrategy::FilterOnly
                        && self.filtered.contains_key(&alias));
                if !joined {
                    plan.bindings.extend([self.facts.node_binding(
                        &alias,
                        self.facts.column(edges[index], column)?,
                        None,
                    )?]);
                    continue;
                }
                let membership = if node.hydration == HydrationStrategy::Join && node.use_narrowing
                {
                    let scan_alias = format!("{edge}n");
                    let scope = self.facts.child_scope(self.facts.bindings.root())?;
                    let source = self.facts.edge_scan(scope, index, &scan_alias, false)?;
                    let relation = source.relation();
                    let hop = &self.facts.hops[index];
                    let mut predicates = self.facts.edge_predicates(relation, hop, false)?;
                    predicates.extend(self.facts.node_id_predicates(relation, hop)?);
                    let keys = PhysicalPlan {
                        scope,
                        source: self.facts.cascade(
                            source.filter(predicates),
                            index,
                            cascade.as_ref(),
                        )?,
                        outputs: vec![self.facts.projection(
                            scope,
                            OutputValue::Column(self.facts.column(relation, column)?),
                            DEFAULT_PRIMARY_KEY,
                        )?],
                    };
                    let (name, keys) = self.facts.define(
                        self.facts.bindings.root(),
                        format!("_narrow_{alias}"),
                        keys,
                    )?;
                    plan.definitions.push((name, keys));
                    Some((DEFAULT_PRIMARY_KEY, name))
                } else {
                    None
                };
                let scan = self.facts.node_scan(&alias, membership)?;
                let relation = scan.source.relation();
                plan.bindings.extend([self.facts.node_binding(
                    &alias,
                    self.facts.column(edges[index], column)?,
                    Some(relation),
                )?]);
                plan.outputs.extend(scan.outputs);
                plan.source = plan.source.inner_join(
                    scan.source,
                    (
                        self.facts.column(relation, DEFAULT_PRIMARY_KEY)?,
                        self.facts.column(edges[index], column)?,
                    ),
                );
            }
        }
        Ok(plan)
    }

    fn filter_keys(&mut self, index: usize) -> Result<Vec<(StoredColumnRef, DefinitionId)>> {
        let hop = &self.facts.hops[index];
        let (start, end) = hop.direction.edge_columns();
        let endpoints = [(hop.from_node.clone(), start), (hop.to_node.clone(), end)];
        let mut predicates = Vec::new();
        for filter_only in [false, true] {
            for (alias, column) in &endpoints {
                let Some(node) = self.facts.nodes.get(alias) else {
                    continue;
                };
                let eligible = if filter_only {
                    node.hydration == HydrationStrategy::FilterOnly && self.facts.hops.len() >= 2
                } else {
                    node.hydration == HydrationStrategy::Join && node.has_selective_filters()
                };
                if !eligible {
                    continue;
                }
                let first = !self.filtered.contains_key(alias);
                if first {
                    let keys = if filter_only {
                        self.facts.filtered_keys(alias, DEFAULT_PRIMARY_KEY)?
                    } else {
                        let mut keys =
                            self.facts
                                .candidate_keys(alias, DEFAULT_PRIMARY_KEY, vec![])?;
                        keys.source = self.facts.latest(keys.source, vec![], vec![])?;
                        keys
                    };
                    let (name, keys) = self.facts.define(
                        self.facts.bindings.root(),
                        format!("_filter_{alias}"),
                        keys,
                    )?;
                    self.filtered.insert(alias.clone(), name);
                    self.definitions.push((name, keys));
                }
                if first || !filter_only {
                    let stored = self
                        .facts
                        .model
                        .query_backend()
                        .storage()
                        .resolve_column(&self.facts.hops[index].edge_table, column)
                        .map_err(|error| QueryError::Lowering(error.to_string()))?;
                    predicates.push((stored, self.filtered[alias]));
                }
            }
        }
        Ok(predicates)
    }

    fn cascade(
        &mut self,
        index: usize,
        upstream: Option<&PhysicalPlan>,
    ) -> Result<Option<PhysicalPlan>> {
        let hop = &self.facts.hops[index];
        let Some(join) = hop
            .join_prev
            .as_ref()
            .filter(|_| hop.cascade_anchor && index > 0)
        else {
            return Ok(None);
        };
        let previous = &self.facts.hops[index - 1];
        let selective =
            [&previous.from_node, &previous.to_node]
                .into_iter()
                .any(|alias| {
                    self.filtered.contains_key(alias)
                        || self.facts.nodes.get(alias).is_some_and(|node| {
                            !node.node_ids.is_empty() || node.id_range.is_some()
                        })
                });
        if !selective && upstream.is_none() {
            return Ok(None);
        }
        let alias = format!("{}p", join.prev_alias);
        let output_name = join.prev_col.clone();
        let scope = self.facts.child_scope(self.facts.bindings.root())?;
        let source = self.facts.edge_scan(scope, index - 1, &alias, false)?;
        let relation = source.relation();
        let previous = &self.facts.hops[index - 1];
        let mut predicates =
            self.facts
                .filtered_edge_predicates(relation, previous, &mut HashSet::new())?;
        let (start, end) = previous.direction.edge_columns();
        for (node, column) in [
            (previous.from_node.clone(), start),
            (previous.to_node.clone(), end),
        ] {
            if let Some(definition) = self.filtered.get(&node) {
                predicates.push(
                    self.facts
                        .key_membership(self.facts.column(relation, column)?, *definition)?,
                );
            }
        }
        let outputs = vec![self.facts.projection(
            scope,
            OutputValue::Column(self.facts.column(relation, &output_name)?),
            &output_name,
        )?];
        Ok(Some(PhysicalPlan {
            scope,
            source: self
                .facts
                .cascade(source.filter(predicates), index - 1, upstream)?,
            outputs,
        }))
    }

    fn edge(
        &mut self,
        index: usize,
        membership: Vec<(StoredColumnRef, DefinitionId)>,
        cascade: Option<&PhysicalPlan>,
    ) -> Result<(
        PhysicalSource,
        Option<query_data_model::bindings::ColumnRef>,
    )> {
        let hop = &self.facts.hops[index];
        let alias = format!("e{index}");
        let multi_hop = hop.max_hops > 1;
        let dedup = self.facts.hops.len() >= 2;
        let root = self.facts.bindings.root();
        if !multi_hop && !dedup && self.facts.aggregate() {
            let current = self
                .facts
                .model
                .table(&hop.edge_table)
                .is_some_and(|table| {
                    *table.row_semantics() == query_data_model::storage::RowSemantics::Current
                });
            let body = if current {
                root
            } else {
                self.facts.child_scope(root)?
            };
            let scan = self.facts.edge_scan(body, index, &alias, false)?;
            let relation = scan.relation();
            let hop = &self.facts.hops[index];
            let mut predicates =
                self.facts
                    .filtered_edge_predicates(relation, hop, &mut self.tagged)?;
            predicates.extend(self.membership(relation, &membership)?);
            if current {
                return Ok((scan.filter(predicates), None));
            }
            let mut latest = self.facts.latest(scan, vec![], predicates)?;
            let output = self.facts.publish(root, body, relation)?;
            if let PhysicalSource::Latest { relation, .. } = &mut latest {
                *relation = output;
            }
            return Ok((latest, None));
        }
        let mut path = None;
        let edge = if multi_hop {
            let (source, output) = self.facts.multi_hop(root, index, &alias)?;
            path = Some(output);
            let predicates = self.membership(source.relation(), &membership)?;
            self.facts
                .cascade(source.filter(predicates), index, cascade)?
        } else if dedup {
            let (start, end) = hop.direction.edge_columns();
            let narrow_inside = self
                .facts
                .model
                .table(&hop.edge_table)
                .is_some_and(|table| {
                    table
                        .sort_columns()
                        .take(4)
                        .any(|column| column.name() == start || column.name() == end)
                });
            let body = self.facts.child_scope(root)?;
            let scan = self.facts.edge_scan(body, index, &alias, true)?;
            let relation = scan.relation();
            let hop = &self.facts.hops[index];
            let mut input = scan.filter(self.facts.node_id_predicates(relation, hop)?);
            let deletion = self.facts.deletion_column(relation, &hop.edge_table)?;
            if narrow_inside {
                let membership = self.membership(relation, &membership)?;
                input = self
                    .facts
                    .cascade(input.filter(membership), index, cascade)?;
            }
            if let Some(column) = deletion {
                input = input.filter(vec![live(column)]);
            }
            let scoped = self.facts.scoped(root, body, input)?;
            if narrow_inside {
                scoped
            } else {
                let predicates = self.membership(scoped.relation(), &membership)?;
                self.facts
                    .cascade(scoped.filter(predicates), index, cascade)?
            }
        } else {
            let source = self.facts.edge_scan(root, index, &alias, false)?;
            let predicates = self.membership(source.relation(), &membership)?;
            self.facts
                .cascade(source.filter(predicates), index, cascade)?
        };
        let hop = &self.facts.hops[index];
        let relation = edge.relation();
        let mut predicates = if multi_hop {
            Vec::new()
        } else {
            self.facts.edge_predicates(relation, hop, dedup)?
        };
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| {
                    self.facts
                        .property_filter(self.facts.column(relation, property)?, filter)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        self.facts
            .push_denorm_tags(&mut predicates, hop, relation, &mut self.tagged)?;
        if !dedup || multi_hop {
            predicates.extend(self.facts.node_id_predicates(relation, hop)?);
        }
        Ok((edge.filter(predicates), path))
    }

    fn membership(
        &mut self,
        relation: RelationId,
        values: &[(StoredColumnRef, DefinitionId)],
    ) -> Result<Vec<Predicate>> {
        values
            .iter()
            .map(|(stored, definition)| {
                self.facts
                    .key_membership(self.facts.stored_column(relation, *stored)?, *definition)
            })
            .collect()
    }
}
