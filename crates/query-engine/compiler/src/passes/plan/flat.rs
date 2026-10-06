use crate::bindings::Definition;
use query_data_model::QueryDataModel;
use std::collections::{HashMap, HashSet};

use ontology::constants::{
    DEFAULT_PRIMARY_KEY, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};

use super::requirements::{Column, OutputValue, Predicate, Projection, live, property_filter};
use crate::constants::*;
use crate::error::{QueryError, Result};

use super::HydrationStrategy;
use super::context::PlanningContext;
use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource, key_membership};

struct FlatBuilder<'a, 'm, M: QueryDataModel + ?Sized> {
    facts: &'a mut PlanningContext<'m, M>,
    definitions: Vec<(Definition, PhysicalPlan)>,
    filtered: HashMap<String, Definition>,
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
        for index in 0..self.facts.hops.len() {
            let membership = self.filter_keys(index)?;
            let cascade = self.cascade(index, cascades.last().and_then(Option::as_ref))?;
            let edge = self.edge(index, membership, cascade.as_ref())?;
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
                            Column::new(&join.prev_alias, &join.prev_col),
                            Column::new(format!("e{index}"), &join.curr_col),
                        ),
                    )
                }
                None => edge,
            });
            cascades.push(cascade);
        }
        let mut plan = ExecutionPlan {
            source: source.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?,
            definitions: self.definitions,
            outputs: Vec::new(),
            bindings: Vec::new(),
        };
        if !self.facts.aggregate() {
            for (index, hop) in self.facts.hops.iter().enumerate() {
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
                        Projection::new(
                            OutputValue::Column(Column::new(&edge, column)),
                            format!("{prefix}_{suffix}"),
                        )
                    }),
                );
                if hop.max_hops > 1 {
                    plan.outputs.push(Projection::new(
                        OutputValue::Column(Column::new(&edge, PATH_NODES_COLUMN)),
                        format!("{prefix}_{PATH_NODES_COLUMN}"),
                    ));
                }
            }
        }
        let mut visited = HashSet::new();
        for (index, hop) in self.facts.hops.iter().enumerate() {
            let (start, end) = hop.direction.edge_columns();
            for (alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
                let Some(node) = self
                    .facts
                    .nodes
                    .get(alias)
                    .filter(|_| visited.insert(alias))
                else {
                    continue;
                };
                let edge = &format!("e{index}");
                let joined = node.hydration != HydrationStrategy::Skip
                    && !(node.hydration == HydrationStrategy::FilterOnly
                        && self.filtered.contains_key(alias));
                plan.bindings.push(BindingSource {
                    node: alias.clone(),
                    alias: edge.clone(),
                    column: column.into(),
                    joined,
                });
                if !joined {
                    continue;
                }
                let membership = if node.hydration == HydrationStrategy::Join && node.use_narrowing
                {
                    let scan_alias = format!("{edge}n");
                    let mut predicates = self.facts.edge_predicates(&scan_alias, hop, false);
                    predicates.extend(self.facts.node_id_predicates(&scan_alias, hop));
                    let keys = PhysicalPlan {
                        source: PhysicalSource::edge_keys(
                            &mut self.facts.bindings,
                            self.facts.model,
                            hop,
                            &scan_alias,
                            predicates,
                            cascades[index].as_ref(),
                        )?,
                        outputs: vec![Projection::new(
                            OutputValue::Column(Column::new(&scan_alias, column)),
                            DEFAULT_PRIMARY_KEY,
                        )],
                    };
                    let (name, keys) = keys.define(format!("_narrow_{alias}"));
                    plan.definitions.push((name.clone(), keys));
                    Some(key_membership(alias, DEFAULT_PRIMARY_KEY, name))
                } else {
                    None
                };
                let scan = PhysicalPlan::node_scan(
                    &mut self.facts.bindings,
                    self.facts.model,
                    node,
                    membership,
                )?;
                plan.outputs.extend(scan.outputs);
                plan.source = plan.source.inner_join(
                    scan.source,
                    (
                        Column::new(alias, DEFAULT_PRIMARY_KEY),
                        Column::new(edge, column),
                    ),
                );
            }
        }
        Ok(plan)
    }

    fn filter_keys(&mut self, index: usize) -> Result<Vec<Predicate>> {
        let hop = &self.facts.hops[index];
        let (start, end) = hop.direction.edge_columns();
        let mut predicates = Vec::new();
        for filter_only in [false, true] {
            for (alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
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
                        PhysicalPlan::filtered_keys(
                            &mut self.facts.bindings,
                            self.facts.model,
                            node,
                            DEFAULT_PRIMARY_KEY,
                        )?
                    } else {
                        let mut keys = PhysicalPlan::candidate_keys(
                            &mut self.facts.bindings,
                            self.facts.model,
                            node,
                            DEFAULT_PRIMARY_KEY,
                            vec![],
                        )?;
                        keys.source = keys.source.latest(
                            &self.facts.bindings,
                            self.facts.model,
                            vec![],
                            vec![],
                        )?;
                        keys
                    };
                    let (name, keys) = keys.define(format!("_filter_{alias}"));
                    self.filtered.insert(alias.clone(), name.clone());
                    self.definitions.push((name, keys));
                }
                if first || !filter_only {
                    predicates.push(key_membership(
                        &format!("e{index}"),
                        column,
                        self.filtered[alias].clone(),
                    ));
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
        let mut predicates =
            self.facts
                .filtered_edge_predicates(&alias, previous, &mut HashSet::new());
        let (start, end) = previous.direction.edge_columns();
        for (node, column) in [(&previous.from_node, start), (&previous.to_node, end)] {
            if let Some(definition) = self.filtered.get(node) {
                predicates.push(key_membership(&alias, column, definition.clone()));
            }
        }
        Ok(Some(PhysicalPlan {
            source: PhysicalSource::edge_keys(
                &mut self.facts.bindings,
                self.facts.model,
                previous,
                &alias,
                predicates,
                upstream,
            )?,
            outputs: vec![Projection::col(&alias, &join.prev_col)],
        }))
    }

    fn edge(
        &mut self,
        index: usize,
        membership: Vec<Predicate>,
        cascade: Option<&PhysicalPlan>,
    ) -> Result<PhysicalSource> {
        let hop = &self.facts.hops[index];
        let alias = format!("e{index}");
        let multi_hop = hop.max_hops > 1;
        let dedup = self.facts.hops.len() >= 2;
        if !multi_hop && !dedup && self.facts.aggregate() {
            let mut predicates = self
                .facts
                .filtered_edge_predicates(&alias, hop, &mut self.tagged);
            predicates.extend(membership);
            if self
                .facts
                .model
                .table(&hop.edge_table)
                .is_some_and(|table| {
                    *table.row_semantics() == query_data_model::storage::RowSemantics::Current
                })
            {
                return Ok(PhysicalSource::scan(
                    &mut self.facts.bindings,
                    self.facts.model,
                    &hop.edge_table,
                    &alias,
                    false,
                    Some(hop.input_index),
                )?
                .filter(predicates));
            }
            return PhysicalSource::scan(
                &mut self.facts.bindings,
                self.facts.model,
                &hop.edge_table,
                &alias,
                false,
                Some(hop.input_index),
            )?
            .latest(&self.facts.bindings, self.facts.model, vec![], predicates);
        }
        let edge = if multi_hop {
            super::hops::multi_hop(
                &mut self.facts.bindings,
                self.facts.model,
                hop,
                &alias,
                &self.facts.nodes,
            )?
            .filter(membership)
            .cascade(hop, &alias, cascade)
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
            let mut input = PhysicalSource::scan(
                &mut self.facts.bindings,
                self.facts.model,
                &hop.edge_table,
                &alias,
                true,
                Some(hop.input_index),
            )?
            .filter(self.facts.node_id_predicates(&alias, hop));
            let outside = if narrow_inside {
                input = input.filter(membership).cascade(hop, &alias, cascade);
                Vec::new()
            } else {
                membership
            };
            let scoped = input.filter(vec![live(&alias)]).scoped(&alias);
            if narrow_inside {
                scoped
            } else {
                scoped.filter(outside).cascade(hop, &alias, cascade)
            }
        } else {
            PhysicalSource::scan(
                &mut self.facts.bindings,
                self.facts.model,
                &hop.edge_table,
                &alias,
                false,
                Some(hop.input_index),
            )?
            .filter(membership)
            .cascade(hop, &alias, cascade)
        };
        let mut predicates = if multi_hop {
            Vec::new()
        } else {
            self.facts.edge_predicates(&alias, hop, dedup)
        };
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| property_filter(&alias, property, filter)),
        );
        self.facts
            .push_denorm_tags(&mut predicates, hop, &alias, &mut self.tagged);
        if !dedup || multi_hop {
            predicates.extend(self.facts.node_id_predicates(&alias, hop));
        }
        Ok(edge.filter(predicates))
    }
}
