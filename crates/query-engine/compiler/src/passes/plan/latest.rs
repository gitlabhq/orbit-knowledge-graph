//! ClickHouse latest-row reads. A `FINAL` scan whose filters bound it by key
//! becomes a `LIMIT 1 BY` read of only those keys; broad scans keep `FINAL`,
//! which merges only the ranges where parts overlap.

use std::collections::HashMap;

use ontology::FieldSelectivity;
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN,
    TARGET_ID_COLUMN, TRAVERSAL_PATH_COLUMN,
};
use query_data_model::OrbitQueryModel;

use crate::input::BooleanExpression;

use super::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};
use super::requirements::{Column, OutputValue, Predicate, Projection, live};

const IDENTITY_COLUMNS: [&str; 3] = [DEFAULT_PRIMARY_KEY, SOURCE_ID_COLUMN, TARGET_ID_COLUMN];

pub(super) fn resolve(execution: &mut ExecutionPlan, model: &(impl OrbitQueryModel + ?Sized)) {
    let facts = Facts { model };
    let source = std::mem::replace(&mut execution.source, placeholder());
    let source = push_filters(source, &[]);
    let mut injected = join_keys(&source, &facts);
    execution.source = facts.rewrite(source, &mut injected);
    for (_, definition) in &mut execution.definitions {
        let source = std::mem::replace(&mut definition.source, placeholder());
        definition.source = facts.rewrite(push_filters(source, &[]), &mut HashMap::new());
    }
}

struct Facts<'a, M: OrbitQueryModel + ?Sized> {
    model: &'a M,
}

#[derive(Clone)]
struct KeySet {
    values: Vec<Column>,
    keys: PhysicalPlan,
}

/// A run of `Filter`, `KeyFilter`, and `Scope` layers over one `FINAL` scan.
struct Chain {
    table: String,
    alias: String,
    relationship: Option<usize>,
    scoped: bool,
    predicates: Vec<Predicate>,
    keys: Vec<KeySet>,
}

impl Chain {
    fn of(source: &PhysicalSource) -> Option<Self> {
        let mut predicates = Vec::new();
        let mut keys = Vec::new();
        let mut scoped = false;
        let mut current = source;
        loop {
            match current {
                PhysicalSource::Filter {
                    predicates: layer,
                    input,
                } => {
                    predicates.extend(layer.iter().cloned());
                    current = input;
                }
                PhysicalSource::KeyFilter {
                    values,
                    keys: set,
                    input,
                } => {
                    keys.push(KeySet {
                        values: values.clone(),
                        keys: set.as_ref().clone(),
                    });
                    current = input;
                }
                PhysicalSource::Scope { input, .. } => {
                    scoped = true;
                    current = input;
                }
                PhysicalSource::Scan {
                    table,
                    alias,
                    final_: true,
                    relationship,
                } => {
                    return Some(Self {
                        table: table.clone(),
                        alias: alias.clone(),
                        relationship: *relationship,
                        scoped,
                        predicates,
                        keys,
                    });
                }
                _ => return None,
            }
        }
    }

    fn scan(&self, final_: bool) -> PhysicalSource {
        PhysicalSource::Scan {
            table: self.table.clone(),
            alias: self.alias.clone(),
            final_,
            relationship: self.relationship,
        }
    }

    fn owns(&self, predicate: &Predicate) -> bool {
        columns(predicate)
            .iter()
            .all(|column| column.source == self.alias)
    }

    /// Superset of the chain's rows, read without `FINAL`, projecting `outputs`.
    fn candidates(&self, outputs: Vec<Projection>) -> PhysicalPlan {
        let predicates = self
            .predicates
            .iter()
            .filter(|predicate| self.owns(predicate))
            .cloned()
            .collect();
        PhysicalPlan {
            source: with_keys(self.scan(false).filter(predicates), &self.keys),
            outputs,
        }
    }
}

impl<M: OrbitQueryModel + ?Sized> Facts<'_, M> {
    fn sort_key(&self, table: &str) -> Option<&[String]> {
        self.model
            .table_sort_key(table)
            .filter(|key| !key.is_empty())
    }

    fn trivial(&self, predicate: &Predicate) -> bool {
        columns(predicate).iter().all(|column| {
            column.name == TRAVERSAL_PATH_COLUMN
                || column.name == DELETED_COLUMN
                || self.model.anchor_foreign_keys().contains_key(&column.name)
        })
    }

    /// The scan narrows itself: by a key, or by a high-selectivity filter.
    fn bounded(&self, chain: &Chain) -> bool {
        !chain.keys.is_empty()
            || chain.predicates.iter().any(|predicate| {
                chain.owns(predicate) && !self.trivial(predicate) && selective_filter(predicate)
            })
    }

    fn rewrite(
        &self,
        source: PhysicalSource,
        injected: &mut HashMap<String, KeySet>,
    ) -> PhysicalSource {
        if let Some(mut chain) = Chain::of(&source) {
            if let Some(key) = injected.remove(&chain.alias) {
                chain.keys.push(key);
            }
            chain.keys = chain
                .keys
                .into_iter()
                .map(|set| KeySet {
                    values: set.values,
                    keys: self.rewrite_plan(set.keys),
                })
                .collect();
            return self.latest(chain);
        }
        match source {
            PhysicalSource::Filter { predicates, input } => {
                self.rewrite(*input, injected).filter(predicates)
            }
            PhysicalSource::KeyFilter {
                values,
                keys,
                input,
            } => PhysicalSource::KeyFilter {
                values,
                keys: Box::new(self.rewrite_plan(*keys)),
                input: Box::new(self.rewrite(*input, injected)),
            },
            PhysicalSource::Scope { alias, input } => PhysicalSource::Scope {
                alias,
                input: Box::new(self.rewrite(*input, injected)),
            },
            PhysicalSource::Join {
                endpoints,
                predicates,
                left,
                right,
            } => PhysicalSource::Join {
                endpoints,
                predicates,
                left: Box::new(self.rewrite(*left, injected)),
                right: Box::new(self.rewrite(*right, injected)),
            },
            PhysicalSource::Union {
                alias,
                arms,
                relationship,
            } => PhysicalSource::Union {
                alias,
                arms: arms.into_iter().map(|arm| self.rewrite_plan(arm)).collect(),
                relationship,
            },
            PhysicalSource::Latest {
                sort_key,
                alias,
                aggregate_condition,
                input,
            } => PhysicalSource::Latest {
                sort_key,
                alias,
                aggregate_condition,
                input: Box::new(self.rewrite(*input, injected)),
            },
            scan @ PhysicalSource::Scan { .. } => scan,
        }
    }

    fn rewrite_plan(&self, plan: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan {
            source: self.rewrite(plan.source, &mut HashMap::new()),
            outputs: plan.outputs,
        }
    }

    /// Keyed scans read only the latest version of their keys. A scan with
    /// a selective non-key filter first finds the keys that match in any
    /// version; the latest version of each is then filtered again.
    fn latest(&self, chain: Chain) -> PhysicalSource {
        let Some(sort_key) = self.sort_key(&chain.table) else {
            return final_scan(chain);
        };
        let in_key =
            |columns: &[Column]| columns.iter().all(|column| sort_key.contains(&column.name));
        let identity = |columns: &[Column]| {
            in_key(columns)
                && columns
                    .iter()
                    .any(|column| IDENTITY_COLUMNS.contains(&column.name.as_str()))
        };
        let keyed = chain
            .predicates
            .iter()
            .any(|predicate| chain.owns(predicate) && identity(&columns(predicate)))
            || chain.keys.iter().any(|set| identity(&set.values));
        let find = !keyed
            && chain.predicates.iter().any(|predicate| {
                chain.owns(predicate)
                    && !in_key(&columns(predicate))
                    && !self.trivial(predicate)
                    && selective_filter(predicate)
            });
        if !keyed && !find {
            return final_scan(chain);
        }

        let (inside, mut outside): (Vec<_>, Vec<_>) = chain
            .predicates
            .iter()
            .cloned()
            .partition(|predicate| chain.owns(predicate) && in_key(&columns(predicate)));
        let (inside_keys, outside_keys): (Vec<_>, Vec<_>) = chain
            .keys
            .iter()
            .cloned()
            .partition(|set| in_key(&set.values));
        let mut input = with_keys(chain.scan(false).filter(inside), &inside_keys);
        if find {
            let values: Vec<Column> = sort_key
                .iter()
                .map(|column| Column::new(&chain.alias, column))
                .collect();
            input = PhysicalSource::KeyFilter {
                keys: Box::new(
                    chain.candidates(
                        values
                            .iter()
                            .map(|column| Projection::col(&column.source, &column.name))
                            .collect(),
                    ),
                ),
                values,
                input: Box::new(input),
            };
        }
        let mut source = PhysicalSource::Latest {
            sort_key: sort_key.to_vec(),
            alias: chain.alias.clone(),
            aggregate_condition: vec![],
            input: Box::new(input),
        };
        if chain.scoped {
            source = PhysicalSource::Scope {
                alias: chain.alias.clone(),
                input: Box::new(source),
            };
        }
        if !outside
            .iter()
            .any(|predicate| matches!(predicate, Predicate::Live { .. }))
        {
            outside.push(live(&chain.alias));
        }
        with_keys(source, &outside_keys).filter(outside)
    }
}

fn final_scan(chain: Chain) -> PhysicalSource {
    let mut source = with_keys(chain.scan(true), &chain.keys);
    if chain.scoped {
        source = PhysicalSource::Scope {
            alias: chain.alias.clone(),
            input: Box::new(source),
        };
    }
    source.filter(chain.predicates)
}

fn with_keys(source: PhysicalSource, keys: &[KeySet]) -> PhysicalSource {
    keys.iter()
        .fold(source, |input, set| PhysicalSource::KeyFilter {
            values: set.values.clone(),
            keys: Box::new(set.keys.clone()),
            input: Box::new(input),
        })
}

/// Kinds, tags, and enum or boolean properties have few distinct values, so
/// they never bound a scan on their own.
fn selective_filter(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Property { selectivity, .. } => *selectivity == FieldSelectivity::High,
        Predicate::Ids { .. } | Predicate::IdRange { .. } | Predicate::Membership { .. } => true,
        Predicate::Boolean(expression) => selective_expression(expression),
        _ => false,
    }
}

fn selective_expression(expression: &BooleanExpression<Box<Predicate>>) -> bool {
    match expression {
        BooleanExpression::Leaf(leaf) => selective_filter(leaf),
        BooleanExpression::And(children) => children.iter().any(selective_expression),
        BooleanExpression::Not(_) => false,
    }
}

/// Pushes filters from above down to the `FINAL` scans and `UNION ALL` arms
/// they constrain. ClickHouse pushes them into `FINAL` itself, but not
/// through the `LIMIT 1 BY` that replaces it.
fn push_filters(source: PhysicalSource, inherited: &[Predicate]) -> PhysicalSource {
    if let Some(chain) = Chain::of(&source) {
        let pushed: Vec<Predicate> = inherited
            .iter()
            .filter(|predicate| !columns(predicate).is_empty() && chain.owns(predicate))
            .cloned()
            .collect();
        return source.filter(pushed);
    }
    match source {
        PhysicalSource::Filter { predicates, input } => {
            let mut all = inherited.to_vec();
            all.extend(predicates.iter().cloned());
            push_filters(*input, &all).filter(predicates)
        }
        PhysicalSource::KeyFilter {
            values,
            keys,
            input,
        } => PhysicalSource::KeyFilter {
            values,
            keys,
            input: Box::new(push_filters(*input, inherited)),
        },
        PhysicalSource::Scope { alias, input } => PhysicalSource::Scope {
            alias,
            input: Box::new(push_filters(*input, inherited)),
        },
        PhysicalSource::Join {
            endpoints,
            predicates,
            left,
            right,
        } => PhysicalSource::Join {
            endpoints,
            predicates,
            left: Box::new(push_filters(*left, inherited)),
            right: Box::new(push_filters(*right, inherited)),
        },
        PhysicalSource::Union {
            alias,
            arms,
            relationship,
        } => {
            let arms = arms
                .into_iter()
                .map(|arm| {
                    let mapped = inherited
                        .iter()
                        .filter_map(|predicate| map_into_arm(predicate, &alias, &arm.outputs))
                        .collect();
                    PhysicalPlan {
                        source: push_filters(arm.source.filter(mapped), &[]),
                        outputs: arm.outputs,
                    }
                })
                .collect();
            PhysicalSource::Union {
                alias,
                arms,
                relationship,
            }
        }
        other => other,
    }
}

/// Rewrites a predicate on a `UNION ALL` alias to the arm columns it projects.
fn map_into_arm(predicate: &Predicate, alias: &str, outputs: &[Projection]) -> Option<Predicate> {
    let referenced = columns(predicate);
    if referenced.is_empty() || referenced.iter().any(|column| column.source != alias) {
        return None;
    }
    let lookup = |name: &str| {
        outputs.iter().find_map(|output| match &output.value {
            OutputValue::Column(column) if output.name == name => Some(column.clone()),
            _ => None,
        })
    };
    let mut mapped = predicate.clone();
    remap(&mut mapped, &lookup)?;
    Some(mapped)
}

fn remap(predicate: &mut Predicate, lookup: &impl Fn(&str) -> Option<Column>) -> Option<()> {
    let same = |column: &mut Column| -> Option<()> {
        *column = lookup(&column.name)?;
        Some(())
    };
    let alias_of = |alias: &mut String, name: &str| -> Option<()> {
        let column = lookup(name).filter(|column| column.name == name)?;
        *alias = column.source;
        Some(())
    };
    match predicate {
        Predicate::Boolean(expression) => {
            let mut ok = true;
            expression.visit_mut(&mut |leaf| ok &= remap(leaf, lookup).is_some());
            ok.then_some(())
        }
        Predicate::Property { column, .. }
        | Predicate::Ids { column, .. }
        | Predicate::IdRange { column, .. }
        | Predicate::EntityKind { column, .. }
        | Predicate::Tags { column, .. }
        | Predicate::Membership { column, .. } => same(column),
        Predicate::ColumnsEqual { left, right } | Predicate::ColumnEq { left, right } => {
            same(left)?;
            same(right)
        }
        Predicate::Live { alias } => alias_of(alias, DELETED_COLUMN),
        Predicate::RelationshipKinds { alias, .. } => alias_of(alias, RELATIONSHIP_KIND_COLUMN),
    }
}

/// For each join between an unbounded `FINAL` scan keyed on the join column
/// and a bounded side, the keys the bounded side can reach. A scan that
/// receives keys is bounded for the joins after it.
fn join_keys(
    source: &PhysicalSource,
    facts: &Facts<'_, impl OrbitQueryModel + ?Sized>,
) -> HashMap<String, KeySet> {
    let mut chains = HashMap::new();
    let mut pairs = Vec::new();
    collect(source, &mut chains, &mut pairs);
    let mut injected = HashMap::new();
    let mut changed = true;
    while changed {
        changed = false;
        for (target, source) in pairs.iter().flat_map(|(a, b)| [(a, b), (b, a)]) {
            let (Some(target_chain), Some(source_chain)) =
                (chains.get(&target.source), chains.get(&source.source))
            else {
                continue;
            };
            let keyed = facts
                .sort_key(&target_chain.table)
                .is_some_and(|key| key.contains(&target.name));
            if injected.contains_key(&target.source)
                || !keyed
                || facts.bounded(target_chain)
                || !facts.bounded(source_chain)
            {
                continue;
            }
            let set = KeySet {
                values: vec![target.clone()],
                keys: source_chain.candidates(vec![Projection::col(&source.source, &source.name)]),
            };
            if let Some(chain) = chains.get_mut(&target.source) {
                chain.keys.push(set.clone());
            }
            injected.insert(target.source.clone(), set);
            changed = true;
        }
    }
    injected
}

fn collect(
    source: &PhysicalSource,
    chains: &mut HashMap<String, Chain>,
    pairs: &mut Vec<(Column, Column)>,
) {
    if let Some(chain) = Chain::of(source) {
        chains.insert(chain.alias.clone(), chain);
        return;
    }
    match source {
        PhysicalSource::Join {
            endpoints,
            left,
            right,
            ..
        } => {
            pairs.push(endpoints.clone());
            collect(left, chains, pairs);
            collect(right, chains, pairs);
        }
        PhysicalSource::Filter { input, .. }
        | PhysicalSource::KeyFilter { input, .. }
        | PhysicalSource::Scope { input, .. } => collect(input, chains, pairs),
        _ => {}
    }
}

fn columns(predicate: &Predicate) -> Vec<Column> {
    match predicate {
        Predicate::Boolean(expression) => {
            expression.leaves().flat_map(|leaf| columns(leaf)).collect()
        }
        Predicate::Property { column, .. }
        | Predicate::Ids { column, .. }
        | Predicate::IdRange { column, .. }
        | Predicate::EntityKind { column, .. }
        | Predicate::Tags { column, .. }
        | Predicate::Membership { column, .. } => vec![column.clone()],
        Predicate::ColumnsEqual { left, right } | Predicate::ColumnEq { left, right } => {
            vec![left.clone(), right.clone()]
        }
        Predicate::Live { alias } => vec![Column::new(alias, DELETED_COLUMN)],
        Predicate::RelationshipKinds { alias, .. } => {
            vec![Column::new(alias, RELATIONSHIP_KIND_COLUMN)]
        }
    }
}

fn placeholder() -> PhysicalSource {
    PhysicalSource::Scan {
        table: String::new(),
        alias: String::new(),
        final_: false,
        relationship: None,
    }
}
