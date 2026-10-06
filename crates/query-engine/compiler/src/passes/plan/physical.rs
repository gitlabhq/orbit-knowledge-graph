use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, node_outputs, node_predicates,
    property_filter,
};
use crate::error::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::bindings::{ColumnRef, RelationId, RelationSource};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

use super::context::PlanningContext;
use super::{Hop, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub definitions: Vec<(crate::bindings::Definition, PhysicalPlan)>,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub joined: bool,
}

impl BindingSource {
    pub(super) fn table(alias: &str) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            column: DEFAULT_PRIMARY_KEY.into(),
            joined: true,
        }
    }
}

pub(super) fn key_membership(
    alias: &str,
    column: &str,
    name: crate::bindings::Definition,
) -> Predicate {
    Predicate::Membership {
        column: Column::new(alias, column),
        key: name.exports()[0].clone(),
        definition: name,
    }
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<Projection>,
}

#[derive(Clone)]
pub enum PhysicalSource {
    KeyFilter {
        value: Column,
        keys: Box<PhysicalPlan>,
        input: Box<Self>,
    },
    Union {
        alias: String,
        arms: Vec<PhysicalPlan>,
        relationship: usize,
    },
    Scan {
        relation: RelationId,
        alias: String,
        final_: bool,
        relationship: Option<usize>,
    },
    Filter {
        predicates: Vec<Predicate>,
        input: Box<Self>,
    },
    Scope {
        alias: String,
        input: Box<PhysicalPlan>,
    },
    Join {
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
        left: Box<Self>,
        right: Box<Self>,
    },
    Latest {
        sort_key: Vec<ColumnRef>,
        alias: String,
        aggregate_condition: Vec<Predicate>,
        input: Box<Self>,
    },
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn scan(
        &mut self,
        table: &str,
        alias: &str,
        final_: bool,
        relationship: Option<usize>,
    ) -> Result<PhysicalSource> {
        let storage = self.model.query_backend().storage();
        let table = storage
            .resolve_table(table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let relation = self
            .bindings
            .scan(storage, self.bindings.root(), table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        Ok(PhysicalSource::Scan {
            relation,
            alias: alias.into(),
            final_,
            relationship,
        })
    }

    pub fn current_rows(
        &mut self,
        table: &str,
        alias: &str,
        columns: &[String],
        predicates: Vec<Predicate>,
    ) -> Result<PhysicalSource> {
        let scan = self.scan(table, alias, false, None)?;
        let table = self.model.table(table).expect("bound scan table");
        if *table.row_semantics() == query_data_model::storage::RowSemantics::Current {
            return Ok(scan.filter(predicates));
        }
        let mut outputs = Vec::new();
        for name in columns
            .first()
            .map(String::as_str)
            .into_iter()
            .chain([ontology::DELETED_COLUMN])
            .chain(columns.iter().skip(1).map(String::as_str))
        {
            if !outputs
                .iter()
                .any(|projection: &Projection| projection.name.name() == name)
            {
                outputs.push(Projection::col(alias, name));
            }
        }
        Ok(PhysicalSource::Scope {
            alias: alias.into(),
            input: Box::new(PhysicalPlan {
                source: self.latest(scan, predicates, vec![])?,
                outputs,
            }),
        }
        .filter(vec![super::requirements::live(alias)]))
    }

    pub(super) fn latest(
        &self,
        source: PhysicalSource,
        predicates: Vec<Predicate>,
        aggregate_condition: Vec<Predicate>,
    ) -> Result<PhysicalSource> {
        if let PhysicalSource::Filter {
            predicates: mut existing,
            input,
        } = source
        {
            existing.extend(predicates);
            return self.latest(*input, existing, aggregate_condition);
        }
        let PhysicalSource::Scan {
            relation,
            ref alias,
            ..
        } = source
        else {
            return Err(QueryError::Lowering(
                "latest-row selection requires a stored scan".into(),
            ));
        };
        let RelationSource::Scan(table) = self
            .bindings
            .source(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
        else {
            return Err(QueryError::Lowering(
                "latest-row source is not a stored table".into(),
            ));
        };
        let layout = self.model.query_backend().storage().table(*table);
        if layout.sort_key().is_empty() {
            return Err(QueryError::Lowering(format!(
                "table '{}' has no latest-row key",
                layout.name()
            )));
        }
        let sort_key = layout
            .sort_key()
            .iter()
            .map(|column| {
                self.bindings
                    .stored_column(
                        self.bindings.root(),
                        relation,
                        query_data_model::storage::StoredColumnRef {
                            table: *table,
                            column: *column,
                        },
                    )
                    .map_err(|error| QueryError::Lowering(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PhysicalSource::Latest {
            sort_key,
            alias: alias.clone(),
            aggregate_condition,
            input: Box::new(source.filter(predicates)),
        })
    }
}

impl PhysicalSource {
    pub fn scoped(self, alias: &str) -> Self {
        Self::Scope {
            alias: alias.into(),
            input: Box::new(PhysicalPlan {
                source: self,
                outputs: vec![],
            }),
        }
    }
    pub(super) fn inner_join(self, right: Self, endpoints: (Column, Column)) -> Self {
        Self::Join {
            endpoints,
            predicates: vec![],
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub(crate) fn filter(self, predicates: Vec<Predicate>) -> Self {
        if predicates.is_empty() {
            self
        } else {
            Self::Filter {
                predicates,
                input: Box::new(self),
            }
        }
    }

    pub(super) fn cascade(self, hop: &Hop, alias: &str, upstream: Option<&PhysicalPlan>) -> Self {
        match upstream {
            Some(keys) => Self::KeyFilter {
                value: Column::new(
                    alias,
                    &hop.join_prev.as_ref().expect("cascade join").curr_col,
                ),
                keys: Box::new(keys.clone()),
                input: Box::new(self),
            },
            None => self,
        }
    }
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn node(&self, alias: &str) -> Result<&NodePlan> {
        self.nodes
            .get(alias)
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
    }

    fn node_source(&mut self, alias: &str, final_: bool) -> Result<PhysicalSource> {
        let table = self
            .node(alias)?
            .table
            .as_deref()
            .and_then(|table| self.model.table(table))
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' has no table")))?;
        self.scan(table.name(), alias, final_, None)
    }

    pub(super) fn edge_keys(
        &mut self,
        index: usize,
        alias: &str,
        predicates: Vec<Predicate>,
        upstream: Option<&PhysicalPlan>,
    ) -> Result<PhysicalSource> {
        let source = PhysicalSource::Filter {
            predicates,
            input: Box::new(self.edge_scan(index, alias, false)?),
        };
        Ok(source.cascade(&self.hops[index], alias, upstream))
    }

    pub(super) fn edge_scan(
        &mut self,
        index: usize,
        alias: &str,
        final_: bool,
    ) -> Result<PhysicalSource> {
        let hop = &self.hops[index];
        let table = self.model.table(&hop.edge_table).ok_or_else(|| {
            QueryError::Lowering(format!("unknown edge table '{}'", hop.edge_table))
        })?;
        self.scan(table.name(), alias, final_, Some(hop.input_index))
    }

    pub fn candidate_keys(
        &mut self,
        alias: &str,
        column: &str,
        extra: Vec<Predicate>,
    ) -> Result<PhysicalPlan> {
        self.keys(alias, column, false, extra)
    }

    pub fn filtered_keys(&mut self, alias: &str, column: &str) -> Result<PhysicalPlan> {
        self.keys(alias, column, true, vec![])
    }

    fn keys(
        &mut self,
        alias: &str,
        column: &str,
        final_: bool,
        extra: Vec<Predicate>,
    ) -> Result<PhysicalPlan> {
        let mut predicates = node_predicates(self.node(alias)?);
        predicates.extend(extra);
        Ok(PhysicalPlan {
            source: self.node_source(alias, final_)?.filter(predicates),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(alias, column)),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn node_scan(&mut self, alias: &str, narrowing: Option<Predicate>) -> Result<PhysicalPlan> {
        let mut source = self.node_source(alias, narrowing.is_none())?;
        let node = self.node(alias)?;
        if let Some(narrowing) = narrowing {
            let table = self
                .model
                .table(node.table.as_deref().expect("bound node table"))
                .expect("bound node table");
            let mut predicates = vec![narrowing];
            predicates.extend(
                node.filters
                    .iter()
                    .filter(|(column, filter)| {
                        table
                            .column_id(column)
                            .is_ok_and(|column| table.sort_key().contains(&column))
                            && filter.filter.rhs_column.is_none()
                    })
                    .map(|(column, filter)| property_filter(&node.alias, column, filter)),
            );
            if table
                .column_id(DEFAULT_PRIMARY_KEY)
                .is_ok_and(|column| table.sort_key().contains(&column))
            {
                if !node.node_ids.is_empty() {
                    predicates.push(id_list(&node.alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
                }
                if let Some(range) = &node.id_range {
                    predicates.push(Predicate::IdRange {
                        column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        start: range.start,
                        end: range.end,
                    });
                }
            }
            source = self.latest(source, predicates, vec![])?;
        }
        Ok(PhysicalPlan {
            source: source.filter(node_predicates(node)).scoped(&node.alias),
            outputs: node_outputs(node),
        })
    }

    pub fn node_plan(&mut self, alias: &str) -> Result<PhysicalPlan> {
        let source = self.node_source(alias, true)?;
        let node = self.node(alias)?;
        Ok(PhysicalPlan {
            outputs: node_outputs(node),
            source: source.filter(node_predicates(node)),
        })
    }
}

impl PhysicalPlan {
    pub fn define(self, hint: impl Into<String>) -> (crate::bindings::Definition, Self) {
        let definition = crate::bindings::Definition::new(
            hint,
            self.outputs
                .iter()
                .map(|output| output.name.clone())
                .collect(),
        );
        (definition, self)
    }
}
