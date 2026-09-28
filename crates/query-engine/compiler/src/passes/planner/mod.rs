mod bind;
mod candidate;
mod explain;
mod lower;
mod optimize;
mod physical_clickhouse;
mod physical_duckdb;

use crate::ast;
use crate::error::Result;
use crate::input::{AggFunction, FilterOp, Input, TruncateUnit};
use crate::passes::plan::HydrationCompileOptions;
use query_data_model::{QueryBackendCatalog, QueryDataModel};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

pub use bind::{bind, bind_with_options};
pub use explain::{explain, explain_clickhouse, explain_duckdb};
pub use lower::lower_duckdb;
pub use optimize::optimize;
pub use physical_clickhouse::plan_clickhouse;
pub use physical_duckdb::plan_duckdb;

pub struct PlannedClickHouse {
    pub bound: BoundCatalog<query_data_model::ClickHouseDataModel>,
    pub candidate: Candidate<ClickHouse>,
}

pub struct PlannedDuckDb {
    pub bound: BoundCatalog<query_data_model::DuckDbDataModel>,
    pub candidate: Candidate<DuckDb>,
}

pub fn clickhouse(
    input: Input,
    model: Arc<query_data_model::ClickHouseDataModel>,
    hydration_options: HydrationCompileOptions,
) -> Result<PlannedClickHouse> {
    let (bound, logical) = bind_with_options(input, model, hydration_options)?;
    let (bound, logical) = optimize(bound, logical);
    let selected = plan_clickhouse(&bound, logical)?.selected;
    Ok(PlannedClickHouse {
        bound,
        candidate: selected.candidate,
    })
}

pub fn duckdb(
    input: Input,
    model: Arc<query_data_model::DuckDbDataModel>,
) -> Result<PlannedDuckDb> {
    let (bound, logical) = bind(input, model)?;
    let selected = plan_duckdb(&bound, logical)?.selected;
    Ok(PlannedDuckDb {
        bound,
        candidate: selected.candidate,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutputId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InputNodeId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InputRelationshipId(pub usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalColumn(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnKey {
    pub relation: RelationId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumn {
    pub relation: RelationId,
    pub name: String,
    pub data_type: Option<ontology::DataType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Float(String),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(ColumnId),
    Output(OutputId),
    Literal(Value),
    Compare {
        op: CompareOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Filter {
        op: FilterOp,
        left: Box<Self>,
        right: Option<Box<Self>>,
        data_type: Option<ontology::DataType>,
    },
    And(Vec<Self>),
    Or(Vec<Self>),
    In {
        value: Box<Self>,
        values: Vec<Value>,
        data_type: Option<ontology::DataType>,
    },
    DateTrunc {
        unit: TruncateUnit,
        value: Box<Self>,
    },
    Aggregate {
        function: AggFunction,
        value: Option<Box<Self>>,
    },
    Array(Vec<Self>),
    Tuple(Vec<Self>),
    JsonObject(Vec<(String, Self)>),
    Stringify(Box<Self>),
    ListContains {
        list: Box<Self>,
        values: Vec<Value>,
    },
    TokenMatch {
        value: Box<Self>,
        token: Value,
    },
    PathPrefixAny {
        value: Box<Self>,
        paths: Vec<String>,
    },
}

impl Expr {
    fn compare(self, op: CompareOp, right: impl Into<Self>) -> Self {
        Self::Compare {
            op,
            left: Box::new(self),
            right: Box::new(right.into()),
        }
    }

    fn eq(self, right: impl Into<Self>) -> Self {
        self.compare(CompareOp::Eq, right)
    }

    fn le(self, right: impl Into<Self>) -> Self {
        self.compare(CompareOp::Le, right)
    }

    fn ge(self, right: impl Into<Self>) -> Self {
        self.compare(CompareOp::Ge, right)
    }

    fn columns(&self) -> BTreeSet<ColumnId> {
        let mut columns = BTreeSet::new();
        self.visit(&mut |expression| {
            if let Self::Column(column) = expression {
                columns.insert(*column);
            }
        });
        columns
    }

    fn visit(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        match self {
            Self::Compare { left, right, .. } => {
                left.visit(visitor);
                right.visit(visitor);
            }
            Self::Filter { left, right, .. } => {
                left.visit(visitor);
                right.iter().for_each(|right| right.visit(visitor));
            }
            Self::And(values) | Self::Or(values) | Self::Array(values) | Self::Tuple(values) => {
                values.iter().for_each(|value| value.visit(visitor));
            }
            Self::In { value, .. }
            | Self::DateTrunc { value, .. }
            | Self::Stringify(value)
            | Self::ListContains { list: value, .. }
            | Self::TokenMatch { value, .. }
            | Self::PathPrefixAny { value, .. } => value.visit(visitor),
            Self::Aggregate { value, .. } => {
                value.iter().for_each(|value| value.visit(visitor));
            }
            Self::JsonObject(entries) => entries.iter().for_each(|(_, value)| value.visit(visitor)),
            Self::Column(_) | Self::Output(_) | Self::Literal(_) => {}
        }
    }
}

impl From<ColumnId> for Expr {
    fn from(column: ColumnId) -> Self {
        Self::Column(column)
    }
}

impl From<OutputId> for Expr {
    fn from(output: OutputId) -> Self {
        Self::Output(output)
    }
}

impl From<i64> for Expr {
    fn from(value: i64) -> Self {
        Self::Literal(Value::Int(value))
    }
}

impl From<bool> for Expr {
    fn from(value: bool) -> Self {
        Self::Literal(Value::Bool(value))
    }
}

impl From<String> for Expr {
    fn from(value: String) -> Self {
        Self::Literal(Value::String(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedExpr {
    pub expression: Expr,
    pub output: OutputId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub expression: Expr,
    pub descending: bool,
}

pub trait Flavor: Debug + Clone + Copy + PartialEq + Eq + 'static {
    type Scan: Debug + Clone + PartialEq + Eq + ScanRelation;
    type CurrentRows: Debug + Clone + PartialEq + Eq;
    type Extension: Debug + Clone + PartialEq + Eq;
}

pub trait ScanRelation {
    fn relation(&self) -> RelationId;
    fn column_count(&self) -> usize;
}

impl ScanRelation for LogicalScan {
    fn relation(&self) -> RelationId {
        self.relation
    }

    fn column_count(&self) -> usize {
        0
    }
}

impl ScanRelation for PhysicalScan<ClickHouseAccess> {
    fn relation(&self) -> RelationId {
        self.relation
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }
}

impl ScanRelation for PhysicalScan<DuckDbAccess> {
    fn relation(&self) -> RelationId {
        self.relation
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Plan<F: Flavor> {
    pub operator: Operator<F>,
    pub inputs: Vec<Self>,
}

impl<F: Flavor> Plan<F> {
    fn leaf(operator: Operator<F>) -> Self {
        Self {
            operator,
            inputs: vec![],
        }
    }

    fn unary(operator: Operator<F>, input: Self) -> Self {
        Self {
            operator,
            inputs: vec![input],
        }
    }

    fn filter(self, mut predicates: Vec<Expr>) -> Self {
        match predicates.len() {
            0 => self,
            1 => Self::unary(Operator::Filter(predicates.pop().unwrap()), self),
            _ => Self::unary(Operator::Filter(Expr::And(predicates)), self),
        }
    }

    fn project(self, columns: Vec<NamedExpr>) -> Self {
        Self::unary(Operator::Project(columns), self)
    }

    fn aggregate(self, groups: Vec<NamedExpr>, metrics: Vec<NamedExpr>) -> Self {
        Self::unary(Operator::Aggregate { groups, metrics }, self)
    }

    fn semi_join(self, lookup: Self, condition: Expr) -> Self {
        Self {
            operator: Operator::SemiJoin(condition),
            inputs: vec![self, lookup],
        }
    }

    fn current_rows(self, keys: Vec<Expr>, strategy: F::CurrentRows) -> Self {
        Self::unary(Operator::CurrentRows { keys, strategy }, self)
    }

    fn sort(self, keys: Vec<SortKey>) -> Self {
        Self::unary(Operator::Sort(keys), self)
    }

    fn limit(self, count: u32) -> Self {
        Self::unary(Operator::Limit(count), self)
    }

    fn union(inputs: Vec<Self>) -> Self {
        Self {
            operator: Operator::Union,
            inputs,
        }
    }

    fn union_or_single(mut inputs: Vec<Self>) -> Self {
        match inputs.len() {
            1 => inputs.pop().unwrap(),
            _ => Self::union(inputs),
        }
    }

    fn join(inputs: impl IntoIterator<Item = Self>, conditions: Vec<Expr>) -> Self {
        let mut inputs: Vec<_> = inputs.into_iter().collect();
        if inputs.len() == 1 {
            inputs.pop().unwrap()
        } else {
            Self {
                operator: Operator::Join(conditions),
                inputs,
            }
        }
    }

    fn visit(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        self.inputs.iter().for_each(|input| input.visit(visitor));
    }

    fn relation(&self) -> Option<RelationId> {
        match &self.operator {
            Operator::Scan(scan) => Some(scan.relation()),
            Operator::Bind(relation) => Some(*relation),
            Operator::CurrentRows { .. }
            | Operator::Filter(_)
            | Operator::Project(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::SemiJoin(_) => self.inputs.first().and_then(Self::relation),
            _ => None,
        }
    }

    fn visible_relations(&self) -> BTreeSet<RelationId> {
        match &self.operator {
            Operator::Bind(relation) => BTreeSet::from([*relation]),
            Operator::Scan(scan) => BTreeSet::from([scan.relation()]),
            Operator::CurrentRows { .. }
            | Operator::Filter(_)
            | Operator::Project(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::SemiJoin(_) => self
                .inputs
                .first()
                .map(Self::visible_relations)
                .unwrap_or_default(),
            _ => self
                .inputs
                .iter()
                .flat_map(Self::visible_relations)
                .collect(),
        }
    }

    fn map_expressions(mut self, map: &mut impl FnMut(Expr) -> Expr) -> Self {
        self.operator = match self.operator {
            Operator::Filter(expression) => Operator::Filter(rewrite(expression, map)),
            Operator::Project(columns) => Operator::Project(map_named(columns, map)),
            Operator::Join(conditions) => Operator::Join(
                conditions
                    .into_iter()
                    .map(|condition| rewrite(condition, map))
                    .collect(),
            ),
            Operator::SemiJoin(condition) => Operator::SemiJoin(rewrite(condition, map)),
            Operator::Aggregate { groups, metrics } => Operator::Aggregate {
                groups: map_named(groups, map),
                metrics: map_named(metrics, map),
            },
            Operator::Sort(keys) => Operator::Sort(
                keys.into_iter()
                    .map(|key| SortKey {
                        expression: rewrite(key.expression, map),
                        descending: key.descending,
                    })
                    .collect(),
            ),
            Operator::CurrentRows { keys, strategy } => Operator::CurrentRows {
                keys: keys.into_iter().map(|key| rewrite(key, map)).collect(),
                strategy,
            },
            operator => operator,
        };
        self.inputs = self
            .inputs
            .into_iter()
            .map(|input| input.map_expressions(map))
            .collect();
        self
    }
}

struct JoinEditor<F: Flavor> {
    inputs: Vec<Plan<F>>,
    conditions: Vec<Expr>,
}

impl<F: Flavor> JoinEditor<F> {
    fn new(plan: Plan<F>) -> Option<Self> {
        let Operator::Join(conditions) = plan.operator else {
            return None;
        };
        Some(Self {
            inputs: plan.inputs,
            conditions,
        })
    }

    fn retain_inputs(&mut self, keep: impl Fn(&Plan<F>) -> bool) {
        self.inputs.retain(keep);
    }

    fn retain_conditions(&mut self, keep: impl Fn(&Expr) -> bool) {
        self.conditions.retain(keep);
    }

    fn add_conditions(&mut self, conditions: impl IntoIterator<Item = Expr>) {
        for condition in conditions {
            if !self.conditions.contains(&condition) {
                self.conditions.push(condition);
            }
        }
    }

    fn remove_inputs(&mut self, mut indexes: Vec<usize>) {
        indexes.sort_unstable_by(|left, right| right.cmp(left));
        for index in indexes {
            self.inputs.remove(index);
        }
    }

    fn remove_condition(&mut self, index: usize) {
        self.conditions.remove(index);
    }

    fn inputs(&self) -> &[Plan<F>] {
        &self.inputs
    }

    fn condition_entries(&self) -> impl Iterator<Item = (usize, &Expr)> {
        self.conditions.iter().enumerate()
    }

    fn finish(mut self) -> Plan<F> {
        if self.inputs.len() == 1 {
            self.inputs.pop().unwrap()
        } else {
            Plan {
                operator: Operator::Join(self.conditions),
                inputs: self.inputs,
            }
        }
    }
}

fn map_named(columns: Vec<NamedExpr>, map: &mut impl FnMut(Expr) -> Expr) -> Vec<NamedExpr> {
    columns
        .into_iter()
        .map(|column| NamedExpr {
            expression: rewrite(column.expression, map),
            output: column.output,
        })
        .collect()
}

fn rewrite(expression: Expr, map: &mut impl FnMut(Expr) -> Expr) -> Expr {
    let expression = match expression {
        Expr::Compare { op, left, right } => Expr::Compare {
            op,
            left: Box::new(rewrite(*left, map)),
            right: Box::new(rewrite(*right, map)),
        },
        Expr::Filter {
            op,
            left,
            right,
            data_type,
        } => Expr::Filter {
            op,
            left: Box::new(rewrite(*left, map)),
            right: right.map(|right| Box::new(rewrite(*right, map))),
            data_type,
        },
        Expr::And(values) => Expr::And(
            values
                .into_iter()
                .map(|value| rewrite(value, map))
                .collect(),
        ),
        Expr::Or(values) => Expr::Or(
            values
                .into_iter()
                .map(|value| rewrite(value, map))
                .collect(),
        ),
        Expr::In {
            value,
            values,
            data_type,
        } => Expr::In {
            value: Box::new(rewrite(*value, map)),
            values,
            data_type,
        },
        Expr::DateTrunc { unit, value } => Expr::DateTrunc {
            unit,
            value: Box::new(rewrite(*value, map)),
        },
        Expr::Aggregate { function, value } => Expr::Aggregate {
            function,
            value: value.map(|value| Box::new(rewrite(*value, map))),
        },
        Expr::Array(values) => Expr::Array(
            values
                .into_iter()
                .map(|value| rewrite(value, map))
                .collect(),
        ),
        Expr::Tuple(values) => Expr::Tuple(
            values
                .into_iter()
                .map(|value| rewrite(value, map))
                .collect(),
        ),
        Expr::JsonObject(entries) => Expr::JsonObject(
            entries
                .into_iter()
                .map(|(key, value)| (key, rewrite(value, map)))
                .collect(),
        ),
        Expr::Stringify(value) => Expr::Stringify(Box::new(rewrite(*value, map))),
        Expr::ListContains { list, values } => Expr::ListContains {
            list: Box::new(rewrite(*list, map)),
            values,
        },
        Expr::TokenMatch { value, token } => Expr::TokenMatch {
            value: Box::new(rewrite(*value, map)),
            token,
        },
        Expr::PathPrefixAny { value, paths } => Expr::PathPrefixAny {
            value: Box::new(rewrite(*value, map)),
            paths,
        },
        expression => expression,
    };
    map(expression)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator<F: Flavor> {
    Scan(F::Scan),
    Filter(Expr),
    Project(Vec<NamedExpr>),
    Join(Vec<Expr>),
    SemiJoin(Expr),
    Aggregate {
        groups: Vec<NamedExpr>,
        metrics: Vec<NamedExpr>,
    },
    Union,
    Bind(RelationId),
    Sort(Vec<SortKey>),
    Limit(u32),
    CurrentRows {
        keys: Vec<Expr>,
        strategy: F::CurrentRows,
    },
    Extension(F::Extension),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Logical;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalScan {
    pub relation: RelationId,
}

impl Flavor for Logical {
    type Scan = LogicalScan;
    type CurrentRows = ();
    type Extension = ();
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationOrigin {
    Node {
        input: InputNodeId,
        entity: query_data_model::EntityId,
    },
    Edge {
        input: Option<InputRelationshipId>,
        relationships: Vec<query_data_model::RelationshipId>,
        depth: Option<u32>,
        hop: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelation {
    pub origin: RelationOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOutput {
    pub name: String,
}

pub struct BoundCatalog<M: QueryDataModel = query_data_model::ClickHouseDataModel> {
    pub input: Input,
    pub model: Arc<M>,
    pub relations: BTreeMap<RelationId, BoundRelation>,
    pub column_ids: BTreeMap<ColumnKey, ColumnId>,
    pub columns: BTreeMap<ColumnId, BoundColumn>,
    pub outputs: BTreeMap<OutputId, BoundOutput>,
}

impl<M: QueryDataModel> Clone for BoundCatalog<M> {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            model: Arc::clone(&self.model),
            relations: self.relations.clone(),
            column_ids: self.column_ids.clone(),
            columns: self.columns.clone(),
            outputs: self.outputs.clone(),
        }
    }
}

impl<M: QueryDataModel> std::fmt::Debug for BoundCatalog<M> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundCatalog")
            .field("relations", &self.relations.len())
            .field("columns", &self.columns.len())
            .field("outputs", &self.outputs.len())
            .finish()
    }
}

impl<M: QueryDataModel> BoundCatalog<M> {
    fn relation(&self, relation: RelationId) -> &BoundRelation {
        &self.relations[&relation]
    }

    fn relations(&self) -> impl Iterator<Item = (RelationId, &BoundRelation)> {
        self.relations
            .iter()
            .map(|(relation, metadata)| (*relation, metadata))
    }

    fn column(&self, column: ColumnId) -> &BoundColumn {
        &self.columns[&column]
    }

    fn column_id(&self, relation: RelationId, name: &str) -> Option<ColumnId> {
        self.column_ids
            .get(&ColumnKey {
                relation,
                name: name.into(),
            })
            .copied()
    }

    fn columns_for(&self, relation: RelationId) -> impl Iterator<Item = ColumnId> + '_ {
        self.columns
            .iter()
            .filter_map(move |(id, column)| (column.relation == relation).then_some(*id))
    }

    fn node_input(&self, relation: RelationId) -> Option<InputNodeId> {
        match self.relation(relation).origin {
            RelationOrigin::Node { input, .. } => Some(input),
            _ => None,
        }
    }

    fn node_relation(&self, name: &str) -> Option<RelationId> {
        self.relations.iter().find_map(|(relation, metadata)| {
            let RelationOrigin::Node { input, .. } = metadata.origin else {
                return None;
            };
            (self.input.nodes[input.0].id == name).then_some(*relation)
        })
    }

    fn relationship_name(&self, relationship: query_data_model::RelationshipId) -> &str {
        &self.model.graph().relationship(relationship).name
    }

    fn entity_name(&self, entity: query_data_model::EntityId) -> &str {
        &self.model.graph().entity(entity).name
    }

    fn remove_relations(&mut self, removed: &BTreeSet<RelationId>) {
        self.relations
            .retain(|relation, _| !removed.contains(relation));
        self.columns
            .retain(|_, column| !removed.contains(&column.relation));
        self.column_ids
            .retain(|key, _| !removed.contains(&key.relation));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLayout {
    pub table: TableName,
    pub columns: BTreeSet<PhysicalColumn>,
    pub sort_key: Vec<PhysicalColumn>,
    pub global: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAccess {
    pub layout: TableLayout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTableAccess {
    pub layouts: Vec<TableLayout>,
    pub columns: BTreeMap<ColumnId, PhysicalColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyAccess {
    pub relationship: RelationId,
    pub holder: RelationId,
    pub referenced: RelationId,
    pub column: PhysicalColumn,
    pub substitutions: BTreeMap<ColumnId, Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePropertyAccess {
    pub edge: RelationId,
    pub source: ColumnId,
    pub column: ColumnId,
    pub edge_column: PhysicalColumn,
    pub tokens: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub root: Plan<Logical>,
    pub scope_requirements: Vec<crate::scope::ScopeProof>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHouse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckDb;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalScan<A> {
    pub relation: RelationId,
    pub access: A,
    pub columns: BTreeSet<ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseAccess {
    Table(TableAccess),
    EdgeTables(EdgeTableAccess),
}

impl ClickHouseAccess {
    fn edge_columns(&self) -> Option<&BTreeMap<ColumnId, PhysicalColumn>> {
        match self {
            Self::EdgeTables(access) => Some(&access.columns),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuckDbAccess {
    Table(TableAccess),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseCurrentRows {
    Final,
    LimitBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckDbCurrentRows;

impl Flavor for ClickHouse {
    type Scan = PhysicalScan<ClickHouseAccess>;
    type CurrentRows = ClickHouseCurrentRows;
    type Extension = ();
}

impl Flavor for DuckDb {
    type Scan = PhysicalScan<DuckDbAccess>;
    type CurrentRows = DuckDbCurrentRows;
    type Extension = ();
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cost {
    pub scans: u32,
    pub final_reads: u32,
    pub joins: u32,
    pub semi_joins: u32,
    pub union_arms: u32,
    pub columns_read: u32,
    pub residual_filters: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Candidate<B: Flavor> {
    pub plan: Plan<B>,
    pub cost: Cost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSet<B: Flavor> {
    pub candidates: Vec<Candidate<B>>,
}

impl<B: Flavor> Default for CandidateSet<B> {
    fn default() -> Self {
        Self { candidates: vec![] }
    }
}

impl<B: Flavor> CandidateSet<B> {
    fn insert(&mut self, candidate: Candidate<B>) {
        if let Some(current) = self
            .candidates
            .iter_mut()
            .find(|current| current.plan == candidate.plan)
        {
            if candidate.cost < current.cost {
                *current = candidate;
            }
            return;
        }
        self.candidates.push(candidate);
    }

    fn select(self) -> Option<SelectedPlan<B>> {
        self.candidates
            .into_iter()
            .min_by_key(|candidate| candidate.cost)
            .map(|candidate| SelectedPlan { candidate })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectedPlan<B: Flavor> {
    pub candidate: Candidate<B>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanningResult<B: Flavor> {
    pub logical: LogicalPlan,
    pub selected: SelectedPlan<B>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LoweredMetadata {
    pub node_sources: std::collections::HashMap<String, (String, String)>,
    pub aliases: crate::aliases::AliasManager,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<ast::OrderExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredEdge {
    pub column_prefix: String,
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LoweredPlan {
    pub ast: ast::Node,
    pub metadata: LoweredMetadata,
    pub explain: String,
}
