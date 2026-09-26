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
use ontology::Ontology;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

pub use bind::bind;
pub use explain::{explain, explain_clickhouse, explain_duckdb};
pub use lower::{lower_clickhouse, lower_duckdb};
pub use optimize::optimize;
pub use physical_clickhouse::plan_clickhouse;
pub use physical_duckdb::plan_duckdb;

pub fn clickhouse(
    input: Input,
    ontology: Arc<Ontology>,
) -> Result<(
    BoundCatalog,
    Candidate<ClickHouse>,
    Vec<crate::scope::ScopeProof>,
    String,
)> {
    let (bound, logical) = bind(input, ontology)?;
    let (bound, logical) = optimize(bound, logical);
    let scope_requirements = logical.scope_requirements.clone();
    let selected = plan_clickhouse(&bound, logical)?.selected;
    let explain = explain_clickhouse(&bound, &selected.candidate.plan);
    Ok((bound, selected.candidate, scope_requirements, explain))
}

pub fn duckdb(
    input: Input,
    ontology: Arc<Ontology>,
) -> Result<(
    BoundCatalog,
    Candidate<DuckDb>,
    Vec<crate::scope::ScopeProof>,
    String,
)> {
    let (bound, logical) = bind(input, ontology)?;
    let (bound, logical) = optimize(bound, logical);
    let scope_requirements = logical.scope_requirements.clone();
    let selected = plan_duckdb(&bound, logical)?.selected;
    let explain = explain_duckdb(&bound, &selected.candidate.plan);
    Ok((bound, selected.candidate, scope_requirements, explain))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationshipId(pub u32);

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
pub struct Tokenizer(pub String);

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
            | Self::TokenMatch { value, .. } => value.visit(visitor),
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
    type Facts: Debug + Clone + PartialEq + Eq;
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

    fn replace_inputs(&mut self, replace: impl Fn(&Plan<F>) -> bool, replacement: Plan<F>) {
        let mut replacement = Some(replacement);
        self.inputs = std::mem::take(&mut self.inputs)
            .into_iter()
            .filter_map(|input| {
                if replace(&input) {
                    replacement.take()
                } else {
                    Some(input)
                }
            })
            .collect();
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
    type Facts = ();
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationOrigin {
    Node {
        input: InputNodeId,
    },
    Edge {
        input: Option<InputRelationshipId>,
        depth: Option<u32>,
        hop: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelation {
    pub origin: RelationOrigin,
    pub entity: Option<EntityId>,
    pub relationships: Vec<RelationshipId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundEntity {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelationship {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOutput {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct BoundCatalog {
    pub input: Input,
    pub ontology: Arc<Ontology>,
    pub relations: BTreeMap<RelationId, BoundRelation>,
    pub column_ids: BTreeMap<ColumnKey, ColumnId>,
    pub columns: BTreeMap<ColumnId, BoundColumn>,
    pub entity_ids: BTreeMap<String, EntityId>,
    pub entities: BTreeMap<EntityId, BoundEntity>,
    pub relationship_ids: BTreeMap<String, RelationshipId>,
    pub relationships: BTreeMap<RelationshipId, BoundRelationship>,
    pub outputs: BTreeMap<OutputId, BoundOutput>,
}

impl BoundCatalog {
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
            RelationOrigin::Node { input } => Some(input),
            _ => None,
        }
    }

    fn edge_relation(&self, input: InputRelationshipId) -> Option<RelationId> {
        self.relations.iter().find_map(|(relation, metadata)| {
            matches!(metadata.origin, RelationOrigin::Edge { input: Some(candidate), .. } if candidate == input)
                .then_some(*relation)
        })
    }

    fn node_relation(&self, name: &str) -> Option<RelationId> {
        self.relations.iter().find_map(|(relation, metadata)| {
            let RelationOrigin::Node { input } = metadata.origin else {
                return None;
            };
            (self.input.nodes[input.0].id == name).then_some(*relation)
        })
    }

    fn relationship_name(&self, relationship: RelationshipId) -> &str {
        &self.relationships[&relationship].name
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
pub enum PhysicalRelation {
    Node {
        relation: RelationId,
        layout: TableLayout,
    },
    Edge {
        relation: RelationId,
        layouts: Vec<TableLayout>,
    },
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
pub struct DenormalizedAccess {
    pub scan_relation: RelationId,
    pub layout: TableLayout,
    pub relations: BTreeSet<RelationId>,
    pub columns: BTreeMap<ColumnId, PhysicalColumn>,
    pub residual_filters: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePropertyAccess {
    pub edge: RelationId,
    pub source: ColumnId,
    pub column: ColumnId,
    pub edge_column: PhysicalColumn,
    pub tokens: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextIndexAccess {
    pub column: ColumnId,
    pub tokenizer: Tokenizer,
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
    DenormalizedJoin(DenormalizedAccess),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseExtension {
    FusedNeighbors {
        outgoing: Expr,
        incoming: Expr,
        columns: Vec<NamedExpr>,
    },
}

impl Flavor for ClickHouse {
    type Scan = PhysicalScan<ClickHouseAccess>;
    type CurrentRows = ClickHouseCurrentRows;
    type Extension = ClickHouseExtension;
    type Facts = ClickHouseFacts;
}

impl Flavor for DuckDb {
    type Scan = PhysicalScan<DuckDbAccess>;
    type CurrentRows = DuckDbCurrentRows;
    type Extension = ();
    type Facts = DuckDbFacts;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClickHouseFacts {
    pub foreign_keys: Vec<ForeignKeyAccess>,
    pub denormalized_joins: Vec<DenormalizedAccess>,
    pub edge_properties: Vec<EdgePropertyAccess>,
    pub text_indexes: Vec<TextIndexAccess>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DuckDbFacts;

#[derive(Debug)]
pub struct BackendCatalog<'catalog, B: Flavor> {
    pub bound: &'catalog BoundCatalog,
    pub relations: BTreeMap<RelationId, PhysicalRelation>,
    pub access_paths: BTreeMap<RelationId, Vec<B::Scan>>,
    pub current_rows: BTreeMap<RelationId, Vec<B::CurrentRows>>,
    pub facts: B::Facts,
    pub marker: PhantomData<B>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnBindings {
    pub columns: BTreeMap<ColumnId, Expr>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputBindings {
    pub nodes: BTreeMap<InputNodeId, OutputBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBinding {
    pub relation: RelationId,
    pub primary_key: ColumnId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalProperties {
    pub ordered_by: Vec<SortKey>,
    pub current_relations: BTreeSet<RelationId>,
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
    pub columns: ColumnBindings,
    pub outputs: OutputBindings,
    pub properties: PhysicalProperties,
    pub cost: Cost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyKey {
    pub ordered_by: Vec<SortKey>,
    pub current_relations: BTreeSet<RelationId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSet<B: Flavor> {
    pub candidates: Vec<(PropertyKey, Candidate<B>)>,
}

impl<B: Flavor> Default for CandidateSet<B> {
    fn default() -> Self {
        Self { candidates: vec![] }
    }
}

impl<B: Flavor> CandidateSet<B> {
    fn insert(&mut self, candidate: Candidate<B>) {
        let key = PropertyKey {
            ordered_by: candidate.properties.ordered_by.clone(),
            current_relations: candidate.properties.current_relations.clone(),
        };
        if let Some((_, current)) = self
            .candidates
            .iter_mut()
            .find(|(current, _)| *current == key)
        {
            if candidate.cost < current.cost {
                *current = candidate;
            }
        } else {
            self.candidates.push((key, candidate));
        }
    }

    fn select(self) -> Option<SelectedPlan<B>> {
        self.candidates
            .into_iter()
            .map(|(_, candidate)| candidate)
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

#[derive(Debug, Clone, PartialEq)]
pub enum PlannedQuery {
    ClickHouse(PlanningResult<ClickHouse>),
    DuckDb(PlanningResult<DuckDb>),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LoweredBindings {
    pub columns: BTreeMap<ColumnId, ast::Expr>,
    pub nodes: BTreeMap<InputNodeId, LoweredOutputBinding>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoweredOutputBinding {
    pub primary_key: ast::Expr,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LoweredMetadata {
    pub node_sources: std::collections::HashMap<String, (String, String)>,
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
    pub bindings: LoweredBindings,
    pub metadata: LoweredMetadata,
    pub explain: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const RELATION: RelationId = RelationId(0);
    const EDGE: RelationId = RelationId(1);
    const COLUMN: ColumnId = ColumnId(0);
    const EDGE_COLUMN: ColumnId = ColumnId(1);
    const ENTITY: EntityId = EntityId(0);
    const RELATIONSHIP: RelationshipId = RelationshipId(0);
    const OUTPUT: OutputId = OutputId(0);
    const INPUT_NODE: InputNodeId = InputNodeId(0);
    const INPUT_RELATIONSHIP: InputRelationshipId = InputRelationshipId(0);

    fn layout(table: &str) -> TableLayout {
        TableLayout {
            table: TableName(table.into()),
            columns: BTreeSet::from([
                PhysicalColumn("id".into()),
                PhysicalColumn("traversal_path".into()),
            ]),
            sort_key: vec![
                PhysicalColumn("traversal_path".into()),
                PhysicalColumn("id".into()),
            ],
            global: false,
        }
    }

    fn bound_catalog(input: Input, ontology: Ontology) -> BoundCatalog {
        BoundCatalog {
            input,
            ontology: Arc::new(ontology),
            relations: BTreeMap::from([
                (
                    RELATION,
                    BoundRelation {
                        origin: RelationOrigin::Node { input: INPUT_NODE },
                        entity: Some(ENTITY),
                        relationships: vec![],
                    },
                ),
                (
                    EDGE,
                    BoundRelation {
                        origin: RelationOrigin::Edge {
                            input: Some(INPUT_RELATIONSHIP),
                            depth: Some(1),
                            hop: Some(1),
                        },
                        entity: None,
                        relationships: vec![RELATIONSHIP],
                    },
                ),
            ]),
            column_ids: BTreeMap::from([
                (
                    ColumnKey {
                        relation: RELATION,
                        name: "id".into(),
                    },
                    COLUMN,
                ),
                (
                    ColumnKey {
                        relation: EDGE,
                        name: "source_id".into(),
                    },
                    EDGE_COLUMN,
                ),
            ]),
            columns: BTreeMap::from([
                (
                    COLUMN,
                    BoundColumn {
                        relation: RELATION,
                        name: "id".into(),
                        data_type: Some(ontology::DataType::Int),
                    },
                ),
                (
                    EDGE_COLUMN,
                    BoundColumn {
                        relation: EDGE,
                        name: "source_id".into(),
                        data_type: Some(ontology::DataType::Int),
                    },
                ),
            ]),
            entity_ids: BTreeMap::from([("User".into(), ENTITY)]),
            entities: BTreeMap::from([(
                ENTITY,
                BoundEntity {
                    name: "User".into(),
                },
            )]),
            relationship_ids: BTreeMap::from([("AUTHORED".into(), RELATIONSHIP)]),
            relationships: BTreeMap::from([(
                RELATIONSHIP,
                BoundRelationship {
                    name: "AUTHORED".into(),
                },
            )]),
            outputs: BTreeMap::from([(
                OUTPUT,
                BoundOutput {
                    name: "user_id".into(),
                },
            )]),
        }
    }

    fn logical_plan() -> LogicalPlan {
        LogicalPlan {
            root: Plan {
                operator: Operator::Project(vec![NamedExpr {
                    expression: Expr::Column(COLUMN),
                    output: OUTPUT,
                }]),
                inputs: vec![Plan {
                    operator: Operator::Scan(LogicalScan { relation: RELATION }),
                    inputs: vec![],
                }],
            },
            scope_requirements: vec![],
        }
    }

    fn clickhouse_facts() -> ClickHouseFacts {
        ClickHouseFacts {
            foreign_keys: vec![ForeignKeyAccess {
                relationship: EDGE,
                holder: RELATION,
                referenced: EDGE,
                column: PhysicalColumn("author_id".into()),
                substitutions: BTreeMap::from([(EDGE_COLUMN, Expr::Column(COLUMN))]),
            }],
            denormalized_joins: vec![DenormalizedAccess {
                scan_relation: RELATION,
                layout: layout("gl_denorm_authored"),
                relations: BTreeSet::from([RELATION, EDGE]),
                columns: BTreeMap::from([(COLUMN, PhysicalColumn("t0_id".into()))]),
                residual_filters: vec![Expr::Literal(Value::Bool(true))],
            }],
            edge_properties: vec![EdgePropertyAccess {
                edge: EDGE,
                source: COLUMN,
                column: EDGE_COLUMN,
                edge_column: PhysicalColumn("source_tags".into()),
                tokens: vec![Value::String("state:opened".into())],
            }],
            text_indexes: vec![TextIndexAccess {
                column: COLUMN,
                tokenizer: Tokenizer("splitByNonAlpha".into()),
            }],
        }
    }

    fn clickhouse_candidate() -> Candidate<ClickHouse> {
        Candidate {
            plan: Plan {
                operator: Operator::CurrentRows {
                    keys: vec![Expr::Column(COLUMN)],
                    strategy: ClickHouseCurrentRows::Final,
                },
                inputs: vec![Plan {
                    operator: Operator::Scan(PhysicalScan {
                        relation: RELATION,
                        access: ClickHouseAccess::Table(TableAccess {
                            layout: layout("gl_user"),
                        }),
                        columns: BTreeSet::from([COLUMN]),
                    }),
                    inputs: vec![],
                }],
            },
            columns: ColumnBindings {
                columns: BTreeMap::from([(COLUMN, Expr::Column(COLUMN))]),
            },
            outputs: OutputBindings {
                nodes: BTreeMap::from([(
                    INPUT_NODE,
                    OutputBinding {
                        relation: RELATION,
                        primary_key: COLUMN,
                    },
                )]),
            },
            properties: PhysicalProperties {
                ordered_by: vec![SortKey {
                    expression: Expr::Column(COLUMN),
                    descending: false,
                }],
                current_relations: BTreeSet::from([RELATION]),
            },
            cost: Cost {
                scans: 1,
                final_reads: 1,
                columns_read: 1,
                ..Cost::default()
            },
        }
    }

    fn duckdb_candidate() -> Candidate<DuckDb> {
        Candidate {
            plan: Plan {
                operator: Operator::CurrentRows {
                    keys: vec![Expr::Column(COLUMN)],
                    strategy: DuckDbCurrentRows,
                },
                inputs: vec![Plan {
                    operator: Operator::Scan(PhysicalScan {
                        relation: RELATION,
                        access: DuckDbAccess::Table(TableAccess {
                            layout: layout("gl_user"),
                        }),
                        columns: BTreeSet::from([COLUMN]),
                    }),
                    inputs: vec![],
                }],
            },
            columns: ColumnBindings {
                columns: BTreeMap::from([(COLUMN, Expr::Column(COLUMN))]),
            },
            outputs: OutputBindings {
                nodes: BTreeMap::from([(
                    INPUT_NODE,
                    OutputBinding {
                        relation: RELATION,
                        primary_key: COLUMN,
                    },
                )]),
            },
            properties: PhysicalProperties {
                ordered_by: vec![],
                current_relations: BTreeSet::from([RELATION]),
            },
            cost: Cost {
                scans: 1,
                columns_read: 1,
                ..Cost::default()
            },
        }
    }

    fn property_key(candidate: &Candidate<impl Flavor>) -> PropertyKey {
        PropertyKey {
            ordered_by: candidate.properties.ordered_by.clone(),
            current_relations: candidate.properties.current_relations.clone(),
        }
    }

    #[test]
    fn all_access_paths_and_backend_facts_are_constructible() {
        let accesses = [
            ClickHouseAccess::Table(TableAccess {
                layout: layout("gl_user"),
            }),
            ClickHouseAccess::EdgeTables(EdgeTableAccess {
                layouts: vec![layout("gl_edge")],
                columns: BTreeMap::new(),
            }),
            ClickHouseAccess::DenormalizedJoin(clickhouse_facts().denormalized_joins.remove(0)),
        ];
        assert_eq!(accesses.len(), 3);
        let facts = clickhouse_facts();
        assert_eq!(facts.foreign_keys.len(), 1);
        assert_eq!(facts.denormalized_joins.len(), 1);
        assert_eq!(facts.edge_properties.len(), 1);
        assert_eq!(facts.text_indexes.len(), 1);
    }

    #[test]
    fn skeleton_flow_exercises_every_type_for_both_backends() {
        let input = Input::default();
        let ontology = Ontology::new();
        let bound = bound_catalog(input, ontology);
        let logical = logical_plan();

        let clickhouse_candidate = clickhouse_candidate();
        let clickhouse_catalog = BackendCatalog::<ClickHouse> {
            bound: &bound,
            relations: BTreeMap::from([
                (
                    RELATION,
                    PhysicalRelation::Node {
                        relation: RELATION,
                        layout: layout("gl_user"),
                    },
                ),
                (
                    EDGE,
                    PhysicalRelation::Edge {
                        relation: EDGE,
                        layouts: vec![layout("gl_edge")],
                    },
                ),
            ]),
            access_paths: BTreeMap::from([(
                RELATION,
                vec![PhysicalScan {
                    relation: RELATION,
                    access: ClickHouseAccess::Table(TableAccess {
                        layout: layout("gl_user"),
                    }),
                    columns: BTreeSet::from([COLUMN]),
                }],
            )]),
            current_rows: BTreeMap::from([(
                RELATION,
                vec![ClickHouseCurrentRows::Final, ClickHouseCurrentRows::LimitBy],
            )]),
            facts: clickhouse_facts(),
            marker: PhantomData,
        };
        let clickhouse_candidates = CandidateSet {
            candidates: vec![(
                property_key(&clickhouse_candidate),
                clickhouse_candidate.clone(),
            )],
        };
        let clickhouse_result = PlanningResult {
            logical: logical.clone(),
            selected: SelectedPlan {
                candidate: clickhouse_candidate,
            },
        };

        let duckdb_candidate = duckdb_candidate();
        let duckdb_catalog = BackendCatalog::<DuckDb> {
            bound: &bound,
            relations: BTreeMap::from([(
                RELATION,
                PhysicalRelation::Node {
                    relation: RELATION,
                    layout: layout("gl_user"),
                },
            )]),
            access_paths: BTreeMap::from([(
                RELATION,
                vec![PhysicalScan {
                    relation: RELATION,
                    access: DuckDbAccess::Table(TableAccess {
                        layout: layout("gl_user"),
                    }),
                    columns: BTreeSet::from([COLUMN]),
                }],
            )]),
            current_rows: BTreeMap::from([(RELATION, vec![DuckDbCurrentRows])]),
            facts: DuckDbFacts,
            marker: PhantomData,
        };
        let duckdb_candidates = CandidateSet {
            candidates: vec![(property_key(&duckdb_candidate), duckdb_candidate.clone())],
        };
        let duckdb_result = PlanningResult {
            logical,
            selected: SelectedPlan {
                candidate: duckdb_candidate,
            },
        };

        assert_eq!(clickhouse_catalog.facts.foreign_keys.len(), 1);
        assert!(matches!(duckdb_catalog.facts, DuckDbFacts));
        assert_eq!(clickhouse_candidates.candidates.len(), 1);
        assert_eq!(duckdb_candidates.candidates.len(), 1);

        let planned = [
            PlannedQuery::ClickHouse(clickhouse_result),
            PlannedQuery::DuckDb(duckdb_result),
        ];
        let lowered: Vec<_> = planned
            .into_iter()
            .map(|planned| {
                let candidate = match planned {
                    PlannedQuery::ClickHouse(result) => result.selected.candidate.cost,
                    PlannedQuery::DuckDb(result) => result.selected.candidate.cost,
                };
                LoweredPlan {
                    ast: ast::Node::Query(Box::default()),
                    bindings: LoweredBindings {
                        columns: BTreeMap::from([(COLUMN, ast::Expr::col("r0", "id"))]),
                        nodes: BTreeMap::from([(
                            INPUT_NODE,
                            LoweredOutputBinding {
                                primary_key: ast::Expr::col("r0", "id"),
                            },
                        )]),
                    },
                    metadata: LoweredMetadata::default(),
                    explain: format!("scans={}", candidate.scans),
                }
            })
            .collect();

        assert_eq!(lowered.len(), 2);
        assert!(lowered.iter().all(|plan| plan.explain == "scans=1"));
        assert!(
            lowered
                .iter()
                .all(|plan| plan.bindings.columns.contains_key(&COLUMN))
        );
        assert!(
            lowered
                .iter()
                .all(|plan| plan.bindings.nodes.contains_key(&INPUT_NODE))
        );
        let backhalf: Vec<ast::Node> = lowered.into_iter().map(|plan| plan.ast).collect();
        assert!(
            backhalf
                .iter()
                .all(|node| matches!(node, ast::Node::Query(_)))
        );
    }
}
