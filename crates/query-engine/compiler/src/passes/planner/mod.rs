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
) -> Result<(BoundCatalog, Candidate<ClickHouse>, String)> {
    let (bound, logical) = bind(input, ontology)?;
    let (bound, logical) = optimize(bound, logical);
    let selected = plan_clickhouse(&bound, logical)?.selected;
    let explain = explain_clickhouse(&bound, &selected.candidate.plan);
    Ok((bound, selected.candidate, explain))
}

pub fn duckdb(
    input: Input,
    ontology: Arc<Ontology>,
) -> Result<(BoundCatalog, Candidate<DuckDb>, String)> {
    let (bound, logical) = bind(input, ontology)?;
    let (bound, logical) = optimize(bound, logical);
    let selected = plan_duckdb(&bound, logical)?.selected;
    let explain = explain_duckdb(&bound, &selected.candidate.plan);
    Ok((bound, selected.candidate, explain))
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
        value: Value,
    },
    TokenMatch {
        value: Box<Self>,
        token: Value,
    },
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

    fn visit(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        self.inputs.iter().for_each(|input| input.visit(visitor));
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
        Expr::ListContains { list, value } => Expr::ListContains {
            list: Box::new(rewrite(*list, map)),
            value,
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
    pub edge_column: PhysicalColumn,
    pub token: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextIndexAccess {
    pub column: ColumnId,
    pub tokenizer: Tokenizer,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub root: Plan<Logical>,
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
                edge_column: PhysicalColumn("source_tags".into()),
                token: Value::String("state:opened".into()),
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
