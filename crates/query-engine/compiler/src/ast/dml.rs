//! SQL-oriented Abstract Syntax Tree
//!
//! Intermediate representation between JSON input and SQL output.
//! Each node maps directly to ClickHouse SQL constructs.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

pub use orbit_utils::query_types::{ScalarType, SqlType, TimeZone};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        table: String,
        column: String,
    },
    /// Bare SQL identifier, used for lambda parameters.
    Identifier(String),
    /// Constant value, type inferred from Value.
    Literal(Value),
    Param {
        data_type: SqlType,
        value: Value,
    },
    FuncCall {
        name: Function,
        args: Vec<Expr>,
    },
    EmptyTupleArray(Vec<SqlType>),
    Aggregate {
        function: crate::input::AggFunction,
        argument: Option<Box<Expr>>,
        distinct: bool,
        condition: Option<Box<Expr>>,
    },
    TimeBucket {
        unit: crate::input::TruncateUnit,
        value: Box<Expr>,
    },
    TokenSearch {
        mode: TokenMatchMode,
        value: Box<Expr>,
        query: Box<Expr>,
    },
    Lambda {
        param: String,
        body: Box<Expr>,
    },
    BinaryOp {
        op: Op,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: Op,
        expr: Box<Expr>,
    },
    /// Used for SIP pre-filtering: materialize IDs in a CTE, then filter
    /// multiple tables against the same set.
    InSubquery {
        expr: Box<Expr>,
        cte_name: String,
        column: String,
    },
    /// Like InSubquery but embeds the query directly instead of referencing
    /// a CTE. Used for narrowing when CTE references would trigger
    /// ClickHouse's correlated subquery rejection in parameterized mode.
    InSelect {
        expr: Box<Expr>,
        query: Box<Query>,
    },
    /// Single-row, single-column subquery used as a value; ClickHouse folds it
    /// to a constant before index analysis, so it still drives PK pruning.
    Scalar(Box<Query>),
    Star,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum TokenMatchMode {
    Single,
    All,
    Any,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum Function {
    StartsWith,
    EndsWith,
    Contains,
    Lower,
    ToString,
    ToJson,
    Object,
    If,
    Coalesce,
    ByteLength,
    Substring,
    Concat,
    CountSubstrings,
    Array,
    Tuple,
    ArrayConcat,
    ArrayReverse,
    ArrayResize,
    ArrayContains,
    ArrayContainsAny,
    ArrayContainsAll,
    ArrayFilter,
    ArrayMap,
    ArrayExists,
    Unnest,
    TupleElement,
    ArgMax,
    ArgMaxOrNull,
}

impl Function {
    pub fn accepts_arity(self, count: usize) -> bool {
        match self {
            Self::Array | Self::Tuple => true,
            Self::Object => count.is_multiple_of(2),
            Self::Coalesce | Self::Concat | Self::ArrayConcat => count >= 1,
            Self::Lower
            | Self::ToString
            | Self::ToJson
            | Self::ByteLength
            | Self::ArrayReverse
            | Self::Unnest => count == 1,
            Self::If | Self::Substring => count == 3,
            _ => count == 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum Op {
    #[strum(serialize = "=")]
    Eq,
    #[strum(serialize = "!=")]
    Ne,
    #[strum(serialize = "<")]
    Lt,
    #[strum(serialize = "<=")]
    Le,
    #[strum(serialize = ">")]
    Gt,
    #[strum(serialize = ">=")]
    Ge,
    #[strum(serialize = "IN")]
    In,
    #[strum(serialize = "LIKE")]
    Like,
    #[strum(serialize = "AND")]
    And,
    #[strum(serialize = "OR")]
    Or,
    #[strum(serialize = "NOT")]
    Not,
    #[strum(serialize = "IS NULL")]
    IsNull,
    #[strum(serialize = "IS NOT NULL")]
    IsNotNull,
    #[strum(serialize = "+")]
    Add,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Scan {
        table: String,
        alias: String,
        final_: bool,
        relationship: Option<usize>,
    },
    Join {
        join_type: JoinType,
        left: Box<TableRef>,
        right: Box<TableRef>,
        on: Expr,
    },
    /// Used for multi-hop traversals with unrolled joins.
    Union { queries: Vec<Query>, alias: String },
    /// Used internally for deduplication of aggregation queries.
    Subquery { query: Box<Query>, alias: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum JoinType {
    Inner,
    Left,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl SelectExpr {
    pub fn new(expr: Expr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: Some(alias.into()),
        }
    }

    pub fn col(alias: impl Into<String>, col: impl Into<String>) -> Self {
        let col = col.into();
        Self::new(Expr::col(alias, &col), col)
    }

    pub fn star() -> Self {
        Self {
            expr: Expr::Star,
            alias: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderExpr {
    pub expr: Expr,
    pub desc: bool,
}

impl OrderExpr {
    pub fn asc(expr: Expr) -> Self {
        Self { expr, desc: false }
    }

    pub fn desc(expr: Expr) -> Self {
        Self { expr, desc: true }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub query: Box<Query>,
    pub recursive: bool,
    /// When true, emit `name AS MATERIALIZED (...)` so ClickHouse evaluates
    /// the CTE body once and caches the result. Without this, ClickHouse
    /// inlines non-recursive CTEs at every reference site, re-executing the
    /// scan for each `IN (SELECT ... FROM cte)`.
    pub materialized: bool,
}

impl Cte {
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            query: Box::new(query),
            recursive: false,
            materialized: false,
        }
    }
}

/// Complete SQL query:
/// ```sql
/// WITH cte1 AS (...), cte2 AS (...)
/// SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...
/// UNION ALL SELECT ...
/// SETTINGS key = value
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub ctes: Vec<Cte>,
    pub distinct: bool,
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderExpr>,
    /// `LIMIT n BY col1, col2` — ClickHouse per-group limit (applied after ORDER BY).
    pub limit_by: Option<(u32, Vec<Expr>)>,
    pub limit: Option<u32>,
    /// UNION ALL with this query, used for recursive CTEs.
    pub union_all: Vec<Query>,
}

impl Query {
    pub fn selects_alias(&self, alias: &str) -> bool {
        self.select
            .iter()
            .any(|s| s.alias.as_deref() == Some(alias))
    }
}

impl Default for Query {
    fn default() -> Self {
        Self {
            ctes: vec![],
            distinct: false,
            select: vec![],
            from: TableRef::Scan {
                table: String::new(),
                alias: String::new(),
                final_: false,
                relationship: None,
            },
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit_by: None,
            limit: None,
            union_all: vec![],
        }
    }
}

static SAFE_IDENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid regex"));

/// Table and column names are interpolated as raw identifiers (not parameterized),
/// so they are validated at construction time via [`Insert::new`]. Fields are
/// private to enforce this — use the constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    table: String,
    columns: Vec<String>,
    values: Vec<Vec<Expr>>,
}

impl Insert {
    pub fn new(table: impl Into<String>, columns: Vec<String>, values: Vec<Vec<Expr>>) -> Self {
        let table = table.into();
        debug_assert!(
            SAFE_IDENT.is_match(&table),
            "INSERT table name is not a safe identifier: {table:?}"
        );
        for col in &columns {
            debug_assert!(
                SAFE_IDENT.is_match(col),
                "INSERT column name is not a safe identifier: {col:?}"
            );
        }
        Self {
            table,
            columns,
            values,
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn values(&self) -> &[Vec<Expr>] {
        &self.values
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Query(Box<Query>),
    Insert(Box<Insert>),
}

impl Expr {
    pub fn col(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn ident(name: impl Into<String>) -> Self {
        Expr::Identifier(name.into())
    }

    pub fn lit(value: impl Into<Value>) -> Self {
        Expr::Literal(value.into())
    }

    pub fn param(data_type: SqlType, value: impl Into<Value>) -> Self {
        Expr::Param {
            data_type,
            value: value.into(),
        }
    }

    pub fn string(value: impl Into<String>) -> Self {
        Expr::Param {
            data_type: SqlType::String,
            value: Value::String(value.into()),
        }
    }

    pub fn int(value: i64) -> Self {
        Expr::Param {
            data_type: SqlType::Int64,
            value: Value::Number(value.into()),
        }
    }

    pub fn uint32(value: u32) -> Self {
        Expr::Param {
            data_type: SqlType::UInt32,
            value: Value::Number(value.into()),
        }
    }

    pub fn func(name: Function, args: Vec<Expr>) -> Self {
        Expr::FuncCall { name, args }
    }

    pub fn aggregate(function: crate::input::AggFunction, argument: Option<Expr>) -> Self {
        Self::Aggregate {
            function,
            argument: argument.map(Box::new),
            distinct: false,
            condition: None,
        }
    }

    pub fn lambda(param: impl Into<String>, body: Expr) -> Self {
        Expr::Lambda {
            param: param.into(),
            body: Box::new(body),
        }
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            op: Op::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn binary(op: Op, left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn unary(op: Op, expr: Expr) -> Self {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

    /// Combine expressions with AND, ignoring None values.
    pub fn and_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::And, a, b))
    }

    /// Combine expressions with OR, ignoring None values.
    pub fn or_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::Or, a, b))
    }

    /// Match a column against a set of values.
    /// 0 values → None, 1 value → Eq, N values → IN.
    pub fn col_in(
        table: impl Into<String>,
        column: impl Into<String>,
        data_type: SqlType,
        values: Vec<Value>,
    ) -> Option<Self> {
        match values.len() {
            0 => None,
            1 => Some(Expr::eq(
                Expr::col(table, column),
                Expr::Param {
                    data_type,
                    value: values.into_iter().next().unwrap(),
                },
            )),
            _ => Some(Expr::binary(
                Op::In,
                Expr::col(table, column),
                Expr::Param {
                    data_type: data_type.to_array(),
                    value: Value::Array(values),
                },
            )),
        }
    }

    /// Combine two expressions with AND.
    pub fn and(left: Expr, right: Expr) -> Expr {
        Expr::binary(Op::And, left, right)
    }

    /// Rebuild an AND chain from conjuncts. Returns None if empty.
    pub fn conjoin(exprs: Vec<Expr>) -> Option<Expr> {
        exprs.into_iter().reduce(Expr::and)
    }
}

impl TableRef {
    pub fn with_relationship(mut self, index: usize) -> Self {
        self.set_relationship(index);
        self
    }

    fn set_relationship(&mut self, index: usize) {
        match self {
            Self::Scan { relationship, .. } => *relationship = Some(index),
            Self::Subquery { query, .. } => query.from.set_relationship(index),
            Self::Union { queries, .. } => {
                for query in queries {
                    query.from.set_relationship(index);
                }
            }
            Self::Join { left, right, .. } => {
                left.set_relationship(index);
                right.set_relationship(index);
            }
        }
    }

    pub fn scan(table: impl Into<String>, alias: impl Into<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            final_: false,
            relationship: None,
        }
    }

    pub fn scan_final(table: impl Into<String>, alias: impl Into<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            final_: true,
            relationship: None,
        }
    }

    pub fn join(join_type: JoinType, left: TableRef, right: TableRef, on: Expr) -> Self {
        TableRef::Join {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            on,
        }
    }

    pub fn union_all(queries: Vec<Query>, alias: impl Into<String>) -> Self {
        TableRef::Union {
            queries,
            alias: alias.into(),
        }
    }

    pub fn subquery(query: Query, alias: impl Into<String>) -> Self {
        TableRef::Subquery {
            query: Box::new(query),
            alias: alias.into(),
        }
    }
}
