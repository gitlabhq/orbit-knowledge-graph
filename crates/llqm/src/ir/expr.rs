//! Expression DSL for building query plans.
//!
//! Expressions use named column references (`col("u", "id")`) that get resolved
//! to positional Substrait field references during plan building.

use std::fmt;

/// Column data type, mapping to Substrait types and ClickHouse parameter types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Int64,
    Float64,
    Bool,
    /// `Array(T)` — ClickHouse array type for array-typed parameters.
    Array(Box<DataType>),
    /// DateTime type for temporal columns.
    DateTime,
}

impl DataType {
    pub fn array(inner: DataType) -> Self {
        DataType::Array(Box::new(inner))
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::Int64 => write!(f, "Int64"),
            Self::Float64 => write!(f, "Float64"),
            Self::Bool => write!(f, "Bool"),
            Self::Array(inner) => write!(f, "Array({inner})"),
            Self::DateTime => write!(f, "DateTime"),
        }
    }
}

/// Expression tree for building query plans.
///
/// Uses named column references that get resolved to positional Substrait
/// field references during plan building.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Qualified column reference: `table_alias.column_name`
    Column { table: String, name: String },
    /// Literal value (becomes an auto-numbered ClickHouse parameter)
    Literal(LiteralValue),
    /// Named parameter (bound at runtime, e.g. `{watermark:String}`)
    Param { name: String, data_type: DataType },
    /// Binary operation
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOp, operand: Box<Expr> },
    /// Function call: `name(args...)`
    FuncCall { name: String, args: Vec<Expr> },
    /// `CAST(expr AS type)`
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },
    /// `CASE WHEN c1 THEN v1 ... ELSE vN END`
    IfThen {
        ifs: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    /// `expr IN (v1, v2, ...)`
    InList { expr: Box<Expr>, list: Vec<Expr> },
    /// Verbatim SQL fragment — escape hatch for migration from raw SQL.
    ///
    /// Stored as a `__raw_sql` extension function in the Substrait plan.
    /// Only usable with ClickHouse codegen, not DataFusion.
    Raw(String),
}

/// Literal value types.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Null,
}

impl LiteralValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            LiteralValue::String(s) => Some(s),
            _ => None,
        }
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Like,
    ILike,
    In,
}

impl BinaryOp {
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Add => "+",
            Self::Like => "LIKE",
            Self::ILike => "ILIKE",
            Self::In => "IN",
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
}

impl UnaryOp {
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Not => "NOT",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        }
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// ---------------------------------------------------------------------------
// Chainable methods
// ---------------------------------------------------------------------------

macro_rules! binop {
    ($(#[$attr:meta])* $name:ident, $op:expr) => {
        $(#[$attr])*
        pub fn $name(self, right: Expr) -> Expr {
            Expr::BinaryOp { op: $op, left: Box::new(self), right: Box::new(right) }
        }
    };
}

macro_rules! unop {
    ($(#[$attr:meta])* $name:ident, $op:expr) => {
        $(#[$attr])*
        pub fn $name(self) -> Expr {
            Expr::UnaryOp { op: $op, operand: Box::new(self) }
        }
    };
}

impl Expr {
    binop!(eq, BinaryOp::Eq);
    binop!(ne, BinaryOp::Ne);
    binop!(lt, BinaryOp::Lt);
    binop!(le, BinaryOp::Le);
    binop!(gt, BinaryOp::Gt);
    binop!(ge, BinaryOp::Ge);
    binop!(and, BinaryOp::And);
    binop!(or, BinaryOp::Or);
    binop!(
        #[allow(clippy::should_implement_trait)]
        add,
        BinaryOp::Add
    );
    binop!(like, BinaryOp::Like);
    binop!(ilike, BinaryOp::ILike);
    binop!(is_in, BinaryOp::In);

    unop!(
        #[allow(clippy::should_implement_trait)]
        not,
        UnaryOp::Not
    );
    unop!(is_null, UnaryOp::IsNull);
    unop!(is_not_null, UnaryOp::IsNotNull);

    /// `self IN (v1, v2, ...)` (expanded list).
    pub fn in_list(self, list: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(self),
            list,
        }
    }

    pub fn cast(self, target_type: DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(self),
            target_type,
        }
    }

    /// `startsWith(self, prefix)`
    pub fn starts_with(self, prefix: Expr) -> Expr {
        Expr::FuncCall {
            name: "startsWith".into(),
            args: vec![self, prefix],
        }
    }
}

// ---------------------------------------------------------------------------
// Builder functions (free-standing — kept for multi-arg folding and backward compat)
// ---------------------------------------------------------------------------

/// Qualified column reference.
pub fn col(table: &str, name: &str) -> Expr {
    Expr::Column {
        table: table.into(),
        name: name.into(),
    }
}

/// String literal.
pub fn string(val: &str) -> Expr {
    Expr::Literal(LiteralValue::String(val.into()))
}

/// 64-bit integer literal.
pub fn int(val: i64) -> Expr {
    Expr::Literal(LiteralValue::Int64(val))
}

/// 64-bit float literal.
pub fn float(val: f64) -> Expr {
    Expr::Literal(LiteralValue::Float64(val))
}

/// Boolean literal.
pub fn boolean(val: bool) -> Expr {
    Expr::Literal(LiteralValue::Bool(val))
}

/// NULL literal.
pub fn null() -> Expr {
    Expr::Literal(LiteralValue::Null)
}

/// Named parameter bound at runtime (e.g. `{watermark:String}`).
pub fn param(name: &str, data_type: DataType) -> Expr {
    Expr::Param {
        name: name.into(),
        data_type,
    }
}

/// Fold expressions with `AND`. Requires at least one expression.
pub fn and(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    let mut iter = exprs.into_iter();
    let first = iter.next().expect("and() requires at least one expression");
    iter.fold(first, |acc, e| acc.and(e))
}

/// Fold expressions with `OR`. Requires at least one expression.
pub fn or(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    let mut iter = exprs.into_iter();
    let first = iter.next().expect("or() requires at least one expression");
    iter.fold(first, |acc, e| acc.or(e))
}

/// Generic function call.
pub fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        name: name.into(),
        args,
    }
}

/// `CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE vN] END`
pub fn if_then(ifs: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    Expr::IfThen {
        ifs,
        else_expr: else_expr.map(Box::new),
    }
}

/// Verbatim SQL fragment — escape hatch for migration from raw SQL.
///
/// Only usable with ClickHouse codegen, not DataFusion.
pub fn raw(sql: &str) -> Expr {
    Expr::Raw(sql.into())
}

/// Fold expressions with `AND`, returning `None` if empty.
pub fn and_opt(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
    exprs.into_iter().flatten().reduce(|acc, e| acc.and(e))
}

/// Fold expressions with `OR`, returning `None` if empty.
pub fn or_opt(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
    exprs.into_iter().flatten().reduce(|acc, e| acc.or(e))
}
