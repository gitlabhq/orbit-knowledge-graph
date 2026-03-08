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

impl Expr {
    // -- Comparison --

    pub fn eq(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn ne(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Ne,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn lt(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Lt,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn le(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Le,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn gt(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn ge(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Ge,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    // -- Logical --

    pub fn and(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn or(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(self),
        }
    }

    // -- Null checks --

    pub fn is_null(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOp::IsNull,
            operand: Box::new(self),
        }
    }

    pub fn is_not_null(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            operand: Box::new(self),
        }
    }

    // -- Pattern matching --

    pub fn like(self, pattern: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Like,
            left: Box::new(self),
            right: Box::new(pattern),
        }
    }

    pub fn ilike(self, pattern: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::ILike,
            left: Box::new(self),
            right: Box::new(pattern),
        }
    }

    // -- Set membership --

    /// `self IN set_expr` (binary IN, e.g. against an array parameter).
    pub fn is_in(self, set: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::In,
            left: Box::new(self),
            right: Box::new(set),
        }
    }

    /// `self IN (v1, v2, ...)` (expanded list).
    pub fn in_list(self, list: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(self),
            list,
        }
    }

    // -- Arithmetic --

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    // -- Type conversion --

    pub fn cast(self, target_type: DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(self),
            target_type,
        }
    }

    // -- Convenience --

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

pub fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn ne(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Ne,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn lt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Lt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn le(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Le,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn gt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn ge(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Ge,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Fold expressions with `AND`. Requires at least one expression.
pub fn and(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    let mut iter = exprs.into_iter();
    let first = iter.next().expect("and() requires at least one expression");
    iter.fold(first, |acc, e| Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(acc),
        right: Box::new(e),
    })
}

/// Fold expressions with `OR`. Requires at least one expression.
pub fn or(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    let mut iter = exprs.into_iter();
    let first = iter.next().expect("or() requires at least one expression");
    iter.fold(first, |acc, e| Expr::BinaryOp {
        op: BinaryOp::Or,
        left: Box::new(acc),
        right: Box::new(e),
    })
}

pub fn not(expr: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOp::Not,
        operand: Box::new(expr),
    }
}

pub fn is_null(expr: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOp::IsNull,
        operand: Box::new(expr),
    }
}

pub fn is_not_null(expr: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOp::IsNotNull,
        operand: Box::new(expr),
    }
}

pub fn add(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Add,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Generic function call.
pub fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        name: name.into(),
        args,
    }
}

pub fn cast(expr: Expr, target_type: DataType) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        target_type,
    }
}

/// `CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE vN] END`
pub fn if_then(ifs: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    Expr::IfThen {
        ifs,
        else_expr: else_expr.map(Box::new),
    }
}

/// `expr IN (v1, v2, ...)`
pub fn in_list(expr: Expr, list: Vec<Expr>) -> Expr {
    Expr::InList {
        expr: Box::new(expr),
        list,
    }
}

/// Convenience: `startsWith(expr, prefix)`
pub fn starts_with(expr: Expr, prefix: Expr) -> Expr {
    func("startsWith", vec![expr, prefix])
}

pub fn like(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Like,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn ilike(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::ILike,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn is_in(expr: Expr, set: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::In,
        left: Box::new(expr),
        right: Box::new(set),
    }
}

/// Verbatim SQL fragment — escape hatch for migration from raw SQL.
///
/// Only usable with ClickHouse codegen, not DataFusion.
pub fn raw(sql: &str) -> Expr {
    Expr::Raw(sql.into())
}

/// Fold expressions with `AND`, returning `None` if empty.
/// Filters out `None` items.
pub fn and_opt(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
    exprs.into_iter().flatten().reduce(|acc, e| Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(acc),
        right: Box::new(e),
    })
}

/// Fold expressions with `OR`, returning `None` if empty.
/// Filters out `None` items.
pub fn or_opt(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
    exprs.into_iter().flatten().reduce(|acc, e| Expr::BinaryOp {
        op: BinaryOp::Or,
        left: Box::new(acc),
        right: Box::new(e),
    })
}
