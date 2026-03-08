#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        table: String,
        column: String,
    },
    Raw(String),
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    BinaryOp {
        op: Op,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Param {
        name: String,
        data_type: String,
    },
    FuncCall {
        name: String,
        args: Vec<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    IsNotNull,
}

impl UnaryOp {
    pub fn as_sql(&self) -> &'static str {
        match self {
            UnaryOp::IsNotNull => "IS NOT NULL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Le,
    Gt,
    And,
    Or,
}

impl Op {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::And => "AND",
            Op::Or => "OR",
        }
    }
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

    pub fn bare(expr: Expr) -> Self {
        Self { expr, alias: None }
    }
}

/// Always ascending — cursor pagination is ASC-only.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderExpr {
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Scan {
        table: String,
        alias: Option<String>,
    },
    /// Verbatim FROM clause for complex JOINs from ontology YAML.
    Raw(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderExpr>,
    pub limit: Option<u64>,
}

impl Expr {
    pub fn col(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn raw(sql: impl Into<String>) -> Self {
        Expr::Raw(sql.into())
    }

    pub fn param(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Expr::Param {
            name: name.into(),
            data_type: data_type.into(),
        }
    }

    pub fn func(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::FuncCall {
            name: name.into(),
            args,
        }
    }

    pub fn cast(expr: Expr, data_type: impl Into<String>) -> Self {
        Expr::Cast {
            expr: Box::new(expr),
            data_type: data_type.into(),
        }
    }

    pub fn is_not_null(expr: Expr) -> Self {
        Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            operand: Box::new(expr),
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

    pub fn and_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::And, a, b))
    }

    pub fn or_all(exprs: impl IntoIterator<Item = Option<Expr>>) -> Option<Expr> {
        exprs
            .into_iter()
            .flatten()
            .reduce(|a, b| Expr::binary(Op::Or, a, b))
    }
}

impl TableRef {
    pub fn scan(table: impl Into<String>, alias: Option<String>) -> Self {
        TableRef::Scan {
            table: table.into(),
            alias,
        }
    }
}
