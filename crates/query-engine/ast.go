package engine

// =============================================================================
// AST - SQL-oriented Abstract Syntax Tree
//
// This is the intermediate representation between the LLM's JSON input
// and the final SQL output. Nodes map directly to SQL constructs.
// =============================================================================

// Node is the base interface for all AST nodes
type Node interface {
	node()
}

// Expr represents an expression that produces a value
type Expr interface {
	Node
	expr()
}

// TableRef represents a table reference (scan or join result)
type TableRef interface {
	Node
	tableRef()
}

// =============================================================================
// Expressions
// =============================================================================

// ColumnRef references a column: table.column
type ColumnRef struct {
	Table  string
	Column string
}

func (ColumnRef) node() {}
func (ColumnRef) expr() {}

// Literal is a constant value
type Literal struct {
	Value any // int, string, bool, nil, []any
}

func (Literal) node() {}
func (Literal) expr() {}

// FuncCall is a function call: COUNT(x), SUM(x), etc.
type FuncCall struct {
	Name string
	Args []Expr
}

func (FuncCall) node() {}
func (FuncCall) expr() {}

// BinaryOp is a binary operation: x = y, x AND y, etc.
type BinaryOp struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (BinaryOp) node() {}
func (BinaryOp) expr() {}

// UnaryOp is a unary operation: NOT x, x IS NULL, etc.
type UnaryOp struct {
	Op   Op
	Expr Expr
}

func (UnaryOp) node() {}
func (UnaryOp) expr() {}

// Op represents an operator
type Op string

const (
	// Comparison
	OpEq    Op = "="
	OpNe    Op = "!="
	OpLt    Op = "<"
	OpLe    Op = "<="
	OpGt    Op = ">"
	OpGe    Op = ">="
	OpIn    Op = "IN"
	OpLike  Op = "LIKE"
	OpILike Op = "ILIKE"

	// Logical
	OpAnd Op = "AND"
	OpOr  Op = "OR"
	OpNot Op = "NOT"

	// Null checks
	OpIsNull    Op = "IS NULL"
	OpIsNotNull Op = "IS NOT NULL"

	// Arithmetic (for recursive depth tracking)
	OpAdd Op = "+"
)

// =============================================================================
// Table References
// =============================================================================

// TableScan reads from a physical table
type TableScan struct {
	Table      string // physical table: "nodes" or "edges"
	Alias      string // alias for column references
	TypeFilter string // label filter (node type or relationship type)
}

func (TableScan) node()     {}
func (TableScan) tableRef() {}

// Join combines two table references
type Join struct {
	Type  JoinType
	Left  TableRef
	Right TableRef
	On    Expr
}

func (Join) node()     {}
func (Join) tableRef() {}

type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
	FullJoin  JoinType = "FULL"
	SelfJoin  JoinType = "SELF"
)

// =============================================================================
// Query
// =============================================================================

// Query represents a complete SQL query
type Query struct {
	Select  []SelectExpr
	From    TableRef
	Where   Expr // nil if no filter
	GroupBy []Expr
	OrderBy []OrderExpr
	Limit   int
}

func (Query) node() {}

// SelectExpr is an expression with an optional alias
type SelectExpr struct {
	Expr  Expr
	Alias string
}

// OrderExpr specifies sort order
type OrderExpr struct {
	Expr Expr
	Desc bool
}

// RecursiveCTE is a recursive common table expression (for path finding)
type RecursiveCTE struct {
	Name      string
	Base      *Query
	Recursive *Query
	MaxDepth  int
	Final     *Query
}

func (RecursiveCTE) node() {}

// =============================================================================
// Builder Helpers
// =============================================================================

func Col(table, column string) *ColumnRef {
	return &ColumnRef{Table: table, Column: column}
}

func Lit(v any) *Literal {
	return &Literal{Value: v}
}

func Func(name string, args ...Expr) *FuncCall {
	return &FuncCall{Name: name, Args: args}
}

func Eq(left, right Expr) *BinaryOp {
	return &BinaryOp{Op: OpEq, Left: left, Right: right}
}

func And(exprs ...Expr) Expr {
	var result Expr
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if result == nil {
			result = e
		} else {
			result = &BinaryOp{Op: OpAnd, Left: result, Right: e}
		}
	}
	return result
}

func Or(exprs ...Expr) Expr {
	var result Expr
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if result == nil {
			result = e
		} else {
			result = &BinaryOp{Op: OpOr, Left: result, Right: e}
		}
	}
	return result
}

func Table(table, alias, typeFilter string) *TableScan {
	return &TableScan{Table: table, Alias: alias, TypeFilter: typeFilter}
}

func Join_(jtype JoinType, left, right TableRef, on Expr) *Join {
	return &Join{Type: jtype, Left: left, Right: right, On: on}
}
