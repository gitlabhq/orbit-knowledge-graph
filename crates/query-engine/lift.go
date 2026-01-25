package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// ParameterizedQuery holds a SQL statement with its bound parameters
type ParameterizedQuery struct {
	SQL    string
	Params map[string]any
}

// LiftAstToSql converts an AST node to parameterized SQL
func LiftAstToSql(ast Node) (*ParameterizedQuery, error) {
	params := make(map[string]any)
	switch v := ast.(type) {
	case *Query:
		sql, err := emitSQLQuery(v, params)
		if err != nil {
			return nil, err
		}
		return &ParameterizedQuery{SQL: sql, Params: params}, nil
	default:
		return nil, fmt.Errorf("unknown AST node type: %T", ast)
	}
}

// LiftExprToSql converts an expression AST node to SQL (exported for testing)
func LiftExprToSql(e Expr) (string, map[string]any, error) {
	params := make(map[string]any)
	sql, err := emitExpr(e, params)
	return sql, params, err
}

func emitSQLQuery(ast *Query, params map[string]any) (string, error) {
	var sb strings.Builder

	// SELECT clause
	sb.WriteString("SELECT ")
	for i, sel := range ast.Select {
		if i > 0 {
			sb.WriteString(", ")
		}
		exprSQL, err := emitExpr(sel.Expr, params)
		if err != nil {
			return "", err
		}
		sb.WriteString(exprSQL)
		if sel.Alias != "" {
			sb.WriteString(" AS ")
			sb.WriteString(sel.Alias)
		}
	}

	// FROM clause
	sb.WriteString(" FROM ")
	fromSQL, err := emitTableRef(ast.From, params)
	if err != nil {
		return "", err
	}
	sb.WriteString(fromSQL)

	// WHERE clause
	if ast.Where != nil {
		sb.WriteString(" WHERE ")
		whereSQL, err := emitExpr(ast.Where, params)
		if err != nil {
			return "", err
		}
		sb.WriteString(whereSQL)
	}

	// GROUP BY clause
	if len(ast.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, g := range ast.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			groupSQL, err := emitExpr(g, params)
			if err != nil {
				return "", err
			}
			sb.WriteString(groupSQL)
		}
	}

	// ORDER BY clause
	if len(ast.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, o := range ast.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			orderSQL, err := emitExpr(o.Expr, params)
			if err != nil {
				return "", err
			}
			sb.WriteString(orderSQL)
			if o.Desc {
				sb.WriteString(" DESC")
			} else {
				sb.WriteString(" ASC")
			}
		}
	}

	// LIMIT clause
	if ast.Limit > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(ast.Limit))
	}

	return sb.String(), nil
}

// emitExpr converts an Expr AST node to SQL (internal implementation)
func emitExpr(e Expr, params map[string]any) (string, error) {
	if e == nil {
		return "", fmt.Errorf("nil expression")
	}

	switch v := e.(type) {
	case *ColumnRef:
		// Note: v.Table is the alias (e.g., "n"), not validated against ontology
		// Alias validity is ensured by the lowerer
		return fmt.Sprintf("%s.%s", v.Table, v.Column), nil

	case *Literal:
		return emitLiteral(v.Value, params)

	case *FuncCall:
		var sb strings.Builder
		sb.WriteString(v.Name)
		sb.WriteString("(")
		for i, arg := range v.Args {
			if i > 0 {
				sb.WriteString(", ")
			}
			argSQL, err := emitExpr(arg, params)
			if err != nil {
				return "", err
			}
			sb.WriteString(argSQL)
		}
		sb.WriteString(")")
		return sb.String(), nil

	case *BinaryOp:
		leftSQL, err := emitExpr(v.Left, params)
		if err != nil {
			return "", err
		}
		rightSQL, err := emitExpr(v.Right, params)
		if err != nil {
			return "", err
		}

		// Handle special cases like IN operator
		if v.Op == OpIn {
			return fmt.Sprintf("%s IN %s", leftSQL, rightSQL), nil
		}

		return fmt.Sprintf("(%s %s %s)", leftSQL, v.Op, rightSQL), nil

	case *UnaryOp:
		exprSQL, err := emitExpr(v.Expr, params)
		if err != nil {
			return "", err
		}

		// Handle postfix operators
		if v.Op == OpIsNull || v.Op == OpIsNotNull {
			return fmt.Sprintf("(%s %s)", exprSQL, v.Op), nil
		}

		// Handle prefix operators
		return fmt.Sprintf("(%s %s)", v.Op, exprSQL), nil

	default:
		return "", fmt.Errorf("unknown expression type: %T", e)
	}
}

// emitLiteral converts a literal value to a ClickHouse native placeholder {name:Type}
func emitLiteral(v any, params map[string]any) (string, error) {
	if v == nil {
		return "NULL", nil
	}

	// Array literal for IN clause - store each item separately
	if arr, ok := v.([]any); ok {
		placeholders := make([]string, len(arr))
		for i, item := range arr {
			name := fmt.Sprintf("p%d", len(params))
			params[name] = item
			placeholders[i] = fmt.Sprintf("{%s:%s}", name, chTypeOf(item))
		}
		return "(" + strings.Join(placeholders, ", ") + ")", nil
	}

	// Normal scalar type
	name := fmt.Sprintf("p%d", len(params))
	params[name] = v
	return fmt.Sprintf("{%s:%s}", name, chTypeOf(v)), nil
}

func chTypeOf(v any) string {
	switch v.(type) {
	case string:
		return "String"
	case int, int64:
		return "Int64"
	case int32:
		return "Int32"
	case float32:
		return "Float32"
	case float64:
		return "Float64"
	case bool:
		return "Bool"
	default:
		return "String"
	}
}

// emitTableRef converts a TableRef AST node to SQL
func emitTableRef(t TableRef, params map[string]any) (string, error) {
	switch v := t.(type) {
	case *TableScan:
		var sb strings.Builder
		sb.WriteString(v.Table)
		sb.WriteString(" AS ")
		sb.WriteString(v.Alias)
		return sb.String(), nil

	case *Join:
		var sb strings.Builder

		// Left side
		leftSQL, err := emitTableRef(v.Left, params)
		if err != nil {
			return "", err
		}
		sb.WriteString(leftSQL)

		// Join type and right side
		sb.WriteString(" ")
		sb.WriteString(string(v.Type))
		sb.WriteString(" JOIN ")
		rightSQL, err := emitTableRef(v.Right, params)
		if err != nil {
			return "", err
		}
		sb.WriteString(rightSQL)

		// ON clause
		sb.WriteString(" ON ")
		onSQL, err := emitExpr(v.On, params)
		if err != nil {
			return "", err
		}
		sb.WriteString(onSQL)

		return sb.String(), nil

	default:
		return "", fmt.Errorf("unknown table reference type: %T", t)
	}
}
