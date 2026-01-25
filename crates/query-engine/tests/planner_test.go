package engine_test

import (
	"strings"
	"testing"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine"
)

// Command: go test ./internal/engine/tests/planner_test.go -v

func TestEmitSQLQuery(t *testing.T) {
	tests := []struct {
		name           string
		query          *engine.Query
		expectedSQL    string
		expectedParams map[string]any
	}{
		{
			name: "simple select with filter",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id"), Alias: "node_id"},
					{Expr: engine.Col("n", "label"), Alias: "node_type"},
				},
				From: engine.Table("nodes", "n", ""),
				Where: engine.Eq(
					engine.Col("n", "label"),
					engine.Lit("User"),
				),
				Limit: 10,
			},
			expectedSQL:    "SELECT n.id AS node_id, n.label AS node_type FROM nodes AS n WHERE (n.label = {p0:String}) LIMIT 10",
			expectedParams: map[string]any{"p0": "User"},
		},
		{
			name: "query with join",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id"), Alias: "node_id"},
					{Expr: engine.Col("e", "label"), Alias: "rel_type"},
				},
				From: engine.Join_(engine.InnerJoin,
					engine.Table("nodes", "n", ""),
					engine.Table("edges", "e", ""),
					engine.Eq(engine.Col("n", "id"), engine.Col("e", "source_id")),
				),
			},
			expectedSQL:    "SELECT n.id AS node_id, e.label AS rel_type FROM nodes AS n INNER JOIN edges AS e ON (n.id = e.source_id)",
			expectedParams: map[string]any{},
		},
		{
			name: "query with aggregation and group by",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "label"), Alias: "type"},
					{Expr: engine.Func("COUNT", engine.Col("n", "id")), Alias: "count"},
				},
				From: engine.Table("nodes", "n", ""),
				GroupBy: []engine.Expr{
					engine.Col("n", "label"),
				},
				OrderBy: []engine.OrderExpr{
					{Expr: engine.Func("COUNT", engine.Col("n", "id")), Desc: true},
				},
			},
			expectedSQL:    "SELECT n.label AS type, COUNT(n.id) AS count FROM nodes AS n GROUP BY n.label ORDER BY COUNT(n.id) DESC",
			expectedParams: map[string]any{},
		},
		{
			name: "query with IN operator",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id")},
				},
				From: engine.Table("nodes", "n", ""),
				Where: &engine.BinaryOp{
					Op:    engine.OpIn,
					Left:  engine.Col("n", "label"),
					Right: engine.Lit([]any{"User", "Project", "Group"}),
				},
			},
			expectedSQL:    "SELECT n.id FROM nodes AS n WHERE n.label IN ({p0:String}, {p1:String}, {p2:String})",
			expectedParams: map[string]any{"p0": "User", "p1": "Project", "p2": "Group"},
		},
		{
			name: "query with AND/OR conditions",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id")},
				},
				From: engine.Table("nodes", "n", ""),
				Where: engine.And(
					engine.Eq(engine.Col("n", "label"), engine.Lit("User")),
					engine.Or(
						&engine.BinaryOp{Op: engine.OpGt, Left: engine.Col("n", "created_at"), Right: engine.Lit("2024-01-01")},
						&engine.UnaryOp{Op: engine.OpIsNull, Expr: engine.Col("n", "deleted_at")},
					),
				),
			},
			expectedSQL:    "SELECT n.id FROM nodes AS n WHERE ((n.label = {p0:String}) AND ((n.created_at > {p1:String}) OR (n.deleted_at IS NULL)))",
			expectedParams: map[string]any{"p0": "User", "p1": "2024-01-01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.LiftAstToSql(tt.query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.SQL != tt.expectedSQL {
				t.Errorf("SQL mismatch:\nexpected: %s\ngot:      %s", tt.expectedSQL, result.SQL)
			}
			if len(result.Params) != len(tt.expectedParams) {
				t.Errorf("params count mismatch: expected %d, got %d", len(tt.expectedParams), len(result.Params))
			}
			for k, v := range tt.expectedParams {
				if result.Params[k] != v {
					t.Errorf("param[%s] mismatch: expected %v, got %v", k, v, result.Params[k])
				}
			}
		})
	}
}

func TestEmitExpr(t *testing.T) {
	tests := []struct {
		name           string
		expr           engine.Expr
		expectedSQL    string
		expectedParams map[string]any
	}{
		{
			name:           "column ref",
			expr:           engine.Col("table", "column"),
			expectedSQL:    "table.column",
			expectedParams: map[string]any{},
		},
		{
			name:           "string literal",
			expr:           engine.Lit("hello"),
			expectedSQL:    "{p0:String}",
			expectedParams: map[string]any{"p0": "hello"},
		},
		{
			name:           "string literal with quotes",
			expr:           engine.Lit("it's"),
			expectedSQL:    "{p0:String}",
			expectedParams: map[string]any{"p0": "it's"},
		},
		{
			name:           "int literal",
			expr:           engine.Lit(42),
			expectedSQL:    "{p0:Int64}",
			expectedParams: map[string]any{"p0": 42},
		},
		{
			name:           "bool literal true",
			expr:           engine.Lit(true),
			expectedSQL:    "{p0:Bool}",
			expectedParams: map[string]any{"p0": true},
		},
		{
			name:           "bool literal false",
			expr:           engine.Lit(false),
			expectedSQL:    "{p0:Bool}",
			expectedParams: map[string]any{"p0": false},
		},
		{
			name:           "null literal",
			expr:           engine.Lit(nil),
			expectedSQL:    "NULL",
			expectedParams: map[string]any{},
		},
		{
			name:           "array literal",
			expr:           engine.Lit([]any{1, 2, 3}),
			expectedSQL:    "({p0:Int64}, {p1:Int64}, {p2:Int64})",
			expectedParams: map[string]any{"p0": 1, "p1": 2, "p2": 3},
		},
		{
			name:           "function call",
			expr:           engine.Func("COUNT", engine.Col("t", "id")),
			expectedSQL:    "COUNT(t.id)",
			expectedParams: map[string]any{},
		},
		{
			name:           "binary op",
			expr:           engine.Eq(engine.Col("t", "id"), engine.Lit(5)),
			expectedSQL:    "(t.id = {p0:Int64})",
			expectedParams: map[string]any{"p0": 5},
		},
		{
			name:           "is null",
			expr:           &engine.UnaryOp{Op: engine.OpIsNull, Expr: engine.Col("t", "deleted_at")},
			expectedSQL:    "(t.deleted_at IS NULL)",
			expectedParams: map[string]any{},
		},
		{
			name:           "not operator",
			expr:           &engine.UnaryOp{Op: engine.OpNot, Expr: engine.Col("t", "active")},
			expectedSQL:    "(NOT t.active)",
			expectedParams: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params, err := engine.LiftExprToSql(tt.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tt.expectedSQL {
				t.Errorf("SQL mismatch:\nexpected: %s\ngot:      %s", tt.expectedSQL, sql)
			}
			if len(params) != len(tt.expectedParams) {
				t.Errorf("params count mismatch: expected %d, got %d", len(tt.expectedParams), len(params))
			}
			for k, v := range tt.expectedParams {
				if params[k] != v {
					t.Errorf("param[%s] mismatch: expected %v, got %v", k, v, params[k])
				}
			}
		})
	}
}

func TestEmitSQL_InvalidTypeFilter(t *testing.T) {
	tests := []struct {
		name        string
		query       *engine.Query
		expectedErr string
	}{
		{
			name: "invalid type filter in FROM clause",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id")},
				},
				From:  engine.Table("nodes", "n", "NonexistentType"),
				Limit: 10,
			},
			expectedErr: "type NonexistentType is not a valid node label or relationship type",
		},
		{
			name: "invalid type filter in join left side",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id")},
				},
				From: engine.Join_(engine.InnerJoin,
					engine.Table("nodes", "n", "FakeNodeType"),
					engine.Table("edges", "e", ""),
					engine.Eq(engine.Col("n", "id"), engine.Col("e", "from_id")),
				),
				Limit: 10,
			},
			expectedErr: "type FakeNodeType is not a valid node label or relationship type",
		},
		{
			name: "invalid type filter in join right side",
			query: &engine.Query{
				Select: []engine.SelectExpr{
					{Expr: engine.Col("n", "id")},
				},
				From: engine.Join_(engine.InnerJoin,
					engine.Table("nodes", "n", "User"), // valid
					engine.Table("edges", "e", "INVALID_REL"),
					engine.Eq(engine.Col("n", "id"), engine.Col("e", "from_id")),
				),
				Limit: 10,
			},
			expectedErr: "type INVALID_REL is not a valid node label or relationship type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := engine.LiftAstToSql(tt.query)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Errorf("expected error containing %q, got %q", tt.expectedErr, err.Error())
			}
		})
	}
}
