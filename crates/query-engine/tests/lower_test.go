package engine_test

import (
	"testing"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine"
)

// Command: go test ./internal/engine/tests/lower_test.go -v

func TestLowerTraversal(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "traversal",
		"nodes": [
			{"id": "n", "label": "Note", "filters": {"system": false}},
			{"id": "u", "label": "User"}
		],
		"relationships": [
			{"type": "AUTHORED", "from": "u", "to": "n"}
		],
		"limit": 25,
		"order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ast, err := engine.Lower(input)
	if err != nil {
		t.Fatal(err)
	}

	q, ok := ast.(*engine.Query)
	if !ok {
		t.Fatalf("expected *Query, got %T", ast)
	}

	if q.Limit != 25 {
		t.Errorf("limit: got %d, want 25", q.Limit)
	}
	if len(q.Select) < 2 {
		t.Errorf("select: got %d exprs, want >= 2", len(q.Select))
	}
	if len(q.OrderBy) != 1 {
		t.Errorf("order by: got %d, want 1", len(q.OrderBy))
	}

	t.Log("Tree:\n" + engine.VisualizeNode(q))
	t.Log("SQL:\n" + engine.PrettyPrintQuery(q))
}

func TestLowerAggregation(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "aggregation",
		"nodes": [
			{"id": "n", "label": "Note"},
			{"id": "u", "label": "User"}
		],
		"relationships": [
			{"type": "AUTHORED", "from": "u", "to": "n"}
		],
		"aggregations": [
			{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}
		],
		"aggregation_sort": {"agg_index": 0, "direction": "DESC"},
		"limit": 10
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ast, err := engine.Lower(input)
	if err != nil {
		t.Fatal(err)
	}

	q := ast.(*engine.Query)
	if len(q.GroupBy) == 0 {
		t.Error("expected GROUP BY")
	}

	// Check for COUNT
	hasCount := false
	for _, sel := range q.Select {
		if fc, ok := sel.Expr.(*engine.FuncCall); ok && fc.Name == "COUNT" {
			hasCount = true
		}
	}
	if !hasCount {
		t.Error("expected COUNT in SELECT")
	}

	t.Log("Tree:\n" + engine.VisualizeNode(q))
	t.Log("SQL:\n" + engine.PrettyPrintQuery(q))
}

func TestLowerPathFinding(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "path_finding",
		"nodes": [
			{"id": "start", "label": "Project", "node_ids": [100]},
			{"id": "end", "label": "Project", "node_ids": [200]}
		],
		"relationships": [],
		"path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ast, err := engine.Lower(input)
	if err != nil {
		t.Fatal(err)
	}

	cte, ok := ast.(*engine.RecursiveCTE)
	if !ok {
		t.Fatalf("expected *RecursiveCTE, got %T", ast)
	}

	if cte.MaxDepth != 3 {
		t.Errorf("max_depth: got %d, want 3", cte.MaxDepth)
	}

	t.Logf("CTE: %s, depth=%d", cte.Name, cte.MaxDepth)
	t.Log("Base:\n" + engine.PrettyPrintQuery(cte.Base))
	t.Log("Final:\n" + engine.PrettyPrintQuery(cte.Final))
}

func TestLowerAdvancedFilters(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "traversal",
		"nodes": [{
			"id": "u",
			"label": "User",
			"filters": {
				"created_at": {"op": "gte", "value": "2024-01-01"},
				"state": {"op": "in", "value": ["active", "blocked"]},
				"username": {"op": "contains", "value": "admin"}
			}
		}],
		"relationships": [],
		"limit": 30
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ast, err := engine.Lower(input)
	if err != nil {
		t.Fatal(err)
	}

	q := ast.(*engine.Query)
	if q.Where == nil {
		t.Error("expected WHERE")
	}

	t.Log("Tree:\n" + engine.VisualizeNode(q))
	t.Log("SQL:\n" + engine.PrettyPrintQuery(q))
}

func TestLowerMultiHop(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "traversal",
		"nodes": [
			{"id": "u", "label": "User"},
			{"id": "n", "label": "Note"},
			{"id": "p", "label": "Project"}
		],
		"relationships": [
			{"type": "AUTHORED", "from": "u", "to": "n"},
			{"type": "CONTAINS", "from": "p", "to": "n"}
		],
		"limit": 20
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ast, err := engine.Lower(input)
	if err != nil {
		t.Fatal(err)
	}

	q := ast.(*engine.Query)
	joins := countJoins(q.From)
	if joins < 4 {
		t.Errorf("joins: got %d, want >= 4", joins)
	}

	t.Log("Tree:\n" + engine.VisualizeNode(q))
	t.Log("SQL:\n" + engine.PrettyPrintQuery(q))
}

func countJoins(t engine.TableRef) int {
	if j, ok := t.(*engine.Join); ok {
		return 1 + countJoins(j.Left) + countJoins(j.Right)
	}
	return 0
}

func TestColumnValidation(t *testing.T) {
	t.Run("valid column in order_by", func(t *testing.T) {
		input, err := engine.ParseInput(`{
			"query_type": "traversal",
			"nodes": [
				{"id": "u", "label": "User"}
			],
			"relationships": [],
			"limit": 10,
			"order_by": {"node": "u", "property": "username", "direction": "ASC"}
		}`)
		if err != nil {
			t.Fatal(err)
		}

		_, err = engine.Lower(input)
		if err != nil {
			t.Errorf("expected no error for valid column, got: %v", err)
		}
	})

	t.Run("invalid column in order_by", func(t *testing.T) {
		input, err := engine.ParseInput(`{
			"query_type": "traversal",
			"nodes": [
				{"id": "u", "label": "User"}
			],
			"relationships": [],
			"limit": 10,
			"order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
		}`)
		if err != nil {
			t.Fatal(err)
		}

		_, err = engine.Lower(input)
		if err == nil {
			t.Error("expected error for invalid column, got nil")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("valid column in aggregation", func(t *testing.T) {
		input, err := engine.ParseInput(`{
			"query_type": "aggregation",
			"nodes": [
				{"id": "p", "label": "Project"}
			],
			"relationships": [],
			"aggregations": [
				{"function": "count", "target": "p", "property": "name", "alias": "name_count"}
			],
			"limit": 10
		}`)
		if err != nil {
			t.Fatal(err)
		}

		_, err = engine.Lower(input)
		if err != nil {
			t.Errorf("expected no error for valid column, got: %v", err)
		}
	})

	t.Run("invalid column in aggregation", func(t *testing.T) {
		input, err := engine.ParseInput(`{
			"query_type": "aggregation",
			"nodes": [
				{"id": "p", "label": "Project"}
			],
			"relationships": [],
			"aggregations": [
				{"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}
			],
			"limit": 10
		}`)
		if err != nil {
			t.Fatal(err)
		}

		_, err = engine.Lower(input)
		if err == nil {
			t.Error("expected error for invalid column, got nil")
		}
		t.Logf("Got expected error: %v", err)
	})
}
