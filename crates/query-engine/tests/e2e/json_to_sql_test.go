package engine_test

import (
	"strings"
	"testing"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine"
)

// Command: go test -run TestJsonToSql ./internal/engine/tests/e2e/ -v

func TestJsonToSql(t *testing.T) {
	input, err := engine.ParseInput(`{
		"query_type": "traversal",
		"nodes": [
			{"id": "n", "label": "Note", "filters": {"confidential": true}},
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

	query, err := engine.LiftAstToSql(ast)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("SQL:\n%s\nParams: %v", query.SQL, query.Params)

	// TODO: These are primitive assertions, replace them with AST walking
	if !strings.Contains(query.SQL, "INNER JOIN edges AS e0 ON (u.id = e0.from_id)") {
		t.Errorf("expected INNER JOIN edges AS e0 ON (u.id = e0.from_id) in SQL")
	}
	if !strings.Contains(query.SQL, "INNER JOIN nodes AS n ON (e0.to_id = n.id)") {
		t.Errorf("expected INNER JOIN nodes AS n ON (e0.to_id = n.id) in SQL")
	}
}
