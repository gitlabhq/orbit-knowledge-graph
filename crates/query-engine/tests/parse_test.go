package engine_test

import (
	"encoding/json"
	"testing"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine"
	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine/conf"
)

// Command: go test ./internal/engine/tests/parse_test.go -v

func jsonLiteralToMap(literal string) map[string]any {
	var data map[string]any
	json.Unmarshal([]byte(literal), &data)
	return data
}

func TestParse(t *testing.T) {
	schema := `{"type": "object"}`
	data := jsonLiteralToMap(``)
	err := engine.ValidateSchema(schema, data)
	if err == nil {
		t.Fatal(err)
	}
}

func TestParseInvalid(t *testing.T) {
	schema := `{"type": "object"}`
	data := `"not an object"`

	err := engine.ValidateSchema(schema, data)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseDerivedSchema(t *testing.T) {
	schema, err := conf.DeriveQuerySchema()
	if err != nil {
		t.Fatalf("DeriveQuerySchema failed: %v", err)
	}

	t.Logf("Derived Schema length: %d bytes", len(schema))

	var data map[string]any
	json.Unmarshal([]byte(`{
		"query_type": "traversal",
		"nodes": [
			{"id": "u", "label": "User"},
			{"id": "n", "label": "Note", "filters": {"title": "My Note"}}
		],
		"relationships": [
			{"type": "AUTHORED", "from": "u", "to": "n"}
		],
		"limit": 25,
		"order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
	}`), &data)

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("Marshal schema failed: %v", err)
	}

	// Validate the schema
	err = engine.ValidateSchema(string(schemaBytes), data)
	if err != nil {
		t.Fatalf("ValidateSchema failed: %v", err)
	}
}
