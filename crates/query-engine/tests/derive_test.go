package engine_test

import (
	"slices"
	"testing"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/internal/engine/conf"
	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/ontology"
)

// Command: go test ./internal/engine/tests/derive_test.go -v

func TestLoadOntology(t *testing.T) {
	ont := ontology.GetOntologySchema()

	// Print the struct
	t.Logf("OntologySchema: %+v", ont)

	// Verify basic fields
	if ont.Type != "main" {
		t.Errorf("expected Type 'main', got %q", ont.Type)
	}

	// FIXME: k8s YAML is not playing nice with floats
	// if ont.SchemaVersion != "0.1" {
	// 	t.Errorf("expected SchemaVersion '0.1', got %q", ont.SchemaVersion)
	// }

	coreDomain, ok := ont.Domains["core"]
	if !ok {
		t.Fatal("expected 'core' domain to exist")
	}

	expectedNodes := []string{"User", "Group", "Project", "Note"}
	for _, node := range expectedNodes {
		if _, ok := coreDomain.Nodes[node]; !ok {
			t.Errorf("expected node %q in core domain", node)
		}
	}

	// Verify edges
	expectedEdges := []string{"CONTAINS", "MEMBER_OF", "CREATOR", "OWNER", "AUTHORED"}
	for _, edge := range expectedEdges {
		if _, ok := ont.Edges[edge]; !ok {
			t.Errorf("expected edge %q", edge)
		}
	}
}

func TestExtractNodeLabels(t *testing.T) {
	ont := ontology.GetOntologySchema()

	labels := ont.NodeLabels
	t.Logf("Node Labels: %v", labels)

	expected := []string{"Group", "Note", "Project", "User"}
	if !slices.Equal(labels, expected) {
		t.Errorf("expected labels %v, got %v", expected, labels)
	}
}

func TestExtractRelationshipTypes(t *testing.T) {
	ont := ontology.GetOntologySchema()

	types := ont.RelationshipTypes
	t.Logf("Relationship Types: %v", types)

	expected := []string{"AUTHORED", "CONTAINS", "CREATOR", "MEMBER_OF", "OWNER"}
	if !slices.Equal(types, expected) {
		t.Errorf("expected types %v, got %v", expected, types)
	}
}

func TestExtractNodeProperties(t *testing.T) {
	ont := ontology.GetOntologySchema()

	// Debug: Check Note definition
	noteDef := ont.NodeDefinitions["Note"]
	t.Logf("Note.Properties: %+v", noteDef.Properties)
	t.Logf("Note.AdditionalProperties: %+v", noteDef.AdditionalProperties)

	nodeProps := ont.ExtractNodeProperties()
	t.Logf("Node Properties: %+v", nodeProps)

	// Verify we have properties for all node types
	expectedNodes := []string{"User", "Group", "Project", "Note"}
	for _, node := range expectedNodes {
		if _, ok := nodeProps[node]; !ok {
			t.Errorf("expected properties for node %q", node)
		}
	}

	// Verify Note has created_at from additional_properties
	noteProps, ok := nodeProps["Note"]
	if !ok {
		t.Fatal("Note node should have properties")
	}
	if _, ok := noteProps["created_at"]; !ok {
		t.Error("Note should have 'created_at' property from additional_properties")
	}

	// Verify User node has expected properties
	userProps, ok := nodeProps["User"]
	if !ok {
		t.Fatal("User node should have properties")
	}

	expectedUserProps := []string{"id", "username", "email", "name"}
	for _, prop := range expectedUserProps {
		if _, ok := userProps[prop]; !ok {
			t.Errorf("User should have property %q", prop)
		}
	}

	// Verify property has type information
	idProp := userProps["id"]
	if idProp["type"] != "integer" {
		t.Errorf("User.id type should be 'integer', got %v", idProp["type"])
	}
	if idProp["description"] == "" {
		t.Error("User.id should have a description")
	}

	t.Logf("User.id property: %+v", idProp)
}

func TestDeriveQuerySchema(t *testing.T) {
	schema, err := conf.DeriveQuerySchema()
	if err != nil {
		t.Fatalf("DeriveQuerySchema failed: %v", err)
	}

	t.Logf("Derived Schema length: %d bytes", len(schema))
	t.Logf("Derived Schema: %+v", schema)

	// Verify the schema contains populated enums
	nodes := schema["$defs"].(map[string]any)["NodeLabel"].(map[string]any)["enum"].([]string)
	if !slices.Contains(nodes, "Group") {
		t.Error("schema should contain Group node label")
	}
	edges := schema["$defs"].(map[string]any)["RelationshipTypeName"].(map[string]any)["enum"].([]string)
	if !slices.Contains(edges, "AUTHORED") {
		t.Error("schema should contain AUTHORED relationship type")
	}

	// Verify NodeProperties are populated
	nodeProps := schema["$defs"].(map[string]any)["NodeProperties"].(map[string]map[string]map[string]any)
	if len(nodeProps) == 0 {
		t.Error("NodeProperties should be populated")
	}

	// Check User node has expected properties
	userProps, ok := nodeProps["User"]
	if !ok {
		t.Fatal("User node should have properties")
	}
	if _, ok := userProps["id"]; !ok {
		t.Error("User should have 'id' property")
	}
	if _, ok := userProps["username"]; !ok {
		t.Error("User should have 'username' property")
	}
	if _, ok := userProps["email"]; !ok {
		t.Error("User should have 'email' property")
	}

	// Verify property type information
	idProp := userProps["id"]
	if idProp["type"] != "integer" {
		t.Errorf("User.id type should be 'integer', got %v", idProp["type"])
	}

	t.Logf("User properties: %+v", userProps)
}
