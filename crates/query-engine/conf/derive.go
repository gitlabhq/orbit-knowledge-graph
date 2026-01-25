package conf

import (
	"embed"
	"encoding/json"
	"fmt"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/ontology"
)

//go:embed schema.json
var schemaFS embed.FS

const (
	plannerSchemaPath       = "schema.json"
	definitionsKey          = "$defs"
	nodeLabelKey            = "NodeLabel"
	relationshipTypeNameKey = "RelationshipTypeName"
	nodePropertiesKey       = "NodeProperties"
)

// stores the derived schema, caching it after first load
var derivedQuerySchema map[string]any

// DeriveSchema loads the base schema.json and populates it with ontology-derived enums
func DeriveQuerySchema() (map[string]any, error) {
	// Load embedded base schema
	baseSchema, err := schemaFS.ReadFile(plannerSchemaPath)
	if err != nil {
		return nil, fmt.Errorf("read base schema: %w", err)
	}

	// use embedded ontology (cached singleton) to populate enums for valid query gen
	ont := ontology.GetOntologySchema()

	// Parse schema as generic map to preserve structure
	var schema map[string]any
	if err := json.Unmarshal(baseSchema, &schema); err != nil {
		return nil, fmt.Errorf("parse base schema: %w", err)
	}

	// Get $defs section
	defs, ok := schema[definitionsKey].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema missing %s", definitionsKey)
	}

	// Populate NodeLabel enum
	if nodeLabel, ok := defs[nodeLabelKey].(map[string]any); ok {
		nodeLabel["enum"] = ont.NodeLabels
	}

	// Populate RelationshipTypeName enum
	if relType, ok := defs[relationshipTypeNameKey].(map[string]any); ok {
		relType["enum"] = ont.RelationshipTypes
	}

	// Populate NodeProperties with property definitions per node type
	nodeProperties := ont.ExtractNodeProperties()
	defs[nodePropertiesKey] = nodeProperties

	if derivedQuerySchema == nil {
		derivedQuerySchema = schema
	}

	return derivedQuerySchema, nil
}
