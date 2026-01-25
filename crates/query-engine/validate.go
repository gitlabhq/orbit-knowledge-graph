package engine

import (
	"fmt"

	"gitlab.com/gitlab-org/knowledge-graph/gkg-service/ontology"
)

var reservedColumnsMap = map[string]bool{
	"id":      true,
	"label":   true,
	"from_id": true,
	"to_id":   true,
	"type":    true,
}

// ValidateColumn checks if a column exists for a given node label
func ValidateColumn(nodeLabel, column string) error {
	// Skip validation for special columns that exist on all nodes/edges
	if reservedColumnsMap[column] {
		return nil
	}

	// If no label specified, we can't validate (will be caught at runtime)
	if nodeLabel == "" {
		return fmt.Errorf("no node label specified")
	}

	ont := ontology.GetOntologySchema()

	nodeDef, ok := ont.NodeDefinitions[nodeLabel]
	if !ok {
		return fmt.Errorf("unknown node label: %s", nodeLabel)
	}

	// Check if column exists in properties or additional_properties
	if _, ok := nodeDef.Properties[column]; ok {
		return nil
	}
	if _, ok := nodeDef.AdditionalProperties[column]; ok {
		return nil
	}

	return fmt.Errorf("column %q does not exist on node type %q", column, nodeLabel)
}

func ValidateTable(table string) error {
	ont := ontology.GetOntologySchema()

	if _, ok := ont.NodeDefinitions[table]; ok {
		return nil
	}

	if _, ok := ont.RelationshipTypesMap[table]; ok {
		return nil
	}

	return fmt.Errorf("table %q is not a valid node or relationship type", table)
}

func ValidateTypeFilter(typeFilter string) error {
	ont := ontology.GetOntologySchema()
	if _, ok := ont.NodeLabelsMap[typeFilter]; ok {
		return nil
	}
	if _, ok := ont.RelationshipTypesMap[typeFilter]; ok {
		return nil
	}
	return fmt.Errorf("type %q is not a valid node label or relationship type", typeFilter)
}
