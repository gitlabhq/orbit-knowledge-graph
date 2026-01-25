package ontology

import (
	"embed"
	"maps"
	"sort"
	"sync"

	"sigs.k8s.io/yaml"
)

//go:embed schema.yaml nodes/**/*.yaml edges/*.yaml
var SchemaFS embed.FS

// OntologySchema represents the main ontology/schema.yaml structure
type OntologySchemaFromYaml struct {
	Type          string               `yaml:"type"`
	SchemaVersion string               `yaml:"schema_version"`
	Description   string               `yaml:"description"`
	Domains       map[string]DomainDef `yaml:"domains"`
	Edges         map[string]string    `yaml:"edges"` // edge name -> file path
}

// DomainDef represents a domain definition
type DomainDef struct {
	Description string            `yaml:"description"`
	Nodes       map[string]string `yaml:"nodes"` // node name -> file path
}

// PropertyDef represents a property definition with type information
type PropertyDef struct {
	Type        string   `yaml:"type" json:"type"`
	Description string   `yaml:"description" json:"description"`
	Nullable    bool     `yaml:"nullable" json:"nullable"`
	Values      []string `yaml:"values" json:"values"` // for enum types
	Status      string   `yaml:"status" json:"status"`
	Internal    any      `yaml:"internal" json:"internal"` // internal metadata, not parsed
}

// NodeDef represents a complete node definition
type NodeDef struct {
	Type                 string                 `yaml:"type" json:"type"`
	Domain               string                 `yaml:"domain" json:"domain"`
	Description          string                 `yaml:"description" json:"description"`
	Properties           map[string]PropertyDef `yaml:"properties" json:"properties"`
	AdditionalProperties map[string]PropertyDef `yaml:"additional_properties" json:"additional_properties"`
}

type OntologySchema struct {
	OntologySchemaFromYaml
	NodeLabelsMap        map[string]bool
	RelationshipTypesMap map[string]bool
	NodeLabels           []string
	RelationshipTypes    []string
	NodeDefinitions      map[string]*NodeDef // node label -> full definition
}

// Singleton
var (
	ontologySchema     *OntologySchema
	ontologySchemaOnce sync.Once
	ontologySchemaErr  error
)

// GetOntologySchema returns the singleton ontology schema, initializing it on first call.
// Thread-safe via sync.Once.
func GetOntologySchema() *OntologySchema {
	ontologySchemaOnce.Do(func() {
		ontologySchema, ontologySchemaErr = loadOntologySchema()
	})
	if ontologySchemaErr != nil {
		panic(ontologySchemaErr)
	}
	return ontologySchema
}

func loadOntologySchema() (*OntologySchema, error) {
	// try loading ontology schema from yaml
	data, err := SchemaFS.ReadFile("schema.yaml")
	if err != nil {
		return nil, err
	}
	var schema OntologySchemaFromYaml
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	// Populate OntologySchema struct with data from yaml
	ont := &OntologySchema{
		OntologySchemaFromYaml: schema,
		NodeLabelsMap:          make(map[string]bool),
		RelationshipTypesMap:   make(map[string]bool),
		NodeLabels:             make([]string, 0, len(schema.Domains)),
		RelationshipTypes:      make([]string, 0, len(schema.Edges)),
		NodeDefinitions:        make(map[string]*NodeDef),
	}

	// Populate NodeLabelsMap, NodeLabels, and load full node definitions
	for _, domain := range schema.Domains {
		for nodeName, nodePath := range domain.Nodes {
			ont.NodeLabelsMap[nodeName] = true
			ont.NodeLabels = append(ont.NodeLabels, nodeName)

			// Load the full node definition from the YAML file
			nodeData, err := SchemaFS.ReadFile(nodePath)
			if err != nil {
				return nil, err
			}
			var nodeDef NodeDef
			if err := yaml.Unmarshal(nodeData, &nodeDef); err != nil {
				return nil, err
			}
			ont.NodeDefinitions[nodeName] = &nodeDef
		}
	}
	sort.Strings(ont.NodeLabels)

	// Populate RelationshipTypesMap and RelationshipTypes
	for edgeName := range schema.Edges {
		ont.RelationshipTypesMap[edgeName] = true
		ont.RelationshipTypes = append(ont.RelationshipTypes, edgeName)
	}
	sort.Strings(ont.RelationshipTypes)

	return ont, nil
}

// ExtractNodeProperties returns a map of node labels to their property definitions
// in a format suitable for JSON Schema injection
func (o *OntologySchema) ExtractNodeProperties() map[string]map[string]map[string]any {
	result := make(map[string]map[string]map[string]any)

	for nodeLabel, nodeDef := range o.NodeDefinitions {
		properties := make(map[string]map[string]any)

		// Process both properties and additional_properties
		allProps := make(map[string]PropertyDef)
		maps.Copy(allProps, nodeDef.Properties)
		maps.Copy(allProps, nodeDef.AdditionalProperties)

		for propName, propDef := range allProps {
			propSchema := make(map[string]any)

			// Map ontology types to JSON Schema types
			jsonType := mapOntologyTypeToJSONSchema(propDef.Type)
			propSchema["type"] = jsonType

			if propDef.Description != "" {
				propSchema["description"] = propDef.Description
			}

			// Add format hints for specific types
			switch propDef.Type {
			case "timestamp":
				propSchema["format"] = "date-time"
			case "date":
				propSchema["format"] = "date"
			case "enum":
				if len(propDef.Values) > 0 {
					propSchema["enum"] = propDef.Values
				}
			}
			properties[propName] = propSchema
		}

		result[nodeLabel] = properties
	}

	return result
}

// mapOntologyTypeToJSONSchema converts ontology types to JSON Schema types
func mapOntologyTypeToJSONSchema(ontologyType string) string {
	switch ontologyType {
	case "int64", "int32", "int16", "int8":
		return "integer"
	case "float64", "float32":
		return "number"
	case "string", "text":
		return "string"
	case "boolean", "bool":
		return "boolean"
	case "timestamp", "date", "datetime":
		return "string"
	case "enum":
		return "string"
	case "array":
		return "array"
	case "object", "json", "jsonb":
		return "object"
	default:
		return "string" // default fallback
	}
}
