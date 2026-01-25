package engine

import (
	"fmt"

	"github.com/xeipuuv/gojsonschema"
)

// ValidateSchema validates a parsed Go value against a JSON schema.
// The value is converted to its JSON representation for validation.
func ValidateSchema(schema string, value any) error {
	schemaLoader := gojsonschema.NewStringLoader(schema)
	dataLoader := gojsonschema.NewGoLoader(value)
	result, err := gojsonschema.Validate(schemaLoader, dataLoader)
	if err != nil {
		return fmt.Errorf("schema validation error: %w", err)
	}
	if !result.Valid() {
		return fmt.Errorf("invalid input: %s", result.Errors())
	}
	return nil
}
