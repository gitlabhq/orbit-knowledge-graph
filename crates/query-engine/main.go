package engine

// Compile validates JSON input against the schema and compiles it to an AST
func Compile(schema, jsonInput string) (Node, error) {
	// Parse into Input struct (single parse of raw JSON)
	input, err := ParseInput(jsonInput)
	if err != nil {
		return nil, err
	}

	// Validate the parsed struct against the schema
	if err := ValidateSchema(schema, input); err != nil {
		return nil, err
	}

	// Lower to AST
	return Lower(input)
}

// func RunQuery(jsonInput string) (Node, error) {
// 	// Compile the query
// 	ast, err := Compile(schema, jsonInput)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Execute the query
// 	return Execute(ast)
// }
