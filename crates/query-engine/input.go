package engine

import (
	"encoding/json"
	"fmt"
)

// =============================================================================
// Input Types - These represent the JSON format that the LLM produces.
// This is NOT the AST - it's just a structured representation of the input.
// =============================================================================

// Input represents the parsed JSON query from the LLM
type Input struct {
	QueryType       string              `json:"query_type"`
	Nodes           []InputNode         `json:"nodes"`
	Relationships   []InputRelationship `json:"relationships,omitempty"`
	Aggregations    []InputAggregation  `json:"aggregations,omitempty"`
	Path            *InputPath          `json:"path,omitempty"`
	Limit           int                 `json:"limit,omitempty"`
	OrderBy         *InputOrderBy       `json:"order_by,omitempty"`
	AggregationSort *InputAggSort       `json:"aggregation_sort,omitempty"`
}

// InputNode represents a node selector in the input
type InputNode struct {
	ID         string                 `json:"id"`
	Label      string                 `json:"label,omitempty"`
	Filters    map[string]InputFilter `json:"filters,omitempty"`
	NodeIDs    []int                  `json:"node_ids,omitempty"`
	IDRange    *InputIDRange          `json:"id_range,omitempty"`
	IDProperty string                 `json:"id_property,omitempty"`
}

// InputIDRange represents an ID range filter
type InputIDRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// InputFilter represents a property filter (parsed from JSON)
type InputFilter struct {
	// For operator-based filters
	Op    string `json:"op,omitempty"`
	Value any    `json:"value,omitempty"`
	// IsSimple is true when the filter is just a value (equality)
	IsSimple bool `json:"-"`
}

// InputRelationship represents a relationship selector
type InputRelationship struct {
	Types     []string               `json:"type"` // normalized to array
	From      string                 `json:"from"`
	To        string                 `json:"to"`
	MinHops   int                    `json:"min_hops,omitempty"`
	MaxHops   int                    `json:"max_hops,omitempty"`
	Direction string                 `json:"direction,omitempty"`
	Filters   map[string]InputFilter `json:"filters,omitempty"`
}

// InputAggregation represents an aggregation specification
type InputAggregation struct {
	Function string `json:"function"`
	Target   string `json:"target,omitempty"`
	GroupBy  string `json:"group_by,omitempty"`
	Property string `json:"property,omitempty"`
	Alias    string `json:"alias,omitempty"`
}

// InputPath represents path finding configuration
type InputPath struct {
	Type     string   `json:"type"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	MaxDepth int      `json:"max_depth"`
	RelTypes []string `json:"rel_types,omitempty"`
}

// InputOrderBy represents ordering specification
type InputOrderBy struct {
	Node      string `json:"node"`
	Property  string `json:"property"`
	Direction string `json:"direction,omitempty"`
}

// InputAggSort represents aggregation sorting
type InputAggSort struct {
	AggIndex  int    `json:"agg_index"`
	Direction string `json:"direction,omitempty"`
}

// =============================================================================
// Parsing
// =============================================================================

// ParseInput parses JSON into the Input structure
func ParseInput(jsonData string) (*Input, error) {
	var raw rawInput
	if err := json.Unmarshal([]byte(jsonData), &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal: %w", err)
	}

	input := &Input{
		QueryType:       raw.QueryType,
		Limit:           raw.Limit,
		OrderBy:         raw.OrderBy,
		AggregationSort: raw.AggregationSort,
		Path:            raw.Path,
	}

	// Default limit
	if input.Limit == 0 {
		input.Limit = 30
	}

	// Parse nodes
	input.Nodes = make([]InputNode, 0, len(raw.Nodes))
	for i, rawNode := range raw.Nodes {
		node, err := parseNode(rawNode)
		if err != nil {
			return nil, fmt.Errorf("node %d: %w", i, err)
		}
		input.Nodes = append(input.Nodes, node)
	}

	// Parse relationships
	input.Relationships = make([]InputRelationship, 0, len(raw.Relationships))
	for i, rawRel := range raw.Relationships {
		rel, err := parseRelationship(rawRel)
		if err != nil {
			return nil, fmt.Errorf("relationship %d: %w", i, err)
		}
		input.Relationships = append(input.Relationships, rel)
	}

	// Parse aggregations
	input.Aggregations = make([]InputAggregation, 0, len(raw.Aggregations))
	for _, rawAgg := range raw.Aggregations {
		var agg InputAggregation
		if err := json.Unmarshal(rawAgg, &agg); err != nil {
			return nil, fmt.Errorf("aggregation: %w", err)
		}
		input.Aggregations = append(input.Aggregations, agg)
	}

	return input, nil
}

// =============================================================================
// Internal parsing helpers
// =============================================================================

type rawInput struct {
	QueryType       string            `json:"query_type"`
	Nodes           []json.RawMessage `json:"nodes"`
	Relationships   []json.RawMessage `json:"relationships,omitempty"`
	Aggregations    []json.RawMessage `json:"aggregations,omitempty"`
	Path            *InputPath        `json:"path,omitempty"`
	Limit           int               `json:"limit,omitempty"`
	OrderBy         *InputOrderBy     `json:"order_by,omitempty"`
	AggregationSort *InputAggSort     `json:"aggregation_sort,omitempty"`
}

type rawNode struct {
	ID         string                     `json:"id"`
	Label      string                     `json:"label,omitempty"`
	Filters    map[string]json.RawMessage `json:"filters,omitempty"`
	NodeIDs    []int                      `json:"node_ids,omitempty"`
	IDRange    *InputIDRange              `json:"id_range,omitempty"`
	IDProperty string                     `json:"id_property,omitempty"`
}

type rawRelationship struct {
	Type      json.RawMessage            `json:"type"`
	From      string                     `json:"from"`
	To        string                     `json:"to"`
	MinHops   int                        `json:"min_hops,omitempty"`
	MaxHops   int                        `json:"max_hops,omitempty"`
	Direction string                     `json:"direction,omitempty"`
	Filters   map[string]json.RawMessage `json:"filters,omitempty"`
}

func parseNode(data json.RawMessage) (InputNode, error) {
	var raw rawNode
	if err := json.Unmarshal(data, &raw); err != nil {
		return InputNode{}, err
	}

	node := InputNode{
		ID:         raw.ID,
		Label:      raw.Label,
		NodeIDs:    raw.NodeIDs,
		IDRange:    raw.IDRange,
		IDProperty: raw.IDProperty,
	}

	if node.IDProperty == "" {
		node.IDProperty = "id"
	}

	// Parse filters
	if len(raw.Filters) > 0 {
		node.Filters = make(map[string]InputFilter, len(raw.Filters))
		for key, rawFilter := range raw.Filters {
			filter, err := parseFilter(rawFilter)
			if err != nil {
				return InputNode{}, fmt.Errorf("filter %q: %w", key, err)
			}
			node.Filters[key] = filter
		}
	}

	return node, nil
}

func parseFilter(data json.RawMessage) (InputFilter, error) {
	// Try operator-based filter first
	var opFilter struct {
		Op    string `json:"op"`
		Value any    `json:"value,omitempty"`
	}
	if err := json.Unmarshal(data, &opFilter); err == nil && opFilter.Op != "" {
		return InputFilter{Op: opFilter.Op, Value: opFilter.Value, IsSimple: false}, nil
	}

	// Simple equality value
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return InputFilter{}, err
	}
	return InputFilter{Value: value, IsSimple: true}, nil
}

func parseRelationship(data json.RawMessage) (InputRelationship, error) {
	var raw rawRelationship
	if err := json.Unmarshal(data, &raw); err != nil {
		return InputRelationship{}, err
	}

	rel := InputRelationship{
		From:      raw.From,
		To:        raw.To,
		MinHops:   raw.MinHops,
		MaxHops:   raw.MaxHops,
		Direction: raw.Direction,
	}

	// Defaults
	if rel.MinHops == 0 {
		rel.MinHops = 1
	}
	if rel.MaxHops == 0 {
		rel.MaxHops = 1
	}
	if rel.Direction == "" {
		rel.Direction = "outgoing"
	}

	// Parse type (string or array)
	var single string
	if err := json.Unmarshal(raw.Type, &single); err == nil {
		rel.Types = []string{single}
	} else {
		if err := json.Unmarshal(raw.Type, &rel.Types); err != nil {
			return InputRelationship{}, fmt.Errorf("type must be string or array")
		}
	}

	// Parse filters
	if len(raw.Filters) > 0 {
		rel.Filters = make(map[string]InputFilter, len(raw.Filters))
		for key, rawFilter := range raw.Filters {
			filter, err := parseFilter(rawFilter)
			if err != nil {
				return InputRelationship{}, fmt.Errorf("filter %q: %w", key, err)
			}
			rel.Filters[key] = filter
		}
	}

	return rel, nil
}
