package engine

import (
	"fmt"
)

// =============================================================================
// Lower: Input → AST
//
// Converts the LLM's JSON input into a SQL-oriented AST.
// =============================================================================

// Lower converts parsed input into an AST node
func Lower(input *Input) (Node, error) {
	switch input.QueryType {
	case "traversal", "pattern":
		return lowerTraversal(input)
	case "aggregation":
		return lowerAggregation(input)
	case "path_finding":
		return lowerPathFinding(input)
	default:
		return nil, fmt.Errorf("unknown query type: %s", input.QueryType)
	}
}

// =============================================================================
// Traversal: SELECT ... FROM nodes JOIN edges JOIN nodes ... WHERE ...
// =============================================================================

func lowerTraversal(input *Input) (*Query, error) {
	// Build FROM with joins
	from, edgeAliases, err := buildFrom(input.Nodes, input.Relationships)
	if err != nil {
		return nil, err
	}

	// Build WHERE from filters
	where, err := buildWhere(input.Nodes, input.Relationships, edgeAliases)
	if err != nil {
		return nil, err
	}

	// Build SELECT - return node IDs
	sel := make([]SelectExpr, 0, len(input.Nodes))
	for _, n := range input.Nodes {
		sel = append(sel, SelectExpr{
			Expr:  Col(n.ID, "id"),
			Alias: n.ID + "_id",
		})
	}

	// Build ORDER BY
	var orderBy []OrderExpr
	if input.OrderBy != nil {
		// Validate the order by column
		nodeLabel := findNodeLabel(input.Nodes, input.OrderBy.Node)
		if err := ValidateColumn(nodeLabel, input.OrderBy.Property); err != nil {
			return nil, fmt.Errorf("invalid order_by: %w", err)
		}
		orderBy = []OrderExpr{{
			Expr: Col(input.OrderBy.Node, input.OrderBy.Property),
			Desc: input.OrderBy.Direction == "DESC",
		}}
	}

	return &Query{
		Select:  sel,
		From:    from,
		Where:   where,
		OrderBy: orderBy,
		Limit:   input.Limit,
	}, nil
}

// =============================================================================
// Aggregation: SELECT agg(...) ... GROUP BY ...
// =============================================================================

func lowerAggregation(input *Input) (*Query, error) {
	from, edgeAliases, err := buildFrom(input.Nodes, input.Relationships)
	if err != nil {
		return nil, err
	}

	where, err := buildWhere(input.Nodes, input.Relationships, edgeAliases)
	if err != nil {
		return nil, err
	}

	sel := make([]SelectExpr, 0, len(input.Aggregations)*2)
	var groupBy []Expr
	grouped := make(map[string]bool)

	for _, agg := range input.Aggregations {
		// Validate aggregation property if specified
		if agg.Property != "" {
			nodeLabel := findNodeLabel(input.Nodes, agg.Target)
			if err := ValidateColumn(nodeLabel, agg.Property); err != nil {
				return nil, fmt.Errorf("invalid aggregation property: %w", err)
			}
		}

		// Add GROUP BY column
		if agg.GroupBy != "" && !grouped[agg.GroupBy] {
			grouped[agg.GroupBy] = true
			groupBy = append(groupBy, Col(agg.GroupBy, "id"))
			sel = append(sel, SelectExpr{
				Expr:  Col(agg.GroupBy, "id"),
				Alias: agg.GroupBy + "_id",
			})
		}

		// Add aggregate function
		alias := agg.Alias
		if alias == "" {
			alias = agg.Function
		}
		sel = append(sel, SelectExpr{
			Expr:  buildAggFunc(agg),
			Alias: alias,
		})
	}

	// ORDER BY aggregation result
	var orderBy []OrderExpr
	if input.AggregationSort != nil && input.AggregationSort.AggIndex < len(input.Aggregations) {
		orderBy = []OrderExpr{{
			Expr: buildAggFunc(input.Aggregations[input.AggregationSort.AggIndex]),
			Desc: input.AggregationSort.Direction == "DESC",
		}}
	}

	return &Query{
		Select:  sel,
		From:    from,
		Where:   where,
		GroupBy: groupBy,
		OrderBy: orderBy,
		Limit:   input.Limit,
	}, nil
}

func buildAggFunc(agg InputAggregation) Expr {
	var arg Expr
	if agg.Property != "" {
		arg = Col(agg.Target, agg.Property)
	} else if agg.Target != "" {
		arg = Col(agg.Target, "id")
	} else {
		arg = Lit(1)
	}

	switch agg.Function {
	case "count":
		return Func("COUNT", arg)
	case "sum":
		return Func("SUM", arg)
	case "avg":
		return Func("AVG", arg)
	case "min":
		return Func("MIN", arg)
	case "max":
		return Func("MAX", arg)
	case "collect":
		return Func("groupArray", arg)
	default:
		return Func(agg.Function, arg)
	}
}

// =============================================================================
// Path Finding: WITH RECURSIVE ...
// =============================================================================

func lowerPathFinding(input *Input) (*RecursiveCTE, error) {
	if input.Path == nil {
		return nil, fmt.Errorf("path config required")
	}

	var startNode, endNode *InputNode
	for i := range input.Nodes {
		if input.Nodes[i].ID == input.Path.From {
			startNode = &input.Nodes[i]
		}
		if input.Nodes[i].ID == input.Path.To {
			endNode = &input.Nodes[i]
		}
	}
	if startNode == nil || endNode == nil {
		return nil, fmt.Errorf("path from/to nodes not found")
	}

	// Base case: start node
	var baseWhere Expr
	if len(startNode.NodeIDs) > 0 {
		baseWhere = Eq(Col("n", "id"), Lit(startNode.NodeIDs[0]))
	}
	base := &Query{
		Select: []SelectExpr{
			{Expr: Col("n", "id"), Alias: "node_id"},
			{Expr: Func("ARRAY", Col("n", "id")), Alias: "path"},
			{Expr: Lit(0), Alias: "depth"},
		},
		From:  Table("nodes", "n", startNode.Label),
		Where: baseWhere,
	}

	// Recursive case: extend path
	recursive := &Query{
		Select: []SelectExpr{
			{Expr: Col("n", "id"), Alias: "node_id"},
			{Expr: Func("arrayConcat", Col("p", "path"), Func("ARRAY", Col("n", "id"))), Alias: "path"},
			{Expr: &BinaryOp{Op: OpAdd, Left: Col("p", "depth"), Right: Lit(1)}, Alias: "depth"},
		},
		From: Join_(InnerJoin,
			Join_(InnerJoin,
				Table("path_cte", "p", ""),
				Table("edges", "e", ""),
				Eq(Col("p", "node_id"), Col("e", "from_id")),
			),
			Table("nodes", "n", endNode.Label),
			Eq(Col("e", "to_id"), Col("n", "id")),
		),
		Where: And(
			&BinaryOp{Op: OpLt, Left: Col("p", "depth"), Right: Lit(input.Path.MaxDepth)},
			&UnaryOp{Op: OpNot, Expr: Func("has", Col("p", "path"), Col("n", "id"))},
		),
	}

	// Final query
	var finalWhere Expr
	if len(endNode.NodeIDs) > 0 {
		finalWhere = Eq(Col("path_cte", "node_id"), Lit(endNode.NodeIDs[0]))
	}
	final := &Query{
		Select: []SelectExpr{
			{Expr: Col("path_cte", "path"), Alias: "path"},
			{Expr: Col("path_cte", "depth"), Alias: "depth"},
		},
		From:    Table("path_cte", "path_cte", ""),
		Where:   finalWhere,
		OrderBy: []OrderExpr{{Expr: Col("path_cte", "depth"), Desc: false}},
		Limit:   input.Limit,
	}

	return &RecursiveCTE{
		Name:      "path_cte",
		Base:      base,
		Recursive: recursive,
		MaxDepth:  input.Path.MaxDepth,
		Final:     final,
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

// buildFrom builds the FROM clause with joins
func buildFrom(nodes []InputNode, rels []InputRelationship) (TableRef, map[int]string, error) {
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("at least one node required")
	}

	edgeAliases := make(map[int]string)

	// Start with first node
	var result TableRef = Table("nodes", nodes[0].ID, nodes[0].Label)

	// Join edges and nodes for each relationship
	for i, rel := range rels {
		edgeAlias := fmt.Sprintf("e%d", i)
		edgeAliases[i] = edgeAlias

		typeFilter := ""
		if len(rel.Types) == 1 && rel.Types[0] != "*" {
			typeFilter = rel.Types[0]
		}

		// Join edge table
		var edgeJoinCond Expr
		switch rel.Direction {
		case "incoming":
			edgeJoinCond = Eq(Col(rel.From, "id"), Col(edgeAlias, "to_id"))
		case "both":
			edgeJoinCond = Or(
				Eq(Col(rel.From, "id"), Col(edgeAlias, "from_id")),
				Eq(Col(rel.From, "id"), Col(edgeAlias, "to_id")),
			)
		default: // outgoing
			edgeJoinCond = Eq(Col(rel.From, "id"), Col(edgeAlias, "from_id"))
		}
		result = Join_(InnerJoin, result, Table("edges", edgeAlias, typeFilter), edgeJoinCond)

		// Join target node
		targetLabel := findNodeLabel(nodes, rel.To)
		var targetJoinCond Expr
		switch rel.Direction {
		case "incoming":
			targetJoinCond = Eq(Col(edgeAlias, "from_id"), Col(rel.To, "id"))
		case "both":
			targetJoinCond = Or(
				Eq(Col(edgeAlias, "to_id"), Col(rel.To, "id")),
				Eq(Col(edgeAlias, "from_id"), Col(rel.To, "id")),
			)
		default:
			targetJoinCond = Eq(Col(edgeAlias, "to_id"), Col(rel.To, "id"))
		}
		result = Join_(InnerJoin, result, Table("nodes", rel.To, targetLabel), targetJoinCond)
	}

	return result, edgeAliases, nil
}

func findNodeLabel(nodes []InputNode, id string) string {
	for _, n := range nodes {
		if n.ID == id {
			return n.Label
		}
	}
	return ""
}

// buildWhere builds the WHERE clause from filters
func buildWhere(nodes []InputNode, rels []InputRelationship, edgeAliases map[int]string) (Expr, error) {
	var conds []Expr

	for _, node := range nodes {
		// Node ID filter
		if len(node.NodeIDs) == 1 {
			conds = append(conds, Eq(Col(node.ID, "id"), Lit(node.NodeIDs[0])))
		} else if len(node.NodeIDs) > 1 {
			conds = append(conds, &BinaryOp{Op: OpIn, Left: Col(node.ID, "id"), Right: Lit(node.NodeIDs)})
		}

		// ID range
		if node.IDRange != nil {
			conds = append(conds,
				&BinaryOp{Op: OpGe, Left: Col(node.ID, "id"), Right: Lit(node.IDRange.Start)},
				&BinaryOp{Op: OpLe, Left: Col(node.ID, "id"), Right: Lit(node.IDRange.End)},
			)
		}

		// Property filters
		for prop, filter := range node.Filters {
			if err := ValidateColumn(node.Label, prop); err != nil {
				return nil, fmt.Errorf("invalid filter column: %w", err)
			}
			conds = append(conds, filterToExpr(node.ID, prop, filter))
		}
	}

	for i, rel := range rels {
		alias := edgeAliases[i]
		for prop, filter := range rel.Filters {
			conds = append(conds, filterToExpr(alias, prop, filter))
		}
	}

	return And(conds...), nil
}

func filterToExpr(table, column string, f InputFilter) Expr {
	col := Col(table, column)

	if f.IsSimple {
		return Eq(col, Lit(f.Value))
	}

	switch f.Op {
	case "eq":
		return Eq(col, Lit(f.Value))
	case "gt":
		return &BinaryOp{Op: OpGt, Left: col, Right: Lit(f.Value)}
	case "lt":
		return &BinaryOp{Op: OpLt, Left: col, Right: Lit(f.Value)}
	case "gte":
		return &BinaryOp{Op: OpGe, Left: col, Right: Lit(f.Value)}
	case "lte":
		return &BinaryOp{Op: OpLe, Left: col, Right: Lit(f.Value)}
	case "in":
		return &BinaryOp{Op: OpIn, Left: col, Right: Lit(f.Value)}
	case "contains":
		return &BinaryOp{Op: OpLike, Left: col, Right: Lit("%" + f.Value.(string) + "%")}
	case "starts_with":
		return &BinaryOp{Op: OpLike, Left: col, Right: Lit(f.Value.(string) + "%")}
	case "ends_with":
		return &BinaryOp{Op: OpLike, Left: col, Right: Lit("%" + f.Value.(string))}
	case "is_null":
		return &UnaryOp{Op: OpIsNull, Expr: col}
	case "is_not_null":
		return &UnaryOp{Op: OpIsNotNull, Expr: col}
	default:
		return Eq(col, Lit(f.Value))
	}
}
