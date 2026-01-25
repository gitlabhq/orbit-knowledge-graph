package engine

import (
	"fmt"
	"strings"
)

// ANSI color codes for terminal output
const (
	colorReset   = "\033[0m"
	colorBold    = "\033[1m"
	colorDim     = "\033[2m"
	colorCyan    = "\033[36m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
)

func keyword(s string) string {
	return colorBold + colorCyan + s + colorReset
}

func tableName(s string) string {
	return colorGreen + s + colorReset
}

func literal(s string) string {
	return colorYellow + s + colorReset
}

func operator(s string) string {
	return colorMagenta + s + colorReset
}

func dimmed(s string) string {
	return colorDim + s + colorReset
}

// VisualizeNode returns a tree-like string representation of any AST node
func VisualizeNode(n Node) string {
	var sb strings.Builder
	visualizeSQLNode(&sb, n, "", true)
	return sb.String()
}

func visualizeSQLNode(sb *strings.Builder, n Node, prefix string, isLast bool) {
	connector := "├── "
	if isLast {
		connector = "└── "
	}

	childPrefix := prefix + "│   "
	if isLast {
		childPrefix = prefix + "    "
	}

	switch v := n.(type) {
	case *Query:
		sb.WriteString(prefix + connector + keyword("Query") + "\n")

		// SELECT
		sb.WriteString(childPrefix + dimmed("├── ") + keyword("SELECT") + "\n")
		for i, sel := range v.Select {
			isLastSel := i == len(v.Select)-1
			selConn := "├── "
			if isLastSel {
				selConn = "└── "
			}
			if sel.Alias != "" {
				sb.WriteString(childPrefix + dimmed("│   "+selConn) + fmt.Sprintf("%s %s %s\n", exprStr(sel.Expr), keyword("AS"), sel.Alias))
			} else {
				sb.WriteString(childPrefix + dimmed("│   "+selConn) + exprStr(sel.Expr) + "\n")
			}
		}

		// FROM
		sb.WriteString(childPrefix + dimmed("├── ") + keyword("FROM") + "\n")
		visualizeTableRef(sb, v.From, childPrefix+"│   ", true)

		// WHERE
		if v.Where != nil {
			sb.WriteString(childPrefix + dimmed("├── ") + keyword("WHERE") + "\n")
			sb.WriteString(childPrefix + dimmed("│   └── ") + exprStr(v.Where) + "\n")
		}

		// GROUP BY
		if len(v.GroupBy) > 0 {
			sb.WriteString(childPrefix + dimmed("├── ") + keyword("GROUP BY") + "\n")
			for i, g := range v.GroupBy {
				isLastG := i == len(v.GroupBy)-1
				gConn := "├── "
				if isLastG {
					gConn = "└── "
				}
				sb.WriteString(childPrefix + dimmed("│   "+gConn) + exprStr(g) + "\n")
			}
		}

		// ORDER BY
		if len(v.OrderBy) > 0 {
			sb.WriteString(childPrefix + dimmed("├── ") + keyword("ORDER BY") + "\n")
			for i, o := range v.OrderBy {
				isLastO := i == len(v.OrderBy)-1
				oConn := "├── "
				if isLastO {
					oConn = "└── "
				}
				dir := "ASC"
				if o.Desc {
					dir = "DESC"
				}
				sb.WriteString(childPrefix + dimmed("│   "+oConn) + fmt.Sprintf("%s %s\n", exprStr(o.Expr), keyword(dir)))
			}
		}

		// LIMIT
		if v.Limit > 0 {
			sb.WriteString(childPrefix + dimmed("└── ") + keyword("LIMIT") + " " + fmt.Sprintf("%d\n", v.Limit))
		}

	case *RecursiveCTE:
		sb.WriteString(prefix + connector + keyword("WITH RECURSIVE") + " " + tableName(v.Name) + "\n")

		sb.WriteString(childPrefix + dimmed("├── ") + "Base Case:\n")
		visualizeSQLNode(sb, v.Base, childPrefix+"│   ", false)

		sb.WriteString(childPrefix + dimmed("├── ") + fmt.Sprintf("Recursive Case (max_depth=%s):\n", literal(fmt.Sprintf("%d", v.MaxDepth))))
		visualizeSQLNode(sb, v.Recursive, childPrefix+"│   ", false)

		sb.WriteString(childPrefix + dimmed("└── ") + "Final Query:\n")
		visualizeSQLNode(sb, v.Final, childPrefix+"    ", true)

	default:
		sb.WriteString(prefix + connector + fmt.Sprintf("%T\n", n))
	}
}

func visualizeTableRef(sb *strings.Builder, t TableRef, prefix string, isLast bool) {
	connector := "├── "
	if isLast {
		connector = "└── "
	}

	childPrefix := prefix + "│   "
	if isLast {
		childPrefix = prefix + "    "
	}

	switch v := t.(type) {
	case *TableScan:
		label := ""
		if v.TypeFilter != "" {
			label = fmt.Sprintf(" [%s]", literal(v.TypeFilter))
		}
		sb.WriteString(prefix + dimmed(connector) + fmt.Sprintf("TableScan(%s %s %s)%s\n", tableName(v.Table), keyword("AS"), v.Alias, label))

	case *Join:
		sb.WriteString(prefix + dimmed(connector) + keyword(fmt.Sprintf("%s JOIN", v.Type)) + "\n")
		visualizeTableRef(sb, v.Left, childPrefix, false)
		visualizeTableRef(sb, v.Right, childPrefix, false)
		sb.WriteString(childPrefix + dimmed("└── ") + keyword("ON") + " " + exprStr(v.On) + "\n")
	}
}

// exprStr returns a concise string representation of an expression
func exprStr(e Expr) string {
	if e == nil {
		return "<nil>"
	}

	switch v := e.(type) {
	case *ColumnRef:
		return fmt.Sprintf("%s.%s", v.Table, colorBlue+v.Column+colorReset)

	case *Literal:
		switch val := v.Value.(type) {
		case string:
			return literal(fmt.Sprintf("'%s'", val))
		case []any:
			strs := make([]string, len(val))
			for i, item := range val {
				strs[i] = fmt.Sprintf("%v", item)
			}
			return literal("[" + strings.Join(strs, ", ") + "]")
		default:
			return literal(fmt.Sprintf("%v", v.Value))
		}

	case *FuncCall:
		args := make([]string, len(v.Args))
		for i, arg := range v.Args {
			args[i] = exprStr(arg)
		}
		return fmt.Sprintf("%s(%s)", colorGreen+v.Name+colorReset, strings.Join(args, ", "))

	case *BinaryOp:
		return fmt.Sprintf("(%s %s %s)", exprStr(v.Left), operator(string(v.Op)), exprStr(v.Right))

	case *UnaryOp:
		if v.Op == OpIsNull || v.Op == OpIsNotNull {
			return fmt.Sprintf("(%s %s)", exprStr(v.Expr), operator(string(v.Op)))
		}
		return fmt.Sprintf("(%s %s)", operator(string(v.Op)), exprStr(v.Expr))

	default:
		return fmt.Sprintf("%T", e)
	}
}

// PrettyPrintQuery returns a more readable SQL-like representation
func PrettyPrintQuery(q *Query) string {
	var sb strings.Builder

	// SELECT
	sb.WriteString(keyword("SELECT") + "\n")
	for i, sel := range q.Select {
		sb.WriteString("  ")
		sb.WriteString(exprStr(sel.Expr))
		if sel.Alias != "" {
			sb.WriteString(" " + keyword("AS") + " ")
			sb.WriteString(sel.Alias)
		}
		if i < len(q.Select)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	// FROM
	sb.WriteString(keyword("FROM") + "\n")
	sb.WriteString(tableRefStr(q.From, "  "))

	// WHERE
	if q.Where != nil {
		sb.WriteString(keyword("WHERE") + "\n")
		sb.WriteString("  " + exprStr(q.Where) + "\n")
	}

	// GROUP BY
	if len(q.GroupBy) > 0 {
		sb.WriteString(keyword("GROUP BY") + "\n")
		for i, g := range q.GroupBy {
			sb.WriteString("  " + exprStr(g))
			if i < len(q.GroupBy)-1 {
				sb.WriteString(",")
			}
			sb.WriteString("\n")
		}
	}

	// ORDER BY
	if len(q.OrderBy) > 0 {
		sb.WriteString(keyword("ORDER BY") + "\n")
		for i, o := range q.OrderBy {
			sb.WriteString("  " + exprStr(o.Expr))
			if o.Desc {
				sb.WriteString(" " + keyword("DESC"))
			}
			if i < len(q.OrderBy)-1 {
				sb.WriteString(",")
			}
			sb.WriteString("\n")
		}
	}

	// LIMIT
	if q.Limit > 0 {
		sb.WriteString(keyword("LIMIT") + " " + fmt.Sprintf("%d\n", q.Limit))
	}

	return sb.String()
}

func tableRefStr(t TableRef, indent string) string {
	switch v := t.(type) {
	case *TableScan:
		filter := ""
		if v.TypeFilter != "" {
			filter = fmt.Sprintf(" %s label = %s", keyword("WHERE"), literal(fmt.Sprintf("'%s'", v.TypeFilter)))
		}
		return fmt.Sprintf("%s%s %s %s%s\n", indent, tableName(v.Table), keyword("AS"), v.Alias, filter)

	case *Join:
		var sb strings.Builder
		sb.WriteString(tableRefStr(v.Left, indent))
		sb.WriteString(fmt.Sprintf("%s%s\n", indent, keyword(fmt.Sprintf("%s JOIN", v.Type))))
		sb.WriteString(tableRefStr(v.Right, indent+"  "))
		sb.WriteString(fmt.Sprintf("%s%s %s\n", indent+"  ", keyword("ON"), exprStr(v.On)))
		return sb.String()

	default:
		return fmt.Sprintf("%s%T\n", indent, t)
	}
}
