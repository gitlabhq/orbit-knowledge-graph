//! pest parser, syntax-error rendering, and the table of constructs the
//! grammar parses only so they can be rejected with a hint.

use compiler::QueryError;
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct CypherParser;

pub type P<'i> = Pair<'i, Rule>;

pub fn parse(source: &str) -> Result<P<'_>, QueryError> {
    let mut pairs = CypherParser::parse(Rule::statement, source)
        .map_err(|e| QueryError::Syntax(render_error(source, &e)))?;
    Ok(pairs.next().expect("statement rule produces one pair"))
}

pub fn line_col(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let before = &source[..offset];
    let line = before.matches('\n').count() + 1;
    let col = before.rfind('\n').map_or(offset, |i| offset - i - 1) + 1;
    (line, col)
}

pub fn located(source: &str, offset: usize, msg: impl AsRef<str>) -> String {
    let (line, col) = line_col(source, offset);
    format!("line {line}, column {col}: {}", msg.as_ref())
}

/// Walks the parse tree and rejects every construct openCypher spells but the
/// DSL cannot express. Shape-dependent rejections (AS in a traversal, an
/// undirected edge in an aggregation) live with the query-type lowering.
pub fn reject_unsupported(source: &str, statement: &P<'_>) -> Result<(), QueryError> {
    let mut stack = vec![statement.clone()];
    while let Some(pair) = stack.pop() {
        if let Some(msg) = unsupported(&pair) {
            return Err(QueryError::Validation(located(
                source,
                pair.as_span().start(),
                msg,
            )));
        }
        // IS NOT NULL is the one place NOT is fine.
        if pair.as_rule() != Rule::null_test {
            stack.extend(pair.into_inner());
        }
    }
    Ok(())
}

fn unsupported(pair: &P<'_>) -> Option<&'static str> {
    let has = |rule: Rule| pair.clone().into_inner().any(|c| c.as_rule() == rule);
    let count = |rule: Rule| {
        pair.clone()
            .into_inner()
            .filter(|c| c.as_rule() == rule)
            .count()
    };
    Some(match pair.as_rule() {
        Rule::kw_optional => "OPTIONAL MATCH is not supported; every pattern is required",
        Rule::kw_distinct => {
            "DISTINCT is not supported: results are deduplicated entity sets and the DSL has no distinct aggregate"
        }
        Rule::skip_clause => {
            "SKIP/OFFSET are not supported; paginate with the keyset cursor supplied beside the statement"
        }
        Rule::nulls_order => "NULLS FIRST/LAST is not supported; NULL sort keys always sort last",
        Rule::kw_not => {
            "NOT is not supported; filters are conjunctions (use IS NOT NULL, or IN with the complementary set)"
        }
        Rule::kw_or | Rule::kw_xor => {
            "OR/XOR are not supported; filters on one selector AND-combine"
        }
        Rule::inline_where => {
            "WHERE inside a pattern is GQL syntax; move the predicate to the statement WHERE"
        }
        Rule::gql_quantifier => {
            "`{m,n}` is the GQL quantifier; openCypher spells a hop range `*m..n` inside the brackets"
        }
        Rule::gql_arrow_left | Rule::gql_arrow_right | Rule::gql_dash => {
            "`->`, `<-`, `-` are GQL abbreviations; openCypher spells them `-->`, `<--`, `--`"
        }
        Rule::gql_temporal => {
            "`DATE '...'` is the GQL literal; write date('...') or a plain string, the column type drives the binding"
        }
        Rule::null => "NULL is not a value; use IS NULL or IS NOT NULL",
        Rule::comp_op if matches!(pair.as_str(), "<>" | "!=") => {
            "`<>`/`!=` are not supported (no not-equal operator); for enumerations use IN with the complementary set"
        }
        Rule::comp_op if pair.as_str() == "=~" => {
            "`=~` regex matching is not supported; use STARTS WITH, ENDS WITH, CONTAINS, or a token function"
        }
        Rule::path_fn_name if !pair.as_str().eq_ignore_ascii_case("shortestPath") => {
            "allShortestPaths() is not supported; only shortestPath() exists"
        }
        Rule::func_arg if has(Rule::star) => {
            "count(*) is not supported; name the node to count, e.g. count(n)"
        }
        Rule::node_pattern if count(Rule::node_label) > 1 => {
            "a node has exactly one label (one entity per selector)"
        }
        Rule::node_pattern if !has(Rule::ident) => {
            "every node needs a variable, e.g. `(n:Label)`; the DSL uses it as the selector id"
        }
        Rule::pattern if has(Rule::ident) && !has(Rule::path_function) => {
            "path variables are only supported with shortestPath(); traversal results are entity sets, not paths"
        }
        Rule::order_by_clause if count(Rule::sort_item) > 1 => "ORDER BY takes exactly one key",
        Rule::statement if count(Rule::match_clause) > 1 => {
            "only one MATCH clause is supported; join patterns with a comma or a shared variable"
        }
        _ => return None,
    })
}

fn render_error(source: &str, err: &pest::error::Error<Rule>) -> String {
    let offset = match err.location {
        pest::error::InputLocation::Pos(p) | pest::error::InputLocation::Span((p, _)) => p,
    };
    let found = token_at(source, offset);
    let mut msg = if found.is_empty() {
        located(source, offset, "unexpected end of statement")
    } else {
        located(source, offset, format!("unexpected `{found}`"))
    };
    if let pest::error::ErrorVariant::ParsingError { positives, .. } = &err.variant {
        let mut expected: Vec<String> = positives.iter().map(|r| describe(*r)).collect();
        expected.sort_unstable();
        expected.dedup();
        if !expected.is_empty() {
            msg.push_str(&format!("; expected {}", expected.join(", ")));
        }
    }
    if let Some(hint) = hint_for(&found) {
        msg.push_str(&format!(". {hint}"));
    }
    msg
}

fn token_at(source: &str, offset: usize) -> String {
    let rest = source[offset.min(source.len())..].trim_start();
    let Some(first) = rest.chars().next() else {
        return String::new();
    };
    if first.is_alphanumeric() || first == '_' {
        return rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
    }
    let two: String = rest.chars().take(2).collect();
    match two.as_str() {
        "<>" | "!=" | "=~" | "<-" | "->" | "--" | ".." => two,
        _ => first.to_string(),
    }
}

/// Keyword rules read as their keyword; the rest get a short noun.
fn describe(rule: Rule) -> String {
    let name = format!("{rule:?}");
    if let Some(kw) = name
        .strip_prefix("kw_")
        .or_else(|| name.strip_suffix("_kw"))
    {
        return kw.to_ascii_uppercase();
    }
    match rule {
        Rule::EOI => "end of statement",
        Rule::pattern | Rule::chain | Rule::node_pattern => "a node pattern `(var:Label)`",
        Rule::edge_pattern | Rule::bracket_edge | Rule::arrow_edge => "a relationship `-[:TYPE]->`",
        Rule::edge_body => "a relationship body `[var:TYPE*1..3 {prop: value}]`",
        Rule::node_label | Rule::type_spec => ":Label",
        Rule::range_literal => "*min..max",
        Rule::property_map | Rule::property_kv => "{prop: value}",
        Rule::condition | Rule::term | Rule::predicate | Rule::paren_condition => "a predicate",
        Rule::predicate_tail | Rule::comp_op => "a comparison operator",
        Rule::operand | Rule::return_item | Rule::return_body | Rule::sort_item => "an expression",
        Rule::func_call | Rule::func_arg => "a function call",
        Rule::property_ref => "var.property",
        Rule::ident | Rule::plain_ident | Rule::backtick_ident => "an identifier",
        Rule::value | Rule::list | Rule::string | Rule::integer | Rule::float | Rule::boolean => {
            "a value"
        }
        Rule::parameter | Rule::parameter_name => "$parameter",
        Rule::path_function | Rule::path_fn_name => "shortestPath(",
        _ => return name.replace('_', " "),
    }
    .to_string()
}

/// Hints keyed by the token the parser stopped at. Each names the DSL
/// limitation or the openCypher spelling, never a parser state.
fn hint_for(token: &str) -> Option<&'static str> {
    Some(match token.to_ascii_uppercase().as_str() {
        "CREATE" | "MERGE" | "SET" | "REMOVE" | "DELETE" | "DETACH" | "INSERT" | "DROP"
        | "ALTER" => "The openCypher frontend is read-only: only MATCH ... RETURN is accepted.",
        "CALL" | "YIELD" | "USE" | "SESSION" | "START" | "COMMIT" | "ROLLBACK" => {
            "Procedures, graph selection, and transactions are not supported."
        }
        "WITH" | "UNWIND" | "NEXT" | "LET" | "FOR" | "FILTER" => {
            "One MATCH ... RETURN statement per query; WITH/UNWIND chaining is not supported."
        }
        "UNION" | "EXCEPT" | "INTERSECT" | "OTHERWISE" => "Composite queries are not supported.",
        "EXISTS" | "CASE" | "WHEN" | "THEN" | "ELSE" | "COALESCE" => {
            "Only `var.property <op> value` predicates are supported; no EXISTS, CASE, or COALESCE."
        }
        "END" | "ALL" | "OF" | "ON" | "DO" | "ADD" | "UNIQUE" | "REQUIRE" | "CONSTRAINT"
        | "MANDATORY" | "SCALAR" => {
            "This word is reserved in openCypher; write it in backticks to use it as a variable."
        }
        "ANY" | "SHORTEST" | "WALK" | "TRAIL" | "SIMPLE" | "ACYCLIC" => {
            "GQL path prefixes and modes are not supported; use shortestPath((a)-[:TYPE*..3]->(b))."
        }
        "GROUP" => "GROUP BY is not openCypher; every non-aggregate RETURN item is a group key.",
        "~" => "`~` edges and `=~` regex matching are not supported; use -[:TYPE]- for undirected.",
        "+" | "/" | "%" | "^" | "*" => {
            "Arithmetic is not supported; compare a property with a literal or parameter."
        }
        "&" | "!" | "|" => {
            "Label expressions are not supported: one label per node; relationship types combine as :A|B."
        }
        "$" => {
            "Parameters stand in for a value, an IN list, or LIMIT; parameter maps `(n $props)` are not supported."
        }
        "[" => {
            "List comprehensions and indexing are not supported; `[...]` is only valid after IN."
        }
        "{" => "Map projection `u {.a, .b}` is not openCypher 9; write `u, u.a, u.b`.",
        "." => "Nested property access is not supported; use `var.property`.",
        ">" => "A relationship has one direction: -[]->, <-[]-, or -[]-.",
        ":" => "Relationship types combine with `|` (`:A|B`), not `:`; a node has one label.",
        _ => return None,
    })
}
