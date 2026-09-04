//! openCypher text frontend for the graph query compiler.
//!
//! Parses one read-only statement in an openCypher 9 (M23) subset and lowers
//! it to [`compiler::Input`], the same structure the JSON frontend
//! deserializes. The statement never becomes DSL JSON and is never checked
//! against `graph_query.schema.json`; it enters the compiler at
//! [`compiler::compile_from_input`], so the Input-level validators and every
//! later pass run unchanged. The checks only the JSON schema layers provide
//! (identifier shape, `limit` and `cursor.page_size` bounds, literal sizes)
//! are enforced here against the constants below.
//!
//! Design: `docs/design-documents/querying/opencypher_frontend.md`.

mod lower;

use compiler::input::InputCursor;
use compiler::{CompiledQueryContext, Input, QueryError, SecurityContext};
use ontology::Ontology;

pub use lower::is_valid_identifier;

/// Request-level parameter bindings for `$name` placeholders.
pub type Parameters = serde_json::Map<String, serde_json::Value>;

// Mirrors of `config/schemas/graph_query.schema.json`. The compiler re-checks
// every other cap in `Validator::check_depth`; these are the ones only the
// schema enforces for the JSON frontend.
/// `limit` maximum.
pub const MAX_LIMIT: u32 = 1000;
/// `cursor.page_size` maximum.
pub const MAX_PAGE_SIZE: u32 = 1000;
/// `FilterValue` string `maxLength`.
pub const MAX_FILTER_STRING_CHARS: usize = 1024;
/// `FilterValue` list `maxItems`.
pub const MAX_IN_VALUES: usize = 100;

// Parser hardening. Not configurable: they describe what a well-formed
// statement can look like, and a cap that moved between environments would
// make the byte-equivalence suite environment-dependent.
/// Longest statement the parser accepts.
pub const MAX_STATEMENT_BYTES: usize = 64 * 1024;
/// Deepest bracket nesting (`(`, `[`, `{`) the parser accepts; the grammar
/// recurses on parenthesized conditions and list literals.
pub const MAX_NESTING_DEPTH: usize = 32;
/// Most `$name` bindings a request may carry.
pub const MAX_PARAMETERS: usize = 64;
/// Largest serialized parameter object.
pub const MAX_PARAMETERS_BYTES: usize = 64 * 1024;

/// Parse and lower one statement. Parameters are substituted before the
/// query hash is taken, so a cursor issued for `LIMIT $n` under one binding
/// is rejected under another.
pub fn lower(
    statement: &str,
    parameters: &Parameters,
    ontology: &Ontology,
) -> compiler::Result<Input> {
    check_size(statement)?;
    check_nesting(statement)?;
    check_parameters(parameters)?;
    let (input, query_hash) = lower::lower(statement, parameters, ontology)?;
    Ok(input.with_query_hash(query_hash))
}

/// Attach keyset pagination. `after` is decoded against the query hash by the
/// compiler's `validate_input` phase.
pub fn attach_cursor(
    input: &mut Input,
    page_size: u32,
    after: Option<String>,
) -> compiler::Result<()> {
    if page_size == 0 || page_size > MAX_PAGE_SIZE {
        return Err(QueryError::Validation(format!(
            "cursor.page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}"
        )));
    }
    input.cursor = Some(InputCursor::new(page_size, after));
    Ok(())
}

/// Lower and compile in one step.
pub fn compile(
    statement: &str,
    parameters: &Parameters,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> compiler::Result<CompiledQueryContext> {
    let input = lower(statement, parameters, ontology)?;
    compiler::compile_from_input(input, ontology, ctx)
}

fn check_size(statement: &str) -> compiler::Result<()> {
    if statement.len() > MAX_STATEMENT_BYTES {
        return Err(QueryError::Syntax(format!(
            "statement is {} bytes; the maximum is {MAX_STATEMENT_BYTES}",
            statement.len()
        )));
    }
    Ok(())
}

fn check_parameters(parameters: &Parameters) -> compiler::Result<()> {
    if parameters.len() > MAX_PARAMETERS {
        return Err(QueryError::LimitExceeded(format!(
            "parameter count ({}) must not exceed {MAX_PARAMETERS}",
            parameters.len()
        )));
    }
    let bytes = serde_json::to_vec(parameters).map_or(usize::MAX, |v| v.len());
    if bytes > MAX_PARAMETERS_BYTES {
        return Err(QueryError::LimitExceeded(format!(
            "parameters ({bytes} bytes serialized) must not exceed {MAX_PARAMETERS_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Counts bracket depth outside string and backtick literals so a statement
/// made of open parentheses is rejected before the recursive parser sees it.
fn check_nesting(statement: &str) -> compiler::Result<()> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (offset, c) in statement.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' && q != '`' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => quote = Some(c),
            '(' | '[' | '{' => {
                depth += 1;
                if depth > MAX_NESTING_DEPTH {
                    let (line, col) = lower::line_col(statement, offset);
                    return Err(QueryError::Syntax(format!(
                        "line {line}, column {col}: brackets nest deeper than {MAX_NESTING_DEPTH}"
                    )));
                }
            }
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}
