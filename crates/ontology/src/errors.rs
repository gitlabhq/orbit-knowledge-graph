//! Error formatting for ontology validation messages.
//!
//! Helpers that build actionable "valid values: …" suffixes when a validation
//! check fails, capping the list so error messages stay readable.
//!
//! [`format_candidate_list`] is the shared truncation kernel used by both the
//! ontology's own field-validation errors and the compiler's JSON Schema error
//! enrichment, avoiding duplicated formatting logic.

use crate::constants::NODE_RESERVED_COLUMNS;
use crate::entities::NodeEntity;

/// Maximum number of candidates to enumerate in a validation error before
/// truncating to a count. Keeps messages actionable without dumping a full
/// allowlist onto one line; the complete list is available via the schema.
pub const MAX_CANDIDATES: usize = 10;

/// Format a capped, deduplicated candidate list for inclusion in an error
/// message.
///
/// - `label` — the noun describing the candidates (e.g. `"values"`, `"fields"`).
/// - `names` — the deduplicated candidate names, in display order.
/// - `schema_hint` — optional extra hint appended when the list is truncated
///   (e.g. `"with expand_nodes"`). Pass `None` for the default phrasing.
///
/// Returns an empty string when `names` is empty. Otherwise returns a string
/// of the form `. Valid <label>: a, b, c` or `. Valid <label> include: a, b, c
/// (and N more — call get_graph_schema[ <hint>] for the full list)`.
pub fn format_candidate_list(
    label: &str,
    names: &[impl AsRef<str>],
    schema_hint: Option<&str>,
) -> String {
    if names.is_empty() {
        return String::new();
    }

    let total = names.len();
    let shown: Vec<&str> = names
        .iter()
        .take(MAX_CANDIDATES)
        .map(|s| s.as_ref())
        .collect();
    let joined = shown.join(", ");

    if total > MAX_CANDIDATES {
        let hint = schema_hint.map(|h| format!(" {h}")).unwrap_or_default();
        format!(
            ". Valid {label} include: {joined} (and {} more \
             — call get_graph_schema{hint} for the full list)",
            total - MAX_CANDIDATES,
        )
    } else {
        format!(". Valid {label}: {joined}")
    }
}

pub(crate) fn describe_valid_fields(node: &NodeEntity) -> String {
    let mut seen = std::collections::HashSet::new();
    let names: Vec<&str> = NODE_RESERVED_COLUMNS
        .iter()
        .copied()
        .chain(node.fields.iter().map(|f| f.name.as_str()))
        .filter(|name| seen.insert(*name))
        .collect();

    format_candidate_list("fields", &names, Some("with expand_nodes"))
}
