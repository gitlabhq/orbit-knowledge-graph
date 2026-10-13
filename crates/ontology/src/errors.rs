//! Error formatting for ontology validation messages.
//!
//! Helpers that build actionable "valid values: …" suffixes when a validation
//! check fails.
//!
//! [`format_candidate_list`] is shared by both the ontology's own
//! field-validation errors and the compiler's JSON Schema error enrichment.

/// Format a deduplicated candidate list for inclusion in an error message.
///
/// - `label` — the noun describing the candidates (e.g. `"values"`, `"fields"`).
/// - `names` — the deduplicated candidate names, in display order.
///
/// Returns an empty string when `names` is empty.
pub fn format_candidate_list(label: &str, names: &[impl AsRef<str>]) -> String {
    if names.is_empty() {
        return String::new();
    }

    let joined = names
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ");
    format!(". Valid {label}: {joined}")
}
