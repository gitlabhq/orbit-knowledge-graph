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
