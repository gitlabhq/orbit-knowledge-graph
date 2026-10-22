/// `http` caps the request URI at ~64 KiB and the clickhouse crate puts every
/// param there, so a large `Array(String)` param must be split to fit.
const MAX_ARRAY_PARAM_BYTES: usize = 48 * 1024;
const MAX_URL_ENCODED_BYTES_PER_CHAR: usize = 3;
const PARAM_FRAMING_BYTES: usize = 12;

pub fn strings_per_array_param<S: AsRef<str>>(values: &[S]) -> usize {
    let longest = values
        .iter()
        .map(|value| value.as_ref().len())
        .max()
        .unwrap_or(0);
    let worst_case_bytes = longest * MAX_URL_ENCODED_BYTES_PER_CHAR + PARAM_FRAMING_BYTES;

    (MAX_ARRAY_PARAM_BYTES / worst_case_bytes).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_per_array_param_keeps_chunks_under_uri_cap() {
        let long_value = "x".repeat(500);
        let values = vec![long_value.as_str(); 100];

        let count = strings_per_array_param(&values);

        assert!(count > 1);
        let worst_case = 500 * MAX_URL_ENCODED_BYTES_PER_CHAR + PARAM_FRAMING_BYTES;
        assert!(count * worst_case <= MAX_ARRAY_PARAM_BYTES);
    }
}
