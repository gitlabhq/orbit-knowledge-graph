//! Compile-time access to `config/versions.yaml`.

const VERSIONS: &str = include_str!(env!("VERSIONS_FILE"));

/// Value of `key` in `config/versions.yaml`. Panics at compile time (when
/// used in a `const`) if the key is missing.
pub const fn pinned(key: &str) -> &'static str {
    let bytes = VERSIONS.as_bytes();
    let key = key.as_bytes();
    let mut line_start = 0;
    while line_start < bytes.len() {
        let mut line_end = line_start;
        while line_end < bytes.len() && bytes[line_end] != b'\n' {
            line_end += 1;
        }
        if matches_key(bytes, line_start, line_end, key) {
            let mut value_start = line_start + key.len() + 1;
            while value_start < line_end && bytes[value_start] == b' ' {
                value_start += 1;
            }
            let mut value_end = line_end;
            while value_end > value_start && bytes[value_end - 1] == b' ' {
                value_end -= 1;
            }
            return slice_str(value_start, value_end);
        }
        line_start = line_end + 1;
    }
    panic!("key missing from config/versions.yaml")
}

/// Runtime lookup of `key` in arbitrary `versions.yaml` text (e.g. a
/// `git show` of another revision).
pub fn lookup<'a>(versions: &'a str, key: &str) -> Option<&'a str> {
    versions
        .lines()
        .find_map(|l| l.strip_prefix(key)?.strip_prefix(':'))
        .map(str::trim)
}

/// `pinned(key)` parsed as an unsigned integer.
pub const fn pinned_u32(key: &str) -> u32 {
    let bytes = pinned(key).as_bytes();
    let mut n = 0u32;
    let mut i = 0;
    while i < bytes.len() {
        assert!(bytes[i].is_ascii_digit(), "pinned value is not a u32");
        n = n * 10 + (bytes[i] - b'0') as u32;
        i += 1;
    }
    n
}

const fn matches_key(bytes: &[u8], start: usize, end: usize, key: &[u8]) -> bool {
    if end - start <= key.len() || bytes[start + key.len()] != b':' {
        return false;
    }
    let mut i = 0;
    while i < key.len() {
        if bytes[start + i] != key[i] {
            return false;
        }
        i += 1;
    }
    true
}

const fn slice_str(start: usize, end: usize) -> &'static str {
    let (_, tail) = VERSIONS.as_bytes().split_at(start);
    let (value, _) = tail.split_at(end - start);
    match core::str::from_utf8(value) {
        Ok(s) => s,
        Err(_) => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_every_pin() {
        assert!(pinned_u32("schema") > 0);
        assert!(pinned("query_dsl").contains('.'));
        assert!(pinned("raw_output_format").contains('.'));
        assert!(pinned("goon_output_format").contains('.'));
        assert!(pinned("duckdb").starts_with('v'));
        assert_eq!(pinned("gitlab_system_note_actions").len(), 40);
    }

    #[test]
    fn lookup_matches_const_scanner() {
        assert_eq!(lookup(VERSIONS, "schema"), Some(pinned("schema")));
        assert_eq!(lookup(VERSIONS, "raw"), None);
    }

    #[test]
    fn prefix_keys_do_not_match() {
        // "raw_output_format" must not be found by scanning for "raw".
        let result = std::panic::catch_unwind(|| pinned("raw"));
        assert!(result.is_err());
    }
}
