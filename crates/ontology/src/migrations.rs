use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

#[must_use]
pub fn embedded_sources() -> BTreeMap<String, String> {
    crate::loading::embedded_files().into_iter().collect()
}

#[must_use]
pub fn sha256_hex(input: &str) -> String {
    use std::fmt::Write as _;

    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
