//! Relative-path validation for ontology archives and embedded assets.

use std::path::{Component, Path};

pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_safe_relative_path_accepts_normal_rejects_traversal() {
        assert!(is_safe_relative_path(Path::new("src/main.rs")));
        assert!(is_safe_relative_path(Path::new("a/b/c.txt")));
        assert!(!is_safe_relative_path(Path::new("../escape")));
        assert!(!is_safe_relative_path(Path::new("a/../../b")));
        assert!(!is_safe_relative_path(Path::new("/abs/path")));
        assert!(!is_safe_relative_path(Path::new("./rel")));
    }
}
