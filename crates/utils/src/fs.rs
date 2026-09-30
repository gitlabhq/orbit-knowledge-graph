use std::path::Path;

/// `true` when `path` has only normal components — safe to join under a root,
/// with no `..`/`.`/root/prefix that could climb out.
pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
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
