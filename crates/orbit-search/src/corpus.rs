pub const EXCLUDE_LIKE: &[&str] = &[
    "spec/%",
    "%/spec/%",
    "ee/spec/%",
    "qa/spec/%",
    "tests/%",
    "%/tests/%",
    "test/%",
    "%/test/%",
    "%_spec.rb",
    "%_test.rb",
    "%_test.rs",
    "%_test.go",
    "%.spec.%",
    "%.test.%",
    "__mocks__/%",
    "%/__mocks__/%",
    "__tests__/%",
    "%/__tests__/%",
    "fixtures/%",
    "%/fixtures/%",
    "qa/%",
    "%/qa/%",
    "%Tests.java",
    "%TestCase.java",
    "%IT.java",
    "mocks/%",
    "%/mocks/%",
    "%/mocks.go",
    "%.Test/%",
    "%.Tests/%",
    "%.pb.go",
    "%_pb.rb",
    "%.pb.cc",
    "%.pb.h",
    "%.pb.rs",
    "proto/%",
    "%/proto/%",
    "%/generated/%",
    "generated/%",
    "node_modules/%",
    "%/node_modules/%",
    "vendor/%",
    "%/vendor/%",
    "target/%",
    "%/target/%",
    "dist/%",
    "%/dist/%",
    "build/%",
    "%/build/%",
];

pub const EXCLUDE_REGEX: &[&str] = &[
    r"(^|/)[a-z]+_tests?/",
    r"(^|/)mock_[a-z_]+\.go$",
    r"(^|/)(test_[^/]+|conftest)\.py$",
    r"/src/[a-zA-Z0-9]*[Tt]est[a-zA-Z0-9]*/",
];

pub const DEFAULT_SOURCE_EXTS: &[&str] = &[
    "rs", "rb", "py", "js", "ts", "vue", "jsx", "tsx", "mjs", "cjs", "go", "java", "kt", "kts",
    "scala", "cs", "cpp", "c", "h", "hpp", "swift", "php", "rake", "md",
];

/// Config formats whose definitions are searchable but carry no signature
/// line for `repo-map` to render.
pub const CONFIG_EXTS: &[&str] = &["yml", "yaml"];

pub fn search_corpus_exts() -> Vec<String> {
    DEFAULT_SOURCE_EXTS
        .iter()
        .chain(CONFIG_EXTS)
        .map(|s| s.to_string())
        .collect()
}

pub fn ext_regex(exts: &[String]) -> String {
    let alt = exts
        .iter()
        .map(|e| regex_escape(e))
        .collect::<Vec<_>>()
        .join("|");
    format!(r"\.({alt})$")
}

fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_regex_escapes_and_anchors() {
        assert_eq!(ext_regex(&["rs".into()]), r"\.(rs)$");
        assert_eq!(ext_regex(&["c++".into()]), r"\.(c\+\+)$");
    }
}
