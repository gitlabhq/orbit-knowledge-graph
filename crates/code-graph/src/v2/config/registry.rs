use super::lang::Language;
use rustc_hash::FxHashMap;
use std::sync::LazyLock;
use strum::IntoEnumIterator;

static EXTENSION_MAP: LazyLock<FxHashMap<&'static str, Language>> = LazyLock::new(|| {
    let mut map = FxHashMap::default();
    for lang in Language::iter() {
        for ext in lang.file_extensions() {
            map.insert(*ext, lang);
        }
    }
    map
});

fn detect_language_from_extension(extension: &str) -> Option<Language> {
    EXTENSION_MAP.get(extension).copied()
}

pub fn detect_language_from_path(path: &str) -> Option<Language> {
    let name = path.rsplit('/').next().unwrap_or(path);
    if is_container_build_file(name) {
        return Some(Language::Dockerfile);
    }
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())?;
    detect_language_from_extension(ext)
}

/// `Dockerfile`, `Containerfile` and their dotted variants (`Dockerfile.dev`)
/// carry no language extension.
fn is_container_build_file(name: &str) -> bool {
    ["Dockerfile", "Containerfile"].iter().any(|base| {
        name == *base
            || name
                .strip_prefix(base)
                .is_some_and(|rest| rest.starts_with('.'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_build_files_are_detected_by_name() {
        for path in [
            "Dockerfile",
            "Containerfile",
            "Dockerfile.dev",
            "svc/Containerfile.prod",
            "build/app.dockerfile",
        ] {
            assert_eq!(
                detect_language_from_path(path),
                Some(Language::Dockerfile),
                "{path}"
            );
        }
        assert_eq!(detect_language_from_path("docs/Dockerfile-notes.md"), None);
    }

    #[test]
    fn detect_all_extensions() {
        assert_eq!(detect_language_from_extension("py"), Some(Language::Python));
        assert_eq!(detect_language_from_extension("java"), Some(Language::Java));
        assert_eq!(detect_language_from_extension("rb"), Some(Language::Ruby));
        assert_eq!(detect_language_from_extension("kt"), Some(Language::Kotlin));
        assert_eq!(detect_language_from_extension("cs"), Some(Language::CSharp));
        assert_eq!(detect_language_from_extension("rs"), Some(Language::Rust));
        assert_eq!(
            detect_language_from_extension("js"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("jsx"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("mjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("cjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("vue"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("graphql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("gql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("json"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("tsx"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("mts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("cts"),
            Some(Language::TypeScript)
        );
        assert_eq!(detect_language_from_extension("md"), None);
    }

    #[test]
    fn detect_by_path() {
        assert_eq!(
            detect_language_from_path("src/index.js"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.jsx"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.cjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/component.vue"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/query.graphql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.tsx"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.mts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/main.java"),
            Some(Language::Java)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
        assert_eq!(detect_language_from_path("Makefile"), None);
        // Pure extension lookup: minified-bundle exclusion is the CodeFilter
        // denylist's job, not this mapping's.
        assert_eq!(
            detect_language_from_path("vendor/jquery.min.js"),
            Some(Language::JavaScript)
        );
    }
}
