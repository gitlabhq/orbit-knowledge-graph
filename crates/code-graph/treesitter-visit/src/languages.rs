//! Supported programming languages.

use crate::Language;
use crate::tree_sitter::{LanguageExt, TSLanguage};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents all supported languages.
/// Variants are always available, but tree-sitter parsing requires the corresponding feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SupportLang {
    Bash,
    C,
    Cpp,
    Python,
    Ruby,
    TypeScript,
    Tsx,
    JavaScript,
    Java,
    Go,
    CSharp,
    Kotlin,
    Rust,
    Php,
}

impl fmt::Display for SupportLang {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Language for SupportLang {
    fn kind_to_id(&self, kind: &str) -> u16 {
        self.get_ts_language()
            .id_for_node_kind(kind, /*named*/ true)
    }

    fn field_to_id(&self, field: &str) -> Option<u16> {
        self.get_ts_language()
            .field_id_for_name(field)
            .map(|f| f.get())
    }
}

impl LanguageExt for SupportLang {
    fn get_ts_language(&self) -> TSLanguage {
        match self {
            #[cfg(feature = "tree-sitter-bash")]
            Self::Bash => tree_sitter_bash::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-bash"))]
            Self::Bash => panic!("tree-sitter-bash feature not enabled"),

            #[cfg(feature = "tree-sitter-c")]
            Self::C => tree_sitter_c::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-c"))]
            Self::C => panic!("tree-sitter-c feature not enabled"),

            #[cfg(feature = "tree-sitter-cpp")]
            Self::Cpp => tree_sitter_cpp::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-cpp"))]
            Self::Cpp => panic!("tree-sitter-cpp feature not enabled"),

            #[cfg(feature = "tree-sitter-python")]
            Self::Python => tree_sitter_python::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-python"))]
            Self::Python => panic!("tree-sitter-python feature not enabled"),

            #[cfg(feature = "tree-sitter-ruby")]
            Self::Ruby => tree_sitter_ruby::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-ruby"))]
            Self::Ruby => panic!("tree-sitter-ruby feature not enabled"),

            #[cfg(feature = "tree-sitter-typescript")]
            Self::TypeScript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            #[cfg(not(feature = "tree-sitter-typescript"))]
            Self::TypeScript => panic!("tree-sitter-typescript feature not enabled"),

            #[cfg(feature = "tree-sitter-typescript")]
            Self::Tsx | Self::JavaScript => tree_sitter_typescript::LANGUAGE_TSX.into(),
            #[cfg(not(feature = "tree-sitter-typescript"))]
            Self::Tsx | Self::JavaScript => panic!("tree-sitter-typescript feature not enabled"),

            #[cfg(feature = "tree-sitter-java")]
            Self::Java => tree_sitter_java::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-java"))]
            Self::Java => panic!("tree-sitter-java feature not enabled"),

            #[cfg(feature = "tree-sitter-go")]
            Self::Go => tree_sitter_go::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-go"))]
            Self::Go => panic!("tree-sitter-go feature not enabled"),

            #[cfg(feature = "tree-sitter-c-sharp")]
            Self::CSharp => tree_sitter_c_sharp::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-c-sharp"))]
            Self::CSharp => panic!("tree-sitter-c-sharp feature not enabled"),

            #[cfg(feature = "tree-sitter-kotlin")]
            Self::Kotlin => tree_sitter_kotlin::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-kotlin"))]
            Self::Kotlin => panic!("tree-sitter-kotlin feature not enabled"),

            #[cfg(feature = "tree-sitter-rust")]
            Self::Rust => tree_sitter_rust::LANGUAGE.into(),
            #[cfg(not(feature = "tree-sitter-rust"))]
            Self::Rust => panic!("tree-sitter-rust feature not enabled"),

            #[cfg(feature = "tree-sitter-php")]
            Self::Php => tree_sitter_php::LANGUAGE_PHP.into(),
            #[cfg(not(feature = "tree-sitter-php"))]
            Self::Php => panic!("tree-sitter-php feature not enabled"),
        }
    }

    fn kind_names(&self) -> std::sync::Arc<[&'static str]> {
        thread_local! {
            static CACHE: std::cell::RefCell<
                std::collections::HashMap<SupportLang, std::sync::Arc<[&'static str]>>,
            > = std::cell::RefCell::new(std::collections::HashMap::new());
        }
        CACHE.with(|c| {
            if let Some(cached) = c.borrow().get(self).cloned() {
                return cached;
            }
            let ts = self.get_ts_language();
            let names: std::sync::Arc<[&'static str]> = (0..ts.node_kind_count())
                .map(|id| ts.node_kind_for_id(id as u16).unwrap_or(""))
                .collect();
            c.borrow_mut().insert(*self, names.clone());
            names
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "tree-sitter-python")]
    fn test_python_parsing() {
        let root = SupportLang::Python.ast_grep("def hello(): pass");
        assert!(!root.source().is_empty());
    }

    #[test]
    #[cfg(feature = "tree-sitter-typescript")]
    fn test_typescript_parsing() {
        let root = SupportLang::TypeScript.ast_grep("function hello() {}");
        assert!(!root.source().is_empty());
    }

    #[test]
    #[cfg(feature = "tree-sitter-ruby")]
    fn test_ruby_parsing() {
        let root = SupportLang::Ruby.ast_grep("def hello; end");
        assert!(!root.source().is_empty());
    }

    #[test]
    #[cfg(feature = "tree-sitter-php")]
    fn test_php_parsing() {
        let root = SupportLang::Php.ast_grep("<?php function hello() {}");
        assert!(!root.source().is_empty());
    }
}
