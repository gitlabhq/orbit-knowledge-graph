pub use code_graph_types::{
    CanonicalDefinition, CanonicalFileResult, CanonicalFqn, CanonicalImport, CanonicalReference,
    DefKind, FqnPart, ToCanonical,
};

use crate::parser::SupportedLanguage;

/// Per-language configuration for canonical conversion.
pub struct LangConfig {
    pub fqn_separator: &'static str,
}

pub fn lang_config(lang: SupportedLanguage) -> LangConfig {
    match lang {
        SupportedLanguage::Ruby => LangConfig {
            fqn_separator: "::",
        },
        SupportedLanguage::Python => LangConfig { fqn_separator: "." },
        SupportedLanguage::Kotlin => LangConfig { fqn_separator: "." },
        SupportedLanguage::Java => LangConfig { fqn_separator: "." },
        SupportedLanguage::CSharp => LangConfig { fqn_separator: "." },
        SupportedLanguage::TypeScript => LangConfig {
            fqn_separator: "::",
        },
        SupportedLanguage::Rust => LangConfig {
            fqn_separator: "::",
        },
    }
}
