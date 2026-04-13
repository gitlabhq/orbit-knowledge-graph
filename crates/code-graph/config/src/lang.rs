use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};
use treesitter_visit::{LanguageExt, SupportLang};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    EnumString,
    AsRefStr,
    Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum Language {
    Ruby,
    Python,
    TypeScript,
    Kotlin,
    CSharp,
    Java,
    Rust,
}

impl Language {
    pub const fn file_extensions(&self) -> &'static [&'static str] {
        match self {
            Self::Ruby => &["rb", "rbw", "rake", "gemspec"],
            Self::Python => &["py"],
            Self::TypeScript => &["ts", "js"],
            Self::Kotlin => &["kt", "kts"],
            Self::CSharp => &["cs"],
            Self::Java => &["java"],
            Self::Rust => &["rs"],
        }
    }

    pub const fn exclude_extensions(&self) -> &'static [&'static str] {
        match self {
            Self::TypeScript => &["min.js"],
            _ => &[],
        }
    }

    pub const fn fqn_separator(&self) -> &'static str {
        match self {
            Self::Ruby | Self::TypeScript | Self::Rust => "::",
            Self::Python | Self::Kotlin | Self::CSharp | Self::Java => ".",
        }
    }

    pub const fn names(&self) -> &'static [&'static str] {
        match self {
            Self::Ruby => &["ruby"],
            Self::Python => &["python"],
            Self::TypeScript => &["typescript", "javascript"],
            Self::Kotlin => &["kotlin"],
            Self::CSharp => &["csharp"],
            Self::Java => &["java"],
            Self::Rust => &["rust"],
        }
    }

    pub const fn to_support_lang(&self) -> SupportLang {
        match self {
            Self::Ruby => SupportLang::Ruby,
            Self::Python => SupportLang::Python,
            Self::TypeScript => SupportLang::TypeScript,
            Self::Kotlin => SupportLang::Kotlin,
            Self::CSharp => SupportLang::CSharp,
            Self::Java => SupportLang::Java,
            Self::Rust => SupportLang::Rust,
        }
    }

    pub fn parse_ast(
        &self,
        code: &str,
    ) -> treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>> {
        self.to_support_lang().ast_grep(code)
    }
}
