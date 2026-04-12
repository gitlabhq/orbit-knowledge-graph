use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ruby => "ruby",
            Self::Python => "python",
            Self::TypeScript => "typescript",
            Self::Kotlin => "kotlin",
            Self::CSharp => "csharp",
            Self::Java => "java",
            Self::Rust => "rust",
        }
    }

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
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
