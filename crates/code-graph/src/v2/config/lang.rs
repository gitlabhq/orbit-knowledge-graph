use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};
use treesitter_visit::SupportLang;

/// Groups of languages that can resolve symbols across each other's
/// files. Languages in the same family share a single `CodeGraph`
/// during pipeline processing, so cross-language imports (e.g. C++
/// `#include`-ing a C header) resolve naturally.
///
/// Languages not in a multi-language family get their own
/// single-language family via `Standalone`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LanguageFamily {
    /// C and C++ share an include-based resolution graph. The C++
    /// tree-sitter grammar is a superset of C, so `.h` headers
    /// parsed with the C grammar coexist cleanly with `.cpp` files.
    CFamily,
    /// Java and Kotlin compile to the same bytecode and share
    /// package-based FQN resolution. Fully bidirectional.
    Jvm,
    /// Standalone language: gets its own isolated CodeGraph.
    Standalone(Language),
}

impl std::fmt::Display for LanguageFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CFamily => write!(f, "c_family"),
            Self::Jvm => write!(f, "jvm"),
            Self::Standalone(lang) => write!(f, "{lang}"),
        }
    }
}

// Declares the Language enum and all per-variant property methods from
// a single declarative table.
macro_rules! define_languages {
    ($(
        $variant:ident => {
            $(support_lang: $sl:ident,)?
            extensions: [$($ext:literal),+ $(,)?],
            separator: $sep:literal,
            names: [$($name:literal),+ $(,)?] $(,)?
        }
    ),+ $(,)?) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash,
            Serialize, Deserialize,
            EnumIter, EnumString, AsRefStr, Display,
        )]
        #[strum(serialize_all = "snake_case")]
        pub enum Language {
            $($variant),+
        }

        impl Language {
            pub const fn file_extensions(&self) -> &'static [&'static str] {
                match self { $(Self::$variant => &[$($ext),+]),+ }
            }


            pub const fn fqn_separator(&self) -> &'static str {
                match self { $(Self::$variant => $sep),+ }
            }

            pub const fn names(&self) -> &'static [&'static str] {
                match self { $(Self::$variant => &[$($name),+]),+ }
            }

            /// Returns the tree-sitter grammar for this language, if one exists.
            /// Custom-parser languages (e.g. Vue, Svelte) return `None`.
            pub const fn to_support_lang(&self) -> Option<SupportLang> {
                match self {
                    $(Self::$variant => define_languages!(@support_lang $($sl)?),)+
                }
            }

            /// Returns the [`LanguageFamily`] this language belongs to.
            /// Languages in the same family share a `CodeGraph` during
            /// pipeline processing and can resolve symbols across each other.
            pub const fn family(&self) -> LanguageFamily {
                match self {
                    Self::C | Self::Cpp => LanguageFamily::CFamily,
                    Self::Java | Self::Kotlin => LanguageFamily::Jvm,
                    _ => LanguageFamily::Standalone(*self),
                }
            }

            /// Parse with tree-sitter, optionally CPU-budgeted; `Err` if there is no grammar or the parse aborted.
            pub fn parse_ast(
                &self,
                code: &str,
                budget: Option<std::time::Duration>,
            ) -> Result<
                treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>>,
                String,
            > {
                let lang = self
                    .to_support_lang()
                    .ok_or_else(|| format!("{self} has no tree-sitter grammar"))?;
                let mut guard = treesitter_visit::ParseGuard::default();
                if let Some(b) = budget {
                    guard = guard.with_budget(b);
                }
                treesitter_visit::Root::try_new(code, lang, &guard)
            }
        }
    };
    (@support_lang $sl:ident) => { Some(SupportLang::$sl) };
    (@support_lang) => { None };
}

define_languages! {
    Bash => {
        support_lang: Bash,
        extensions: ["sh", "bash", "zsh"],
        separator: ".",
        names: ["bash", "shell", "sh"],
    },
    C => {
        support_lang: C,
        extensions: ["c", "h"],
        separator: "::",
        names: ["c"],
    },
    Cpp => {
        support_lang: Cpp,
        extensions: ["cpp", "cc", "cxx", "hpp", "hh", "hxx"],
        separator: "::",
        names: ["cpp", "c++"],
    },
    Elixir => {
        support_lang: Elixir,
        extensions: ["ex", "exs"],
        separator: ".",
        names: ["elixir"],
    },
    Ruby => {
        support_lang: Ruby,
        extensions: ["rb", "rbw", "rake", "gemspec"],
        separator: "::",
        names: ["ruby"],
    },
    Python => {
        support_lang: Python,
        extensions: ["py"],
        separator: ".",
        names: ["python"],
    },
    JavaScript => {
        support_lang: JavaScript,
        // `.svelte` and `.astro` are intentionally absent: OXC's
        // PartialLoader accepts them, but the pipeline has no test
        // coverage for either, so we do not claim support. Add them
        // back alongside the first fixture suite that exercises them.
        extensions: [
            "js",
            "jsx",
            "mjs",
            "cjs",
            "vue",
            "graphql",
            "gql",
            "json"
        ],
        separator: "::",
        names: ["javascript", "js"],
    },
    TypeScript => {
        support_lang: TypeScript,
        extensions: ["ts", "tsx", "mts", "cts"],
        separator: "::",
        names: ["typescript", "ts"],
    },
    Kotlin => {
        support_lang: Kotlin,
        extensions: ["kt", "kts"],
        separator: ".",
        names: ["kotlin"],
    },
    CSharp => {
        support_lang: CSharp,
        extensions: ["cs"],
        separator: ".",
        names: ["csharp"],
    },
    Java => {
        support_lang: Java,
        extensions: ["java"],
        separator: ".",
        names: ["java"],
    },
    Go => {
        support_lang: Go,
        extensions: ["go"],
        separator: ".",
        names: ["go", "golang"],
    },
    Rust => {
        support_lang: Rust,
        extensions: ["rs"],
        separator: "::",
        names: ["rust"],
    },
    Php => {
        support_lang: Php,
        extensions: ["php", "phtml", "php3", "php4", "php5", "php7", "phps"],
        separator: "\\",
        names: ["php"],
    },
    Hcl => {
        support_lang: Hcl,
        extensions: ["tf", "tfvars"],
        separator: ".",
        names: ["hcl", "terraform"],
    },
    Swift => {
        support_lang: Swift,
        extensions: ["swift"],
        separator: ".",
        names: ["swift"],
    },
    Lua => {
        support_lang: Lua,
        extensions: ["lua"],
        separator: ".",
        names: ["lua"],
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn javascript_support_lang_is_distinct_from_typescript() {
        assert_eq!(
            Language::JavaScript.to_support_lang(),
            Some(SupportLang::JavaScript)
        );
        assert_eq!(
            Language::TypeScript.to_support_lang(),
            Some(SupportLang::TypeScript)
        );
    }

    #[test]
    fn javascript_and_typescript_extensions_are_split() {
        assert_eq!(
            Language::JavaScript.file_extensions(),
            &["js", "jsx", "mjs", "cjs", "vue", "graphql", "gql", "json"]
        );
        assert_eq!(
            Language::TypeScript.file_extensions(),
            &["ts", "tsx", "mts", "cts"]
        );
    }
}
