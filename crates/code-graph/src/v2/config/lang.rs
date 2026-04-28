use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};
use treesitter_visit::{LanguageExt, SupportLang};

// Declares the Language enum and all per-variant property methods from
// a single declarative table.
macro_rules! define_languages {
    ($(
        $variant:ident => {
            $(support_lang: $sl:ident,)?
            extensions: [$($ext:literal),+ $(,)?],
            exclude: [$($excl:literal),* $(,)?],
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

            pub const fn exclude_extensions(&self) -> &'static [&'static str] {
                match self { $(Self::$variant => &[$($excl),*]),+ }
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

            /// Parse source with tree-sitter. Panics if the language has no grammar.
            pub fn parse_ast(
                &self,
                code: &str,
            ) -> treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>> {
                self.to_support_lang()
                    .unwrap_or_else(|| panic!("{self} has no tree-sitter grammar"))
                    .ast_grep(code)
            }
        }
    };
    (@support_lang $sl:ident) => { Some(SupportLang::$sl) };
    (@support_lang) => { None };
}

define_languages! {
    Ruby => {
        support_lang: Ruby,
        extensions: ["rb", "rbw", "rake", "gemspec"],
        exclude: [],
        separator: "::",
        names: ["ruby"],
    },
    Python => {
        support_lang: Python,
        extensions: ["py"],
        exclude: [],
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
        exclude: [".min.js"],
        separator: "::",
        names: ["javascript", "js"],
    },
    TypeScript => {
        support_lang: TypeScript,
        extensions: ["ts", "tsx", "mts", "cts"],
        exclude: [],
        separator: "::",
        names: ["typescript", "ts"],
    },
    Kotlin => {
        support_lang: Kotlin,
        extensions: ["kt", "kts"],
        exclude: [],
        separator: ".",
        names: ["kotlin"],
    },
    CSharp => {
        support_lang: CSharp,
        extensions: ["cs"],
        exclude: [],
        separator: ".",
        names: ["csharp"],
    },
    Java => {
        support_lang: Java,
        extensions: ["java"],
        exclude: [],
        separator: ".",
        names: ["java"],
    },
    Go => {
        support_lang: Go,
        extensions: ["go"],
        exclude: ["_test.go"],
        separator: ".",
        names: ["go", "golang"],
    },
    Rust => {
        support_lang: Rust,
        extensions: ["rs"],
        exclude: [],
        separator: "::",
        names: ["rust"],
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
