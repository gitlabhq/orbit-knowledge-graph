use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};
use treesitter_visit::{LanguageExt, SupportLang};

// Declares the Language enum and all per-variant property methods from
// a single declarative table.
macro_rules! define_languages {
    ($(
        $variant:ident => {
            support_lang: $sl:ident,
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

            pub const fn to_support_lang(&self) -> SupportLang {
                match self { $(Self::$variant => SupportLang::$sl),+ }
            }

            pub fn parse_ast(
                &self,
                code: &str,
            ) -> treesitter_visit::Root<treesitter_visit::tree_sitter::StrDoc<SupportLang>> {
                self.to_support_lang().ast_grep(code)
            }
        }
    };
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
    TypeScript => {
        support_lang: TypeScript,
        extensions: ["ts", "js"],
        exclude: ["min.js"],
        separator: "::",
        names: ["typescript", "javascript"],
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
    Rust => {
        support_lang: Rust,
        extensions: ["rs"],
        exclude: [],
        separator: "::",
        names: ["rust"],
    },
}
