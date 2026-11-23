#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, strum::IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum SupportLang {
    Bash,
    C,
    Cpp,
    #[serde(rename = "csharp")]
    CSharp,
    Dockerfile,
    Elixir,
    Go,
    Haskell,
    Hcl,
    Java,
    JavaScript,
    Kotlin,
    Lua,
    Markdown,
    #[serde(rename = "ocaml")]
    OCaml,
    Php,
    Python,
    Ruby,
    Rust,
    Scala,
    Swift,
    TypeScript,
    Tsx,
    Zig,
}

#[derive(serde::Deserialize)]
pub struct LangEntry {
    extensions: Vec<String>,
    #[serde(default)]
    file_names: Option<FileNames>,
    #[serde(default)]
    aliases: Vec<String>,
    grammar: String,
    #[serde(default)]
    fqn_separator: Option<String>,
    #[serde(default, rename = "package_markers")]
    _package_markers: Vec<String>,
    #[serde(default)]
    index_names: Vec<String>,
    #[serde(default, rename = "source_root")]
    _source_root: Option<String>,
}

/// Case-insensitive file-name rules for languages whose files often carry no
/// language extension (`Dockerfile`, `app.Dockerfile`, `Dockerfile.dev`).
/// They apply only when the file's extension names no language.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileNames {
    #[serde(default)]
    exact: Vec<String>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    suffixes: Vec<String>,
    /// Extensions that mark a template or backup, never the language itself.
    #[serde(default)]
    ignored_extensions: Vec<String>,
}

impl FileNames {
    fn matches(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        let ignored = name
            .rsplit_once('.')
            .is_some_and(|(_, ext)| self.ignored_extensions.iter().any(|i| i == ext));
        !ignored
            && (self.exact.contains(&name)
                || self.prefixes.iter().any(|p| name.starts_with(p.as_str()))
                || self.suffixes.iter().any(|s| name.ends_with(s.as_str())))
    }
}

impl LangEntry {
    pub fn extensions(&self) -> &[String] {
        &self.extensions
    }
}

#[derive(serde::Deserialize)]
struct Family {
    members: Vec<SupportLang>,
}

#[derive(serde::Deserialize)]
struct LangConfig {
    #[serde(default)]
    families: std::collections::HashMap<String, Family>,
    languages: std::collections::HashMap<SupportLang, LangEntry>,
}

pub fn all_languages() -> impl Iterator<Item = (SupportLang, &'static LangEntry)> {
    LANG_CONFIG.languages.iter().map(|(l, e)| (*l, e))
}

static LANG_CONFIG: std::sync::LazyLock<LangConfig> = std::sync::LazyLock::new(|| {
    let yaml = include_str!("../config/languages.yaml");
    orbit_utils::yaml::from_str(yaml).expect("failed to parse languages.yaml")
});

pub fn lang_yaml(lang_id: SupportLang) -> Option<&'static str> {
    match lang_id {
        SupportLang::Python => Some(include_str!("../langs/python.yaml")),
        SupportLang::TypeScript | SupportLang::Tsx | SupportLang::JavaScript => {
            Some(include_str!("../langs/typescript.yaml"))
        }
        SupportLang::Rust => Some(include_str!("../langs/rust.yaml")),
        SupportLang::Go => Some(include_str!("../langs/go.yaml")),
        SupportLang::Php => Some(include_str!("../langs/php.yaml")),
        SupportLang::Java => Some(include_str!("../langs/java.yaml")),
        SupportLang::Kotlin => Some(include_str!("../langs/kotlin.yaml")),
        SupportLang::Ruby => Some(include_str!("../langs/ruby.yaml")),
        SupportLang::CSharp => Some(include_str!("../langs/csharp.yaml")),
        SupportLang::C => Some(include_str!("../langs/c.yaml")),
        SupportLang::Cpp => Some(include_str!("../langs/cpp.yaml")),
        SupportLang::Scala => Some(include_str!("../langs/scala.yaml")),
        SupportLang::Bash => Some(include_str!("../langs/bash.yaml")),
        SupportLang::Dockerfile => Some(include_str!("../langs/dockerfile.yaml")),
        SupportLang::Elixir => Some(include_str!("../langs/elixir.yaml")),
        SupportLang::Lua => Some(include_str!("../langs/lua.yaml")),
        SupportLang::Swift => Some(include_str!("../langs/swift.yaml")),
        SupportLang::Zig => Some(include_str!("../langs/zig.yaml")),
        SupportLang::Markdown => Some(include_str!("../langs/markdown.yaml")),
        _ => None,
    }
}

impl SupportLang {
    pub fn from_extension(ext: &str) -> Option<Self> {
        for (lang, entry) in &LANG_CONFIG.languages {
            if entry.extensions.iter().any(|e| e == ext) {
                return Some(*lang);
            }
        }
        None
    }

    pub fn from_path(path: &str) -> Option<Self> {
        let extension = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str());
        if let Some(lang) = extension.and_then(Self::from_extension) {
            return Some(lang);
        }
        let name = path.rsplit('/').next().unwrap_or(path);
        LANG_CONFIG
            .languages
            .iter()
            .find(|(_, entry)| entry.file_names.as_ref().is_some_and(|f| f.matches(name)))
            .map(|(lang, _)| *lang)
    }

    /// The family this language indexes with, from `families:` in
    /// `config/languages.yaml`; a language in none stands alone under its own name.
    pub fn family(self) -> &'static str {
        LANG_CONFIG
            .families
            .iter()
            .find(|(_, f)| f.members.contains(&self))
            .map_or_else(|| self.into(), |(name, _)| name.as_str())
    }

    /// Whether a rule file exists for the language; a grammar alone only
    /// lists files.
    pub fn has_rules(self) -> bool {
        lang_yaml(self).is_some()
    }

    /// The language that stands for a family name (`js_ts`, `rust`): the
    /// family's first member, or the language itself.
    pub fn from_family(name: &str) -> Option<Self> {
        match LANG_CONFIG.families.get(name) {
            Some(family) => family.members.first().copied(),
            None => all_languages()
                .map(|(lang, _)| lang)
                .find(|lang| <&str>::from(*lang) == name),
        }
    }

    /// The family's members in declared order; a standalone language alone.
    pub fn family_members(self) -> Vec<Self> {
        LANG_CONFIG
            .families
            .get(self.family())
            .map_or_else(|| vec![self], |f| f.members.clone())
    }

    /// The grammar package `config/languages.yaml` names for the language.
    pub fn grammar(self) -> &'static str {
        &LANG_CONFIG.languages[&self].grammar
    }

    pub fn fqn_separator(&self) -> &'static str {
        LANG_CONFIG.languages[self]
            .fqn_separator
            .as_deref()
            .unwrap_or(".")
    }

    pub fn index_names(&self) -> &[String] {
        &LANG_CONFIG.languages[self].index_names
    }

    /// Resolve a language from a user-provided string like "js", "python", "rs", "typescript".
    /// Checks aliases first (from languages.yaml), then falls back to extension matching.
    pub fn from_alias(s: &str) -> Option<Self> {
        let lower = s.to_ascii_lowercase();
        for (lang, entry) in &LANG_CONFIG.languages {
            if entry.aliases.iter().any(|a| a == &lower) {
                return Some(*lang);
            }
        }
        Self::from_extension(&lower)
    }

    /// Strip this language's file extension from a path. Returns the stem if any
    /// extension matched, or the original path unchanged.
    pub fn strip_extension<'a>(&self, path: &'a str) -> &'a str {
        for ext in &LANG_CONFIG.languages[self].extensions {
            let dotted = DOTTED_EXTENSIONS.get(ext.as_str());
            if let Some(dot_ext) = dotted
                && let Some(stem) = path.strip_suffix(dot_ext)
            {
                return stem;
            }
        }
        path
    }
}

/// Pre-computed ".ext" strings for each extension in the config.
static DOTTED_EXTENSIONS: std::sync::LazyLock<std::collections::HashMap<&'static str, String>> =
    std::sync::LazyLock::new(|| {
        let mut m = std::collections::HashMap::new();
        for entry in LANG_CONFIG.languages.values() {
            for ext in &entry.extensions {
                m.entry(ext.as_str()).or_insert_with(|| format!(".{ext}"));
            }
        }
        m
    });

impl crate::engine::Engine {
    /// The code-indexing engine for a language's family: a frontend per
    /// member, the language's own first, each with its rule file, over the
    /// code pipeline.
    pub fn family(language: SupportLang) -> crate::engine::EngineBuilder {
        let mut members = language.family_members();
        members.sort_by_key(|&m| m != language);
        let source = crate::engine::PipelineSource {
            pipeline: crate::config::PIPELINE,
            rules: crate::config::RULES,
            libs: crate::config::LIBS,
        };
        members.into_iter().fold(
            crate::engine::Engine::builder().pipeline(source),
            |builder, member| {
                builder.frontend(crate::frontend::for_language(member), lang_yaml(member))
            },
        )
    }
}
