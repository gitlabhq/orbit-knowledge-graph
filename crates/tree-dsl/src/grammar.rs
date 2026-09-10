use crate::lang::Lang;
use crate::tree::{NONE, Node, Tree};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SupportLang {
    Bash,
    C,
    Cpp,
    #[serde(rename = "csharp")]
    CSharp,
    Elixir,
    Go,
    Haskell,
    Hcl,
    Java,
    JavaScript,
    Kotlin,
    Lua,
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
struct LangEntry {
    extensions: Vec<String>,
    #[serde(default)]
    aliases: Vec<String>,
    grammar: String,
    #[serde(default)]
    fqn_separator: Option<String>,
    #[serde(default)]
    package_markers: Vec<String>,
    #[serde(default)]
    index_names: Vec<String>,
    #[serde(default)]
    source_root: Option<String>,
}

#[derive(serde::Deserialize)]
struct LangConfig {
    languages: std::collections::HashMap<SupportLang, LangEntry>,
}

static LANG_CONFIG: std::sync::LazyLock<LangConfig> = std::sync::LazyLock::new(|| {
    let yaml = include_str!("../config/languages.yaml");
    serde_yaml::from_str(yaml).expect("failed to parse languages.yaml")
});

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
        let ext = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())?;
        Self::from_extension(ext)
    }

    pub fn ts_language(&self) -> tree_sitter::Language {
        let entry = &LANG_CONFIG.languages[self];
        grammar_to_ts_language(&entry.grammar)
    }

    pub fn extensions(&self) -> &[String] {
        &LANG_CONFIG.languages[self].extensions
    }

    pub fn fqn_separator(&self) -> &'static str {
        LANG_CONFIG.languages[self]
            .fqn_separator
            .as_deref()
            .unwrap_or(".")
    }

    pub fn package_markers(&self) -> &[String] {
        &LANG_CONFIG.languages[self].package_markers
    }

    pub fn has_package_marker_climb(&self) -> bool {
        LANG_CONFIG.languages[self].source_root.as_deref() == Some("package_marker_climb")
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

fn grammar_to_ts_language(grammar: &str) -> tree_sitter::Language {
    match grammar {
        "tree-sitter-bash" => tree_sitter_bash::LANGUAGE.into(),
        "tree-sitter-c" => tree_sitter_c::LANGUAGE.into(),
        "tree-sitter-cpp" => tree_sitter_cpp::LANGUAGE.into(),
        "tree-sitter-c-sharp" => tree_sitter_c_sharp::LANGUAGE.into(),
        "tree-sitter-elixir" => tree_sitter_elixir::LANGUAGE.into(),
        "tree-sitter-go" => tree_sitter_go::LANGUAGE.into(),
        "tree-sitter-hcl" => tree_sitter_hcl::LANGUAGE.into(),
        "tree-sitter-java" => tree_sitter_java::LANGUAGE.into(),
        "tree-sitter-kotlin-sg" => tree_sitter_kotlin_sg::LANGUAGE.into(),
        "tree-sitter-lua" => tree_sitter_lua::LANGUAGE.into(),
        "tree-sitter-php" => tree_sitter_php::LANGUAGE_PHP.into(),
        "tree-sitter-python" => tree_sitter_python::LANGUAGE.into(),
        "tree-sitter-ruby" => tree_sitter_ruby::LANGUAGE.into(),
        "tree-sitter-rust" => tree_sitter_rust::LANGUAGE.into(),
        "tree-sitter-scala" => tree_sitter_scala::LANGUAGE.into(),
        "tree-sitter-swift" => tree_sitter_swift::LANGUAGE.into(),
        "tree-sitter-typescript" => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tree-sitter-typescript-tsx" => tree_sitter_typescript::LANGUAGE_TSX.into(),
        "tree-sitter-haskell" => tree_sitter_haskell::LANGUAGE.into(),
        "tree-sitter-ocaml" => tree_sitter_ocaml::LANGUAGE_OCAML.into(),
        "tree-sitter-zig" => tree_sitter_zig::LANGUAGE.into(),
        _ => panic!("unknown grammar: {grammar}"),
    }
}

pub fn from_tree_sitter(
    source: &str,
    ts_tree: &tree_sitter::Tree,
    lang: &mut Lang,
    label: &str,
) -> Tree {
    let root = ts_tree.root_node();
    let mut nodes: Vec<Node> = Vec::with_capacity(root.descendant_count());
    let mut parent_stack: Vec<u32> = Vec::with_capacity(64);
    let mut cursor = root.walk();
    let mut done = false;

    loop {
        let ts = cursor.node();
        let id = nodes.len() as u32;
        let parent = parent_stack.last().copied().unwrap_or(NONE);

        let kind = lang.kind(ts.kind());
        let field = cursor.field_name().map_or(0, |f| lang.field(f));
        let sym = if ts.is_named() {
            let text = &source[ts.start_byte()..ts.end_byte()];
            lang.syms.get(text)
        } else {
            0
        };

        nodes.push(Node {
            kind,
            field,
            named: ts.is_named(),
            synth: false,
            dead: false,
            id: 0,
            size: 0,
            parent,
            sym,
            start: ts.start_byte() as u32,
            end: ts.end_byte() as u32,
        });

        if cursor.goto_first_child() {
            parent_stack.push(id);
            continue;
        }

        nodes[id as usize].size = 1;

        if cursor.goto_next_sibling() {
            continue;
        }

        loop {
            if !cursor.goto_parent() {
                done = true;
                break;
            }
            let parent_id = parent_stack.pop().unwrap();
            nodes[parent_id as usize].size = nodes.len() as u32 - parent_id;
            if cursor.goto_next_sibling() {
                break;
            }
        }
        if done {
            break;
        }
    }
    let mut tree = Tree::from_nodes(nodes);
    tree.label = label.to_string();
    if !label.is_empty() && !tree.nodes.is_empty() {
        tree.nodes[0].sym = lang.syms.get(label);
    }
    tree
}

pub fn parse(source: &str, support_lang: SupportLang, lang: &mut Lang, label: &str) -> Tree {
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&support_lang.ts_language()).unwrap();
    let ts_tree = parser.parse(source.as_bytes(), None).unwrap();
    from_tree_sitter(source, &ts_tree, lang, label)
}
