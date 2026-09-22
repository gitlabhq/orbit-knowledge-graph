use crate::intern::Lang;
use crate::tree::{Node, Tree};

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
    #[serde(default, rename = "package_markers")]
    _package_markers: Vec<String>,
    #[serde(default)]
    index_names: Vec<String>,
    #[serde(default, rename = "source_root")]
    _source_root: Option<String>,
}

#[derive(serde::Deserialize)]
struct LangConfig {
    languages: std::collections::HashMap<SupportLang, LangEntry>,
}

static LANG_CONFIG: std::sync::LazyLock<LangConfig> = std::sync::LazyLock::new(|| {
    let yaml = include_str!("../config/languages.yaml");
    serde_yaml::from_str(yaml).expect("failed to parse languages.yaml")
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
        SupportLang::Elixir => Some(include_str!("../langs/elixir.yaml")),
        SupportLang::Lua => Some(include_str!("../langs/lua.yaml")),
        SupportLang::Swift => Some(include_str!("../langs/swift.yaml")),
        SupportLang::Zig => Some(include_str!("../langs/zig.yaml")),
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
        let ext = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())?;
        Self::from_extension(ext)
    }

    /// The language whose rule file this one shares. TypeScript, TSX, and
    /// JavaScript differ only in grammar and form one graph.
    pub fn pipeline(self) -> Self {
        match self {
            Self::Tsx | Self::JavaScript => Self::TypeScript,
            other => other,
        }
    }

    pub fn ts_language(&self) -> tree_sitter::Language {
        let entry = &LANG_CONFIG.languages[self];
        grammar_to_ts_language(&entry.grammar)
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

fn from_tree_sitter(
    source: &str,
    ts_tree: &tree_sitter::Tree,
    lang: &Lang,
    kind_map: &[u16],
    field_map: &[u16],
    label: &str,
) -> Tree {
    let ts_root = ts_tree.root_node();
    let mut cursor = ts_root.walk();

    let kind = kind_map
        .get(cursor.node().kind_id() as usize)
        .copied()
        .unwrap_or(0);
    let sp = cursor.node().start_position();
    let ep = cursor.node().end_position();
    let root_sym = if !label.is_empty() {
        lang.syms.intern(label)
    } else if cursor.node().is_named() {
        let text = &source[cursor.node().start_byte()..cursor.node().end_byte()];
        lang.syms.intern(text)
    } else {
        0
    };

    let mut tree = Tree::with_capacity(
        ts_root.descendant_count(),
        Node {
            kind,
            field: 0,
            named: cursor.node().is_named(),
            synth: false,
            sym: root_sym,
            start: cursor.node().start_byte() as u32,
            end: cursor.node().end_byte() as u32,
            start_row: sp.row as u32,
            start_col: sp.column as u32,
            end_row: ep.row as u32,
            end_col: ep.column as u32,
        },
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    let root_nid = tree.root;
    let mut parent_stack: Vec<indextree::NodeId> = vec![root_nid];

    if !cursor.goto_first_child() {
        return tree;
    }

    loop {
        let ts = cursor.node();
        let kind = kind_map.get(ts.kind_id() as usize).copied().unwrap_or(0);
        let field = cursor
            .field_id()
            .map_or(0, |f| field_map.get(f.get() as usize).copied().unwrap_or(0));
        let sym = if ts.is_named() && ts.named_child_count() == 0 {
            lang.syms.intern(&source[ts.start_byte()..ts.end_byte()])
        } else {
            0
        };
        let sp = ts.start_position();
        let ep = ts.end_position();

        let parent = *parent_stack.last().unwrap();
        let nid = tree.append(
            parent,
            Node {
                kind,
                field,
                named: ts.is_named(),
                synth: false,
                sym,
                start: ts.start_byte() as u32,
                end: ts.end_byte() as u32,
                start_row: sp.row as u32,
                start_col: sp.column as u32,
                end_row: ep.row as u32,
                end_col: ep.column as u32,
            },
        );

        if cursor.goto_first_child() {
            parent_stack.push(nid);
            continue;
        }

        if cursor.goto_next_sibling() {
            continue;
        }

        loop {
            if !cursor.goto_parent() {
                return tree;
            }
            parent_stack.pop();
            if cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

struct TsCache {
    lang: Option<SupportLang>,
    kinds: Vec<u16>,
    fields: Vec<u16>,
    parser: tree_sitter::Parser,
}

impl TsCache {
    fn ensure(&mut self, support_lang: SupportLang, lang: &Lang) {
        if self.lang == Some(support_lang) {
            return;
        }
        let ts = support_lang.ts_language();
        self.kinds = (0..ts.node_kind_count() as u16)
            .map(|id| ts.node_kind_for_id(id).map_or(0, |s| lang.intern_kind(s)))
            .collect();
        self.fields = std::iter::once(0)
            .chain(
                (1..=ts.field_count() as u16)
                    .map(|id| ts.field_name_for_id(id).map_or(0, |s| lang.intern_field(s))),
            )
            .collect();
        self.parser.set_language(&ts).unwrap();
        self.lang = Some(support_lang);
    }
}

thread_local! {
    static CACHE: std::cell::RefCell<TsCache> = std::cell::RefCell::new(TsCache {
        lang: None, kinds: Vec::new(), fields: Vec::new(),
        parser: tree_sitter::Parser::new(),
    });
}

pub fn parse(source: &str, support_lang: SupportLang, lang: &Lang, label: &str) -> Tree {
    CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.ensure(support_lang, lang);
        let ts_tree = c.parser.parse(source.as_bytes(), None).unwrap();
        from_tree_sitter(source, &ts_tree, lang, &c.kinds, &c.fields, label)
    })
}
