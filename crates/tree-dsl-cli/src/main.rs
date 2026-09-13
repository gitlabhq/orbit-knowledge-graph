use std::path::Path;
use std::time::Instant;

use clap::{Parser, Subcommand};

use tree_dsl::grammar::SupportLang;
use tree_dsl::tree::EdgeKind;

#[derive(Parser)]
#[command(name = "tree-dsl", about = "Code indexing CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse a single file and print its annotation stream (no cross-file resolution)
    Parse {
        /// File to parse (omit for stdin)
        file: Option<String>,
        /// Read source from stdin
        #[arg(long)]
        stdin: bool,
        /// Override language detection
        #[arg(short, long)]
        lang: Option<String>,
        /// Output stage
        #[arg(long, default_value = "tagged")]
        stage: Stage,
    },
    /// Index files and print the graph
    Index {
        /// File or directory to index
        path: String,
        /// Override language detection
        #[arg(short, long)]
        lang: Option<String>,
    },
    /// Run a YAML test suite
    Test {
        /// Path to YAML test file
        file: Option<String>,
        /// Inline YAML test suite
        #[arg(long)]
        inline: Option<String>,
    },
}

#[derive(Clone, clap::ValueEnum)]
enum Stage {
    /// Raw tree-sitter CST (before rewrites)
    Cst,
    /// Tree after rewrites (before SSA)
    Ast,
    /// Full pipeline output with edges (default)
    Ssa,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Parse {
            file,
            stdin,
            lang,
            stage,
        } => cmd_parse(file, stdin, lang, stage),
        Commands::Index { path, lang } => cmd_index(&path, lang),
        Commands::Test { file, inline } => cmd_test(file, inline),
    }
}

// ── parse ──

fn cmd_parse(
    file: Option<String>,
    stdin: bool,
    lang_override: Option<String>,
    stage: Stage,
) -> anyhow::Result<()> {
    let (path, source) = if stdin || file.is_none() {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        let name = file.as_deref().unwrap_or("<stdin>");
        (name.to_string(), buf)
    } else {
        let p = file.as_deref().unwrap();
        (p.to_string(), std::fs::read_to_string(p)?)
    };

    let lang_id = resolve_lang(lang_override.as_deref(), Some(&path));

    match stage {
        Stage::Cst => {
            let mut lang = tree_dsl::lang::Lang::new();
            let tree = tree_dsl::grammar::parse(&source, lang_id, &mut lang, &path);
            print_tree(&tree, &lang);
        }
        Stage::Ast => {
            let (pipeline, mut lang) = tree_dsl::pipeline::Pipeline::for_lang(lang_id);
            let mut tree = tree_dsl::grammar::parse(&source, lang_id, &mut lang, &path);
            for stage in &pipeline.rewrite_stages {
                tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, stage);
            }
            print_tree(&tree, &lang);
        }
        Stage::Ssa => {
            let (tree, lang, _) = tree_dsl::parse(lang_id, &path, &source);
            print_tree(&tree, &lang);
            print_edges(&tree, &lang);
        }
    }
    Ok(())
}

fn print_tree(tree: &tree_dsl::tree::Tree, lang: &tree_dsl::lang::Lang) {
    for (i, n) in tree.nodes.iter().enumerate() {
        if n.dead {
            continue;
        }
        if !n.named && n.sym == 0 {
            continue;
        }

        let kind_display = lang.kind_name(n.kind).to_string();

        let field_str = if n.field != 0 {
            format!(" field={}", lang.fields.resolve(n.field as u32))
        } else {
            String::new()
        };

        let sym_str = if n.sym != 0 {
            let text = lang.syms.resolve(n.sym);
            if text.len() > 40 {
                format!(" sym={:?}", &text[..40])
            } else {
                format!(" sym={:?}", text)
            }
        } else {
            String::new()
        };

        println!(
            "{:>4}  {:<30}{}{} [{}-{}]",
            i, kind_display, field_str, sym_str, n.start, n.end
        );
    }
}

fn print_edges(tree: &tree_dsl::tree::Tree, lang: &tree_dsl::lang::Lang) {
    if tree.edges().is_empty() {
        return;
    }
    println!("edges:");
    for e in tree.edges().iter() {
        let from = node_label(tree, lang, e.from.node);
        let to = node_label(tree, lang, e.to.node);
        println!("  {} --[{}]--> {}", from, edge_name(e.kind), to);
    }
}

fn node_label(tree: &tree_dsl::tree::Tree, lang: &tree_dsl::lang::Lang, node: u32) -> String {
    if node == 0 {
        return "<root>".to_string();
    }
    if node as usize >= tree.nodes.len() {
        return format!("?{node}");
    }
    for c in tree.children(node) {
        let cn = tree.node(c);
        if cn.field != 0 && cn.sym != 0 {
            return format!("{}[{}]", lang.syms.resolve(cn.sym), node);
        }
    }
    let n = &tree.nodes[node as usize];
    if n.sym != 0 {
        let s = lang.syms.resolve(n.sym);
        let short = s.lines().next().unwrap_or(s);
        if short.len() > 40 {
            format!("{}...[{}]", &short[..40], node)
        } else {
            format!("{}[{}]", short, node)
        }
    } else {
        format!("?{}", node)
    }
}

fn edge_name(kind: EdgeKind) -> &'static str {
    kind.name()
}

// ── index ──

fn cmd_index(path: &str, lang_override: Option<String>) -> anyhow::Result<()> {
    let t0 = Instant::now();

    let p = Path::new(path);
    let files: Vec<(String, String)> = if p.is_dir() {
        collect_files(p)
    } else {
        let content = std::fs::read_to_string(p)?;
        let rel = p.file_name().unwrap().to_string_lossy().to_string();
        vec![(rel, content)]
    };

    let lang_id = resolve_lang(
        lang_override.as_deref(),
        files.first().map(|(p, _)| p.as_str()),
    );

    let result = tree_dsl::index(lang_id, &files);
    let elapsed = t0.elapsed();

    let deftype_k = result.lang.lookup_kind("__deftype");
    let import_k = result.lang.lookup_kind("__import");
    let mut total_defs = 0usize;
    let mut total_imports = 0usize;
    let mut total_intra_edges = 0usize;

    for tree in &result.trees {
        let path = result.lang.syms.resolve(tree.nodes[0].sym);
        let defs = tree.nodes.iter().filter(|n| n.kind == deftype_k).count();
        let imports = tree.nodes.iter().filter(|n| n.kind == import_k).count();
        total_defs += defs;
        total_imports += imports;
        total_intra_edges += tree.edges().len();
        println!(
            "{}: {} defs, {} imports, {} edges",
            path,
            defs,
            imports,
            tree.edges().len()
        );
    }

    if !result.cross_edges.is_empty() {
        println!();
        println!("cross-file edges: {}", result.cross_edges.len());
        for ce in &result.cross_edges {
            let from_path = result
                .lang
                .syms
                .resolve(result.trees[ce.from.tree as usize].nodes[0].sym);
            let to_path = result
                .lang
                .syms
                .resolve(result.trees[ce.to.tree as usize].nodes[0].sym);
            let from = node_label(
                &result.trees[ce.from.tree as usize],
                &result.lang,
                ce.from.node,
            );
            let to = node_label(&result.trees[ce.to.tree as usize], &result.lang, ce.to.node);
            println!("  {}:{} --> {}:{}", from_path, from, to_path, to);
        }
    }

    eprintln!();
    eprintln!("--- stats ---");
    eprintln!("files:        {}", result.trees.len());
    eprintln!("definitions:  {}", total_defs);
    eprintln!("imports:      {}", total_imports);
    eprintln!("intra edges:  {}", total_intra_edges);
    eprintln!("cross edges:  {}", result.cross_edges.len());
    eprintln!("time:         {:.2}s", elapsed.as_secs_f64());
    Ok(())
}

fn collect_files(dir: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|e| e.to_str())
                .and_then(SupportLang::from_extension)
                .is_some()
            && let Ok(content) = std::fs::read_to_string(path)
        {
            let rel = path
                .strip_prefix(dir)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string();
            files.push((rel, content));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

// ── test ──

fn cmd_test(file: Option<String>, inline: Option<String>) -> anyhow::Result<()> {
    let yaml = match (file, inline) {
        (Some(path), _) => std::fs::read_to_string(&path)?,
        (_, Some(text)) => text,
        (None, None) => anyhow::bail!("provide a YAML file path or --inline"),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(tree_dsl_tests::runner::run_yaml_suite(&yaml));
    Ok(())
}

// ── helpers ──

fn resolve_lang(override_: Option<&str>, path_hint: Option<&str>) -> SupportLang {
    if let Some(s) = override_
        && let Some(lang) = SupportLang::from_alias(s)
    {
        return lang;
    }
    path_hint
        .and_then(SupportLang::from_path)
        .unwrap_or(SupportLang::Python)
}
