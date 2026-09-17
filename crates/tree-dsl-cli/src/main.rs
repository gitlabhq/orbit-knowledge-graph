use std::path::Path;
use std::time::Instant;

use clap::{Parser, Subcommand};

use tree_dsl::tree::EdgeKind;
use tree_dsl::treesitter::SupportLang;

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
    /// Apply rewrite rules and print the result
    Rewrite {
        /// File to parse (omit for stdin)
        file: Option<String>,
        /// Read source from stdin
        #[arg(long)]
        stdin: bool,
        /// Override language detection
        #[arg(short, long)]
        lang: Option<String>,
        /// Match pattern(s), paired with --replace
        #[arg(long, short)]
        r#match: Vec<String>,
        /// Replace template(s), paired with --match
        #[arg(long, short)]
        replace: Vec<String>,
        /// Apply language rewrites first ("all" or stage index), then apply the rules
        #[arg(long)]
        after: Option<String>,
    },
    /// Index files and print the graph
    Index {
        /// File or directory to index
        path: String,
        /// Override language detection
        #[arg(short, long)]
        lang: Option<String>,
        /// Skip saving the serialized graph
        #[arg(long)]
        no_save: bool,
    },
    /// Run a YAML test suite
    #[cfg(feature = "test-runner")]
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
    /// After display pass (SSA + display decorations)
    Display,
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
        Commands::Rewrite {
            file,
            stdin,
            lang,
            r#match,
            replace,
            after,
        } => cmd_rewrite(file, stdin, lang, r#match, replace, after),
        Commands::Index {
            path,
            lang,
            no_save,
        } => cmd_index(&path, lang, no_save),
        #[cfg(feature = "test-runner")]
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
            let lang = tree_dsl::intern::Lang::new();
            let tree = tree_dsl::treesitter::parse(&source, lang_id, &lang, &path);
            print_tree(&tree, &lang);
        }
        Stage::Ast => {
            let (pipeline, lang) = tree_dsl::pipeline::Pipeline::for_lang(lang_id);
            let mut tree = tree_dsl::treesitter::parse(&source, lang_id, &lang, &path);
            for stage in &pipeline.rewrite_stages {
                tree_dsl::pattern::apply_rewrites(&mut tree, &lang, stage);
            }
            print_tree(&tree, &lang);
        }
        Stage::Ssa => {
            let (tree, edges, lang, _) = tree_dsl::parse(lang_id, &path, &source);
            print_tree(&tree, &lang);
            print_edges(&tree, &edges, &lang);
        }
        Stage::Display => {
            let (tree, edges, lang, _pipeline) = tree_dsl::parse(lang_id, &path, &source);
            let yaml = tree_dsl::treesitter::lang_yaml(lang_id).expect("no lang yaml");
            let config = tree_dsl::rules::load_lang_full(yaml, &lang);
            let mut trees = vec![tree];
            tree_dsl::display::apply_display(&mut trees, &edges, &lang, &config.display_rules);
            print_tree(&trees[0], &lang);
            print_edges(&trees[0], &edges, &lang);
        }
    }
    Ok(())
}

fn cmd_rewrite(
    file: Option<String>,
    stdin: bool,
    lang_override: Option<String>,
    patterns: Vec<String>,
    templates: Vec<String>,
    after: Option<String>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        patterns.len() == templates.len(),
        "--match and --replace must be provided in equal pairs ({} vs {})",
        patterns.len(),
        templates.len()
    );

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
    let (pipeline, lang) = tree_dsl::pipeline::Pipeline::for_lang(lang_id);
    let mut tree = tree_dsl::treesitter::parse(&source, lang_id, &lang, &path);

    if let Some(ref stop) = after {
        let limit: usize = if stop == "all" {
            pipeline.rewrite_stages.len()
        } else {
            stop.parse().unwrap_or(pipeline.rewrite_stages.len())
        };
        for stage in pipeline.rewrite_stages.iter().take(limit) {
            tree_dsl::pattern::apply_rewrites(&mut tree, &lang, stage);
        }
    }

    let rules: Vec<_> = patterns
        .iter()
        .zip(templates.iter())
        .map(|(pat, tpl)| {
            let tpl = tpl.clone();
            tree_dsl::pattern::Rewrite::new(&lang, pat, move |c| {
                tree_dsl::pattern::Out::Replace(c.template(&tpl))
            })
        })
        .collect();
    tree_dsl::pattern::apply_rewrites(&mut tree, &lang, &rules);

    print_tree(&tree, &lang);
    Ok(())
}

fn print_tree(tree: &tree_dsl::tree::Tree, lang: &tree_dsl::intern::Lang) {
    for c in std::iter::once(tree.root()).chain(tree.root().descendants()) {
        if !c.named() && c.sym() == 0 {
            continue;
        }

        let kind_display = lang.kind_name(c.kind()).to_string();

        let field_str = if c.field() != 0 {
            format!(" field={}", lang.fields.resolve(c.field() as u32))
        } else {
            String::new()
        };

        let sym_str = if c.sym() != 0 {
            let text = lang.syms.resolve(c.sym());
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
            c.index(),
            kind_display,
            field_str,
            sym_str,
            c.start(),
            c.end()
        );
    }
}

fn print_edges(
    tree: &tree_dsl::tree::Tree,
    edges: &[tree_dsl::tree::Edge],
    lang: &tree_dsl::intern::Lang,
) {
    if edges.is_empty() {
        return;
    }
    println!("edges:");
    for e in edges {
        let from = node_label(tree, lang, e.from_node);
        let to = node_label(tree, lang, e.to_node);
        println!("  {} --[{}]--> {}", from, edge_name(e.kind), to);
    }
}

fn node_label(tree: &tree_dsl::tree::Tree, lang: &tree_dsl::intern::Lang, node: u32) -> String {
    let c = tree.cursor(node);
    for child in c.children() {
        if child.field() != 0 && child.sym() != 0 {
            return format!("{}[{}]", lang.syms.resolve(child.sym()), node);
        }
    }
    if c.sym() != 0 {
        let s = lang.syms.resolve(c.sym());
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

fn cmd_index(path: &str, lang_override: Option<String>, no_save: bool) -> anyhow::Result<()> {
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

    let mut total_defs = 0usize;
    let mut total_imports = 0usize;

    for tree in &result.trees {
        for c in tree.root().descendants() {
            if tree_dsl::canonical::has_def_type(c) {
                total_defs += 1;
            } else if c.is(tree_dsl::canonical::Canonical::Import)
                || c.is(tree_dsl::canonical::Canonical::ImportType)
            {
                total_imports += 1;
            }
        }
    }

    eprintln!();
    eprintln!("--- stats ---");
    eprintln!("files:        {}", result.trees.len());
    eprintln!("definitions:  {}", total_defs);
    eprintln!("imports:      {}", total_imports);
    eprintln!("edges:        {}", result.edges.len());
    eprintln!("parse:        {:.2}s", result.timings.parse_s);
    eprintln!("resolve:      {:.2}s", result.timings.resolve_s);
    eprintln!("total:        {:.2}s", elapsed.as_secs_f64());

    if !no_save {
        let graphs_dir = dirs::home_dir()
            .unwrap_or_else(|| Path::new(".").to_path_buf())
            .join(".orbit/var/graphs");
        std::fs::create_dir_all(&graphs_dir)?;
        let name = Path::new(path)
            .file_name()
            .unwrap_or(std::ffi::OsStr::new("graph"))
            .to_string_lossy();
        let snap_path = graphs_dir.join(format!("{name}.bin"));
        let t_save = Instant::now();
        result.save(&snap_path)?;
        let save_s = t_save.elapsed().as_secs_f64();
        let size_mb = std::fs::metadata(&snap_path)?.len() as f64 / (1024.0 * 1024.0);
        eprintln!(
            "saved:        {} ({:.1} MB, {:.2}s)",
            snap_path.display(),
            size_mb,
            save_s
        );
    }
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

#[cfg(feature = "test-runner")]
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
