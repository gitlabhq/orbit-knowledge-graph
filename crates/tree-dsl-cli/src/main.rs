use std::path::Path;
use std::time::Instant;

use clap::{Parser, Subcommand};

use tree_dsl::pipeline::{
    self, Display, Each, ItemPhase, Observer, Parse, Rewrite, SourceFile, Workset,
};
use tree_dsl::sentinel::Limits;
use tree_dsl::tree::EdgeKind;
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, Pipeline, State};

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
        let p = file
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no file given"))?;
        (p.to_string(), std::fs::read_to_string(p)?)
    };

    let lang_id = resolve_lang(lang_override.as_deref(), Some(&path));

    let env = Env::with_limits(lang_id, Limits::UNLIMITED)?;
    let source = SourceFile {
        path: path.clone(),
        content: source,
    };
    let context = Context::new(&env);
    match stage {
        Stage::Cst => {
            let parsed = inspect(context, source, Parse)?;
            print_tree(&parsed.0, &env.lang);
        }
        Stage::Ast => {
            let rewritten = inspect(context, source, Parse.pipe(Rewrite))?;
            print_tree(&rewritten.0, &env.lang);
        }
        Stage::Ssa => {
            let state = pipeline::index(context, vec![source])?.into_value().state;
            print_graph(&state, &env.lang);
        }
        Stage::Display => {
            let state = pipeline::index(context, vec![source])?
                .then(Display)?
                .into_value()
                .state;
            print_graph(&state, &env.lang);
        }
    }
    Ok(())
}

fn print_graph(state: &State, lang: &tree_dsl::intern::Lang) {
    print_tree(&state.trees[0], lang);
    print_edges(&state.trees[0], &state.edges, lang);
}

/// One file through the given per-file steps, with no corpus filtering: the
/// user named this file, so it is parsed with the language they chose.
fn inspect<P>(context: Context<'_>, source: SourceFile, steps: P) -> anyhow::Result<P::Output>
where
    P: ItemPhase<SourceFile> + Sync,
    P::Output: Send,
{
    let workset = Workset {
        state: State::new(context.env),
        items: vec![source],
        dirty: Default::default(),
        manifests: Default::default(),
    };
    let (context, workset) = Pipeline::new(context, workset).then(Each(steps))?.finish();
    workset
        .items
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("{}", context.report.skipped[0]))
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
        let p = file
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no file given"))?;
        (p.to_string(), std::fs::read_to_string(p)?)
    };

    let lang_id = resolve_lang(lang_override.as_deref(), Some(&path));
    let env = tree_dsl::Env::for_lang(lang_id)?;
    let mut tree = tree_dsl::treesitter::parse(&source, lang_id, &env.lang, &path)?;

    if let Some(ref stop) = after {
        let limit: usize = if stop == "all" {
            env.rewrite_stages.len()
        } else {
            stop.parse().unwrap_or(env.rewrite_stages.len())
        };
        for stage in env.rewrite_stages.iter().take(limit) {
            let _ = tree_dsl::pattern::apply_rewrites(&mut tree, &env.lang, stage, &[]);
        }
    }

    let rules: Vec<_> = patterns
        .iter()
        .zip(templates.iter())
        .map(|(pat, tpl)| {
            let tpl = tpl.clone();
            tree_dsl::pattern::Rewrite::new(&env.lang, pat, move |c| {
                Ok(tree_dsl::pattern::Out::Replace(
                    c.template(&tpl)?,
                    None,
                    None,
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let _ = tree_dsl::pattern::apply_rewrites(&mut tree, &env.lang, &rules, &[]);

    print_tree(&tree, &env.lang);
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
    let root = Path::new(path);
    let paths = collect_paths(root);

    // One graph per pipeline; a repo mixing Python and TypeScript gets two.
    let mut by_lang: std::collections::BTreeMap<String, (SupportLang, Vec<String>)> =
        Default::default();
    for rel in paths {
        let lang = resolve_lang(lang_override.as_deref(), Some(&rel)).pipeline();
        by_lang
            .entry(format!("{lang:?}"))
            .or_insert_with(|| (lang, Vec::new()))
            .1
            .push(rel);
    }

    let graphs_dir = dirs::home_dir()
        .unwrap_or_else(|| Path::new(".").to_path_buf())
        .join(".orbit/var/graphs");
    let repo = root
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("{path} has no file name"))?
        .to_string_lossy()
        .to_string();
    let base = if root.is_dir() {
        root.to_path_buf()
    } else {
        root.parent().unwrap_or(Path::new(".")).to_path_buf()
    };

    // Pipelines are independent graphs; one pipeline's serial resolve phases
    // overlap another's parallel parse.
    use rayon::prelude::*;
    let reports: Vec<anyhow::Result<Summary>> = by_lang
        .into_par_iter()
        .map(|(name, (lang_id, paths))| {
            let env = Env::for_lang(lang_id)?;
            let base = base.clone();
            let sources = paths.into_iter().filter_map(move |rel| {
                let content = std::fs::read_to_string(base.join(&rel)).ok()?;
                Some(SourceFile { path: rel, content })
            });
            let context = Context::new(&env).observe(Progress(name.clone()));
            let (context, resolved) = pipeline::index(context, sources)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .finish();
            let state = resolved.state;
            let mut summary = Summary::of(&name, &state, &context.report);
            if !no_save {
                std::fs::create_dir_all(&graphs_dir)?;
                let snap_path = graphs_dir.join(format!("{repo}.{}.bin", name.to_lowercase()));
                let t_save = Instant::now();
                state.save(&env, &snap_path)?;
                let size_mb = std::fs::metadata(&snap_path)?.len() as f64 / (1024.0 * 1024.0);
                summary.line.push_str(&format!(
                    "\nsaved:        {} ({size_mb:.1} MB, {:.2}s)",
                    snap_path.display(),
                    t_save.elapsed().as_secs_f64()
                ));
            }
            Ok(summary)
        })
        .collect();

    let mut total = Summary::default();
    for report in reports {
        let summary = report?;
        eprintln!("{}", summary.line);
        total.files += summary.files;
        total.defs += summary.defs;
        total.imports += summary.imports;
        total.edges += summary.edges;
    }

    eprintln!();
    eprintln!("files:        {}", total.files);
    eprintln!("defs:         {}", total.defs);
    eprintln!("imports:      {}", total.imports);
    eprintln!("edges:        {}", total.edges);
    eprintln!("total:        {:.2}s", t0.elapsed().as_secs_f64());
    Ok(())
}

/// Phase boundaries and skipped files on stderr while a pipeline runs.
struct Progress(String);

impl Observer for Progress {
    fn started(&mut self, phase: &str) {
        eprintln!("{:<12} {phase}...", self.0);
    }

    fn skipped(&mut self, killed: &tree_dsl::sentinel::Killed) {
        eprintln!("{:<12} skipped: {killed}", self.0);
    }

    fn failed(&mut self, phase: &str, error: &tree_dsl::error::Error) {
        eprintln!("{:<12} {phase} failed: {error}", self.0);
    }
}

#[derive(Default)]
struct Summary {
    line: String,
    files: usize,
    defs: usize,
    imports: usize,
    edges: usize,
}

impl Summary {
    /// One line per pipeline: counts, then how long each phase took.
    fn of(name: &str, state: &State, report: &tree_dsl::pipeline::Report) -> Self {
        let (mut defs, mut imports) = (0usize, 0usize);
        for tree in &state.trees {
            for c in tree.root().descendants() {
                if c.is(tree_dsl::canonical::Canonical::Def) {
                    defs += 1;
                } else if c.is(tree_dsl::canonical::Canonical::Import)
                    || c.is(tree_dsl::canonical::Canonical::ImportType)
                {
                    imports += 1;
                }
            }
        }
        let phases: Vec<String> = report
            .phases
            .iter()
            .map(|p| format!("{} {:.2}s", p.name, p.elapsed.as_secs_f64()))
            .collect();
        Self {
            line: format!(
                "{name:<12} files {:>6}  defs {:>7}  imports {:>7}  edges {:>7}\n              {}",
                state.trees.len(),
                defs,
                imports,
                state.edges.len(),
                phases.join(", ")
            ),
            files: state.trees.len(),
            defs,
            imports,
            edges: state.edges.len(),
        }
    }
}

/// Relative paths of every parseable file under `root`, or `root` itself.
fn collect_paths(root: &Path) -> Vec<String> {
    if root.is_file() {
        return vec![
            root.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        ];
    }
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.into_path())
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .and_then(|e| e.to_str())
                    .and_then(SupportLang::from_extension)
                    .is_some()
        })
        .map(|p| {
            p.strip_prefix(root)
                .unwrap_or(&p)
                .to_string_lossy()
                .to_string()
        })
        .collect()
}

#[cfg(feature = "test-runner")]
fn cmd_test(file: Option<String>, inline: Option<String>) -> anyhow::Result<()> {
    let yaml = match (file, inline) {
        (Some(path), _) => std::fs::read_to_string(&path)?,
        (_, Some(text)) => text,
        (None, None) => anyhow::bail!("provide a YAML file path or --inline"),
    };

    tree_dsl_tests::runner::run_yaml_suite(&yaml);
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
