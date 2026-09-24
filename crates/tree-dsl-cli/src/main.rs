use std::path::Path;
use std::time::Instant;

use clap::{Parser, Subcommand};

use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use tree_dsl::pipeline::{
    Canonicalize, Display, Each, Insert, ItemPhase, Link, Observer, Parse, Resolve, Rewrite,
    SourceFile, Workset,
};
use tree_dsl::sentinel::Limits;
use tree_dsl::tree::EdgeKind;
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, Pipeline, State};
use tree_dsl::{inventory, templates};

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
            let parsed = first(inspect(context, source, Parse)?)?;
            print_tree(&parsed.0, &env.lang);
        }
        Stage::Ast => {
            let rewritten = first(inspect(context, source, Parse.pipe(Rewrite))?)?;
            print_tree(&rewritten.0, &env.lang);
        }
        Stage::Ssa => {
            let linked = inspect(
                context,
                source,
                Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link),
            )?;
            let state = linked.then(Insert)?.then(Resolve)?.into_value().state;
            print_graph(&state, &env.lang);
        }
        Stage::Display => {
            let linked = inspect(
                context,
                source,
                Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link),
            )?;
            let state = linked
                .then(Insert)?
                .then(Resolve)?
                .then(Display)?
                .into_value()
                .state;
            print_graph(&state, &env.lang);
        }
    }
    Ok(())
}

/// One in-memory file through the given per-file steps, with no repository
/// walk: the user named this file, so it is parsed with the language they
/// chose.
fn inspect<'e, P>(
    context: Context<'e>,
    source: SourceFile,
    steps: P,
) -> anyhow::Result<Pipeline<'e, Workset<Vec<P::Output>>>>
where
    P: ItemPhase<SourceFile> + Sync,
    P::Output: Send,
{
    let workset = Workset {
        state: State::new(context.env),
        items: vec![source],
        dirty: Default::default(),
        listed: Default::default(),
    };
    Ok(Pipeline::new(context, workset).then(Each(steps))?)
}

fn first<T>(pipeline: Pipeline<'_, Workset<Vec<T>>>) -> anyhow::Result<T> {
    let (context, workset) = pipeline.finish();
    workset
        .items
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("{}", context.report.skipped[0]))
}

fn print_graph(state: &State, lang: &tree_dsl::intern::Lang) {
    print_tree(&state.trees[0], lang);
    print_edges(&state.trees[0], &state.edges, lang);
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
    let t_walk = Instant::now();
    let (root, inventory) = if root.is_dir() {
        (root.to_path_buf(), inventory::walk(root)?.into_inner())
    } else {
        let parent = root.parent().unwrap_or(Path::new(".")).to_path_buf();
        let name = root
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("{path} has no file name"))?
            .to_string_lossy()
            .to_string();
        (parent.clone(), inventory::classify(&parent, [name]))
    };
    eprintln!(
        "walk          {} files, {} to parse  {:.2}s",
        inventory.len(),
        inventory
            .iter()
            .filter(|e| e.decision == Decision::Parse)
            .count(),
        t_walk.elapsed().as_secs_f64()
    );

    // One graph per pipeline; a repo mixing Python and TypeScript gets two.
    // Files that are not parsed belong to the repository, not a language,
    // so the first pipeline records them.
    let mut by_lang: std::collections::BTreeMap<String, (SupportLang, Vec<FileInventoryEntry>)> =
        Default::default();
    let mut unparsed = Vec::new();
    for entry in inventory {
        let lang = SupportLang::from_path(&entry.path).map(|l| l.pipeline());
        match (entry.decision, lang) {
            (Decision::Parse, Some(lang)) => {
                let lang = lang_override
                    .as_deref()
                    .and_then(SupportLang::from_alias)
                    .unwrap_or(lang);
                by_lang
                    .entry(format!("{lang:?}"))
                    .or_insert_with(|| (lang, Vec::new()))
                    .1
                    .push(entry);
            }
            _ => unparsed.push(entry),
        }
    }
    if let Some((_, entries)) = by_lang.values_mut().next() {
        entries.append(&mut unparsed);
    }

    let graphs_dir = dirs::home_dir()
        .unwrap_or_else(|| Path::new(".").to_path_buf())
        .join(".orbit/var/graphs");
    let repo = Path::new(path)
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("{path} has no file name"))?
        .to_string_lossy()
        .to_string();

    // Pipelines are independent graphs; one pipeline's serial resolve phases
    // overlap another's parallel parse.
    use rayon::prelude::*;
    let reports: Vec<anyhow::Result<Summary>> = by_lang
        .into_par_iter()
        .map(|(name, (lang_id, entries))| {
            let env = Env::for_lang(lang_id)?;
            let context = Context::new(&env).observe(Progress(name.clone()));
            let (context, resolved) = templates::index(context, &root, entries)
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
