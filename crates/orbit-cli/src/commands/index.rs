pub(crate) mod incremental;

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use arrow::array::{Array, StringArray};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

use super::setup::spec;
use crate::database::GraphReader;
use crate::tui;
use crate::workspace::{self, GitInfo};

pub(crate) fn run(path: PathBuf, options: incremental::Options, detach: bool) -> Result<()> {
    if detach {
        return start_in_background(&path, &options);
    }
    let person_is_watching = std::io::stdout().is_terminal();
    install_tracing(options.verbose, person_is_watching);
    if !person_is_watching {
        let options = incremental::Options {
            cancel: cancel_on_ctrl_c(),
            ..options
        };
        for output in incremental::index(path, options)? {
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        return Ok(());
    }

    tui::intro("Orbit index")?;
    let stats = options.stats;
    let outcome = index_showing_progress(path, options);
    if let Err(error) = &outcome
        && let Some(NoRepositoryFound(folder)) = error.downcast_ref()
    {
        return show_how_to_pick_a_repository(folder);
    }
    let outputs = close_screen_on_failure(outcome)?;
    if stats {
        for output in &outputs {
            tui::card("Slowest files", output.slowest_files().join("\n"))?;
        }
    }
    if let Some(name) = suggested_grep(&outputs) {
        tui::card("Try it", grep_command_line(&name))?;
    }
    tui::outro("Done.")
}

fn start_in_background(path: &Path, options: &incremental::Options) -> Result<()> {
    let (status, pid) = match incremental::index_detached(path, options)? {
        incremental::DetachedIndex::Running { pid } => ("indexing", pid),
        incremental::DetachedIndex::Finished { pid } => ("indexed", pid),
    };
    println!("{}", serde_json::json!({ "status": status, "pid": pid }));
    Ok(())
}

pub(crate) fn index_with_progress(path: PathBuf, db: Option<PathBuf>) -> Result<Option<String>> {
    let outputs = index_showing_progress(
        path,
        incremental::Options {
            db,
            ..Default::default()
        },
    )?;
    Ok(suggested_grep(&outputs))
}

pub(crate) fn index_before_first_query(path: PathBuf, db: Option<PathBuf>) -> Result<()> {
    let options = incremental::Options {
        db,
        skip_current: true,
        ..Default::default()
    };
    if !std::io::stderr().is_terminal() {
        return incremental::index(path, options).map(drop);
    }
    tui::intro("Orbit index")?;
    close_screen_on_failure(index_showing_progress(path, options))?;
    tui::outro("Indexed.")
}

pub(crate) fn collect(
    path: PathBuf,
    stats: bool,
    db: Option<PathBuf>,
) -> Result<Vec<incremental::IndexOutput>> {
    incremental::index(
        path,
        incremental::Options {
            stats,
            db,
            ..Default::default()
        },
    )
}

fn index_showing_progress(
    path: PathBuf,
    options: incremental::Options,
) -> Result<Vec<incremental::IndexOutput>> {
    let _control_echo_off = tui::turn_off_control_echo();
    incremental::index(
        path,
        incremental::Options {
            cancel: cancel_on_ctrl_c(),
            progress: true,
            ..options
        },
    )
}

fn close_screen_on_failure<T>(outcome: Result<T>) -> Result<T> {
    match outcome {
        Err(error) if !tui::is_cancelled(&error) => {
            tui::outro_cancel(&error)?;
            Err(error)
        }
        outcome => outcome,
    }
}

fn cancel_on_ctrl_c() -> Arc<AtomicBool> {
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            flag.store(true, Ordering::Relaxed);
        }
        if tokio::signal::ctrl_c().await.is_ok() {
            tui::restore_control_echo();
            std::process::exit(130);
        }
    });
    cancel
}

fn suggested_grep(outputs: &[incremental::IndexOutput]) -> Option<String> {
    let output = outputs.first()?;
    let git = workspace::git_info(Path::new(output.path())).ok()?;
    most_referenced_definition(&git, Some(PathBuf::from(output.database_path())))
}

#[derive(Debug)]
pub(crate) struct NoRepositoryFound(pub PathBuf);

impl std::fmt::Display for NoRepositoryFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "no git repository found in {}. Pass a repository path, or a directory containing one.",
            self.0.display()
        )
    }
}

impl std::error::Error for NoRepositoryFound {}

fn show_how_to_pick_a_repository(folder: &Path) -> Result<()> {
    let example = example_repository_path();
    let steps = [
        ("Index a repository by path:", index_command_line(example)),
        ("Or go into it first:", format!("cd {example}")),
        ("", index_command_line(".")),
    ]
    .map(|(label, command)| format!("{label:<27}  {command}"))
    .join("\n");

    tui::card(
        "No git repository here",
        format!(
            "{} has no git repository in it.\n\n{steps}",
            folder.display()
        ),
    )?;
    tui::outro("Nothing indexed.")
}

pub(crate) fn grep_command_line(definition_name: &str) -> String {
    let is_plain_word = definition_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_');
    let argument = match is_plain_word {
        true => definition_name.to_string(),
        false => format!("'{}'", definition_name.replace('\'', "'\\''")),
    };
    format!("{} grep {argument}", spec::launcher())
}

pub(crate) fn index_command_line(path: &str) -> String {
    let is_plain_path = path
        .chars()
        .all(|c| c.is_alphanumeric() || "_-./~:\\".contains(c));
    let argument = match is_plain_path {
        true => path.to_string(),
        false => format!("'{}'", path.replace('\'', "'\\''")),
    };
    format!("{} workspace index {argument}", spec::launcher())
}

pub(crate) fn example_repository_path() -> &'static str {
    if cfg!(windows) {
        r"C:\code\app"
    } else if cfg!(target_os = "macos") {
        "~/Projects/app"
    } else {
        "~/src/app"
    }
}

fn most_referenced_definition(git: &GitInfo, db: Option<PathBuf>) -> Option<String> {
    let batches = GraphReader::open(db)
        .ok()?
        .query_arrow_json(
            "SELECT d.name FROM gl_definition d JOIN gl_edge e ON e.target_id = d.id \
             WHERE d.project_id = ?1 AND d.commit_sha = ?2 AND length(d.name) > 3 \
             GROUP BY d.name ORDER BY count(*) DESC, d.name LIMIT 1",
            &[git.project_id.into(), git.commit_sha.clone().into()],
        )
        .ok()?;
    let names = batches
        .first()?
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()?;
    (!names.is_empty()).then(|| names.value(0).to_string())
}

fn install_tracing(verbose: bool, person_is_watching: bool) {
    let level = match (verbose, person_is_watching) {
        (true, _) => Level::DEBUG,
        (false, true) => Level::ERROR,
        (false, false) => Level::WARN,
    };
    let span_events = match verbose {
        true => FmtSpan::CLOSE,
        false => FmtSpan::NONE,
    };
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(verbose)
        .with_level(verbose)
        .with_ansi(true)
        .without_time()
        .with_span_events(span_events)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
