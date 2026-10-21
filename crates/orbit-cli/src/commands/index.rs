pub(crate) mod incremental;

use std::path::PathBuf;

use anyhow::Result;
use arrow::array::{Array, StringArray};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

use super::setup::spec;
use crate::database::GraphReader;
use crate::workspace::GitInfo;

pub(crate) fn index_and_suggest(path: PathBuf, db: Option<PathBuf>) -> Result<Option<String>> {
    let git = crate::workspace::git_info(&crate::workspace::git_toplevel(&path)?)?;
    index_quietly(path, db.clone())?;
    Ok(most_referenced_definition(&git, db))
}

pub(crate) fn index_before_first_query(path: PathBuf, db: Option<PathBuf>) -> Result<()> {
    index_quietly(path, db).map(drop)
}

pub(crate) fn collect(
    path: PathBuf,
    stats: bool,
    db: Option<PathBuf>,
) -> Result<Vec<Vec<incremental::FamilyOutput>>> {
    incremental::index(
        path,
        incremental::Options {
            stats,
            verbose: false,
            db,
            budget_seconds: None,
        },
    )
}

fn index_quietly(
    path: PathBuf,
    db: Option<PathBuf>,
) -> Result<Vec<Vec<incremental::FamilyOutput>>> {
    collect(path, false, db)
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
    format!("{} index {argument}", spec::launcher())
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
