use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{Result, bail};
use arrow::array::{Array, StringArray};
use cliclack::{Theme, ThemeState};
use serde::Deserialize;

use super::changes::{self, Report};
use super::detect::Machine;
use super::plan::{self, Plan, Selection};
use super::spec::{self, AssistantSpec};
use super::{Component, Options, Target};

pub(crate) fn install(options: Options, target: Target, machine: &Machine) -> Result<()> {
    let interactive = interactive(&options)?;
    let detected = machine.installed_assistants();
    let mut selection = Selection::for_install(&options, &detected)?;
    cliclack::intro(format!(
        "Orbit setup ({})",
        component_list(&selection.components)
    ))?;

    if interactive {
        selection.assistants = choose_assistants(
            "Which agents should use Orbit?",
            &selection.assistants,
            &detection_hints(&detected, machine),
        )?;
    }
    if selection.assistants.is_empty() {
        cliclack::outro_cancel(format!(
            "No agent selected. Name one to configure it: orbit setup <{}>",
            spec::names().join("|")
        ))?;
        return Ok(());
    }

    let plan = plan::build(&selection, &target)?;
    if options.dry_run {
        show_plan(&plan, "Plan")?;
        show_paths(&plan)?;
        cliclack::outro("Dry run: nothing written.")?;
        return Ok(());
    }

    let mut report = Report::default();
    let applied = changes::install(&selection, &target, &mut report);
    if options.verbose {
        show_every_file(&report)?;
    }
    applied?;
    show_plan(&plan, "Configured")?;
    let next_step = match options.index {
        true => index_current_repository()?,
        false => None,
    };
    cliclack::outro(match next_step {
        Some(NextStep::Grep(name)) => {
            format!("Done. Try: {} grep \"{name}\"", spec::launcher())
        }
        Some(NextStep::Ask) => "Done. Ask your agent where a function is defined.".to_string(),
        None => format!(
            "Done. Run {} index in a repository, then ask your agent where a function is defined.",
            spec::launcher()
        ),
    })?;
    Ok(())
}

enum NextStep {
    Grep(String),
    Ask,
}

fn index_current_repository() -> Result<Option<NextStep>> {
    let cwd = std::env::current_dir()?;
    let repos = crate::workspace::Workspace::open_default()?.resolve_repos(&cwd)?;
    if repos.is_empty() {
        return Ok(None);
    }

    let command = format!("{} index .", spec::launcher());
    let spinner = cliclack::spinner();
    spinner.start(&command);
    let output = launcher()?
        .args(["index", "."])
        .current_dir(&cwd)
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success() {
        spinner.error(format!("{command}  failed with {}", output.status));
        return Ok(None);
    }
    let summaries = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<IndexSummary>()
        .filter_map(Result::ok)
        .map(|summary| {
            format!(
                "{} files, {} definitions, {:.0}s",
                with_thousands(summary.graph.files),
                with_thousands(summary.graph.definitions),
                summary.time_seconds
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    spinner.stop(format!("{command}  {summaries}"));
    Ok(Some(
        most_referenced_definition(&cwd).map_or(NextStep::Ask, NextStep::Grep),
    ))
}

fn launcher() -> Result<Command> {
    Ok(match spec::launcher() {
        spec::GLAB_LAUNCHER => {
            let mut glab = Command::new("glab");
            glab.arg("orbit");
            glab
        }
        _ => Command::new(std::env::current_exe()?),
    })
}

#[derive(Deserialize)]
struct IndexSummary {
    time_seconds: f64,
    graph: IndexedGraph,
}

#[derive(Deserialize)]
struct IndexedGraph {
    files: usize,
    definitions: usize,
}

fn most_referenced_definition(repo: &std::path::Path) -> Option<String> {
    let indexed = crate::workspace::open_indexed(Some(repo.to_path_buf()), None).ok()?;
    let batches = indexed
        .client
        .query_arrow_json(
            "SELECT d.name FROM gl_definition d JOIN gl_edge e ON e.target_id = d.id \
             WHERE d.project_id = ?1 AND d.commit_sha = ?2 AND length(d.name) > 3 \
             GROUP BY d.name ORDER BY count(*) DESC, d.name LIMIT 1",
            &[
                indexed.git.project_id.into(),
                indexed.git.commit_sha.clone().into(),
            ],
        )
        .ok()?;
    let names = batches
        .first()?
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()?;
    (!names.is_empty()).then(|| names.value(0).to_string())
}

fn with_thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

pub(crate) fn uninstall(options: Options, target: Target) -> Result<()> {
    let interactive = interactive(&options)?;
    let mut selection = Selection::for_uninstall(&options)?;
    cliclack::intro(format!(
        "Orbit uninstall ({})",
        component_list(&selection.components)
    ))?;

    if interactive {
        selection.assistants = choose_assistants(
            "Remove Orbit from which agents?",
            &selection.assistants,
            &BTreeMap::new(),
        )?;
    }
    if selection.assistants.is_empty() {
        cliclack::outro_cancel("No agent selected.")?;
        return Ok(());
    }

    let plan = plan::build(&selection, &target)?;
    if options.dry_run {
        show_plan(&plan, "Plan")?;
        show_paths(&plan)?;
        cliclack::outro("Dry run: nothing removed.")?;
        return Ok(());
    }

    let mut report = Report::default();
    let removed = changes::remove(&selection, &target, &mut report);
    if options.verbose {
        show_every_file(&report)?;
    }
    removed?;
    show_plan(&plan, "Removed")?;
    cliclack::outro("Done. Backups (*.orbit-backup) were kept.")?;
    Ok(())
}

fn interactive(options: &Options) -> Result<bool> {
    if options.yes {
        return Ok(false);
    }
    if !std::io::stdin().is_terminal() {
        bail!("stdin is not a terminal; pass --yes to proceed without a prompt");
    }
    Ok(true)
}

fn detection_hints(
    detected: &[(&'static AssistantSpec, PathBuf)],
    machine: &Machine,
) -> BTreeMap<&'static str, String> {
    spec::all()
        .iter()
        .map(|assistant| {
            let found = detected
                .iter()
                .find(|(candidate, _)| candidate.name == assistant.name)
                .map(|(_, path)| machine.abbreviate(path));
            (
                assistant.name.as_str(),
                found.unwrap_or_else(|| "not detected".to_string()),
            )
        })
        .collect()
}

fn component_list(components: &BTreeSet<Component>) -> String {
    components
        .iter()
        .map(|component| component.label())
        .collect::<Vec<_>>()
        .join(", ")
}

fn choose_assistants(
    prompt: &str,
    preselected: &[&'static AssistantSpec],
    hints: &BTreeMap<&str, String>,
) -> Result<Vec<&'static AssistantSpec>> {
    let mut picker = cliclack::multiselect(prompt).required(false);
    for assistant in spec::all() {
        let hint = hints
            .get(assistant.name.as_str())
            .cloned()
            .unwrap_or_default();
        picker = picker.item(assistant.name.as_str(), &assistant.title, hint);
    }
    picker = picker.initial_values(preselected.iter().map(|a| a.name.as_str()).collect());

    cliclack::set_theme(PickerKeysFooter);
    let chosen = picker.interact();
    cliclack::reset_theme();
    let chosen = chosen?;
    Ok(spec::all()
        .iter()
        .filter(|assistant| chosen.contains(&assistant.name.as_str()))
        .collect())
}

struct PickerKeysFooter;

impl Theme for PickerKeysFooter {
    fn format_footer_with_message(&self, state: &ThemeState, message: &str) -> String {
        struct Stock;
        impl Theme for Stock {}

        let keys = match state {
            ThemeState::Active => "space toggles, enter confirms",
            _ => "",
        };
        let footer = [message, keys]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("  ");
        Stock.format_footer_with_message(state, &footer)
    }
}

fn show_plan(plan: &Plan, title: &str) -> Result<()> {
    let width = plan
        .assistants
        .iter()
        .map(|assistant| assistant.title.len())
        .max()
        .unwrap_or_default();
    let rows = plan
        .assistants
        .iter()
        .map(|assistant| {
            let components = match assistant.changes.is_empty() {
                true => "nothing selected applies".to_string(),
                false => assistant
                    .changes
                    .iter()
                    .map(|(component, _)| component.label())
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            format!("{:<width$}   {components}", assistant.title)
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(cliclack::note(title, rows)?)
}

fn show_paths(plan: &Plan) -> Result<()> {
    let mut by_component: BTreeMap<Component, BTreeSet<&str>> = BTreeMap::new();
    for assistant in &plan.assistants {
        for (component, paths) in &assistant.changes {
            by_component
                .entry(*component)
                .or_default()
                .extend(paths.iter().map(String::as_str));
        }
    }
    let mut rows: Vec<String> = Vec::new();
    for (component, paths) in by_component {
        rows.push(component.label().to_string());
        rows.extend(paths.into_iter().map(|path| format!("  {path}")));
    }
    Ok(cliclack::note(
        format!("Files in {}", plan.scope),
        rows.join("\n"),
    )?)
}

fn show_every_file(report: &Report) -> Result<()> {
    let mut cards: Vec<(&str, Vec<String>)> = Vec::new();
    for outcome in &report.outcomes {
        let line = format!("{}  {}", outcome.label, outcome.action);
        match cards.last_mut() {
            Some((group, lines)) if *group == outcome.group => lines.push(line),
            _ => cards.push((&outcome.group, vec![line])),
        }
    }
    for (group, lines) in cards {
        cliclack::note(group, lines.join("\n"))?;
    }
    Ok(())
}
