use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::{Result, bail};
use cliclack::{Theme, ThemeState};

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
        show_paths(&plan)?;
        cliclack::outro("Dry run: nothing written.")?;
        return Ok(());
    }
    if interactive
        && !confirm(format!(
            "Apply to {} agent(s) in {}?",
            plan.assistants.len(),
            plan.scope
        ))?
    {
        cliclack::outro_cancel("Nothing written.")?;
        return Ok(());
    }

    let mut report = Report::default();
    let applied = changes::install(&selection, &target, &mut report);
    show_report(&report)?;
    applied?;
    cliclack::outro("Done. Undo any time with `orbit uninstall`.")?;
    Ok(())
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
        show_paths(&plan)?;
        cliclack::outro("Dry run: nothing removed.")?;
        return Ok(());
    }
    if interactive
        && !confirm(format!(
            "Remove Orbit from {} agent(s) in {}?",
            plan.assistants.len(),
            plan.scope
        ))?
    {
        cliclack::outro_cancel("Nothing removed.")?;
        return Ok(());
    }

    let mut report = Report::default();
    let removed = changes::remove(&selection, &target, &mut report);
    show_report(&report)?;
    removed?;
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

fn confirm(question: impl Display) -> Result<bool> {
    Ok(cliclack::confirm(question).initial_value(true).interact()?)
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

fn show_plan(plan: &Plan) -> Result<()> {
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
    Ok(cliclack::note("Plan", rows)?)
}

fn show_paths(plan: &Plan) -> Result<()> {
    show_plan(plan)?;
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
    cliclack::note(format!("Files in {}", plan.scope), rows.join("\n"))?;
    Ok(())
}

fn show_report(report: &Report) -> Result<()> {
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
