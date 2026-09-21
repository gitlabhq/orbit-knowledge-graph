use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::{Result, bail};

use super::changes::{self, Report};
use super::detect::Machine;
use super::plan::{self, Plan, Selection};
use super::spec::{self, AssistantSpec};
use super::{Component, Options, Target};

pub(crate) fn install(options: Options, target: Target, machine: &Machine) -> Result<()> {
    let interactive = interactive(&options)?;
    cliclack::intro("Orbit setup")?;

    let detected = machine.installed_assistants();
    let mut selection = Selection::for_install(&options, &detected)?;
    if interactive {
        selection.assistants = choose_assistants(
            "Which assistants should use Orbit?",
            &selection.assistants,
            &detection_hints(&detected, machine),
        )?;
        selection.components = choose_components(&selection.components)?;
    }
    if selection.assistants.is_empty() {
        cliclack::outro_cancel(format!(
            "No assistant selected. Name one to configure it: orbit setup <{}>",
            spec::names().join("|")
        ))?;
        return Ok(());
    }

    show_plan(&plan::build(&selection, &target)?)?;
    if options.dry_run {
        cliclack::outro("Dry run: nothing written.")?;
        return Ok(());
    }
    if interactive
        && !confirm(format!(
            "Apply to {} assistant(s)?",
            selection.assistants.len()
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
    cliclack::intro("Orbit uninstall")?;

    let mut selection = Selection::for_uninstall(&options)?;
    if interactive {
        selection.assistants = choose_assistants(
            "Remove Orbit from which assistants?",
            &selection.assistants,
            &BTreeMap::new(),
        )?;
    }
    if selection.assistants.is_empty() {
        cliclack::outro_cancel("No assistant selected.")?;
        return Ok(());
    }

    show_plan(&plan::build(&selection, &target)?)?;
    if options.dry_run {
        cliclack::outro("Dry run: nothing removed.")?;
        return Ok(());
    }
    if interactive
        && !confirm(format!(
            "Remove Orbit from {} assistant(s)?",
            selection.assistants.len()
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
    let chosen = picker
        .initial_values(preselected.iter().map(|a| a.name.as_str()).collect())
        .interact()?;
    Ok(spec::all()
        .iter()
        .filter(|assistant| chosen.contains(&assistant.name.as_str()))
        .collect())
}

fn choose_components(preselected: &BTreeSet<Component>) -> Result<BTreeSet<Component>> {
    let mut picker = cliclack::multiselect("What should each assistant get?").required(false);
    for component in Component::ALL {
        picker = picker.item(component, component.label(), component.hint());
    }
    let chosen = picker
        .initial_values(preselected.iter().copied().collect())
        .interact()?;
    Ok(chosen.into_iter().collect())
}

fn show_plan(plan: &Plan) -> Result<()> {
    for assistant in &plan.assistants {
        let body = if assistant.changes.is_empty() {
            "nothing selected applies to this assistant".to_string()
        } else {
            assistant
                .changes
                .iter()
                .map(|(component, place)| format!("{:<13} {place}", component.label()))
                .collect::<Vec<_>>()
                .join("\n")
        };
        cliclack::note(&assistant.title, body)?;
    }
    Ok(cliclack::log::remark(&plan.scope)?)
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
