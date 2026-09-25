use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::Component;
use super::components::{Outcome, Report};
use super::detect::Machine;
use super::index_repo::{self, Indexed};
use super::plan::Plan;
use super::spec::{self, Agent};
use crate::tui::Choice;

pub(super) fn join_component_labels(components: &BTreeSet<Component>) -> String {
    components
        .iter()
        .map(|component| component.label())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn detected_location_hints(
    detected: &[(Agent, PathBuf)],
    machine: &Machine,
) -> BTreeMap<String, String> {
    spec::agents()
        .map(|agent| {
            let hint = detected
                .iter()
                .find(|(candidate, _)| candidate.name == agent.name)
                .map(|(_, path)| machine.display_with_tilde(path))
                .unwrap_or_else(|| "not detected".to_string());
            (agent.name.clone(), hint)
        })
        .collect()
}

pub(super) fn agent_picker_choices(
    agents: &[Agent],
    location_hints: &BTreeMap<String, String>,
) -> Vec<Choice> {
    agents
        .iter()
        .map(|agent| Choice {
            key: agent.name.clone(),
            label: agent.title.clone(),
            hint: location_hints.get(&agent.name).cloned().unwrap_or_default(),
        })
        .collect()
}

pub(super) fn format_components_per_agent(plan: &Plan) -> String {
    align_columns(plan.agents.iter().map(|agent| {
        let components = match agent.components.is_empty() {
            true => "nothing selected applies".to_string(),
            false => agent
                .components
                .iter()
                .map(|(component, _)| component.label())
                .collect::<Vec<_>>()
                .join(", "),
        };
        (agent.title.as_str(), components)
    }))
}

pub(super) fn format_files_per_component(plan: &Plan) -> String {
    let mut files_by_component: BTreeMap<Component, BTreeSet<&str>> = BTreeMap::new();
    for agent in &plan.agents {
        for (component, paths) in &agent.components {
            files_by_component
                .entry(*component)
                .or_default()
                .extend(paths.iter().map(String::as_str));
        }
    }
    let mut rows: Vec<String> = Vec::new();
    for (component, paths) in files_by_component {
        rows.push(component.label().to_string());
        rows.extend(paths.into_iter().map(|path| format!("  {path}")));
    }
    rows.join("\n")
}

pub(super) fn format_try_it_command(indexed: Option<&Indexed>) -> Option<String> {
    match indexed {
        None => Some(index_repo::index_command_line()),
        Some(indexed) => indexed
            .suggested_grep
            .as_deref()
            .map(crate::commands::index::grep_command_line),
    }
}

pub(super) fn format_closing_line(indexed: Option<&Indexed>) -> &'static str {
    match indexed {
        None => "Done. Run it in a repository, then ask your agent where a function is defined.",
        Some(Indexed {
            suggested_grep: None,
            ..
        }) => "Done. Ask your agent where a function is defined.",
        Some(_) => "Done.",
    }
}

pub(super) fn format_outcomes_per_component(report: &Report) -> Vec<(String, String)> {
    group_outcomes_by_component(report)
        .into_iter()
        .map(|(component, outcomes)| {
            let rows = outcomes
                .iter()
                .map(|outcome| (outcome.label.as_str(), outcome.action.clone()));
            (component.to_string(), align_columns(rows))
        })
        .collect()
}

pub(super) fn format_removed_files_per_component(report: &Report) -> String {
    let groups = group_outcomes_by_component(report);
    if groups.is_empty() {
        return "nothing to remove".to_string();
    }
    align_columns(groups.iter().map(|(component, outcomes)| {
        let mut listed: BTreeSet<&str> = BTreeSet::new();
        let mut files: Vec<String> = Vec::new();
        for outcome in outcomes {
            if !listed.insert(outcome.label.as_str()) {
                continue;
            }
            files.push(match outcome.action.starts_with("kept") {
                true => format!("{} (kept)", outcome.label),
                false => outcome.label.clone(),
            });
        }
        (*component, files.join(", "))
    }))
}

fn align_columns<'a>(rows: impl Iterator<Item = (&'a str, String)>) -> String {
    let rows: Vec<(&str, String)> = rows.collect();
    let width = rows
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or_default();
    rows.iter()
        .map(|(name, detail)| format!("{name:<width$}   {detail}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn group_outcomes_by_component(report: &Report) -> Vec<(&str, Vec<&Outcome>)> {
    let mut groups: Vec<(&str, Vec<&Outcome>)> = Vec::new();
    for outcome in &report.outcomes {
        match groups.last_mut() {
            Some((component, outcomes)) if *component == outcome.group => outcomes.push(outcome),
            _ => groups.push((outcome.group.as_str(), vec![outcome])),
        }
    }
    groups
}
