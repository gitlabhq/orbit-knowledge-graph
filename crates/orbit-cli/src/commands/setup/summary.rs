use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::Component;
use super::components::{Outcome, Report};
use super::detect::Machine;
use super::index_repo::IndexOutcome;
use super::plan::Plan;
use super::spec::{self, Agent};
use crate::commands::index::{example_repository_path, grep_command_line, index_command_line};
use crate::tui::{Choice, align_columns};

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
            section: None,
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

pub(super) fn format_try_it_command(outcome: &IndexOutcome) -> Option<String> {
    match outcome {
        IndexOutcome::OutsideRepository => Some(index_command_line(example_repository_path())),
        IndexOutcome::NotIndexed { index_path } => Some(index_command_line(index_path)),
        IndexOutcome::Indexed { suggested_grep } => {
            suggested_grep.as_deref().map(grep_command_line)
        }
    }
}

pub(super) fn format_closing_line(outcome: &IndexOutcome) -> &'static str {
    match outcome {
        IndexOutcome::OutsideRepository => {
            "Done. This folder is not a git repository, so nothing was indexed."
        }
        IndexOutcome::NotIndexed { .. } => {
            "Done. Run it, then ask your agent where a function is defined."
        }
        IndexOutcome::Indexed {
            suggested_grep: None,
        } => "Done. Ask your agent where a function is defined.",
        IndexOutcome::Indexed { .. } => "Done.",
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
