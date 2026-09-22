use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::Component;
use super::components::{Outcome, Report};
use super::detect::Machine;
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
    let width = plan
        .agents
        .iter()
        .map(|agent| agent.title.len())
        .max()
        .unwrap_or_default();
    plan.agents
        .iter()
        .map(|agent| {
            let components = match agent.components.is_empty() {
                true => "nothing selected applies".to_string(),
                false => agent
                    .components
                    .iter()
                    .map(|(component, _)| component.label())
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            format!("{:<width$}   {components}", agent.title)
        })
        .collect::<Vec<_>>()
        .join("\n")
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

pub(super) fn format_outcomes_per_component(report: &Report) -> Vec<(String, String)> {
    group_outcomes_by_component(report)
        .into_iter()
        .map(|(component, outcomes)| {
            let lines: Vec<String> = outcomes
                .iter()
                .map(|outcome| format!("{}  {}", outcome.label, outcome.action))
                .collect();
            (component.to_string(), lines.join("\n"))
        })
        .collect()
}

pub(super) fn format_removed_files_per_component(report: &Report) -> String {
    let groups = group_outcomes_by_component(report);
    if groups.is_empty() {
        return "nothing was installed".to_string();
    }
    let width = groups
        .iter()
        .map(|(component, _)| component.len())
        .max()
        .unwrap_or_default();
    groups
        .iter()
        .map(|(component, outcomes)| {
            let mut files: Vec<String> = Vec::new();
            for outcome in outcomes {
                let already_listed = files.iter().any(|file| file.starts_with(&outcome.label));
                if already_listed {
                    continue;
                }
                files.push(match outcome.action.starts_with("kept") {
                    true => format!("{} (kept)", outcome.label),
                    false => outcome.label.clone(),
                });
            }
            format!("{component:<width$}   {}", files.join(", "))
        })
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
