use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::Component;
use super::components::{Outcome, Report};
use super::detect::Machine;
use super::plan::Plan;
use super::spec::{self, Agent};
use crate::tui::Choice;

pub(super) fn component_list(components: &BTreeSet<Component>) -> String {
    components
        .iter()
        .map(|component| component.label())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn detection_hints(
    detected: &[(Agent, PathBuf)],
    machine: &Machine,
) -> BTreeMap<String, String> {
    spec::all()
        .map(|agent| {
            let hint = detected
                .iter()
                .find(|(candidate, _)| candidate.name == agent.name)
                .map(|(_, path)| machine.abbreviate(path))
                .unwrap_or_else(|| "not detected".to_string());
            (agent.name.clone(), hint)
        })
        .collect()
}

pub(super) fn agent_choices(hints: &BTreeMap<String, String>) -> Vec<Choice> {
    spec::all()
        .map(|agent| Choice {
            key: agent.name.clone(),
            label: agent.title.clone(),
            hint: hints.get(&agent.name).cloned().unwrap_or_default(),
        })
        .collect()
}

pub(super) fn plan_rows(plan: &Plan) -> String {
    let width = plan
        .assistants
        .iter()
        .map(|assistant| assistant.title.len())
        .max()
        .unwrap_or_default();
    plan.assistants
        .iter()
        .map(|assistant| {
            let components = match assistant.components.is_empty() {
                true => "nothing selected applies".to_string(),
                false => assistant
                    .components
                    .iter()
                    .map(|(component, _)| component.label())
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            format!("{:<width$}   {components}", assistant.title)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn paths_rows(plan: &Plan) -> String {
    let mut by_component: BTreeMap<Component, BTreeSet<&str>> = BTreeMap::new();
    for assistant in &plan.assistants {
        for (component, paths) in &assistant.components {
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
    rows.join("\n")
}

pub(super) fn report_cards(report: &Report) -> Vec<(String, String)> {
    by_component(report)
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

pub(super) fn removed_rows(report: &Report) -> String {
    let groups = by_component(report);
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
                let file = match outcome.action.starts_with("kept") {
                    true => format!("{} (kept)", outcome.label),
                    false => outcome.label.clone(),
                };
                if !files.iter().any(|known| known.starts_with(&outcome.label)) {
                    files.push(file);
                }
            }
            format!("{component:<width$}   {}", files.join(", "))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn by_component(report: &Report) -> Vec<(&str, Vec<&Outcome>)> {
    let mut groups: Vec<(&str, Vec<&Outcome>)> = Vec::new();
    for outcome in &report.outcomes {
        match groups.last_mut() {
            Some((component, outcomes)) if *component == outcome.group => outcomes.push(outcome),
            _ => groups.push((outcome.group.as_str(), vec![outcome])),
        }
    }
    groups
}
