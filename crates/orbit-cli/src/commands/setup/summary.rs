use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::Component;
use super::components::Report;
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
    let mut cards: Vec<(String, Vec<String>)> = Vec::new();
    for outcome in &report.outcomes {
        let line = format!("{}  {}", outcome.label, outcome.action);
        match cards.last_mut() {
            Some((group, lines)) if *group == outcome.group => lines.push(line),
            _ => cards.push((outcome.group.clone(), vec![line])),
        }
    }
    cards
        .into_iter()
        .map(|(group, lines)| (group, lines.join("\n")))
        .collect()
}
