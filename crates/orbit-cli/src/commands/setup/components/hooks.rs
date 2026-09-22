use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;

use super::json;
use super::{
    Installer, Report, backup_once, file_mentions, remove_file_and_empty_parents, write_file,
};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};

pub(super) struct Hooks;

impl Installer for Hooks {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let files: BTreeSet<String> = agent
            .json_merges
            .iter()
            .map(|merge| &merge.file)
            .chain(agent.template_files.iter().map(|file| &file.path))
            .chain(agent.registrations.iter().map(|entry| &entry.file))
            .map(|scoped| target.resolve(scoped).map(|(_, label)| label))
            .collect::<Result<_>>()?;
        Ok(files.into_iter().collect())
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for agent in agents {
            install_for_agent(*agent, target, report)?;
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for agent in agents {
            remove_for_agent(*agent, target, report)?;
        }
        Ok(())
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        let merged = agent.json_merges.iter().any(|merge| {
            target
                .resolve(&merge.file)
                .is_ok_and(|(path, _)| file_mentions(&path, &merge.marker))
        });
        let templated = agent.template_files.iter().any(|template_file| {
            target
                .resolve(&template_file.path)
                .is_ok_and(|(path, _)| path.exists())
        });
        merged || templated
    }
}

fn install_for_agent(agent: Agent, target: &Target, report: &mut Report) -> Result<()> {
    for merge in &agent.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        let entries: Vec<Value> = merge
            .entries
            .iter()
            .map(substitute_launcher_in_json)
            .collect();
        let mut root = json::read_object(&path)?;
        if path.exists() {
            backup_once(&path, &label, report)?;
        }
        json::replace_marked_entries(&mut root, &merge.path, &merge.marker, &entries)
            .with_context(|| format!("failed to update {}", path.display()))?;
        json::write_object(&path, &root)?;
        report.note(&label, "orbit entries installed");
    }

    for template_file in &agent.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if path.exists() {
            backup_once(&path, &label, report)?;
        }
        write_file(&path, template_file.render())?;
        report.note(&label, "written");
    }

    for registration in &agent.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        let value = target.registration_value(&registration.value)?;
        let (_, value_label) = target.resolve(&registration.value)?;
        let mut root = json::read_object(&path)?;
        if json::append_unique(&mut root, &registration.path, &value)
            .with_context(|| format!("failed to update {}", path.display()))?
        {
            if path.exists() {
                backup_once(&path, &label, report)?;
            }
            json::write_object(&path, &root)?;
            report.note(&label, format!("{value_label} registered"));
        }
    }

    Ok(())
}

fn remove_for_agent(agent: Agent, target: &Target, report: &mut Report) -> Result<()> {
    for merge in &agent.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        if !path.exists() {
            continue;
        }
        let mut root = json::read_object(&path)?;
        if json::remove_marked_entries(&mut root, &merge.path, &merge.marker) {
            json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
        }
    }

    for template_file in &agent.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if !path.exists() {
            continue;
        }
        let current = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if !template_file.is_unmodified(&current) {
            report.note(&label, "kept (edited since install; delete it by hand)");
            continue;
        }
        remove_file_and_empty_parents(&path, target)?;
        report.note(&label, "removed");
    }

    for registration in &agent.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        if !path.exists() {
            continue;
        }
        let value = target.registration_value(&registration.value)?;
        let mut root = json::read_object(&path)?;
        if json::remove_value(&mut root, &registration.path, &value) {
            json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
        }
    }

    Ok(())
}

fn substitute_launcher_in_json(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace("{{orbit}}", spec::launcher())),
        Value::Array(items) => {
            Value::Array(items.iter().map(substitute_launcher_in_json).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), substitute_launcher_in_json(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}
