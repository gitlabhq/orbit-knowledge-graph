use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;

use super::json;
use super::{Installer, Report, backup_once, remove_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};

pub(super) struct Hooks;

impl Installer for Hooks {
    fn plan(&self, assistant: Agent, target: &Target) -> Result<Vec<String>> {
        let files: BTreeSet<String> = assistant
            .json_merges
            .iter()
            .map(|merge| &merge.file)
            .chain(assistant.template_files.iter().map(|file| &file.path))
            .chain(assistant.registrations.iter().map(|entry| &entry.file))
            .map(|scoped| target.resolve(scoped).map(|(_, label)| label))
            .collect::<Result<_>>()?;
        Ok(files.into_iter().collect())
    }

    fn install(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for assistant in assistants {
            install_for(*assistant, target, report)?;
        }
        Ok(())
    }

    fn remove(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for assistant in assistants {
            remove_for(*assistant, target, report)?;
        }
        Ok(())
    }
}

fn install_for(assistant: Agent, target: &Target, report: &mut Report) -> Result<()> {
    for merge in &assistant.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        let entries: Vec<Value> = merge.entries.iter().map(resolve_launcher).collect();
        let mut root = json::read_object(&path)?;
        if path.exists() {
            backup_once(&path, &label, report)?;
        }
        json::merge_owned(&mut root, &merge.path, &merge.marker, &entries)
            .with_context(|| format!("failed to update {}", path.display()))?;
        json::write_object(&path, &root)?;
        report.note(&label, "orbit entries installed");
    }

    for template_file in &assistant.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if path.exists() {
            backup_once(&path, &label, report)?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&path, template_file.contents())
            .with_context(|| format!("failed to write {}", path.display()))?;
        report.note(&label, "written");
    }

    for registration in &assistant.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        let value = target.registration_value(&registration.value)?;
        let mut root = json::read_object(&path)?;
        if json::register(&mut root, &registration.path, &value)
            .with_context(|| format!("failed to update {}", path.display()))?
        {
            if path.exists() {
                backup_once(&path, &label, report)?;
            }
            json::write_object(&path, &root)?;
            report.note(&label, format!("{} registered", registration.value.project));
        }
    }

    Ok(())
}

fn remove_for(assistant: Agent, target: &Target, report: &mut Report) -> Result<()> {
    for merge in &assistant.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        if !path.exists() {
            continue;
        }
        let mut root = json::read_object(&path)?;
        if json::remove_owned(&mut root, &merge.path, &merge.marker) {
            json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
        }
    }

    for template_file in &assistant.template_files {
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
        remove_file(&path, target)?;
        report.note(&label, "removed");
    }

    for registration in &assistant.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        if !path.exists() {
            continue;
        }
        let value = target.registration_value(&registration.value)?;
        let mut root = json::read_object(&path)?;
        if json::deregister(&mut root, &registration.path, &value) {
            json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
        }
    }

    Ok(())
}

fn resolve_launcher(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace("{{orbit}}", spec::launcher())),
        Value::Array(items) => Value::Array(items.iter().map(resolve_launcher).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), resolve_launcher(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}
