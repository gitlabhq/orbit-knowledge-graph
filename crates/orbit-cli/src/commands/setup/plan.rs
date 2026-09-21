use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Context, Result};

use super::components::{self, Report};
use super::spec::{self, Agent};
use super::{Component, Options, Target};

pub(super) struct Selection {
    pub(super) assistants: Vec<Agent>,
    pub(super) components: BTreeSet<Component>,
}

impl Selection {
    pub(super) fn for_install(
        options: &Options,
        detected: &[(Agent, PathBuf)],
    ) -> Result<Selection> {
        let assistants = if options.all {
            spec::all().collect()
        } else if !options.assistants.is_empty() {
            named_specs(&options.assistants)?
        } else {
            detected.iter().map(|(assistant, _)| *assistant).collect()
        };
        Ok(Selection {
            assistants,
            components: options.components.clone(),
        })
    }

    pub(super) fn names(&self) -> Vec<String> {
        self.assistants
            .iter()
            .map(|agent| agent.name.clone())
            .collect()
    }

    pub(super) fn choose(&mut self, names: &[String]) -> Result<()> {
        self.assistants = named_specs(names)?;
        Ok(())
    }

    pub(super) fn for_uninstall(options: &Options) -> Result<Selection> {
        let assistants = if options.assistants.is_empty() {
            spec::all().collect()
        } else {
            named_specs(&options.assistants)?
        };
        Ok(Selection {
            assistants,
            components: Component::ALL.into_iter().collect(),
        })
    }
}

fn named_specs(names: &[String]) -> Result<Vec<Agent>> {
    let unique: BTreeSet<&String> = names.iter().collect();
    unique
        .into_iter()
        .map(|name| spec::get(name).with_context(|| format!("unknown assistant {name:?}")))
        .collect()
}

pub(super) struct Plan {
    pub(super) scope: String,
    pub(super) assistants: Vec<AssistantPlan>,
}

impl Plan {
    pub(super) fn only_reported(mut self, report: &Report) -> Plan {
        for assistant in &mut self.assistants {
            assistant.components.retain(|(_, paths)| {
                paths.iter().any(|path| {
                    report
                        .outcomes
                        .iter()
                        .any(|outcome| path.starts_with(&outcome.label))
                })
            });
        }
        self
    }
}

pub(super) struct AssistantPlan {
    pub(super) title: String,
    pub(super) components: Vec<(Component, Vec<String>)>,
}

pub(super) fn build(selection: &Selection, target: &Target) -> Result<Plan> {
    let mut assistants: Vec<AssistantPlan> = Vec::new();
    for assistant in &selection.assistants {
        let mut planned: Vec<(Component, Vec<String>)> = Vec::new();
        for component in &selection.components {
            let paths = components::for_component(*component).plan(*assistant, target)?;
            if !paths.is_empty() {
                planned.push((*component, paths));
            }
        }
        assistants.push(AssistantPlan {
            title: assistant.title.clone(),
            components: planned,
        });
    }
    Ok(Plan {
        scope: target.describe(),
        assistants,
    })
}
