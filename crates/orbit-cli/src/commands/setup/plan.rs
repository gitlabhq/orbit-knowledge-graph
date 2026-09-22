use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Context, Result};

use super::components;
use super::spec::{self, Agent};
use super::{Component, Options, Target};

pub(super) struct Selection {
    pub(super) agents: Vec<Agent>,
    pub(super) components: BTreeSet<Component>,
}

impl Selection {
    pub(super) fn from_setup_options(
        options: &Options,
        detected: &[(Agent, PathBuf)],
    ) -> Result<Selection> {
        let agents = if options.all {
            spec::agents().collect()
        } else if !options.agents.is_empty() {
            agents_named(&options.agents)?
        } else {
            detected.iter().map(|(agent, _)| *agent).collect()
        };
        Ok(Selection {
            agents,
            components: options.components.clone(),
        })
    }

    pub(super) fn from_uninstall_options(options: &Options, target: &Target) -> Result<Selection> {
        let components: BTreeSet<Component> = Component::ALL.into_iter().collect();
        let agents = if options.agents.is_empty() {
            components::installed_agents(&components, target)
        } else {
            agents_named(&options.agents)?
        };
        Ok(Selection { agents, components })
    }

    pub(super) fn selected_agent_names(&self) -> Vec<String> {
        self.agents.iter().map(|agent| agent.name.clone()).collect()
    }

    pub(super) fn with_agents_named(mut self, names: &[String]) -> Result<Selection> {
        self.agents = agents_named(names)?;
        Ok(self)
    }
}

fn agents_named(names: &[String]) -> Result<Vec<Agent>> {
    let unique: BTreeSet<&String> = names.iter().collect();
    unique
        .into_iter()
        .map(|name| spec::agent_named(name).with_context(|| format!("unknown agent {name:?}")))
        .collect()
}

pub(super) struct Plan {
    pub(super) scope: String,
    pub(super) agents: Vec<AgentPlan>,
}

pub(super) struct AgentPlan {
    pub(super) title: String,
    pub(super) components: Vec<(Component, Vec<String>)>,
}

pub(super) fn for_selection(selection: &Selection, target: &Target) -> Result<Plan> {
    let mut agents: Vec<AgentPlan> = Vec::new();
    for agent in &selection.agents {
        let mut planned: Vec<(Component, Vec<String>)> = Vec::new();
        for component in &selection.components {
            let paths = components::installer_for(*component).plan(*agent, target)?;
            if !paths.is_empty() {
                planned.push((*component, paths));
            }
        }
        agents.push(AgentPlan {
            title: agent.title.clone(),
            components: planned,
        });
    }
    Ok(Plan {
        scope: target.scope_label(),
        agents,
    })
}
