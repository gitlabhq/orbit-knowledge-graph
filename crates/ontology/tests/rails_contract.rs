use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use ontology::{Ontology, RequiredRole};
use serde::Deserialize;

#[derive(Deserialize)]
struct Snapshot {
    classes: BTreeMap<String, RailsClass>,
    policies: BTreeMap<String, RailsPolicy>,
    resource_types: BTreeMap<String, ResourceType>,
    roles: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Deserialize)]
struct RailsClass {
    policy: String,
}

#[derive(Deserialize)]
struct RailsPolicy {
    delegates: Vec<String>,
    enables: BTreeSet<String>,
    overrides: BTreeSet<String>,
}

#[derive(Deserialize)]
struct ResourceType {
    classes: Vec<String>,
}

impl Snapshot {
    fn policy_enables<'a>(
        &'a self,
        policy: &'a str,
        ability: &str,
        seen: &mut BTreeSet<&'a str>,
    ) -> bool {
        if !seen.insert(policy) {
            return false;
        }
        let rules = &self.policies[policy];
        rules.enables.contains(ability)
            || (!rules.overrides.contains(ability)
                && rules.delegates.iter().any(|target| {
                    self.classes
                        .get(target)
                        .is_some_and(|class| self.policy_enables(&class.policy, ability, seen))
                }))
    }

    fn lowest_role(&self, ability: &str) -> Option<u32> {
        let floors = self.roles.get(ability)?;
        let role = floors.get("project").or_else(|| floors.get("group"))?;
        Some(match role.as_str() {
            "planner" => 15,
            _ => serde_json::from_value::<RequiredRole>(role.as_str().into())
                .unwrap_or_else(|e| panic!("unknown Rails role {role}: {e}"))
                .as_access_level(),
        })
    }

    fn violations(&self) -> Vec<String> {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let mut violations = Vec::new();
        let mut abilities_by_type: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for node in ontology.nodes() {
            let config = node.redaction.as_ref().unwrap_or_else(|| {
                panic!(
                    "{} has no redaction block, so Rails never checks it",
                    node.name
                )
            });
            let (node, ability) = (&node.name, config.ability.as_str());
            abilities_by_type
                .entry(&config.resource_type)
                .or_default()
                .insert(ability);
            if !ability.starts_with("read_") {
                violations.push(format!(
                    "{node}: ability {ability:?} is not an explicit read_ ability"
                ));
            }
            let Some(resource_type) = self.resource_types.get(&config.resource_type) else {
                violations.push(format!(
                    "{node}: resource_type {:?} is not registered in Rails",
                    config.resource_type
                ));
                continue;
            };
            for class in &resource_type.classes {
                if !self.policy_enables(&self.classes[class].policy, ability, &mut BTreeSet::new())
                {
                    violations.push(format!("{node}: Rails {class} cannot be granted {ability}"));
                }
            }
            if let Some(rails_floor) = self.lowest_role(ability)
                && config.required_role.as_access_level() < rails_floor
            {
                violations.push(format!(
                    "{node}: required_role {:?} is below the lowest Rails role for {ability} ({rails_floor})",
                    config.required_role
                ));
            }
        }
        for (resource_type, abilities) in abilities_by_type {
            if abilities.len() > 1 {
                violations.push(format!(
                    "resource_type {resource_type:?} uses more than one ability: {abilities:?}"
                ));
            }
        }
        violations.sort();
        violations
    }
}

fn check(edit: impl FnOnce(&mut Snapshot)) -> Option<String> {
    let Some(path) = std::env::var_os("RAILS_CONTRACT_SNAPSHOT").map(PathBuf::from) else {
        eprintln!("RAILS_CONTRACT_SNAPSHOT is not set; run `mise run rails-contract:export` first");
        return None;
    };
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut snapshot: Snapshot =
        serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    edit(&mut snapshot);
    Some(snapshot.violations().join("\n"))
}

#[test]
fn redaction_config_matches_rails() {
    let violations = check(|_| {}).unwrap_or_default();
    assert!(
        violations.is_empty(),
        "Rails contract violations:\n{violations}"
    );
}

#[test]
fn checker_reports_each_rule() {
    let Some(violations) = check(|snapshot| {
        snapshot.resource_types.remove("note");
        snapshot
            .policies
            .get_mut("ProjectPolicy")
            .unwrap()
            .enables
            .remove("read_build");
        snapshot.roles.insert(
            "read_vulnerability".into(),
            BTreeMap::from([("project".into(), "developer".into())]),
        );
    }) else {
        return;
    };
    for expected in [
        "Note: resource_type \"note\" is not registered in Rails",
        "Job: Rails Ci::Build cannot be granted read_build",
        "Vulnerability: required_role SecurityManager is below the lowest Rails role for read_vulnerability (30)",
    ] {
        assert!(
            violations.contains(expected),
            "missing {expected:?} in:\n{violations}"
        );
    }
}
