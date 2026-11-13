use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use orbit_authorization::Module;
use orbit_server::authorization::gitlab::{GitLabSession, Role};
use query_data_model::catalog::Check;
use query_data_model::{Catalog, ClickHouse};
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
            _ => serde_json::from_value::<Role>(role.as_str().into())
                .unwrap_or_else(|e| panic!("unknown Rails role {role}: {e}"))
                as u32,
        })
    }

    fn violations(&self) -> Vec<String> {
        let catalog = Catalog::<ClickHouse>::embedded("server").expect("embedded model must load");
        let access = catalog.model().access();
        let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
        let options = serde_json::Value::Object(config.providers[&config.identify].options.clone());
        let session = GitLabSession::build(serde_json::from_value(options).unwrap()).unwrap();
        let mut violations = Vec::new();
        let mut abilities_by_type: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for entity in catalog.graph().entities() {
            let declared = access.entity(entity.id);
            let check = declared.checks.iter().find_map(|check| match check {
                Check::Rows {
                    permission,
                    resource,
                    ..
                } => Some((resource, permission)),
                Check::Delegated { .. } => None,
            });
            let floor = session.role(&access.permission(declared.permission).name);
            let (Some((resource, permission)), Some(floor)) = (check, floor) else {
                panic!(
                    "{} has no Rails check, so Rails never checks it",
                    entity.name
                )
            };
            let floor = floor as u32;
            let redaction = Redaction {
                resource,
                ability: &access.permission(*permission).name,
            };
            let (entity, ability) = (&entity.name, redaction.ability);
            abilities_by_type
                .entry(redaction.resource)
                .or_default()
                .insert(ability);
            if !ability.starts_with("read_") {
                violations.push(format!(
                    "{entity}: ability {ability:?} is not an explicit read_ ability"
                ));
            }
            let Some(resource_type) = self.resource_types.get(redaction.resource) else {
                violations.push(format!(
                    "{entity}: resource_type {:?} is not registered in Rails",
                    redaction.resource
                ));
                continue;
            };
            for class in &resource_type.classes {
                if !self.policy_enables(&self.classes[class].policy, ability, &mut BTreeSet::new())
                {
                    violations.push(format!(
                        "{entity}: Rails {class} cannot be granted {ability}"
                    ));
                }
            }
            if let Some(rails_floor) = self.lowest_role(ability)
                && floor < rails_floor
            {
                violations.push(format!(
                    "{entity}: role floor {floor} is below the lowest Rails role for {ability} ({rails_floor})"
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

/// The Rails resource and ability one entity's row check names.
struct Redaction<'m> {
    resource: &'m String,
    ability: &'m str,
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
        "Vulnerability: role floor 25 is below the lowest Rails role for read_vulnerability (30)",
    ] {
        assert!(
            violations.contains(expected),
            "missing {expected:?} in:\n{violations}"
        );
    }
}
