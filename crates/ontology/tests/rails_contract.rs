use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use ontology::{Ontology, RedactionConfig};
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
    fn load(path: &Path) -> Self {
        let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
    }

    fn class_enables(&self, class: &str, ability: &str) -> bool {
        let mut seen = BTreeSet::new();
        self.policy_enables(&self.classes[class].policy, ability, &mut seen)
    }

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
        floors
            .get("project")
            .or_else(|| floors.get("group"))
            .map(|role| access_level(role))
    }
}

fn access_level(role: &str) -> u32 {
    match role {
        "guest" => 10,
        "planner" => 15,
        "reporter" => 20,
        "security_manager" => 25,
        "developer" => 30,
        "maintainer" => 40,
        "owner" => 50,
        other => panic!("unknown Rails role {other}"),
    }
}

fn snapshot_paths() -> Vec<PathBuf> {
    let dir = Path::new(env!("CONFIG_DIR")).join("rails-contract");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no snapshots in {}", dir.display());
    paths
}

fn redaction_configs(ontology: &Ontology) -> Vec<(&str, &RedactionConfig)> {
    ontology
        .nodes()
        .map(|node| {
            let config = node.redaction.as_ref().unwrap_or_else(|| {
                panic!(
                    "{} has no redaction block, so Rails never checks it",
                    node.name
                )
            });
            (node.name.as_str(), config)
        })
        .collect()
}

fn contract_violations(ontology: &Ontology, snapshot: &Snapshot) -> Vec<String> {
    let mut violations = Vec::new();
    let mut abilities_by_type: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    for (node, config) in redaction_configs(ontology) {
        let ability = config.ability.as_str();
        abilities_by_type
            .entry(&config.resource_type)
            .or_default()
            .insert(ability);
        if !ability.starts_with("read_") {
            violations.push(format!(
                "{node}: ability {ability:?} is not an explicit read_ ability"
            ));
        }
        let Some(resource_type) = snapshot.resource_types.get(&config.resource_type) else {
            violations.push(format!(
                "{node}: resource_type {:?} is not registered in Rails",
                config.resource_type
            ));
            continue;
        };
        for class in &resource_type.classes {
            if !snapshot.class_enables(class, ability) {
                violations.push(format!("{node}: Rails {class} cannot be granted {ability}"));
            }
        }
        if let Some(rails_floor) = snapshot.lowest_role(ability)
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

#[test]
fn redaction_config_matches_every_rails_snapshot() {
    let ontology = Ontology::load_embedded().expect("embedded ontology must load");
    let failures: Vec<String> = snapshot_paths()
        .iter()
        .flat_map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            contract_violations(&ontology, &Snapshot::load(path))
                .into_iter()
                .map(move |violation| format!("{name}: {violation}"))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "Rails contract violations:\n{}",
        failures.join("\n")
    );
}

#[test]
fn checker_reports_each_rule() {
    let ontology = Ontology::load_embedded().expect("embedded ontology must load");
    let path = snapshot_paths().into_iter().next().unwrap();
    let mut snapshot = Snapshot::load(&path);
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

    let violations = contract_violations(&ontology, &snapshot).join("\n");
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
