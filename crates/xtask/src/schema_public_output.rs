//! Recorded digests of the schema returned by each public introspection encoding.
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use ontology::introspection::{
    IntrospectionScope, SchemaResponse, build_node_schema_response, build_schema_response,
};
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::tools::{ExecutorError, OutputFormat, ToolService};
use prost::Message;
use sha2::{Digest, Sha256};

const SNAPSHOT: &str = "config/schema-public-output.json";

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn outputs(ontology: &Ontology) -> Result<BTreeMap<String, String>> {
    outputs_with_encoder(ontology, ToolService::encode_schema_toon)
}

fn expansion_key(expand: &[String]) -> String {
    if expand.is_empty() {
        "summary".to_string()
    } else {
        expand.join("+")
    }
}

fn outputs_with_encoder(
    ontology: &Ontology,
    encode: impl Fn(&SchemaResponse) -> Result<String, ExecutorError>,
) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let mut expansions = vec![Vec::new(), vec!["*".to_string()]];
    expansions.extend(ontology.nodes().map(|node| vec![node.name.clone()]));
    let pair: Vec<String> = ontology
        .nodes()
        .take(2)
        .map(|node| node.name.clone())
        .collect();
    if pair.len() == 2 {
        expansions.push(pair.clone());
    }
    for expand in expansions {
        let key = expansion_key(&expand);
        let response = build_schema_response(ontology, IntrospectionScope::All, &expand);
        result.insert(
            format!("raw/{key}"),
            digest(&serde_json::to_vec(&response)?),
        );
        result.insert(format!("toon/{key}"), digest(encode(&response)?.as_bytes()));
        let llm = ToolService::build_schema_toon(ontology, &expand)?;
        result.insert(format!("rpc/toon/{key}"), digest(llm.as_bytes()));
        for format in [OutputFormat::Raw, OutputFormat::Llm] {
            let value = ToolService::render_graph_schema(ontology, &expand, format)?;
            let name = if format == OutputFormat::Raw {
                "raw"
            } else {
                "toon"
            };
            let bytes = match value {
                serde_json::Value::String(text) => text.into_bytes(),
                value => serde_json::to_vec(&value)?,
            };
            result.insert(format!("command/{name}/{key}"), digest(&bytes));
        }
        let structured = OrbitServiceImpl::build_structured_schema(ontology, &expand);
        result.insert(
            format!("structured/{key}"),
            digest(&structured.encode_to_vec()),
        );
    }
    for scope in [IntrospectionScope::All, IntrospectionScope::Local] {
        let scope_name = if scope == IntrospectionScope::Local {
            "local"
        } else {
            "remote"
        };
        let mut scoped_expansions = vec![Vec::new(), vec!["*".to_string()]];
        scoped_expansions.extend(ontology.nodes().map(|node| vec![node.name.clone()]));
        if pair.len() == 2 {
            scoped_expansions.push(pair.clone());
        }
        for expand in scoped_expansions {
            let key = expansion_key(&expand);
            let response = build_schema_response(ontology, scope, &expand);
            result.insert(
                format!("schema/{scope_name}/raw/{key}"),
                digest(&serde_json::to_vec(&response)?),
            );
            result.insert(
                format!("schema/{scope_name}/toon/{key}"),
                digest(encode(&response)?.as_bytes()),
            );
            if key == "summary" || key == "*" {
                result.insert(
                    format!("gql/{scope_name}/raw/{key}"),
                    digest(&serde_json::to_vec(&response)?),
                );
                result.insert(
                    format!("gql/{scope_name}/toon/{key}"),
                    digest(encode(&response)?.as_bytes()),
                );
            }
        }
        for node in ontology.nodes() {
            if scope == IntrospectionScope::Local
                && !ontology.local_entity_names().contains(&node.name.as_str())
            {
                continue;
            }
            let response = build_node_schema_response(ontology, scope, &node.name);
            result.insert(
                format!("gql/{scope_name}/raw/{}", node.name),
                digest(&serde_json::to_vec(&response)?),
            );
            result.insert(
                format!("gql/{scope_name}/toon/{}", node.name),
                digest(encode(&response)?.as_bytes()),
            );
        }
    }
    Ok(result)
}

fn git_show(base: &str, path: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(["show", &format!("{base}:{path}")])
        .output()?;
    if output.status.success() {
        Ok(Some(String::from_utf8(output.stdout)?))
    } else {
        Ok(None)
    }
}

fn new_elements_have_current_pin(
    current: &Ontology,
    target: &Ontology,
    pin: &semver::Version,
) -> Result<()> {
    for node in current.nodes() {
        let previous = target.get_node(&node.name);
        match previous {
            Some(old) if node.introduced_in != old.introduced_in => {
                bail!(
                    "node {} introduced_in changed from {} to {}",
                    node.name,
                    old.introduced_in,
                    node.introduced_in
                );
            }
            None if &node.introduced_in != pin => {
                bail!(
                    "new node {} must set introduced_in explicitly to {pin} (defaults to {})",
                    node.name,
                    ontology::DEFAULT_INTRODUCED_IN
                );
            }
            _ => {}
        }
        for field in &node.fields {
            let old =
                previous.and_then(|node| node.fields.iter().find(|old| old.name == field.name));
            match old {
                Some(old) if field.introduced_in != old.introduced_in => {
                    bail!(
                        "property {}.{} introduced_in changed from {} to {}",
                        node.name,
                        field.name,
                        old.introduced_in,
                        field.introduced_in
                    );
                }
                None if &field.introduced_in != pin => {
                    bail!(
                        "new property {}.{} must set introduced_in explicitly to {pin} (defaults to {})",
                        node.name,
                        field.name,
                        ontology::DEFAULT_INTRODUCED_IN
                    );
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn target_ontology(base: &str) -> Result<Ontology> {
    let versions = git_show(base, "config/versions.yaml")?
        .ok_or_else(|| anyhow!("cannot read {base}:config/versions.yaml"))?;
    let version = orbit_versions::parse(&versions)?.schema;
    let path = format!("config/ontology-archives/v{version}.tar.gz");
    let bytes = Command::new("git")
        .args(["show", &format!("{base}:{path}")])
        .output()?;
    if !bytes.status.success() {
        bail!("cannot read {base}:{path}");
    }
    Ok(OntologyArchive::from_bytes(version, &bytes.stdout)?.load_ontology()?)
}

fn pin_order_against(base: &str) -> Result<Option<Ordering>> {
    let Some(versions) = git_show(base, "config/versions.yaml")? else {
        bail!("cannot read target versions.yaml at {base}");
    };
    if !versions
        .lines()
        .any(|line| line.starts_with("graph_schema_api:"))
    {
        return Ok(None); // First introduction of the public API pin.
    }
    Ok(Some(
        orbit_versions::VERSIONS
            .graph_schema_api
            .cmp(&orbit_versions::parse(&versions)?.graph_schema_api),
    ))
}

fn require_pin_bump(
    current: &str,
    target: Option<&str>,
    pin_order: Option<Ordering>,
    base: &str,
) -> Result<()> {
    if pin_order == Some(Ordering::Less) {
        bail!("graph_schema_api cannot be lower than the target pin at {base}");
    }
    if target != Some(current) && pin_order == Some(Ordering::Equal) {
        bail!(
            "rendered public schema differs from {base}; graph_schema_api must be greater than the target pin"
        );
    }
    Ok(())
}

fn current_ontology() -> Result<Ontology> {
    OntologyArchive::bundled(orbit_versions::VERSIONS.schema)?
        .ok_or_else(|| anyhow!("current ontology archive is missing; run `mise schema:bump`"))?
        .load_ontology()
        .map_err(Into::into)
}

pub fn run(check: bool, base: &str, skip_pin_check: bool) -> Result<()> {
    let ontology = current_ontology()?;
    let current = outputs(&ontology)?;
    let rendered = format!("{}\n", serde_json::to_string_pretty(&current)?);
    if !check {
        fs::write(SNAPSHOT, rendered).context("writing public schema output snapshot")?;
        println!("wrote {SNAPSHOT}");
        return Ok(());
    }
    let committed = fs::read_to_string(SNAPSHOT).context(
        "public schema output snapshot missing; regenerate with cargo xtask schema-public-output",
    )?;
    if committed != rendered {
        bail!("public schema output snapshot is stale; run cargo xtask schema-public-output");
    }
    new_elements_have_current_pin(
        &ontology,
        &target_ontology(base)?,
        ontology.graph_schema_api(),
    )?;
    if !skip_pin_check {
        require_pin_bump(
            &rendered,
            git_show(base, SNAPSHOT)?.as_deref(),
            pin_order_against(base)?,
            base,
        )?;
    }
    println!("public schema output and graph_schema_api match {base}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_output_is_rendered_from_the_served_archive() {
        let archive = OntologyArchive::bundled(orbit_versions::VERSIONS.schema)
            .unwrap()
            .unwrap();
        let served = current_ontology().unwrap();
        archive.validate_current_api_pin().unwrap();
        assert_eq!(served.graph_schema_api(), archive.graph_schema_api());
        assert_eq!(
            outputs(&served).unwrap(),
            outputs(&archive.load_ontology().unwrap()).unwrap()
        );
        let legacy = OntologyArchive::bundled(99).unwrap().unwrap();
        assert_eq!(
            outputs(&legacy.load_ontology().unwrap()).unwrap(),
            outputs(&Ontology::load_embedded().unwrap()).unwrap()
        );
    }

    #[test]
    fn newly_introduced_nodes_and_properties_use_the_current_pin() {
        let target = Ontology::new().with_nodes(["Existing"]);
        let current = target.clone().with_nodes(["New"]);
        let bumped_pin = semver::Version::new(1, 1, 0);
        let node_error = new_elements_have_current_pin(&current, &target, &bumped_pin)
            .unwrap_err()
            .to_string();
        assert!(
            node_error.contains("new node New must set introduced_in explicitly to 1.1.0"),
            "{node_error}"
        );
        let only_property = target
            .clone()
            .with_fields("Existing", [("new_property", ontology::DataType::String)]);
        let property_error = new_elements_have_current_pin(&only_property, &target, &bumped_pin)
            .unwrap_err()
            .to_string();
        assert!(
            property_error.contains(
                "new property Existing.new_property must set introduced_in explicitly to 1.1.0"
            ),
            "{property_error}"
        );

        let introduced = only_property
            .modify_field("Existing", "new_property", |field| {
                field.introduced_in = bumped_pin.clone();
            })
            .unwrap();
        assert!(new_elements_have_current_pin(&introduced, &target, &bumped_pin).is_ok());

        let legacy = OntologyArchive::bundled(99)
            .unwrap()
            .unwrap()
            .load_ontology()
            .unwrap();
        assert!(new_elements_have_current_pin(&legacy, &Ontology::new(), &bumped_pin).is_err());
        let matching_node = Ontology::new().with_nodes(["User"]);
        assert!(new_elements_have_current_pin(&legacy, &matching_node, &bumped_pin).is_err());
    }

    #[test]
    fn existing_versions_are_immutable_against_the_target() {
        let target = Ontology::load_embedded().unwrap();
        assert!(new_elements_have_current_pin(&target, &target, target.graph_schema_api()).is_ok());
        let overlay = tempfile::tempdir().unwrap();
        let file = overlay.path().join("nodes/core/user.yaml");
        fs::create_dir_all(file.parent().unwrap()).unwrap();

        fs::write(
            &file,
            "introduced_in: '1.0.0'\nproperties:\n  id:\n    introduced_in: '1.0.0'\n",
        )
        .unwrap();
        let explicit = Ontology::load_embedded_with_overlay(overlay.path()).unwrap();
        assert!(
            new_elements_have_current_pin(&explicit, &target, target.graph_schema_api()).is_ok()
        );
        assert!(
            new_elements_have_current_pin(&target, &explicit, target.graph_schema_api()).is_ok()
        );
        assert_eq!(outputs(&explicit).unwrap(), outputs(&target).unwrap());

        fs::write(&file, "introduced_in: '0.9.0'\n").unwrap();
        let node_change = Ontology::load_embedded_with_overlay(overlay.path()).unwrap();
        let error = new_elements_have_current_pin(&node_change, &target, target.graph_schema_api())
            .unwrap_err()
            .to_string();
        assert!(error.contains("node User introduced_in changed"), "{error}");

        fs::write(&file, "properties:\n  id:\n    introduced_in: '0.9.0'\n").unwrap();
        let field_change = Ontology::load_embedded_with_overlay(overlay.path()).unwrap();
        let error =
            new_elements_have_current_pin(&field_change, &target, target.graph_schema_api())
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("property User.id introduced_in changed"),
            "{error}"
        );
    }

    #[test]
    fn a_lower_pin_fails_even_if_output_is_unchanged() {
        assert!(require_pin_bump("same", Some("same"), Some(Ordering::Less), "target").is_err());
    }

    #[test]
    fn a_multi_node_expansion_has_its_own_output_hash() {
        let ontology = Ontology::load_embedded().unwrap();
        let pair: Vec<_> = ontology
            .nodes()
            .take(2)
            .map(|node| node.name.clone())
            .collect();
        let hashes = outputs(&ontology).unwrap();
        assert!(hashes.contains_key(&format!("command/raw/{}", pair.join("+"))));
    }

    #[test]
    fn edge_only_changes_invalidate_the_rendered_schema() {
        let ontology = Ontology::load_embedded().unwrap();
        let before = outputs(&ontology).unwrap();
        let overlay = tempfile::tempdir().unwrap();
        let file = overlay.path().join("edges/calls.yaml");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, "description: Additional call relationship details\n").unwrap();
        let after =
            outputs(&Ontology::load_embedded_with_overlay(overlay.path()).unwrap()).unwrap();
        assert_ne!(before["structured/summary"], after["structured/summary"]);
        let before = serde_json::to_string(&before).unwrap();
        let after = serde_json::to_string(&after).unwrap();
        assert!(require_pin_bump(&after, Some(&before), Some(Ordering::Equal), "target").is_err());
    }

    #[test]
    fn encoder_changes_invalidate_the_rendered_schema() {
        let ontology = Ontology::load_embedded().unwrap();
        let before = outputs(&ontology).unwrap();
        let after = outputs_with_encoder(&ontology, |response| {
            ToolService::encode_schema_toon(response).map(|encoded| format!("{encoded}\n"))
        })
        .unwrap();
        assert_eq!(before["raw/summary"], after["raw/summary"]);
        assert_ne!(before["toon/summary"], after["toon/summary"]);
        let before = serde_json::to_string(&before).unwrap();
        let after = serde_json::to_string(&after).unwrap();
        assert!(require_pin_bump(&after, Some(&before), Some(Ordering::Equal), "target").is_err());
    }

    #[test]
    fn etl_only_node_changes_leave_the_rendered_schema_intact() {
        let ontology = Ontology::load_embedded().unwrap();
        let before = outputs(&ontology).unwrap();
        let overlay = tempfile::tempdir().unwrap();
        let file = overlay.path().join("nodes/core/user.yaml");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, "sort_key: [id, username]\n").unwrap();
        let after =
            outputs(&Ontology::load_embedded_with_overlay(overlay.path()).unwrap()).unwrap();
        assert_eq!(before, after);
        let before = serde_json::to_string(&before).unwrap();
        assert!(require_pin_bump(&before, Some(&before), Some(Ordering::Equal), "target").is_ok());
    }
}
