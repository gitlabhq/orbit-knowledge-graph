//! Recorded digests of the schema returned by each public introspection encoding.
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

fn outputs_with_encoder(
    ontology: &Ontology,
    encode: impl Fn(&SchemaResponse) -> Result<String, ExecutorError>,
) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let mut expansions = vec![Vec::new(), vec!["*".to_string()]];
    expansions.extend(ontology.nodes().map(|node| vec![node.name.clone()]));
    for expand in expansions {
        let key = expand.first().map_or("summary", String::as_str);
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
        for expand in scoped_expansions {
            let key = expand.first().map_or("summary", String::as_str);
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

fn new_elements_have_current_pin(current: &Ontology, target: &Ontology) -> Result<()> {
    let pin = current.graph_schema_api();
    for node in current.nodes() {
        let previous = target.get_node(&node.name);
        if previous.is_none() && &node.introduced_in != pin {
            bail!("new node {} must have introduced_in {pin}", node.name);
        }
        for field in &node.fields {
            if previous.is_some_and(|old| old.fields.iter().any(|f| f.name == field.name)) {
                continue;
            }
            if &field.introduced_in != pin {
                bail!(
                    "new property {}.{} must have introduced_in {pin}",
                    node.name,
                    field.name
                );
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

fn pin_is_newer(base: &str) -> Result<bool> {
    let Some(versions) = git_show(base, "config/versions.yaml")? else {
        bail!("cannot read target versions.yaml at {base}");
    };
    if !versions
        .lines()
        .any(|line| line.starts_with("graph_schema_api:"))
    {
        return Ok(true); // First introduction of the public API pin.
    }
    Ok(orbit_versions::VERSIONS.graph_schema_api
        > orbit_versions::parse(&versions)?.graph_schema_api)
}

fn require_pin_bump(
    current: &str,
    target: Option<&str>,
    pin_is_newer: bool,
    base: &str,
) -> Result<()> {
    if target != Some(current) && !pin_is_newer {
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

pub fn run(check: bool, base: &str) -> Result<()> {
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
    new_elements_have_current_pin(&ontology, &target_ontology(base)?)?;
    require_pin_bump(
        &rendered,
        git_show(base, SNAPSHOT)?.as_deref(),
        pin_is_newer(base)?,
        base,
    )?;
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
        assert_ne!(
            outputs(&legacy.load_ontology().unwrap()).unwrap()["raw/summary"],
            outputs(&Ontology::load_embedded().unwrap()).unwrap()["raw/summary"]
        );
    }

    #[test]
    fn newly_introduced_nodes_and_properties_use_the_current_pin() {
        let target = Ontology::new().with_nodes(["Existing"]);
        let current = target
            .clone()
            .with_nodes(["New"])
            .with_fields("Existing", [("new_property", ontology::DataType::String)]);
        assert!(new_elements_have_current_pin(&current, &target).is_ok());

        let legacy = OntologyArchive::bundled(99)
            .unwrap()
            .unwrap()
            .load_ontology()
            .unwrap();
        assert!(new_elements_have_current_pin(&legacy, &Ontology::new()).is_err());
        let matching_node = Ontology::new().with_nodes(["User"]);
        assert!(new_elements_have_current_pin(&legacy, &matching_node).is_err());
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
        assert!(require_pin_bump(&after, Some(&before), false, "target").is_err());
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
        assert!(require_pin_bump(&after, Some(&before), false, "target").is_err());
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
        assert!(require_pin_bump(&before, Some(&before), false, "target").is_ok());
    }
}
