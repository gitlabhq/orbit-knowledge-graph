//! Recorded digests of the schema returned by each public introspection encoding.
use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;
use ontology::introspection::{
    IntrospectionScope, SchemaResponse, build_node_schema_response, build_schema_response,
};
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::tools::{ExecutorError, ToolService};
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

pub fn run(check: bool, base: &str) -> Result<()> {
    let current = outputs(&Ontology::load_embedded().map_err(|e| anyhow!(e))?)?;
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
