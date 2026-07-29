//! `cargo xtask channel-widening check`: fail on channel_allowlist widening
//! (ADR 013 §9).
//!
//! Compares each entity's resolved channel set at a base ref against the
//! working tree. If any entity's resolved set gained members — most notably
//! by adding `external_agent` or moving from `[internal_only]` to
//! `[all_interfaces]` — the check fails unless the widening-approval label
//! is present in `$MR_LABELS`.
//!
//! CODEOWNERS operates on file paths; this operates on *semantic diffs* of
//! `channel_allowlist` values, which is what the ADR actually cares about.
//! A rename of the entity file, or a reformatting of the block, doesn't
//! trigger it; a substantive gain in channels does.
//!
//! Narrowing (removing channels, moving from `[all_interfaces]` to
//! `[internal_only]`) never trips this check — that's the safe direction
//! and shouldn't require friction.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use anyhow::{Context, Result, bail};
use ontology::{Channel, ChannelAllowlist};

#[cfg(test)]
use ontology::ChannelGroup;

const APPROVAL_LABEL: &str = "channel-widening-approved";
const NODES_DIR: &str = "config/ontology/nodes";

pub fn run(base_ref: Option<String>) -> Result<()> {
    let base_ref = base_ref.unwrap_or_else(|| "origin/main".to_string());
    let base = allowlists_at(Some(&base_ref))?;
    let head = allowlists_at(None)?;

    let mut widened: Vec<(String, BTreeSet<Channel>, BTreeSet<Channel>)> = Vec::new();
    for (entity, head_set) in &head {
        let empty = BTreeSet::new();
        let base_set = base.get(entity).unwrap_or(&empty);
        if head_set.difference(base_set).next().is_some() {
            widened.push((entity.clone(), base_set.clone(), head_set.clone()));
        }
    }

    if widened.is_empty() {
        println!("No entities gained channels; check passes.");
        return Ok(());
    }

    println!("The following entities' resolved channel sets gained members:");
    for (entity, base_set, head_set) in &widened {
        let gained: Vec<_> = head_set.difference(base_set).map(fmt_channel).collect();
        println!(
            "  {entity}: +{{{}}} (base: {{{}}} → head: {{{}}})",
            gained.join(", "),
            fmt_set(base_set),
            fmt_set(head_set),
        );
    }

    let labels = std::env::var("MR_LABELS").unwrap_or_default();
    let labels: BTreeSet<&str> = labels
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if labels.contains(APPROVAL_LABEL) {
        println!("\nLabel `{APPROVAL_LABEL}` present; widening is approved.");
        return Ok(());
    }

    bail!(
        "Widening a `channel_allowlist` (especially adding `external_agent` or resolving to \
         `all_interfaces`) is a deliberate product/monetization decision per ADR 013 §9. Apply the \
         `{APPROVAL_LABEL}` label to the MR after Product/Security sign-off, or narrow the diff so \
         the resolved sets don't grow."
    );
}

fn fmt_channel(c: &Channel) -> String {
    <&str>::from(*c).to_string()
}

fn fmt_set(set: &BTreeSet<Channel>) -> String {
    if set.is_empty() {
        return "empty".to_string();
    }
    set.iter().map(fmt_channel).collect::<Vec<_>>().join(", ")
}

/// Load every node YAML at `ref_name` (or the working tree if `None`),
/// resolve each `channel_allowlist`, and return `node_type → resolved set`.
///
/// Nodes present in the working tree but absent from the base ref appear
/// with an empty base set — which is correct: a brand-new entity that ships
/// with `[all_interfaces]` should trip the gate rather than sneak in without
/// review. Nodes present at the base ref but absent from the working tree
/// simply don't show up in `head`; deletions never widen anything.
fn allowlists_at(ref_name: Option<&str>) -> Result<BTreeMap<String, BTreeSet<Channel>>> {
    let mut out = BTreeMap::new();

    let files = match ref_name {
        Some(r) => list_ref_files(r)?,
        None => list_working_tree_files()?,
    };

    for path in files {
        let text = match ref_name {
            Some(r) => match git_show(r, &path) {
                Ok(t) => t,
                // A file added in this MR won't exist at the base ref; skip
                // silently so the working-tree walk handles it.
                Err(_) => continue,
            },
            None => std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?,
        };

        let Some((name, set)) = parse_node_channel_set(&text)? else {
            continue;
        };
        out.insert(name, set);
    }

    Ok(out)
}

fn list_ref_files(ref_name: &str) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["ls-tree", "-r", "--name-only", ref_name, NODES_DIR])
        .output()
        .with_context(|| format!("running git ls-tree {ref_name}"))?;
    if !output.status.success() {
        bail!(
            "git ls-tree {ref_name} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|s| s.ends_with(".yaml"))
        .map(|s| s.to_string())
        .collect())
}

fn list_working_tree_files() -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(NODES_DIR)
        .into_iter()
        .filter_map(Result::ok)
    {
        if entry.file_type().is_file() && entry.path().extension().is_some_and(|e| e == "yaml") {
            out.push(entry.path().display().to_string());
        }
    }
    Ok(out)
}

fn git_show(ref_name: &str, path: &str) -> Result<String> {
    let output = Command::new("git")
        .args(["show", &format!("{ref_name}:{path}")])
        .output()
        .with_context(|| format!("git show {ref_name}:{path}"))?;
    if !output.status.success() {
        bail!("git show failed for {ref_name}:{path}");
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[derive(serde::Deserialize)]
struct NodeYaml {
    node_type: Option<String>,
    redaction: Option<Redaction>,
}

#[derive(serde::Deserialize)]
struct Redaction {
    // `Option` deliberately: `None` means "the YAML at that ref does not have
    // a `channel_allowlist` field at all," which is pre-ADR-013. That is a
    // distinct case from `Some(ChannelAllowlist([]))` — explicit fail-closed —
    // and it drives the base-side treatment below.
    channel_allowlist: Option<ChannelAllowlist>,
}

fn parse_node_channel_set(text: &str) -> Result<Option<(String, BTreeSet<Channel>)>> {
    let parsed: NodeYaml = match serde_yaml::from_str(text) {
        Ok(v) => v,
        // Non-node YAML in the tree (edge files, derived entities) — skip.
        Err(_) => return Ok(None),
    };
    let Some(name) = parsed.node_type else {
        return Ok(None);
    };
    // Pre-ADR-013 nodes have no `channel_allowlist` at all. The effective
    // visibility before this ADR was "every channel" (there was no gate), so
    // when we compare the initial migration against a pre-ADR base we treat
    // the missing field as `[all_interfaces]`. That way the ontology sweep
    // that populates the explicit default doesn't fire the widening gate on
    // every entity — the sweep is a no-op on effective visibility, and the
    // gate is meant to catch *changes* in effective visibility, not the field
    // becoming explicit for the first time.
    //
    // After ADR 013 ships, every node YAML must carry the field: the JSON
    // schema (`config/schemas/ontology.schema.json`) marks it required and
    // gkg-server's build.rs panics on empty allowlists, so a head that
    // reaches this check with a missing field can't happen — this fallback
    // only ever runs against the pre-ADR-013 base ref during the initial
    // migration.
    let resolved = parsed
        .redaction
        .and_then(|r| r.channel_allowlist)
        .map(|a| a.resolve())
        .unwrap_or_else(|| ontology::ChannelGroup::AllInterfaces.channels());
    Ok(Some((name, resolved)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_node_yaml() {
        let yaml = r#"
node_type: Widget
redaction:
  resource_type: widget
  channel_allowlist: [all_interfaces]
"#;
        let (name, set) = parse_node_channel_set(yaml).unwrap().unwrap();
        assert_eq!(name, "Widget");
        assert_eq!(set, ChannelGroup::AllInterfaces.channels());
    }

    // A YAML with `redaction:` but no `channel_allowlist:` key at all is a
    // pre-ADR-013 shape. The widening gate needs to compare against the
    // *effective* pre-ADR visibility (unrestricted), otherwise the initial
    // migration that populates `[all_interfaces]` looks like widening on
    // every entity. See the docstring on `parse_node_channel_set` for why
    // this fallback is safe against a head accidentally dropping the field.
    #[test]
    fn parse_pre_adr013_node_yields_all_interfaces() {
        let yaml = r#"
node_type: Widget
redaction:
  resource_type: widget
"#;
        let (_name, set) = parse_node_channel_set(yaml).unwrap().unwrap();
        assert_eq!(set, ChannelGroup::AllInterfaces.channels());
    }

    // An *explicitly* empty `channel_allowlist: []` is the fail-closed
    // choice — nobody sees the entity — and stays distinct from the
    // "field missing" case above.
    #[test]
    fn parse_explicit_empty_allowlist_yields_empty_set() {
        let yaml = r#"
node_type: Widget
redaction:
  resource_type: widget
  channel_allowlist: []
"#;
        let (_name, set) = parse_node_channel_set(yaml).unwrap().unwrap();
        assert!(set.is_empty());
    }
}
