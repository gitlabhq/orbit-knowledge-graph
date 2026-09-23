use std::collections::{BTreeMap, BTreeSet};

use comrak::nodes::NodeValue;
use comrak::{Arena, Options, parse_document};

const MANIFEST: &str = "SKILL.md";
const SLOT_PREFIX: &str = "<!-- orbit:include local:";
const SECTION_PREFIX: &str = "<!-- orbit:section ";
const SECTION_END: &str = "<!-- /orbit:section -->";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkerTree {
    Remote,
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MarkerKind {
    Slot(String),
    SectionStart(String),
    SectionEnd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Marker {
    line: usize,
    kind: MarkerKind,
}

#[cfg(feature = "skill-validation")]
pub(crate) fn parse_markers(content: &str, tree: MarkerTree) -> Result<BTreeSet<String>, String> {
    let markers = marker_events(content, tree)?;
    Ok(markers
        .into_iter()
        .filter_map(|marker| match marker.kind {
            MarkerKind::Slot(id) | MarkerKind::SectionStart(id) => Some(id),
            MarkerKind::SectionEnd => None,
        })
        .collect())
}

/// Missing marker pairs must not fail composition across release versions.
pub fn compose_skill_manifests(remote: &str, local: &str) -> Result<String, String> {
    let remote = remote.replace("\r\n", "\n");
    let local = local.replace("\r\n", "\n");
    let remote_markers = marker_events(&remote, MarkerTree::Remote)?;
    let local_markers = marker_events(&local, MarkerTree::Local)?;
    let local_lines: Vec<_> = local.split_inclusive('\n').collect();

    let mut sections = BTreeMap::new();
    let mut section_order = Vec::new();
    let mut open: Option<(String, usize)> = None;
    for marker in local_markers {
        match marker.kind {
            MarkerKind::SectionStart(id) => open = Some((id, marker.line)),
            MarkerKind::SectionEnd => {
                let (id, start) = open
                    .take()
                    .ok_or_else(|| "local section end has no start".to_string())?;
                let body = local_lines[start..marker.line.saturating_sub(1)].concat();
                section_order.push(id.clone());
                sections.insert(id, body);
            }
            MarkerKind::Slot(_) => unreachable!("local parser rejects slots"),
        }
    }

    let slots: BTreeMap<_, _> = remote_markers
        .into_iter()
        .filter_map(|marker| match marker.kind {
            MarkerKind::Slot(id) => Some((marker.line, id)),
            _ => None,
        })
        .collect();
    let mut matched = BTreeSet::new();
    let mut output = String::new();
    for (index, line) in remote.split_inclusive('\n').enumerate() {
        let line_number = index + 1;
        if let Some(id) = slots.get(&line_number) {
            if let Some(body) = sections.get(id) {
                output.push_str(body);
                matched.insert(id.clone());
            }
        } else {
            output.push_str(line);
        }
    }

    let unmatched: Vec<_> = section_order
        .iter()
        .filter(|id| !matched.contains(*id))
        .filter_map(|id| sections.get(id))
        .collect();
    if !unmatched.is_empty() {
        if !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str("\n## Local CLI\n\n");
        for (index, body) in unmatched.iter().enumerate() {
            if index > 0 && !output.ends_with("\n\n") {
                output.push('\n');
            }
            output.push_str(body);
        }
    }
    Ok(output)
}

fn marker_events(content: &str, tree: MarkerTree) -> Result<Vec<Marker>, String> {
    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());
    let mut ids = BTreeSet::new();
    let mut section: Option<String> = None;
    let mut markers = Vec::new();

    for node in root.descendants() {
        let data = node.data();
        let marker = match &data.value {
            NodeValue::HtmlBlock(block) => block.literal.trim(),
            NodeValue::HtmlInline(inline) => inline.trim(),
            _ => continue,
        };
        let line = data.sourcepos.start.line;

        let kind = if marker == SECTION_END {
            if tree != MarkerTree::Local {
                return Err(format!("remote {MANIFEST}:{line}: unexpected section end"));
            }
            if section.take().is_none() {
                return Err(format!("local {MANIFEST}:{line}: section end has no start"));
            }
            MarkerKind::SectionEnd
        } else if let Some(id) = exact_marker_id(marker, SLOT_PREFIX) {
            if tree != MarkerTree::Remote {
                return Err(format!(
                    "local {MANIFEST}:{line}: include slot is not allowed"
                ));
            }
            insert_marker_id(&mut ids, id, "slot", line)?;
            MarkerKind::Slot(id.to_string())
        } else if let Some(id) = exact_marker_id(marker, SECTION_PREFIX) {
            if tree != MarkerTree::Local {
                return Err(format!(
                    "remote {MANIFEST}:{line}: section export is not allowed"
                ));
            }
            if let Some(open) = &section {
                return Err(format!(
                    "local {MANIFEST}:{line}: section {id:?} is nested inside {open:?}"
                ));
            }
            insert_marker_id(&mut ids, id, "section", line)?;
            section = Some(id.to_string());
            MarkerKind::SectionStart(id.to_string())
        } else if marker.starts_with("<!-- orbit:") || marker.starts_with("<!-- /orbit:") {
            return Err(format!(
                "{} {MANIFEST}:{line}: malformed Orbit marker {marker:?}",
                tree.name()
            ));
        } else {
            continue;
        };
        markers.push(Marker { line, kind });
    }

    if let Some(id) = section {
        return Err(format!("local {MANIFEST}: section {id:?} is not closed"));
    }
    markers.sort_by_key(|marker| marker.line);
    Ok(markers)
}

impl MarkerTree {
    fn name(self) -> &'static str {
        match self {
            Self::Remote => "remote",
            Self::Local => "local",
        }
    }
}

fn exact_marker_id<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let id = line.strip_prefix(prefix)?.strip_suffix(" -->")?;
    is_valid_id(id).then_some(id)
}

fn is_valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && id.as_bytes()[0].is_ascii_alphanumeric()
}

fn insert_marker_id(
    ids: &mut BTreeSet<String>,
    id: &str,
    kind: &str,
    line: usize,
) -> Result<(), String> {
    if ids.insert(id.to_string()) {
        Ok(())
    } else {
        Err(format!("{MANIFEST}:{line}: duplicate {kind} ID {id:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composition_replaces_drops_appends_and_strips_markers() {
        let remote = "---\r\nname: orbit\r\n---\r\nbefore\r\n<!-- orbit:include local:matched -->\r\n<!-- orbit:include local:missing -->\r\nafter\r\n";
        let local = "intro\n<!-- orbit:section unmatched -->\n## Unmatched\ntext\n<!-- /orbit:section -->\n<!-- orbit:section matched -->\n## Matched\ntext\n<!-- /orbit:section -->\n";
        let composed = compose_skill_manifests(remote, local).unwrap();
        assert!(composed.starts_with("---\nname: orbit\n---\n"));
        assert!(composed.contains("before\n## Matched\ntext\nafter"));
        assert!(composed.contains("## Local CLI\n\n## Unmatched\ntext"));
        assert!(!composed.contains("orbit:include"));
        assert!(!composed.contains("orbit:section"));
    }

    #[test]
    fn marker_examples_in_fences_are_ignored() {
        let remote = "```markdown\n<!-- orbit:include local:example -->\n```\n<!-- orbit:include local:real -->\n";
        let local = "<!-- orbit:section real -->\nreal\n<!-- /orbit:section -->\n";
        let composed = compose_skill_manifests(remote, local).unwrap();
        assert!(composed.contains("<!-- orbit:include local:example -->"));
        assert!(composed.ends_with("real\n"));
    }
}
