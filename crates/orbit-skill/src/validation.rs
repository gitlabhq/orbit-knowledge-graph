use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use comrak::nodes::NodeValue;
use comrak::{Arena, Options, parse_document};
use syn::{Expr, ExprLit, Item, Lit, Meta};

const MANIFEST: &str = "SKILL.md";
const SLOT_PREFIX: &str = "<!-- orbit:include local:";
const SECTION_PREFIX: &str = "<!-- orbit:section ";
const SECTION_END: &str = "<!-- /orbit:section -->";

pub const CLAP_HELP_COMMAND: &str = "help";

#[derive(Debug, PartialEq, Eq)]
pub struct SkillValidation {
    pub remote_commands: BTreeSet<String>,
}

pub fn validate_skill_pair(
    remote_root: impl AsRef<Path>,
    local_root: impl AsRef<Path>,
    commands_source: impl AsRef<Path>,
) -> Result<SkillValidation, String> {
    let remote_root = remote_root.as_ref();
    let local_root = local_root.as_ref();
    let remote = load_tree(remote_root)?;
    let local = load_tree(local_root)?;

    validate_path_namespace(&remote, &local)?;

    let remote_manifest = remote
        .get(MANIFEST)
        .ok_or_else(|| format!("{} is missing {MANIFEST}", remote_root.display()))?;
    let local_manifest = local
        .get(MANIFEST)
        .ok_or_else(|| format!("{} is missing {MANIFEST}", local_root.display()))?;
    let slots = parse_markers(remote_manifest, MarkerTree::Remote)?;
    let sections = parse_markers(local_manifest, MarkerTree::Local)?;
    if slots != sections {
        let missing_sections: Vec<_> = slots.difference(&sections).cloned().collect();
        let missing_slots: Vec<_> = sections.difference(&slots).cloned().collect();
        return Err(format!(
            "skill placeholders and local sections do not match; placeholders without sections: {missing_sections:?}; sections without placeholders: {missing_slots:?}"
        ));
    }

    let union: BTreeSet<_> = remote.keys().chain(local.keys()).cloned().collect();
    validate_links("remote", &remote, &union)?;
    validate_links("local", &local, &union)?;

    let remote_commands = extract_remote_commands_from_tree(&remote);
    if remote_commands.is_empty() {
        return Err("remote skill does not document any Orbit commands".to_string());
    }
    let command_inventory = extract_command_inventory(commands_source.as_ref())?;
    let unknown: Vec<_> = remote_commands
        .difference(&command_inventory)
        .filter(|command| command.as_str() != CLAP_HELP_COMMAND)
        .cloned()
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "remote skill documents commands absent from Commands: {unknown:?}; available commands: {command_inventory:?}"
        ));
    }

    Ok(SkillValidation { remote_commands })
}

fn load_tree(root: &Path) -> Result<BTreeMap<String, String>, String> {
    let mut files = BTreeMap::new();
    collect_files(root, root, &mut files)?;
    Ok(files)
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("reading {}: {error}", directory.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("reading {}: {error}", directory.display()))?
            .path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
            continue;
        }
        if !path.is_file() {
            return Err(format!(
                "skill entry is not a regular file: {}",
                path.display()
            ));
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|error| format!("resolving {}: {error}", path.display()))?;
        let relative = normalized_relative_path(relative)?;
        let content = std::fs::read_to_string(&path)
            .map_err(|error| format!("reading UTF-8 skill file {}: {error}", path.display()))?;
        if files.insert(relative.clone(), content).is_some() {
            return Err(format!("duplicate skill path: {relative}"));
        }
    }
    Ok(())
}

fn normalized_relative_path(path: &Path) -> Result<String, String> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => segments.push(
                segment
                    .to_str()
                    .ok_or_else(|| format!("skill path is not UTF-8: {}", path.display()))?,
            ),
            _ => return Err(format!("skill path is not normalized: {}", path.display())),
        }
    }
    if segments.is_empty() {
        return Err("skill path is empty".to_string());
    }
    Ok(segments.join("/"))
}

fn validate_path_namespace(
    remote: &BTreeMap<String, String>,
    local: &BTreeMap<String, String>,
) -> Result<(), String> {
    let overlaps: Vec<_> = remote
        .keys()
        .filter(|path| path.as_str() != MANIFEST && local.contains_key(*path))
        .cloned()
        .collect();
    if overlaps.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "remote and local skill paths overlap outside {MANIFEST}: {overlaps:?}"
        ))
    }
}

#[derive(Clone, Copy)]
enum MarkerTree {
    Remote,
    Local,
}

fn parse_markers(content: &str, tree: MarkerTree) -> Result<BTreeSet<String>, String> {
    let mut ids = BTreeSet::new();
    let mut section: Option<String> = None;
    let mut fence: Option<(char, usize)> = None;

    for (index, line) in content.replace("\r\n", "\n").lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if update_fence(trimmed, &mut fence) || fence.is_some() {
            continue;
        }

        if trimmed == SECTION_END {
            if !matches!(tree, MarkerTree::Local) {
                return Err(format!(
                    "remote {MANIFEST}:{line_number}: unexpected section end"
                ));
            }
            if section.take().is_none() {
                return Err(format!(
                    "local {MANIFEST}:{line_number}: section end has no start"
                ));
            }
            continue;
        }

        if let Some(id) = exact_marker_id(trimmed, SLOT_PREFIX) {
            if !matches!(tree, MarkerTree::Remote) {
                return Err(format!(
                    "local {MANIFEST}:{line_number}: include slot is not allowed"
                ));
            }
            insert_marker_id(&mut ids, id, "slot", line_number)?;
            continue;
        }

        if let Some(id) = exact_marker_id(trimmed, SECTION_PREFIX) {
            if !matches!(tree, MarkerTree::Local) {
                return Err(format!(
                    "remote {MANIFEST}:{line_number}: section export is not allowed"
                ));
            }
            if let Some(open) = &section {
                return Err(format!(
                    "local {MANIFEST}:{line_number}: section {id:?} is nested inside {open:?}"
                ));
            }
            insert_marker_id(&mut ids, id, "section", line_number)?;
            section = Some(id.to_string());
            continue;
        }

        if trimmed.starts_with("<!-- orbit:") || trimmed.starts_with("<!-- /orbit:") {
            return Err(format!(
                "{} {MANIFEST}:{line_number}: malformed Orbit marker {trimmed:?}",
                match tree {
                    MarkerTree::Remote => "remote",
                    MarkerTree::Local => "local",
                }
            ));
        }
    }

    if let Some(id) = section {
        return Err(format!("local {MANIFEST}: section {id:?} is not closed"));
    }
    Ok(ids)
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
    line_number: usize,
) -> Result<(), String> {
    if ids.insert(id.to_string()) {
        Ok(())
    } else {
        Err(format!(
            "{MANIFEST}:{line_number}: duplicate {kind} ID {id:?}"
        ))
    }
}

fn update_fence(line: &str, fence: &mut Option<(char, usize)>) -> bool {
    let candidate = line.trim_start();
    let Some(marker) = candidate
        .chars()
        .next()
        .filter(|char| matches!(char, '`' | '~'))
    else {
        return false;
    };
    let length = candidate.chars().take_while(|char| char == &marker).count();
    if length < 3 {
        return false;
    }

    match fence {
        Some((open_marker, open_length)) if marker == *open_marker && length >= *open_length => {
            *fence = None;
            true
        }
        None => {
            *fence = Some((marker, length));
            true
        }
        Some(_) => false,
    }
}

fn validate_links(
    tree_name: &str,
    files: &BTreeMap<String, String>,
    union: &BTreeSet<String>,
) -> Result<(), String> {
    for (source, content) in files {
        if !source.ends_with(".md") {
            continue;
        }
        for destination in markdown_link_destinations(content) {
            let Some(destination) = relative_link_path(&destination) else {
                continue;
            };
            let resolved = resolve_link(source, destination)?;
            if !union.contains(&resolved) {
                return Err(format!(
                    "{tree_name} skill link from {source} resolves to missing path {resolved:?}: {destination:?}"
                ));
            }
        }
    }
    Ok(())
}

fn markdown_link_destinations(content: &str) -> Vec<String> {
    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());
    root.descendants()
        .filter_map(|node| match &node.data().value {
            NodeValue::Link(link) | NodeValue::Image(link) => Some(link.url.clone()),
            _ => None,
        })
        .collect()
}

fn relative_link_path(destination: &str) -> Option<&str> {
    let path = destination.split(['#', '?']).next().unwrap_or_default();
    if path.is_empty() || path.starts_with('/') || path.starts_with("//") || has_uri_scheme(path) {
        None
    } else {
        Some(path)
    }
}

fn has_uri_scheme(path: &str) -> bool {
    let Some((scheme, _)) = path.split_once(':') else {
        return false;
    };
    !scheme.is_empty()
        && scheme.bytes().enumerate().all(|(index, byte)| match index {
            0 => byte.is_ascii_alphabetic(),
            _ => byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'),
        })
}

fn resolve_link(source: &str, destination: &str) -> Result<String, String> {
    let parent = Path::new(source).parent().unwrap_or_else(|| Path::new(""));
    let mut segments = Vec::new();
    for component in parent.join(destination).components() {
        match component {
            Component::Normal(segment) => segments.push(
                segment
                    .to_str()
                    .ok_or_else(|| format!("link path is not UTF-8: {destination:?}"))?
                    .to_string(),
            ),
            Component::CurDir => {}
            Component::ParentDir => {
                if segments.pop().is_none() {
                    return Err(format!(
                        "relative link escapes the composed skill tree from {source}: {destination:?}"
                    ));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "link is not relative from {source}: {destination:?}"
                ));
            }
        }
    }
    Ok(segments.join("/"))
}

fn extract_remote_commands_from_tree(files: &BTreeMap<String, String>) -> BTreeSet<String> {
    files
        .iter()
        .filter(|(path, _)| path.ends_with(".md"))
        .flat_map(|(_, content)| markdown_code_snippets(content))
        .flat_map(|snippet| commands_in_snippet(&snippet))
        .collect()
}

fn markdown_code_snippets(content: &str) -> Vec<String> {
    let arena = Arena::new();
    let root = parse_document(&arena, content, &Options::default());
    root.descendants()
        .filter_map(|node| match &node.data().value {
            NodeValue::Code(code) => Some(code.literal.clone()),
            NodeValue::CodeBlock(block) if block.fenced => Some(block.literal.clone()),
            _ => None,
        })
        .collect()
}

fn commands_in_snippet(snippet: &str) -> Vec<String> {
    snippet.lines().flat_map(commands_in_line).collect()
}

fn commands_in_line(line: &str) -> Vec<String> {
    let tokens: Vec<_> = line.split_whitespace().map(clean_command_token).collect();
    let mut commands = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if *token != "orbit" {
            continue;
        }
        let mut command_index = index + 1;
        while let Some(token) = tokens.get(command_index)
            && token.starts_with('-')
        {
            command_index += 1;
        }
        let Some(command) = tokens.get(command_index) else {
            continue;
        };
        if command.contains(['<', '>']) || !is_command_name(command) {
            continue;
        }
        commands.push((*command).to_string());
    }
    commands
}

fn clean_command_token(token: &str) -> &str {
    token.trim_matches(|char: char| {
        matches!(
            char,
            '`' | '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' | ':' | '|'
        )
    })
}

fn is_command_name(command: &str) -> bool {
    !command.is_empty()
        && command
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && command.as_bytes()[0].is_ascii_alphanumeric()
}

fn extract_command_inventory(commands_source: &Path) -> Result<BTreeSet<String>, String> {
    let source = std::fs::read_to_string(commands_source)
        .map_err(|error| format!("reading {}: {error}", commands_source.display()))?;
    let syntax = syn::parse_file(&source)
        .map_err(|error| format!("parsing {}: {error}", commands_source.display()))?;
    let commands = syntax
        .items
        .iter()
        .find_map(|item| match item {
            Item::Enum(item) if item.ident == "Commands" => Some(item),
            _ => None,
        })
        .ok_or_else(|| format!("{} has no Commands enum", commands_source.display()))?;

    commands
        .variants
        .iter()
        .map(|variant| {
            command_name_override(&variant.attrs)
                .map(|name| name.unwrap_or_else(|| to_kebab_case(&variant.ident.to_string())))
        })
        .collect()
}

fn command_name_override(attributes: &[syn::Attribute]) -> Result<Option<String>, String> {
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("command"))
    {
        let Meta::List(arguments) = &attribute.meta else {
            continue;
        };
        let values = arguments
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
            .map_err(|error| format!("parsing #[command(...)]: {error}"))?;
        for value in values {
            let Meta::NameValue(value) = value else {
                continue;
            };
            if !value.path.is_ident("name") {
                continue;
            }
            let Expr::Lit(ExprLit {
                lit: Lit::Str(value),
                ..
            }) = value.value
            else {
                return Err("command name must be a string literal".to_string());
            };
            return Ok(Some(value.value()));
        }
    }
    Ok(None)
}

fn to_kebab_case(name: &str) -> String {
    let characters: Vec<_> = name.chars().collect();
    let mut output = String::new();
    for (index, char) in characters.iter().copied().enumerate() {
        if char.is_ascii_uppercase() {
            let previous = index.checked_sub(1).and_then(|value| characters.get(value));
            let next = characters.get(index + 1);
            let word_boundary = previous.is_some_and(|previous| {
                previous.is_ascii_lowercase()
                    || previous.is_ascii_digit()
                    || (previous.is_ascii_uppercase()
                        && next.is_some_and(|next| next.is_ascii_lowercase()))
            });
            if word_boundary {
                output.push('-');
            }
            output.push(char.to_ascii_lowercase());
        } else {
            output.push(char);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("remote/references")).unwrap();
        std::fs::create_dir_all(root.path().join("local/references/local")).unwrap();
        std::fs::write(
            root.path().join("remote/SKILL.md"),
            "[remote](references/remote.md)\n<!-- orbit:include local:quick-start -->\n`orbit graph-status --project-id 1`\n",
        )
        .unwrap();
        std::fs::write(root.path().join("remote/references/remote.md"), "remote\n").unwrap();
        std::fs::write(
            root.path().join("local/SKILL.md"),
            "<!-- orbit:section quick-start -->\n[local](references/local/sql.md)\n<!-- /orbit:section -->\n",
        )
        .unwrap();
        std::fs::write(root.path().join("local/references/local/sql.md"), "local\n").unwrap();
        std::fs::write(
            root.path().join("main.rs"),
            r#"
                enum Commands {
                    Query,
                    #[command(name = "graph-status")]
                    GraphStatus,
                }
            "#,
        )
        .unwrap();
        root
    }

    fn validate(root: &Path) -> Result<SkillValidation, String> {
        validate_skill_pair(
            root.join("remote"),
            root.join("local"),
            root.join("main.rs"),
        )
    }

    #[test]
    fn validates_bijective_markers_links_namespace_and_commands() {
        let root = fixture();
        let result = validate(root.path()).unwrap();
        assert_eq!(
            result.remote_commands,
            BTreeSet::from(["graph-status".into()])
        );
    }

    #[test]
    fn marker_parser_tolerates_crlf_and_ignores_fenced_examples() {
        let remote = "```markdown\r\n<!-- orbit:include local:ignored -->\r\n```\r\n<!-- orbit:include local:kept -->\r\n";
        let local = "~~~markdown\r\n<!-- orbit:section ignored -->\r\n~~~\r\n<!-- orbit:section kept -->\r\ntext\r\n<!-- /orbit:section -->\r\n";
        assert_eq!(
            parse_markers(remote, MarkerTree::Remote).unwrap(),
            BTreeSet::from(["kept".into()])
        );
        assert_eq!(
            parse_markers(local, MarkerTree::Local).unwrap(),
            BTreeSet::from(["kept".into()])
        );
    }

    #[test]
    fn marker_parser_rejects_invalid_duplicate_nested_and_unbalanced_markers() {
        for remote in [
            "<!-- orbit:include local:Bad -->",
            "<!-- orbit:include local:a -->\n<!-- orbit:include local:a -->",
            "<!-- orbit:section a -->",
            "<!-- /orbit:section -->",
        ] {
            assert!(
                parse_markers(remote, MarkerTree::Remote).is_err(),
                "{remote}"
            );
        }
        for local in [
            "<!-- orbit:section a -->\n<!-- orbit:section b -->\n<!-- /orbit:section -->",
            "<!-- orbit:section a -->",
            "<!-- /orbit:section -->",
            "<!-- orbit:include local:a -->",
        ] {
            assert!(parse_markers(local, MarkerTree::Local).is_err(), "{local}");
        }
    }

    #[test]
    fn validation_rejects_marker_set_drift() {
        let root = fixture();
        std::fs::write(
            root.path().join("local/SKILL.md"),
            "<!-- orbit:section other -->\n<!-- /orbit:section -->\n",
        )
        .unwrap();
        assert!(
            validate(root.path())
                .unwrap_err()
                .contains("placeholders and local sections do not match")
        );
    }

    #[test]
    fn validation_rejects_path_overlap() {
        let root = fixture();
        std::fs::create_dir_all(root.path().join("local/references")).unwrap();
        std::fs::write(root.path().join("local/references/remote.md"), "local").unwrap();
        assert!(validate(root.path()).unwrap_err().contains("overlap"));
    }

    #[test]
    fn links_resolve_across_the_composed_union() {
        let root = fixture();
        std::fs::write(
            root.path().join("remote/SKILL.md"),
            "[local](references/local/sql.md)\n<!-- orbit:include local:quick-start -->\n`orbit query`\n",
        )
        .unwrap();
        assert!(validate(root.path()).is_ok());
    }

    #[test]
    fn validation_rejects_missing_and_escaping_links() {
        for destination in ["references/missing.md", "../outside.md"] {
            let root = fixture();
            std::fs::write(
                root.path().join("remote/SKILL.md"),
                format!("[bad]({destination})\n<!-- orbit:include local:quick-start -->\n"),
            )
            .unwrap();
            assert!(validate(root.path()).is_err(), "{destination}");
        }
    }

    #[test]
    fn command_extraction_reads_inline_and_fenced_code_only() {
        let markdown = r#"
Prose glab orbit made-up does not count.
`glab orbit query request.json` and `orbit <command>` and `glab orbit help`.

```shell
glab orbit --yes repo-map overview
orbit graph-status --project-id 1
```
"#;
        assert_eq!(
            markdown_code_snippets(markdown)
                .iter()
                .flat_map(|snippet| commands_in_snippet(snippet))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "graph-status".to_string(),
                "help".to_string(),
                "query".to_string(),
                "repo-map".to_string(),
            ])
        );
    }

    #[test]
    fn command_inventory_uses_explicit_names_and_kebab_case_defaults() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("main.rs");
        std::fs::write(
            &source,
            r#"
                enum Commands {
                    Query,
                    HookGuard,
                    HTTPServer,
                    #[command(name = "repo-map", hide = true)]
                    RepoMap(Args),
                }
            "#,
        )
        .unwrap();
        assert_eq!(
            extract_command_inventory(&source).unwrap(),
            BTreeSet::from([
                "hook-guard".to_string(),
                "http-server".to_string(),
                "query".to_string(),
                "repo-map".to_string(),
            ])
        );
    }

    #[test]
    fn validation_rejects_empty_command_extraction() {
        let root = fixture();
        std::fs::write(
            root.path().join("remote/SKILL.md"),
            "<!-- orbit:include local:quick-start -->\n",
        )
        .unwrap();
        assert!(
            validate(root.path())
                .unwrap_err()
                .contains("does not document any Orbit commands")
        );
    }

    #[test]
    fn validation_rejects_unknown_documented_commands_but_allows_help() {
        let root = fixture();
        std::fs::write(
            root.path().join("remote/SKILL.md"),
            "<!-- orbit:include local:quick-start -->\n`orbit help`\n`orbit imaginary`\n",
        )
        .unwrap();
        let error = validate(root.path()).unwrap_err();
        assert!(error.contains("imaginary"));
        assert!(!error.contains("absent from Commands: [\"help\""));
    }
}
