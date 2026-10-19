use std::collections::BTreeMap;
use std::sync::LazyLock;

use rust_embed::Embed;

use super::Options;

#[derive(Embed)]
#[folder = "$CONFIG_DIR/grep"]
struct GrepAssets;

/// One search in a query: a kind qualifier such as `mr:` and the words after it, up to the next.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Section {
    pub(super) kinds: Vec<String>,
    pub(super) query: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Parsed {
    pub(super) sections: Vec<Section>,
    pub(super) globs: Vec<String>,
    pub(super) targets: Vec<String>,
}

static ALIASES: LazyLock<BTreeMap<String, String>> = LazyLock::new(|| {
    let file = GrepAssets::get("qualifiers.yaml").expect("config/grep/qualifiers.yaml is embedded");
    let declared: BTreeMap<String, Vec<String>> =
        orbit_utils::yaml::from_slice(&file.data).expect("config/grep/qualifiers.yaml is valid");
    let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");
    let mut aliases = BTreeMap::new();
    let searchable = |node: &str| {
        matches!(node, "Definition" | "File" | "Directory")
            || crate::remote::grep::scope_for(node).is_some()
    };
    for node in ontology.node_names().filter(|node| searchable(node)) {
        aliases.insert(node.to_ascii_lowercase(), node.to_string());
        aliases.insert(snake(node), node.to_string());
    }
    for (node, names) in declared {
        if ontology.get_node(&node).is_some() && searchable(&node) {
            aliases.extend(names.into_iter().map(|name| (name, node.clone())));
        }
    }
    aliases
});

fn snake(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The kind a qualifier name selects: a node grep searches, one of its aliases, or a definition
/// family such as `method`, each also in the plural. Other nodes stay literal, so YAML keys such
/// as `stage:` still search as text.
fn kind_for(name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    [
        Some(lower.as_str()),
        lower.strip_suffix("es"),
        lower.strip_suffix('s'),
    ]
    .into_iter()
    .flatten()
    .find_map(|name| {
        ALIASES
            .get(name)
            .cloned()
            .or_else(|| super::kind::is_family(name).then(|| name.to_string()))
    })
}

fn tokens(query: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for c in query.chars() {
        match c {
            '"' => quoted = !quoted,
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn glob_for(value: &str) -> String {
    match value.contains(['*', '?', '[']) {
        true => value.to_string(),
        false => format!("*{value}*"),
    }
}

pub(super) fn parse(query: &str) -> Option<Parsed> {
    let mut parsed = Parsed::default();
    let mut sections: Vec<(Option<String>, Vec<String>)> = vec![(None, Vec::new())];
    let mut any = false;
    for token in tokens(query) {
        let Some((name, value)) = token.split_once(':') else {
            sections.last_mut().expect("a section").1.push(token);
            continue;
        };
        if value.starts_with(':') || value.starts_with('/') {
            sections.last_mut().expect("a section").1.push(token);
            continue;
        }
        match name.to_ascii_lowercase().as_str() {
            "file" | "path" if !value.is_empty() => parsed.globs.push(glob_for(value)),
            "repo" if !value.is_empty() => parsed.targets.push(value.to_string()),
            _ => match kind_for(name) {
                Some(kind) => {
                    let words = (!value.is_empty()).then(|| value.to_string());
                    sections.push((Some(kind), words.into_iter().collect()));
                }
                None => {
                    sections.last_mut().expect("a section").1.push(token);
                    continue;
                }
            },
        }
        any = true;
    }
    if !any {
        return None;
    }
    let mut merged: Vec<(Option<String>, Vec<String>)> = Vec::new();
    for (kind, words) in sections {
        match merged.last_mut() {
            Some((None, previous)) if words.is_empty() && !previous.is_empty() => {
                merged.last_mut().expect("a section").0 = kind;
            }
            _ if words.is_empty() && kind.is_none() => {}
            _ => merged.push((kind, words)),
        }
    }
    if merged.is_empty() {
        merged.push((None, Vec::new()));
    }
    parsed.sections = merged
        .into_iter()
        .map(|(kind, words)| Section {
            kinds: kind.into_iter().collect(),
            query: Some(match words.is_empty() {
                true => ".".to_string(),
                false => words.join(" "),
            }),
        })
        .collect();
    Some(parsed)
}

/// Whether an argument is a qualifier such as `def:` or `repo:g/p` rather than a path.
fn is_qualifier(word: &str) -> bool {
    word.split_once(':')
        .is_some_and(|(name, value)| match name.to_ascii_lowercase().as_str() {
            "file" | "path" | "repo" => !value.is_empty(),
            _ => !value.starts_with([':', '/']) && kind_for(name).is_some(),
        })
}

/// Agents often write `orbit grep 'words' def:`, with the qualifier outside the quotes. Such
/// arguments join the query unless a path by that name exists.
fn absorb_trailing(query: Option<String>, paths: &mut Vec<String>) -> Option<String> {
    let (qualifiers, rest): (Vec<String>, Vec<String>) = std::mem::take(paths)
        .into_iter()
        .partition(|path| is_qualifier(path) && !std::path::Path::new(path).exists());
    *paths = rest;
    if qualifiers.is_empty() {
        return query;
    }
    let mut words: Vec<String> = query.into_iter().collect();
    words.extend(qualifiers);
    Some(words.join(" "))
}

/// Splits the query into its searches, moving file filters and GitLab targets into `options`
/// and `paths`, which every search shares.
pub(super) fn apply(
    query: Option<String>,
    options: &mut Options,
    paths: &mut Vec<String>,
) -> Vec<Section> {
    let plain = |query| {
        vec![Section {
            kinds: Vec::new(),
            query,
        }]
    };
    if options.fixed {
        return plain(query);
    }
    let query = absorb_trailing(query, paths);
    let Some(parsed) = query.as_deref().and_then(parse) else {
        return plain(query);
    };
    options.globs.extend(parsed.globs);
    paths.extend(parsed.targets);
    parsed.sections
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|item| item.to_string()).collect()
    }

    fn sections(query: &str) -> Vec<(Vec<String>, String)> {
        parse(query)
            .unwrap()
            .sections
            .into_iter()
            .map(|section| (section.kinds, section.query.unwrap()))
            .collect()
    }

    #[test]
    fn each_kind_qualifier_starts_its_own_search() {
        assert_eq!(
            sections("mr:test x y def: run job"),
            vec![
                (strings(&["MergeRequest"]), "test x y".into()),
                (strings(&["Definition"]), "run job".into()),
            ]
        );
        assert_eq!(
            sections("rate limit methods:"),
            vec![(strings(&["methods"]), "rate limit".into())]
        );
        assert_eq!(
            sections("merge_requests: x"),
            vec![(strings(&["MergeRequest"]), "x".into())]
        );
        let parsed = parse("issue:rate limit path:src repo:g/p").unwrap();
        assert_eq!(parsed.globs, strings(&["*src*"]));
        assert_eq!(parsed.targets, strings(&["g/p"]));
        assert_eq!(parse("type:go grep"), None);
        assert_eq!(parse("is:mr x"), None);
        assert_eq!(parse("stage: test"), None);
        let mut paths = strings(&["def:", "src/"]);
        assert_eq!(
            absorb_trailing(Some("x".into()), &mut paths).as_deref(),
            Some("x def:")
        );
        assert_eq!(paths, strings(&["src/"]));
    }
}
