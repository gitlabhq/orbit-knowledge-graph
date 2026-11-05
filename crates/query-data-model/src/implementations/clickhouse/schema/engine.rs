use super::Engine;

impl Engine {
    pub fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let Some((name, rest)) = text.split_once('(') else {
            return valid_name(text).then(|| Self {
                name: text.to_string(),
                arguments: Vec::new(),
            });
        };
        let arguments = rest.strip_suffix(')')?;
        valid_name(name.trim()).then(|| Self {
            name: name.trim().to_string(),
            arguments: split_arguments(arguments),
        })
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn split_arguments(text: &str) -> Vec<String> {
    let mut arguments = Vec::new();
    let (mut depth, mut quoted, mut start) = (0usize, false, 0);
    for (index, character) in text.char_indices() {
        match character {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                arguments.push(text[start..index].trim().to_string());
                start = index + 1;
            }
            _ => {}
        }
    }
    let last = text[start..].trim();
    if !last.is_empty() {
        arguments.push(last.to_string());
    }
    arguments
}

#[cfg(test)]
mod tests {
    use super::Engine;

    #[test]
    fn parses_names_and_top_level_arguments() {
        let engine = Engine::parse("ReplacingMergeTree(_version, _deleted)").unwrap();
        assert_eq!(engine.name, "ReplacingMergeTree");
        assert_eq!(engine.arguments, ["_version", "_deleted"]);
        let nested = Engine::parse("SummingMergeTree((a, b), 'x,y')").unwrap();
        assert_eq!(nested.arguments, ["(a, b)", "'x,y'"]);
        assert_eq!(Engine::parse("Null").unwrap().to_sql(), "Null");
        assert!(Engine::parse("Bad Name(x)").is_none());
        assert!(Engine::parse("MergeTree(x").is_none());
    }

    #[test]
    fn replicated_prefixes_merge_tree_engines_only() {
        let engine = Engine::parse("ReplacingMergeTree(_version, _deleted)")
            .unwrap()
            .replicated();
        assert_eq!(engine.name, "ReplicatedReplacingMergeTree");
        assert_eq!(engine.replicated().name, "ReplicatedReplacingMergeTree");
        for name in ["Null", "SharedMergeTree", "Dictionary"] {
            assert_eq!(Engine::parse(name).unwrap().replicated().name, name);
        }
    }
}
