use std::collections::BTreeMap;

use super::TraversalPath;

#[derive(Default)]
pub struct TraversalPathTrie {
    children: BTreeMap<String, TraversalPathTrie>,
    terminal: bool,
}

impl TraversalPathTrie {
    pub fn from_paths(paths: &[&TraversalPath]) -> Self {
        let mut root = Self::default();
        for path in paths {
            root.insert(path);
        }
        root
    }

    fn insert(&mut self, path: &TraversalPath) {
        let path_segments: Vec<&str> = path.segments().collect();
        debug_assert!(
            !path_segments.is_empty(),
            "TraversalPathTrie::insert called with empty path; an empty terminal would emit \"\" and match every row"
        );
        if path_segments.is_empty() {
            return;
        }
        let mut node = self;
        for seg in path_segments {
            node = node.children.entry(seg.to_string()).or_default();
        }
        node.terminal = true;
    }

    pub fn to_minimal_prefixes(&self) -> Vec<TraversalPath> {
        let mut result = Vec::new();
        self.collect(&mut String::new(), &mut result);
        result
    }

    fn collect(&self, prefix: &mut String, out: &mut Vec<TraversalPath>) {
        if self.terminal {
            let mut p = prefix.clone();
            if !p.is_empty() {
                p.push('/');
            }
            out.push(TraversalPath::new_unchecked(p));
            return;
        }

        for (seg, child) in &self.children {
            let restore_len = prefix.len();
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            child.collect(prefix, out);
            prefix.truncate(restore_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(s: &str) -> TraversalPath {
        TraversalPath::new_unchecked(s)
    }

    #[test]
    fn path_trie_subsumes_children() {
        let t =
            TraversalPathTrie::from_paths(&[&tp("1/100/"), &tp("1/100/200/"), &tp("1/100/201/")]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_keeps_siblings() {
        let t = TraversalPathTrie::from_paths(&[&tp("1/100/"), &tp("1/200/")]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_siblings_under_shared_parent() {
        let t = TraversalPathTrie::from_paths(&[
            &tp("1/100/200/"),
            &tp("1/100/201/"),
            &tp("1/100/202/"),
            &tp("1/200/300/"),
        ]);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 4);
        assert!(result.contains(&tp("1/200/300/")));
    }

    #[test]
    fn path_trie_single_path() {
        let t = TraversalPathTrie::from_paths(&[&tp("1/100/")]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_deduplicates() {
        let t = TraversalPathTrie::from_paths(&[&tp("1/100/"), &tp("1/100/"), &tp("1/200/")]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_deep_subsumption() {
        let t = TraversalPathTrie::from_paths(&[
            &tp("1/"),
            &tp("1/100/"),
            &tp("1/100/200/"),
            &tp("1/100/200/300/"),
        ]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/"]);
    }

    #[test]
    fn path_trie_mixed_orgs() {
        let t = TraversalPathTrie::from_paths(&[&tp("1/100/"), &tp("2/100/")]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "2/100/"]);
    }

    #[test]
    fn path_trie_realistic_38_paths() {
        let mut paths: Vec<TraversalPath> = (100..130).map(|i| tp(&format!("1/10/{i}/"))).collect();
        paths.extend((200..208).map(|i| tp(&format!("1/{i}/"))));
        let refs: Vec<&TraversalPath> = paths.iter().collect();
        let t = TraversalPathTrie::from_paths(&refs);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 38);
    }

    #[test]
    fn path_trie_parent_collapses_many_children() {
        let mut paths: Vec<TraversalPath> = vec![tp("1/10/")];
        paths.extend((100..130).map(|i| tp(&format!("1/10/{i}/"))));
        let refs: Vec<&TraversalPath> = paths.iter().collect();
        let t = TraversalPathTrie::from_paths(&refs);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/10/"]);
    }

    #[test]
    #[should_panic(expected = "empty path")]
    fn path_trie_empty_path_panics_in_debug() {
        TraversalPathTrie::from_paths(&[&tp("")]);
    }
}
