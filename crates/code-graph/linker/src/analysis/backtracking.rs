use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode, FqnType};
use crate::graph::RelationshipType;
use crate::parsing::processor::References;
use internment::ArcIntern;
use parser_core::utils::Range;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

const BACKTRACK_LIMIT: usize = 2;

/// Language-agnostic global backtracking resolver.
///
/// Two-tier name-based join inspired by codescope:
/// 1. Local-first: same-file definitions always win.
/// 2. Global fallback: cross-file lookup, capped at BACKTRACK_LIMIT.
pub struct GlobalBacktracker {
    defn_index: FxHashMap<String, Vec<BacktrackEntry>>,
}

#[derive(Clone)]
struct BacktrackEntry {
    file_path: String,
    range: Range,
}

impl GlobalBacktracker {
    pub fn from_definition_map(
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
    ) -> Self {
        let mut defn_index: FxHashMap<String, Vec<BacktrackEntry>> = FxHashMap::default();

        for ((_, file_path), (node, fqn_type)) in definition_map {
            let name = fqn_type.name().to_string();
            defn_index.entry(name).or_default().push(BacktrackEntry {
                file_path: file_path.clone(),
                range: node.range,
            });
        }

        Self { defn_index }
    }

    pub fn process_dsl_references(
        &self,
        references: &Option<References>,
        relative_path: &str,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        let dsl_refs = match references {
            Some(References::Dsl(refs)) => refs,
            _ => return,
        };

        for raw_ref in dsl_refs {
            let Some(caller_range) =
                self.find_smallest_enclosing(relative_path, &raw_ref.range, definition_map)
            else {
                continue;
            };

            let targets = self.backtrack_ref(&raw_ref.name, relative_path);

            let rel_type = match targets.len() {
                0 => continue,
                1 => RelationshipType::Calls,
                n if n <= BACKTRACK_LIMIT => RelationshipType::AmbiguouslyCalls,
                _ => continue,
            };

            for target in &targets {
                let mut rel = ConsolidatedRelationship::definition_to_definition(
                    ArcIntern::new(relative_path.to_string()),
                    ArcIntern::new(target.file_path.clone()),
                );
                rel.relationship_type = rel_type;
                rel.source_range = ArcIntern::new(raw_ref.range);
                rel.target_range = ArcIntern::new(target.range);
                rel.source_definition_range = Some(ArcIntern::new(caller_range));
                rel.target_definition_range = Some(ArcIntern::new(target.range));
                relationships.push(rel);
            }
        }
    }

    fn backtrack_ref(&self, name: &str, current_file: &str) -> Vec<BacktrackEntry> {
        let Some(candidates) = self.defn_index.get(name) else {
            return Vec::new();
        };

        let local: Vec<_> = candidates
            .iter()
            .filter(|e| e.file_path == current_file)
            .cloned()
            .collect();

        if !local.is_empty() {
            return local;
        }

        candidates
            .iter()
            .filter(|e| e.file_path != current_file)
            .cloned()
            .collect()
    }

    /// Smallest definition whose range contains the reference.
    fn find_smallest_enclosing(
        &self,
        file_path: &str,
        ref_range: &Range,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
    ) -> Option<Range> {
        let mut best: Option<(usize, Range)> = None;

        for ((_, def_file), (node, _)) in definition_map {
            if def_file != file_path {
                continue;
            }
            if !ref_range.is_contained_within(node.range) {
                continue;
            }
            let span = node.range.byte_length();
            if best.as_ref().is_none_or(|(best_span, _)| span < *best_span) {
                best = Some((span, node.range));
            }
        }

        best.map(|(_, range)| range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::types::DefinitionType;
    use parser_core::dsl::types::{DslDefinitionType, DslFqn, DslRawReference};
    use parser_core::utils::Position;

    fn make_range(start_byte: usize, end_byte: usize) -> Range {
        Range::new(
            Position::new(0, start_byte),
            Position::new(0, end_byte),
            (start_byte, end_byte),
        )
    }

    fn make_def_node(_name: &str, fqn_parts: Vec<&str>, range: Range) -> (DefinitionNode, FqnType) {
        let fqn = DslFqn::new(fqn_parts.into_iter().map(String::from).collect());
        let fqn_type = FqnType::Dsl(fqn);
        let def_type = DslDefinitionType {
            label: "Function".to_string(),
        };
        let node = DefinitionNode::new(
            fqn_type.clone(),
            DefinitionType::Dsl(def_type),
            range,
            ArcIntern::new("test.c".to_string()),
        );
        (node, fqn_type)
    }

    #[test]
    fn test_unique_match_creates_calls_edge() {
        let mut def_map = std::collections::HashMap::new();
        let range_foo = make_range(0, 50);
        let range_bar = make_range(60, 120);

        def_map.insert(
            ("foo".to_string(), "file_a.c".to_string()),
            make_def_node("foo", vec!["foo"], range_foo),
        );
        def_map.insert(
            ("bar".to_string(), "file_b.c".to_string()),
            make_def_node("bar", vec!["bar"], range_bar),
        );

        let backtracker = GlobalBacktracker::from_definition_map(&def_map);

        let refs = vec![DslRawReference {
            name: "bar".to_string(),
            range: make_range(10, 15),
            scope_fqn: Some(DslFqn::new(vec!["foo".to_string()])),
        }];

        let mut relationships = Vec::new();
        backtracker.process_dsl_references(
            &Some(References::Dsl(refs)),
            "file_a.c",
            &def_map,
            &mut relationships,
        );

        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].relationship_type, RelationshipType::Calls);
    }

    #[test]
    fn test_ambiguous_match_creates_ambiguous_edges() {
        let mut def_map = std::collections::HashMap::new();
        let range_main = make_range(0, 200);

        def_map.insert(
            ("main".to_string(), "main.c".to_string()),
            make_def_node("main", vec!["main"], range_main),
        );
        // Two definitions of "helper" in different files
        def_map.insert(
            ("helper".to_string(), "util_a.c".to_string()),
            make_def_node("helper", vec!["helper"], make_range(0, 50)),
        );
        def_map.insert(
            ("helper".to_string(), "util_b.c".to_string()),
            make_def_node("helper", vec!["helper"], make_range(0, 50)),
        );

        let backtracker = GlobalBacktracker::from_definition_map(&def_map);

        let refs = vec![DslRawReference {
            name: "helper".to_string(),
            range: make_range(10, 20),
            scope_fqn: Some(DslFqn::new(vec!["main".to_string()])),
        }];

        let mut relationships = Vec::new();
        backtracker.process_dsl_references(
            &Some(References::Dsl(refs)),
            "main.c",
            &def_map,
            &mut relationships,
        );

        assert_eq!(relationships.len(), 2);
        for rel in &relationships {
            assert_eq!(rel.relationship_type, RelationshipType::AmbiguouslyCalls);
        }
    }

    #[test]
    fn test_local_match_wins_over_global() {
        let mut def_map = std::collections::HashMap::new();
        let range_main = make_range(0, 300);
        let range_local_helper = make_range(100, 200);
        let range_external_helper = make_range(0, 50);

        def_map.insert(
            ("main".to_string(), "main.c".to_string()),
            make_def_node("main", vec!["main"], range_main),
        );
        def_map.insert(
            ("helper".to_string(), "main.c".to_string()),
            make_def_node("helper", vec!["helper"], range_local_helper),
        );
        def_map.insert(
            ("helper".to_string(), "other.c".to_string()),
            make_def_node("helper", vec!["helper"], range_external_helper),
        );

        let backtracker = GlobalBacktracker::from_definition_map(&def_map);

        let refs = vec![DslRawReference {
            name: "helper".to_string(),
            range: make_range(10, 20),
            scope_fqn: Some(DslFqn::new(vec!["main".to_string()])),
        }];

        let mut relationships = Vec::new();
        backtracker.process_dsl_references(
            &Some(References::Dsl(refs)),
            "main.c",
            &def_map,
            &mut relationships,
        );

        // Should pick local match only
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].relationship_type, RelationshipType::Calls);
    }

    #[test]
    fn test_too_many_matches_skipped() {
        let mut def_map = std::collections::HashMap::new();
        let range_main = make_range(0, 300);

        def_map.insert(
            ("main".to_string(), "main.c".to_string()),
            make_def_node("main", vec!["main"], range_main),
        );
        // 5 definitions of "get" across different files — exceeds backtrack_limit=2
        for i in 0..5 {
            def_map.insert(
                ("get".to_string(), format!("mod_{i}.c")),
                make_def_node("get", vec!["get"], make_range(0, 50)),
            );
        }

        let backtracker = GlobalBacktracker::from_definition_map(&def_map);

        let refs = vec![DslRawReference {
            name: "get".to_string(),
            range: make_range(10, 20),
            scope_fqn: Some(DslFqn::new(vec!["main".to_string()])),
        }];

        let mut relationships = Vec::new();
        backtracker.process_dsl_references(
            &Some(References::Dsl(refs)),
            "main.c",
            &def_map,
            &mut relationships,
        );

        // Should skip — too ambiguous
        assert_eq!(relationships.len(), 0);
    }
}
