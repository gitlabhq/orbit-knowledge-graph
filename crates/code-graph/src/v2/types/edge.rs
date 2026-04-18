use super::node::DefKind;
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// The high-level edge kind stored in the `relationship_kind` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeKind {
    Contains,
    Defines,
    Imports,
    Calls,
    Extends,
}

/// Node kind in the graph — the source or target of an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
pub enum NodeKind {
    Directory,
    File,
    Definition,
    ImportedSymbol,
}

/// A fully described relationship in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Relationship {
    pub edge_kind: EdgeKind,
    pub source_node: NodeKind,
    pub target_node: NodeKind,
    pub source_def_kind: Option<DefKind>,
    pub target_def_kind: Option<DefKind>,
}

/// V1-compatible labels for structural (non-def-to-def) edges.
const STRUCTURAL_LABELS: &[(NodeKind, NodeKind, &str)] = &[
    (NodeKind::Directory, NodeKind::Directory, "DIR_CONTAINS_DIR"),
    (NodeKind::Directory, NodeKind::File, "DIR_CONTAINS_FILE"),
    (NodeKind::File, NodeKind::Definition, "FILE_DEFINES"),
    (NodeKind::File, NodeKind::ImportedSymbol, "FILE_IMPORTS"),
    (
        NodeKind::Definition,
        NodeKind::ImportedSymbol,
        "DEFINES_IMPORTED_SYMBOL",
    ),
    (
        NodeKind::ImportedSymbol,
        NodeKind::ImportedSymbol,
        "IMPORTED_SYMBOL_TO_IMPORTED_SYMBOL",
    ),
    (
        NodeKind::ImportedSymbol,
        NodeKind::Definition,
        "IMPORTED_SYMBOL_TO_DEFINITION",
    ),
    (
        NodeKind::ImportedSymbol,
        NodeKind::File,
        "IMPORTED_SYMBOL_TO_FILE",
    ),
];

impl Relationship {
    /// Fine-grained label (e.g. "CLASS_TO_METHOD", "DIR_CONTAINS_FILE").
    pub fn label(&self) -> String {
        if let (Some(src), Some(tgt)) = (self.source_def_kind, self.target_def_kind) {
            return format!("{}_TO_{}", src.as_upper_str(), tgt.as_upper_str());
        }
        STRUCTURAL_LABELS
            .iter()
            .find(|(s, t, _)| *s == self.source_node && *t == self.target_node)
            .map(|(_, _, label)| label.to_string())
            .unwrap_or_else(|| self.edge_kind.to_string())
    }
}

// ── Containment rules ───────────────────────────────────────────

macro_rules! define_containment_rules {
    ( $( $parent:ident => [ $( $child:ident ),+ $(,)? ] ; )+ ) => {
        pub fn containment_edge_kind(parent: DefKind, child: DefKind) -> Option<EdgeKind> {
            match (parent, child) {
                $( $( (DefKind::$parent, DefKind::$child) => Some(EdgeKind::Defines), )+ )+
                _ => None,
            }
        }

        pub fn containment_relationship(parent: DefKind, child: DefKind) -> Option<Relationship> {
            containment_edge_kind(parent, child).map(|edge_kind| Relationship {
                edge_kind,
                source_node: NodeKind::Definition,
                target_node: NodeKind::Definition,
                source_def_kind: Some(parent),
                target_def_kind: Some(child),
            })
        }

        #[cfg(test)]
        fn all_valid_containment_pairs() -> Vec<(DefKind, DefKind)> {
            vec![$( $( (DefKind::$parent, DefKind::$child), )+ )+]
        }
    };
}

define_containment_rules! {
    Module => [Class, Interface, Module, Function, Method, Constructor, Lambda, Property, EnumEntry, Other];
    Class => [Class, Interface, Method, Function, Constructor, Lambda, Property, EnumEntry, Other];
    Interface => [Interface, Class, Method, Function, Property, Lambda];
    Method => [Method, Function, Class, Lambda, Interface, Property];
    Function => [Function, Class, Method, Lambda];
    Lambda => [Lambda, Class, Function, Method, Interface, Property];
    Other => [Method, Function, Class, Property, Lambda];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_kind_display() {
        assert_eq!(EdgeKind::Contains.to_string(), "CONTAINS");
        assert_eq!(EdgeKind::Defines.to_string(), "DEFINES");
        assert_eq!(EdgeKind::Imports.to_string(), "IMPORTS");
        assert_eq!(EdgeKind::Calls.to_string(), "CALLS");
    }

    #[test]
    fn edge_kind_roundtrip() {
        assert_eq!(EdgeKind::Contains.as_ref(), "CONTAINS");
        assert_eq!("CONTAINS".parse::<EdgeKind>().unwrap(), EdgeKind::Contains);
        assert_eq!("CALLS".parse::<EdgeKind>().unwrap(), EdgeKind::Calls);
    }

    #[test]
    fn node_kind_display() {
        assert_eq!(NodeKind::Directory.to_string(), "Directory");
        assert_eq!(NodeKind::ImportedSymbol.to_string(), "ImportedSymbol");
    }

    #[test]
    fn containment_valid() {
        assert_eq!(
            containment_edge_kind(DefKind::Class, DefKind::Method),
            Some(EdgeKind::Defines)
        );
        assert_eq!(
            containment_edge_kind(DefKind::Module, DefKind::Class),
            Some(EdgeKind::Defines)
        );
    }

    #[test]
    fn containment_invalid() {
        assert_eq!(
            containment_edge_kind(DefKind::Property, DefKind::Class),
            None
        );
        assert_eq!(
            containment_edge_kind(DefKind::EnumEntry, DefKind::Method),
            None
        );
    }

    #[test]
    fn label_for_containment() {
        let rel = containment_relationship(DefKind::Class, DefKind::Method).unwrap();
        assert_eq!(rel.label(), "CLASS_TO_METHOD");
    }

    #[test]
    fn label_for_structural() {
        let rel = Relationship {
            edge_kind: EdgeKind::Contains,
            source_node: NodeKind::Directory,
            target_node: NodeKind::File,
            source_def_kind: None,
            target_def_kind: None,
        };
        assert_eq!(rel.label(), "DIR_CONTAINS_FILE");
    }

    #[test]
    fn module_contains_all_def_kinds() {
        use strum::IntoEnumIterator;
        for kind in DefKind::iter() {
            assert!(
                containment_edge_kind(DefKind::Module, kind).is_some(),
                "Module should contain {kind:?}",
            );
        }
    }

    #[test]
    fn all_pairs_produce_defines() {
        for (parent, child) in all_valid_containment_pairs() {
            assert_eq!(
                containment_edge_kind(parent, child),
                Some(EdgeKind::Defines),
                "{parent:?} -> {child:?} should be Defines",
            );
        }
    }

    #[test]
    fn def_kind_strum() {
        assert_eq!(DefKind::Class.to_string(), "class");
        assert_eq!(DefKind::EnumEntry.to_string(), "enum_entry");
        assert_eq!("class".parse::<DefKind>().unwrap(), DefKind::Class);
        assert_eq!(DefKind::Method.as_ref(), "method");
    }
}
