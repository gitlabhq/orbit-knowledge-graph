use std::borrow::Cow;

use crate::node::DefKind;
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// The high-level edge kind stored in the `relationship_kind` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeKind {
    Contains,
    Defines,
    Imports,
    Calls,
}

/// Node kind in the graph — the source or target of an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, Display)]
pub enum NodeKind {
    Directory,
    File,
    Definition,
    ImportedSymbol,
}

/// A fully described relationship in the graph. Combines the structural
/// relationship (which node kinds are connected) with the semantic detail
/// (which DefKinds, for definition-to-definition edges).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Relationship {
    pub edge_kind: EdgeKind,
    pub source_node: NodeKind,
    pub target_node: NodeKind,
    /// For definition-to-definition containment edges, the source's DefKind.
    pub source_def_kind: Option<DefKind>,
    /// For definition-to-definition containment edges, the target's DefKind.
    pub target_def_kind: Option<DefKind>,
}

impl Relationship {
    /// Fine-grained label for the edge (e.g. "CLASS_TO_METHOD").
    pub fn label(&self) -> Cow<'static, str> {
        match (self.source_def_kind, self.target_def_kind) {
            (Some(src), Some(tgt)) => {
                format!("{}_TO_{}", src.as_upper_str(), tgt.as_upper_str()).into()
            }
            _ => match (self.source_node, self.target_node, self.edge_kind) {
                (NodeKind::Directory, NodeKind::Directory, _) => "DIR_CONTAINS_DIR".into(),
                (NodeKind::Directory, NodeKind::File, _) => "DIR_CONTAINS_FILE".into(),
                (NodeKind::File, NodeKind::Definition, EdgeKind::Defines) => "FILE_DEFINES".into(),
                (NodeKind::File, NodeKind::ImportedSymbol, EdgeKind::Imports) => {
                    "FILE_IMPORTS".into()
                }
                (NodeKind::Definition, NodeKind::ImportedSymbol, _) => {
                    "DEFINES_IMPORTED_SYMBOL".into()
                }
                (NodeKind::ImportedSymbol, NodeKind::ImportedSymbol, _) => {
                    "IMPORTED_SYMBOL_TO_IMPORTED_SYMBOL".into()
                }
                (NodeKind::ImportedSymbol, NodeKind::Definition, _) => {
                    "IMPORTED_SYMBOL_TO_DEFINITION".into()
                }
                (NodeKind::ImportedSymbol, NodeKind::File, _) => "IMPORTED_SYMBOL_TO_FILE".into(),
                _ => self.edge_kind.to_string().into(),
            },
        }
    }
}

/// Declarative table of valid (parent DefKind, child DefKind) containment
/// relationships. The macro generates `containment_edge_kind()` and
/// `containment_relationship()` which the linker calls when building
/// definition-to-definition edges.
macro_rules! define_containment_rules {
    ( $( $parent:ident => [ $( $child:ident ),+ $(,)? ] ; )+ ) => {
        /// Given a parent and child DefKind, returns the EdgeKind if this
        /// is a valid containment relationship, or None if not.
        pub fn containment_edge_kind(parent: DefKind, child: DefKind) -> Option<EdgeKind> {
            match (parent, child) {
                $(
                    $( (DefKind::$parent, DefKind::$child) => Some(EdgeKind::Defines), )+
                )+
                _ => None,
            }
        }

        /// Build a full Relationship for a definition-to-definition containment edge.
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
            vec![
                $(
                    $( (DefKind::$parent, DefKind::$child), )+
                )+
            ]
        }
    };
}

define_containment_rules! {
    Module => [
        Class, Interface, Module, Function, Method, Constructor,
        Lambda, Property, EnumEntry, Other,
    ];
    Class => [
        Class, Interface, Method, Function, Constructor,
        Lambda, Property, EnumEntry, Other,
    ];
    Interface => [
        Interface, Class, Method, Function, Property, Lambda,
    ];
    Method => [
        Method, Function, Class, Lambda, Interface, Property,
    ];
    Function => [
        Function, Class, Method, Lambda,
    ];
    Lambda => [
        Lambda, Class, Function, Method, Interface, Property,
    ];
    Other => [
        Method, Function, Class, Property, Lambda,
    ];
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
    fn edge_kind_as_ref() {
        assert_eq!(EdgeKind::Contains.as_ref(), "CONTAINS");
    }

    #[test]
    fn edge_kind_from_str() {
        assert_eq!("CONTAINS".parse::<EdgeKind>().unwrap(), EdgeKind::Contains);
        assert_eq!("CALLS".parse::<EdgeKind>().unwrap(), EdgeKind::Calls);
    }

    #[test]
    fn node_kind_display() {
        assert_eq!(NodeKind::Directory.to_string(), "Directory");
        assert_eq!(NodeKind::ImportedSymbol.to_string(), "ImportedSymbol");
    }

    #[test]
    fn basic_containment() {
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
    fn invalid_containment_returns_none() {
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
    fn relationship_label_for_containment() {
        let rel = containment_relationship(DefKind::Class, DefKind::Method).unwrap();
        assert_eq!(rel.label(), "CLASS_TO_METHOD");
        assert_eq!(rel.edge_kind, EdgeKind::Defines);
    }

    #[test]
    fn relationship_label_for_structural() {
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
    fn module_contains_everything() {
        for kind in [
            DefKind::Class,
            DefKind::Interface,
            DefKind::Module,
            DefKind::Function,
            DefKind::Method,
            DefKind::Constructor,
            DefKind::Lambda,
            DefKind::Property,
            DefKind::EnumEntry,
            DefKind::Other,
        ] {
            assert!(
                containment_edge_kind(DefKind::Module, kind).is_some(),
                "Module should contain {:?}",
                kind
            );
        }
    }

    #[test]
    fn all_pairs_produce_defines() {
        for (parent, child) in all_valid_containment_pairs() {
            assert_eq!(
                containment_edge_kind(parent, child),
                Some(EdgeKind::Defines),
                "{:?} -> {:?} should be Defines",
                parent,
                child
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

    #[test]
    fn reference_status_strum() {
        use crate::node::ReferenceStatus;
        assert_eq!(ReferenceStatus::Resolved.to_string(), "resolved");
        assert_eq!(
            "ambiguous".parse::<ReferenceStatus>().unwrap(),
            ReferenceStatus::Ambiguous
        );
    }
}
