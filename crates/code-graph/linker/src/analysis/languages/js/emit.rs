use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
    ImportType, ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use parser_core::utils::Range;

use super::types::*;
use std::collections::HashMap;

pub struct JsEmitted {
    pub definitions: Vec<DefinitionNode>,
    pub imported_symbols: Vec<ImportedSymbolNode>,
    pub relationships: Vec<ConsolidatedRelationship>,
}

impl JsFileAnalysis {
    pub fn emit(&self) -> JsEmitted {
        let path = ArcIntern::new(self.relative_path.clone());
        let def_ranges_by_fqn: HashMap<&str, Range> = self
            .defs
            .iter()
            .map(|def| (def.fqn.as_str(), def.range))
            .collect();
        let mut definitions = Vec::with_capacity(self.defs.len());
        let mut imported_symbols = Vec::with_capacity(self.imports.len());
        let mut relationships = Vec::new();

        for def in &self.defs {
            let def_node = DefinitionNode::new(
                FqnType::Js(def.fqn.clone()),
                DefinitionType::Js(def.kind.as_str()),
                def.range,
                path.clone(),
            );

            if !def.fqn.contains("::") {
                relationships.push(ConsolidatedRelationship {
                    source_path: Some(path.clone()),
                    target_path: Some(path.clone()),
                    kind: RelationshipKind::FileToDefinition,
                    relationship_type: RelationshipType::FileDefines,
                    source_range: ArcIntern::new(Range::empty()),
                    target_range: ArcIntern::new(def.range),
                    ..Default::default()
                });
            }

            definitions.push(def_node);
        }

        for child in &self.defs {
            if let Some(parent_fqn) = child.fqn.rsplit_once("::").map(|(p, _)| p)
                && let Some(parent) = self.defs.iter().find(|d| d.fqn == parent_fqn)
            {
                let rel_type = match (&parent.kind, &child.kind) {
                    (JsDefKind::Class, JsDefKind::Method { .. }) => RelationshipType::ClassToMethod,
                    (JsDefKind::Class, JsDefKind::Getter { .. } | JsDefKind::Setter { .. }) => {
                        RelationshipType::ClassToMethod
                    }
                    (JsDefKind::Class, JsDefKind::Class) => RelationshipType::ClassToClass,
                    (JsDefKind::Class, JsDefKind::Function) => RelationshipType::ClassToMethod,
                    (JsDefKind::Function, JsDefKind::Function) => {
                        RelationshipType::FunctionToFunction
                    }
                    (JsDefKind::Function, JsDefKind::Class) => RelationshipType::FunctionToClass,
                    (JsDefKind::Method { .. }, JsDefKind::Function) => {
                        RelationshipType::MethodToFunction
                    }
                    (JsDefKind::Method { .. }, JsDefKind::Class) => RelationshipType::MethodToClass,
                    (JsDefKind::Interface, JsDefKind::Function) => {
                        RelationshipType::InterfaceToFunction
                    }
                    _ => RelationshipType::ClassToMethod,
                };
                relationships.push(ConsolidatedRelationship {
                    source_path: Some(path.clone()),
                    target_path: Some(path.clone()),
                    kind: RelationshipKind::DefinitionToDefinition,
                    relationship_type: rel_type,
                    source_range: ArcIntern::new(parent.range),
                    target_range: ArcIntern::new(child.range),
                    ..Default::default()
                });
            }
        }

        for imp in &self.imports {
            let (import_type_str, identifier) = match &imp.kind {
                JsImportKind::Named { imported_name } => {
                    let alias = if *imported_name != imp.local_name {
                        Some(imp.local_name.clone())
                    } else {
                        None
                    };
                    (
                        if imp.is_type {
                            "TypeOnlyNamedImport"
                        } else {
                            "NamedImport"
                        },
                        Some(ImportIdentifier {
                            name: imported_name.clone(),
                            alias,
                        }),
                    )
                }
                JsImportKind::Default => (
                    "DefaultImport",
                    Some(ImportIdentifier {
                        name: imp.local_name.clone(),
                        alias: None,
                    }),
                ),
                JsImportKind::Namespace => (
                    "NamespaceImport",
                    Some(ImportIdentifier {
                        name: imp.local_name.clone(),
                        alias: None,
                    }),
                ),
                JsImportKind::CjsRequire { imported_name } => {
                    let identifier_name = imported_name
                        .clone()
                        .unwrap_or_else(|| imp.local_name.clone());
                    let alias = (identifier_name != imp.local_name).then(|| imp.local_name.clone());

                    (
                        "CjsRequire",
                        Some(ImportIdentifier {
                            name: identifier_name,
                            alias,
                        }),
                    )
                }
            };

            let location = ImportedSymbolLocation {
                file_path: self.relative_path.clone(),
                start_byte: imp.range.byte_offset.0 as i64,
                end_byte: imp.range.byte_offset.1 as i64,
                start_line: imp.range.start.line as i32,
                end_line: imp.range.end.line as i32,
                start_col: imp.range.start.column as i32,
                end_col: imp.range.end.column as i32,
            };

            relationships.push(ConsolidatedRelationship {
                source_path: Some(path.clone()),
                target_path: Some(ArcIntern::new(self.relative_path.clone())),
                kind: RelationshipKind::FileToImportedSymbol,
                relationship_type: RelationshipType::FileImports,
                source_range: ArcIntern::new(Range::empty()),
                target_range: ArcIntern::new(location.range()),
                ..Default::default()
            });

            imported_symbols.push(ImportedSymbolNode::new(
                ImportType::Js(import_type_str),
                imp.specifier.clone(),
                identifier,
                location,
            ));
        }

        for call in &self.calls {
            let callee_range = match &call.callee {
                JsCallTarget::Direct { fqn, .. } => def_ranges_by_fqn.get(fqn.as_str()).copied(),
                JsCallTarget::ThisMethod { resolved_range, .. }
                | JsCallTarget::SuperMethod { resolved_range, .. } => *resolved_range,
                JsCallTarget::ImportedCall { .. } => continue,
            };

            let Some(target_range) = callee_range else {
                continue;
            };

            match &call.caller {
                JsCallSite::Definition { range, .. } => {
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(path.clone()),
                        target_path: Some(path.clone()),
                        kind: RelationshipKind::DefinitionToDefinition,
                        relationship_type: RelationshipType::Calls,
                        source_range: ArcIntern::new(call.call_range),
                        target_range: ArcIntern::new(target_range),
                        source_definition_range: Some(ArcIntern::new(*range)),
                        target_definition_range: Some(ArcIntern::new(target_range)),
                        ..Default::default()
                    });
                }
                JsCallSite::ModuleLevel => {
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(path.clone()),
                        target_path: Some(path.clone()),
                        kind: RelationshipKind::FileToDefinition,
                        relationship_type: RelationshipType::Calls,
                        source_range: ArcIntern::new(Range::empty()),
                        target_range: ArcIntern::new(target_range),
                        ..Default::default()
                    });
                }
            }
        }

        JsEmitted {
            definitions,
            imported_symbols,
            relationships,
        }
    }
}
