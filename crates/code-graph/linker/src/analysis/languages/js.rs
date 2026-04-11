use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
    ImportType, ImportedSymbolLocation, ImportedSymbolNode,
};
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc::allocator::Allocator;
use oxc::ast::AstKind;
use oxc::parser::Parser;
use oxc::semantic::{AstNodes, SemanticBuilder};
use oxc::span::{GetSpan, SourceType, Span};
use oxc::syntax::module_record::{ExportExportName, ImportImportName};
use oxc::syntax::scope::ScopeFlags;
use oxc::syntax::symbol::SymbolFlags;
use parser_core::utils::{Position, Range};
use std::collections::HashMap;

use super::js_types::{CjsExport, ExportedBinding, ImportedName, JsModuleInfo, OwnedImportEntry};

pub struct JsAnalyzer;

pub struct JsAnalysisResult {
    pub definitions: Vec<DefinitionNode>,
    pub imported_symbols: Vec<ImportedSymbolNode>,
    pub relationships: Vec<ConsolidatedRelationship>,
}

impl JsAnalyzer {
    pub fn analyze_file(
        source: &str,
        file_path: &str,
        relative_path: &str,
    ) -> Result<JsAnalysisResult, String> {
        let source_type = SourceType::from_path(file_path)
            .map_err(|_| format!("Unknown JS source type: {file_path}"))?;
        let allocator = Allocator::default();
        let parsed = Parser::new(&allocator, source, source_type).parse();

        if parsed.panicked {
            return Err(format!("OXC parser panicked on {file_path}"));
        }

        let semantic_ret = SemanticBuilder::new()
            .with_check_syntax_error(true)
            .build(&parsed.program);
        let semantic = semantic_ret.semantic;
        let scoping = semantic.scoping();
        let nodes = semantic.nodes();

        let path = ArcIntern::new(relative_path.to_string());
        let mut definitions = Vec::new();
        let mut imported_symbols = Vec::new();
        let mut relationships = Vec::new();

        // 1. Extract definitions from symbol table
        for symbol_id in scoping.symbol_ids() {
            let flags = scoping.symbol_flags(symbol_id);

            if flags.is_import() {
                continue;
            }

            let name = scoping.symbol_name(symbol_id);
            let span = scoping.symbol_span(symbol_id);
            let def_type =
                symbol_flags_to_def_type(flags, nodes, scoping.symbol_declaration(symbol_id));

            if def_type.is_empty() {
                continue;
            }

            let fqn = build_fqn(scoping, nodes, symbol_id);
            let is_exported = parsed.module_record.exported_bindings.contains_key(name);
            let _ = is_exported; // TODO: use when is_exported column is added

            let range = span_to_range(span, source);
            let def_node = DefinitionNode::new(
                FqnType::Js(fqn.clone()),
                DefinitionType::Js(def_type),
                range,
                path.clone(),
            );

            // Top-level definition: add file-to-definition relationship
            if !fqn.contains("::") {
                relationships.push(ConsolidatedRelationship {
                    source_path: Some(path.clone()),
                    target_path: Some(path.clone()),
                    kind: RelationshipKind::FileToDefinition,
                    relationship_type: RelationshipType::FileDefines,
                    source_range: ArcIntern::new(Range::empty()),
                    target_range: ArcIntern::new(range),
                    ..Default::default()
                });
            }

            definitions.push(def_node);
        }

        // 2. Extract imports from ModuleRecord
        for entry in &parsed.module_record.import_entries {
            let specifier = entry.module_request.name.to_string();
            let span = entry.module_request.span;

            let (import_type, identifier) = match &entry.import_name {
                ImportImportName::Name(n) => {
                    let imported = n.name.to_string();
                    let local = entry.local_name.name.to_string();
                    let alias = if imported != local {
                        Some(local.clone())
                    } else {
                        None
                    };
                    (
                        if entry.is_type {
                            "TypeOnlyNamedImport"
                        } else {
                            "NamedImport"
                        },
                        Some(ImportIdentifier {
                            name: imported,
                            alias,
                        }),
                    )
                }
                ImportImportName::Default(_) => (
                    "DefaultImport",
                    Some(ImportIdentifier {
                        name: entry.local_name.name.to_string(),
                        alias: None,
                    }),
                ),
                ImportImportName::NamespaceObject => (
                    "NamespaceImport",
                    Some(ImportIdentifier {
                        name: entry.local_name.name.to_string(),
                        alias: None,
                    }),
                ),
            };

            let range = span_to_range(span, source);
            let location = ImportedSymbolLocation {
                file_path: relative_path.to_string(),
                start_byte: range.byte_offset.0 as i64,
                end_byte: range.byte_offset.1 as i64,
                start_line: range.start.line as i32,
                end_line: range.end.line as i32,
                start_col: range.start.column as i32,
                end_col: range.end.column as i32,
            };

            let import_node = ImportedSymbolNode::new(
                ImportType::Js(import_type),
                specifier,
                identifier,
                location.clone(),
            );

            relationships.push(ConsolidatedRelationship {
                source_path: Some(path.clone()),
                target_path: Some(ArcIntern::new(location.file_path.clone())),
                kind: RelationshipKind::FileToImportedSymbol,
                relationship_type: RelationshipType::FileImports,
                source_range: ArcIntern::new(Range::empty()),
                target_range: ArcIntern::new(location.range()),
                ..Default::default()
            });

            imported_symbols.push(import_node);
        }

        // 3. Extract call edges from resolved references
        for symbol_id in scoping.symbol_ids() {
            let _callee_fqn = build_fqn(scoping, nodes, symbol_id);

            for ref_id in scoping.get_resolved_reference_ids(symbol_id) {
                let reference = scoping.get_reference(*ref_id);
                if !reference.flags().is_read() {
                    continue;
                }

                let ref_node_id = reference.node_id();
                let is_call = matches!(
                    nodes.parent_kind(ref_node_id),
                    AstKind::CallExpression(_)
                        | AstKind::NewExpression(_)
                        | AstKind::TaggedTemplateExpression(_)
                );

                if !is_call {
                    continue;
                }

                let caller_scope = reference.scope_id();
                let caller_range = find_enclosing_definition_range(scoping, nodes, caller_scope);
                let call_site_span = nodes.get_node(ref_node_id).span();
                let call_site_range = span_to_range(call_site_span, source);
                let callee_span = scoping.symbol_span(symbol_id);
                let callee_range = span_to_range(callee_span, source);

                if let Some(caller_def_range) = caller_range {
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(path.clone()),
                        target_path: Some(path.clone()),
                        kind: RelationshipKind::DefinitionToDefinition,
                        relationship_type: RelationshipType::Calls,
                        source_range: ArcIntern::new(call_site_range),
                        target_range: ArcIntern::new(callee_range),
                        source_definition_range: Some(ArcIntern::new(caller_def_range)),
                        target_definition_range: Some(ArcIntern::new(callee_range)),
                        ..Default::default()
                    });
                } else {
                    // Module-level call (no enclosing definition)
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(path.clone()),
                        target_path: Some(path.clone()),
                        kind: RelationshipKind::FileToDefinition,
                        relationship_type: RelationshipType::Calls,
                        source_range: ArcIntern::new(Range::empty()),
                        target_range: ArcIntern::new(callee_range),
                        ..Default::default()
                    });
                }
            }
        }

        // 4. Build definition-to-definition parent-child relationships from FQN hierarchy
        let def_fqns: Vec<(String, Range)> = definitions
            .iter()
            .map(|d| (d.fqn.to_string(), d.range))
            .collect();

        for (child_fqn, child_range) in &def_fqns {
            if let Some(parent_fqn) = child_fqn.rsplit_once("::").map(|(p, _)| p.to_string())
                && let Some((_, parent_range)) = def_fqns.iter().find(|(f, _)| *f == parent_fqn)
            {
                relationships.push(ConsolidatedRelationship {
                    source_path: Some(path.clone()),
                    target_path: Some(path.clone()),
                    kind: RelationshipKind::DefinitionToDefinition,
                    relationship_type: RelationshipType::ClassToMethod, // simplified; refine later
                    source_range: ArcIntern::new(*parent_range),
                    target_range: ArcIntern::new(*child_range),
                    ..Default::default()
                });
            }
        }

        Ok(JsAnalysisResult {
            definitions,
            imported_symbols,
            relationships,
        })
    }
}

/// Map OXC SymbolFlags to a definition type string.
fn symbol_flags_to_def_type(
    flags: SymbolFlags,
    nodes: &AstNodes,
    decl_node_id: oxc::semantic::NodeId,
) -> &'static str {
    if flags.is_class() {
        return "Class";
    }
    if flags.is_function() {
        if matches!(
            nodes.parent_kind(decl_node_id),
            AstKind::MethodDefinition(_)
        ) {
            return "Method";
        }
        return "Function";
    }
    if flags.is_interface() {
        return "Interface";
    }
    if flags.is_type_alias() {
        return "TypeAlias";
    }
    if flags.is_enum() {
        return "Enum";
    }
    if flags.intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule) {
        return "Namespace";
    }
    if flags.is_enum_member() {
        return "EnumMember";
    }
    if flags.intersects(SymbolFlags::CatchVariable) {
        return "";
    }
    if flags.is_variable() {
        // Skip function/method parameters: parent node is FormalParameter
        if matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_)) {
            return "";
        }
        return "Variable";
    }
    ""
}

/// Build a fully qualified name by walking scope ancestors.
fn build_fqn(
    scoping: &oxc::semantic::Scoping,
    _nodes: &AstNodes,
    symbol_id: oxc::syntax::symbol::SymbolId,
) -> String {
    let name = scoping.symbol_name(symbol_id).to_string();
    let scope_id = scoping.symbol_scope_id(symbol_id);

    let mut parts = vec![name];
    for ancestor_scope in scoping.scope_ancestors(scope_id).skip(1) {
        let scope_flags = scoping.scope_flags(ancestor_scope);
        if scope_flags.contains(ScopeFlags::Top) {
            break;
        }

        // Find the name of the symbol that created this scope
        if let Some(parent_scope) = scoping.scope_parent_id(ancestor_scope) {
            for (binding_name, &binding_symbol) in scoping.get_bindings(parent_scope) {
                if scoping.symbol_scope_id(binding_symbol) == ancestor_scope
                    || scoping.symbol_declaration(binding_symbol)
                        == scoping.get_node_id(ancestor_scope)
                {
                    // Check if this symbol's declaration created the ancestor scope
                    let sym_flags = scoping.symbol_flags(binding_symbol);
                    if sym_flags.is_function()
                        || sym_flags.is_class()
                        || sym_flags
                            .intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule)
                    {
                        parts.push(binding_name.to_string());
                        break;
                    }
                }
            }
        }
    }

    parts.reverse();
    parts.join("::")
}

/// Convert OXC Span to parser_core Range.
fn span_to_range(span: Span, source: &str) -> Range {
    let start_offset = span.start as usize;
    let end_offset = span.end as usize;

    let (start_line, start_col) = offset_to_line_col(source, start_offset);
    let (end_line, end_col) = offset_to_line_col(source, end_offset);

    Range::new(
        Position::new(start_line, start_col),
        Position::new(end_line, end_col),
        (start_offset, end_offset),
    )
}

fn offset_to_line_col(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let mut line = 0;
    let mut col = 0;
    for (i, ch) in source.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
    }
    (line, col)
}

/// Find the range of the enclosing function/method/class definition for a given scope.
fn find_enclosing_definition_range(
    scoping: &oxc::semantic::Scoping,
    _nodes: &AstNodes,
    scope_id: oxc::syntax::scope::ScopeId,
) -> Option<Range> {
    let mut current = Some(scope_id);
    while let Some(sid) = current {
        let scope_flags = scoping.scope_flags(sid);
        if scope_flags.contains(ScopeFlags::Top) {
            return None;
        }

        // Check if any symbol declares this scope
        if let Some(parent_scope) = scoping.scope_parent_id(sid) {
            for (_, &sym_id) in scoping.get_bindings(parent_scope) {
                let sym_flags = scoping.symbol_flags(sym_id);
                if (sym_flags.is_function() || sym_flags.is_class())
                    && (scoping.symbol_scope_id(sym_id) == sid
                        || scoping.symbol_declaration(sym_id) == scoping.get_node_id(sid))
                {
                    let span = scoping.symbol_span(sym_id);
                    // We need source text for proper conversion but we don't have it here.
                    // Use byte offsets directly.
                    return Some(Range::new(
                        Position::new(0, 0),
                        Position::new(0, 0),
                        (span.start as usize, span.end as usize),
                    ));
                }
            }
        }

        current = scoping.scope_parent_id(sid);
    }
    None
}

/// Extract owned module info for Pass 2 cross-file resolution.
/// Called at the end of Pass 1 while the OXC allocator is still alive.
pub fn extract_module_info(
    source: &str,
    file_path: &str,
    analysis: &JsAnalysisResult,
) -> Result<JsModuleInfo, String> {
    let source_type = SourceType::from_path(file_path)
        .map_err(|_| format!("Unknown source type: {file_path}"))?;
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, source, source_type).parse();
    let semantic_ret = SemanticBuilder::new().build(&parsed.program).semantic;
    let scoping = semantic_ret.scoping();
    let nodes = semantic_ret.nodes();

    let mut exports = HashMap::new();
    let mut imports = Vec::new();
    let mut star_export_sources = Vec::new();

    // Extract exports from ModuleRecord
    for entry in &parsed.module_record.local_export_entries {
        let export_name = match &entry.export_name {
            ExportExportName::Name(n) => n.name.to_string(),
            ExportExportName::Default(_) => "default".to_string(),
            ExportExportName::Null => continue,
        };
        let local_fqn = match &entry.local_name {
            oxc::syntax::module_record::ExportLocalName::Name(n) => n.name.to_string(),
            oxc::syntax::module_record::ExportLocalName::Default(_) => "default".to_string(),
            oxc::syntax::module_record::ExportLocalName::Null => continue,
        };
        let is_default = matches!(entry.export_name, ExportExportName::Default(_));
        exports.insert(
            export_name,
            ExportedBinding {
                local_fqn,
                range: span_to_range(entry.span, source),
                is_type: entry.is_type,
                is_default,
            },
        );
    }

    // Re-exports (indirect)
    for entry in &parsed.module_record.indirect_export_entries {
        if let Some(ref module_request) = entry.module_request {
            let export_name = match &entry.export_name {
                ExportExportName::Name(n) => n.name.to_string(),
                ExportExportName::Default(_) => "default".to_string(),
                ExportExportName::Null => continue,
            };
            exports.insert(
                export_name,
                ExportedBinding {
                    local_fqn: format!("reexport:{}", module_request.name),
                    range: span_to_range(entry.span, source),
                    is_type: entry.is_type,
                    is_default: false,
                },
            );
        }
    }

    // Star re-exports
    for entry in &parsed.module_record.star_export_entries {
        if let Some(ref module_request) = entry.module_request {
            star_export_sources.push(module_request.name.to_string());
        }
    }

    // Extract owned import entries
    for entry in &parsed.module_record.import_entries {
        let specifier = entry.module_request.name.to_string();
        let local_name = entry.local_name.name.to_string();
        let imported_name = match &entry.import_name {
            ImportImportName::Name(n) => ImportedName::Named(n.name.to_string()),
            ImportImportName::Default(_) => ImportedName::Default,
            ImportImportName::NamespaceObject => ImportedName::Namespace,
        };
        imports.push(OwnedImportEntry {
            specifier,
            imported_name,
            local_name,
            is_type: entry.is_type,
            range: span_to_range(entry.module_request.span, source),
        });
    }

    // Extract CJS exports
    let cjs_exports = extract_cjs_exports(nodes, scoping, source);

    // Build definition FQN map from analysis result
    let definition_fqns = analysis
        .definitions
        .iter()
        .map(|d| (d.fqn.to_string(), d.range))
        .collect();

    Ok(JsModuleInfo {
        exports,
        imports,
        star_export_sources,
        cjs_exports,
        has_module_syntax: parsed.module_record.has_module_syntax,
        definition_fqns,
    })
}

/// Scan for CommonJS export patterns: `module.exports = ...` and `exports.foo = ...`
fn extract_cjs_exports(
    nodes: &AstNodes,
    _scoping: &oxc::semantic::Scoping,
    source: &str,
) -> Vec<CjsExport> {
    use oxc::ast::ast::AssignmentTarget;

    let mut exports = Vec::new();

    for node in nodes.iter() {
        if let AstKind::AssignmentExpression(assign) = nodes.kind(node.id()) {
            match &assign.left {
                AssignmentTarget::AssignmentTargetIdentifier(_) => {}
                _ => {
                    // Check for module.exports = ... or exports.foo = ...
                    if let AssignmentTarget::StaticMemberExpression(member) = &assign.left {
                        let prop_name = member.property.name.as_str();

                        // Check if object is `module` and property is `exports`
                        if prop_name == "exports"
                            && let oxc::ast::ast::Expression::Identifier(ident) = &member.object
                            && ident.name == "module"
                        {
                            exports.push(CjsExport::Default {
                                range: span_to_range(assign.span, source),
                            });
                            continue;
                        }

                        // Check if object is `exports` (exports.foo = ...)
                        if let oxc::ast::ast::Expression::Identifier(ident) = &member.object
                            && ident.name == "exports"
                        {
                            exports.push(CjsExport::Named {
                                name: prop_name.to_string(),
                                range: span_to_range(assign.span, source),
                            });
                        }

                        // Check for module.exports.foo = ...
                        if let oxc::ast::ast::Expression::StaticMemberExpression(inner) =
                            &member.object
                            && inner.property.name == "exports"
                            && let oxc::ast::ast::Expression::Identifier(ident) = &inner.object
                            && ident.name == "module"
                        {
                            exports.push(CjsExport::Named {
                                name: prop_name.to_string(),
                                range: span_to_range(assign.span, source),
                            });
                        }
                    }
                }
            }
        }
    }

    exports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_simple_function() {
        let source = r#"
function greet(name: string): string {
    return "Hello, " + name;
}
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        let func_defs: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type.as_str() == "Function")
            .collect();
        assert_eq!(func_defs.len(), 1);
        assert_eq!(func_defs[0].fqn.name(), "greet");
    }

    #[test]
    fn test_analyze_class() {
        let source = r#"
class Calculator {
    add(a: number, b: number): number {
        return a + b;
    }
}
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        let calc = result
            .definitions
            .iter()
            .find(|d| d.fqn.name() == "Calculator");
        assert!(calc.is_some(), "Should find Calculator class");
        assert_eq!(calc.unwrap().definition_type.as_str(), "Class");
    }

    #[test]
    fn test_analyze_imports() {
        let source = r#"
import { useState } from 'react';
import type { FC } from 'react';
import React from 'react';
import * as path from 'path';
"#;
        let result = JsAnalyzer::analyze_file(source, "test.tsx", "test.tsx").unwrap();
        assert_eq!(result.imported_symbols.len(), 4);

        let types: Vec<&str> = result
            .imported_symbols
            .iter()
            .map(|i| i.import_type.as_str())
            .collect();
        assert!(types.contains(&"NamedImport"));
        assert!(types.contains(&"TypeOnlyNamedImport"));
        assert!(types.contains(&"DefaultImport"));
        assert!(types.contains(&"NamespaceImport"));
    }

    #[test]
    fn test_analyze_call_edges() {
        let source = r#"
function foo() { return 1; }
function bar() { return foo(); }
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        let calls: Vec<_> = result
            .relationships
            .iter()
            .filter(|r| r.relationship_type == RelationshipType::Calls)
            .collect();
        assert!(!calls.is_empty(), "Should have at least one CALLS edge");
    }

    #[test]
    fn test_analyze_jsx() {
        let source = r#"
import React from 'react';
function App() {
    return <div>Hello</div>;
}
"#;
        let result = JsAnalyzer::analyze_file(source, "test.tsx", "test.tsx").unwrap();
        assert!(!result.definitions.is_empty());
        let app = result.definitions.iter().find(|d| d.fqn.name() == "App");
        assert!(app.is_some());
    }

    #[test]
    fn test_analyze_interface_and_enum() {
        let source = r#"
interface User {
    name: string;
    age: number;
}
enum Color {
    Red,
    Green,
    Blue,
}
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        let iface = result.definitions.iter().find(|d| d.fqn.name() == "User");
        assert!(iface.is_some());
        assert_eq!(iface.unwrap().definition_type.as_str(), "Interface");

        let color = result.definitions.iter().find(|d| d.fqn.name() == "Color");
        assert!(color.is_some());
        assert_eq!(color.unwrap().definition_type.as_str(), "Enum");
    }

    #[test]
    fn test_analyze_arrow_function() {
        let source = r#"
const greet = (name: string) => `Hello, ${name}`;
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        let greet = result.definitions.iter().find(|d| d.fqn.name() == "greet");
        assert!(greet.is_some());
    }

    #[test]
    fn test_analyze_exports() {
        let source = r#"
export function foo() {}
export default class Bar {}
const baz = 1;
"#;
        let result = JsAnalyzer::analyze_file(source, "test.ts", "test.ts").unwrap();
        assert!(result.definitions.len() >= 2);
    }
}
