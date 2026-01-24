use super::super::types::TypeScriptSwcAst;
use crate::imports::ImportIdentifier;
use crate::typescript::types::{
    TypeScriptFqn, TypeScriptImportType, TypeScriptImportedSymbolInfo, TypeScriptScopeStack,
};
use crate::utils::{Position, Range};
use smallvec::smallvec;
use swc_atoms::Wtf8Atom;
use swc_common::Spanned;
use swc_common::{SourceMap, Span, sync::Lrc};
use swc_ecma_ast::{
    CallExpr, Callee, Expr, ImportDecl, ImportDefaultSpecifier, ImportNamedSpecifier,
    ImportSpecifier, ImportStarAsSpecifier, Lit, ModuleDecl, VarDecl, VarDeclarator,
};
use swc_ecma_visit::{Visit, VisitWith};

/// Extracts imports from an SWC Module AST
pub fn extract_swc_imports(ast: &TypeScriptSwcAst) -> Vec<TypeScriptImportedSymbolInfo> {
    let mut extractor = SwcImportExtractor::new(ast.source_map.clone());
    ast.module.visit_with(&mut extractor);
    extractor.imports
}

/// Visitor that extracts imports from SWC AST nodes
struct SwcImportExtractor {
    imports: Vec<TypeScriptImportedSymbolInfo>,
    scope_stack: TypeScriptScopeStack,
    source_map: Lrc<SourceMap>,
}

impl SwcImportExtractor {
    fn new(source_map: Lrc<SourceMap>) -> Self {
        Self {
            source_map,
            imports: vec![],
            scope_stack: smallvec![],
        }
    }

    fn span_to_range(&self, span: Span) -> Range {
        let lo = self.source_map.lookup_char_pos_adj(span.lo);
        let hi = self.source_map.lookup_char_pos_adj(span.hi);
        Range::new(
            // SWC uses 1-based line numbers, but the codebase expects 0-based indexing
            Position::new(lo.line.saturating_sub(1), lo.col.0),
            Position::new(hi.line.saturating_sub(1), hi.col.0),
            (span.lo.0 as usize, span.hi.0 as usize),
        )
    }

    fn create_import_info(
        &self,
        import_type: TypeScriptImportType,
        import_path: String,
        identifier: Option<ImportIdentifier>,
        span: Span,
    ) -> TypeScriptImportedSymbolInfo {
        let range = self.span_to_range(span);
        let scope = if self.scope_stack.is_empty() {
            None
        } else {
            Some(TypeScriptFqn::new(self.scope_stack.clone()))
        };

        TypeScriptImportedSymbolInfo {
            import_type,
            import_path,
            identifier,
            range,
            scope,
        }
    }

    fn clean_source_string(&self, source: &Wtf8Atom) -> String {
        source
            .to_string_lossy()
            .trim_start_matches(['\'', '"'])
            .trim_end_matches(['\'', '"'])
            .to_string()
    }

    fn extract_string_literal(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Lit(Lit::Str(str_lit)) => Some(self.clean_source_string(&str_lit.value)),
            _ => None,
        }
    }

    fn is_require_or_import_call(&self, call: &CallExpr) -> bool {
        match &call.callee {
            Callee::Expr(expr) => match expr.as_ref() {
                Expr::Ident(ident) => {
                    matches!(ident.sym.as_ref(), "require" | "import")
                }
                _ => false,
            },
            Callee::Import(_) => true, // Handle dynamic import() calls
            _ => false,
        }
    }

    fn extract_call_source(&self, call: &CallExpr) -> Option<String> {
        if self.is_require_or_import_call(call)
            && !call.args.is_empty()
            && let Some(first_arg) = call.args.first()
        {
            return self.extract_string_literal(&first_arg.expr);
        }
        None
    }

    fn handle_import_decl(&mut self, import: &ImportDecl) {
        let source_path = self.clean_source_string(&import.src.value);

        // Handle side effect imports: import 'module'
        if import.specifiers.is_empty() {
            self.imports.push(self.create_import_info(
                TypeScriptImportType::SideEffectImport,
                source_path,
                None,
                import.span(),
            ));
            return;
        }

        // Handle each specifier
        for specifier in &import.specifiers {
            match specifier {
                ImportSpecifier::Named(named) => {
                    self.handle_named_import_specifier(named, &source_path);
                }
                ImportSpecifier::Default(default) => {
                    self.handle_default_import_specifier(default, &source_path);
                }
                ImportSpecifier::Namespace(namespace) => {
                    self.handle_namespace_import_specifier(namespace, &source_path);
                }
            }
        }
    }

    fn handle_named_import_specifier(&mut self, named: &ImportNamedSpecifier, source_path: &str) {
        let import_name = match &named.imported {
            Some(name) => match name {
                swc_ecma_ast::ModuleExportName::Ident(ident) => ident.sym.to_string(),
                swc_ecma_ast::ModuleExportName::Str(str_lit) => {
                    str_lit.value.to_string_lossy().to_string()
                }
            },
            None => named.local.sym.to_string(),
        };

        let local_name = named.local.sym.to_string();

        let (import_type, identifier) = if import_name != local_name {
            // Aliased import: import { foo as bar } from 'module'
            (
                TypeScriptImportType::AliasedImport,
                ImportIdentifier {
                    name: import_name,
                    alias: Some(local_name),
                },
            )
        } else {
            // Named import: import { foo } from 'module'
            (
                TypeScriptImportType::NamedImport,
                ImportIdentifier {
                    name: import_name,
                    alias: None,
                },
            )
        };

        self.imports.push(self.create_import_info(
            import_type,
            source_path.to_string(),
            Some(identifier),
            named.span(),
        ));
    }

    fn handle_default_import_specifier(
        &mut self,
        default: &ImportDefaultSpecifier,
        source_path: &str,
    ) {
        let identifier = ImportIdentifier {
            name: default.local.sym.to_string(),
            alias: None,
        };

        self.imports.push(self.create_import_info(
            TypeScriptImportType::DefaultImport,
            source_path.to_string(),
            Some(identifier),
            default.span(),
        ));
    }

    fn handle_namespace_import_specifier(
        &mut self,
        namespace: &ImportStarAsSpecifier,
        source_path: &str,
    ) {
        let identifier = ImportIdentifier {
            name: "*".to_string(),
            alias: Some(namespace.local.sym.to_string()),
        };

        self.imports.push(self.create_import_info(
            TypeScriptImportType::NamespaceImport,
            source_path.to_string(),
            Some(identifier),
            namespace.span(),
        ));
    }

    fn handle_var_declarator(&mut self, declarator: &VarDeclarator) {
        if let Some(init) = &declarator.init {
            match init.as_ref() {
                // Handle: const foo = require('module') or const foo = import('module')
                Expr::Call(call) => {
                    if let Some(source_path) = self.extract_call_source(call) {
                        self.handle_require_or_import_assignment(declarator, &source_path, call);
                    }
                }
                // Handle: const foo = await require('module') or const foo = await import('module')
                Expr::Await(await_expr) => {
                    if let Expr::Call(call) = await_expr.arg.as_ref()
                        && let Some(source_path) = self.extract_call_source(call)
                    {
                        self.handle_require_or_import_assignment(declarator, &source_path, call);
                    }
                }
                _ => {}
            }
        }
    }

    fn handle_require_or_import_assignment(
        &mut self,
        declarator: &VarDeclarator,
        source_path: &str,
        _call: &CallExpr,
    ) {
        match &declarator.name {
            swc_ecma_ast::Pat::Ident(ident) => {
                // Simple assignment: const foo = require('module')
                let identifier = ImportIdentifier {
                    name: ident.id.sym.to_string(),
                    alias: None,
                };
                self.imports.push(self.create_import_info(
                    TypeScriptImportType::SvaRequireOrImport,
                    source_path.to_string(),
                    Some(identifier),
                    declarator.span(),
                ));
            }
            swc_ecma_ast::Pat::Object(obj_pat) => {
                // Destructuring: const { foo, bar: baz } = require('module')
                for prop in &obj_pat.props {
                    match prop {
                        swc_ecma_ast::ObjectPatProp::KeyValue(kv) => {
                            // Aliased destructuring: { foo: bar }
                            if let (
                                swc_ecma_ast::PropName::Ident(key),
                                swc_ecma_ast::Pat::Ident(value),
                            ) = (&kv.key, kv.value.as_ref())
                            {
                                let identifier = ImportIdentifier {
                                    name: key.sym.to_string(),
                                    alias: Some(value.id.sym.to_string()),
                                };
                                self.imports.push(self.create_import_info(
                                    TypeScriptImportType::AliasedImportOrRequire,
                                    source_path.to_string(),
                                    Some(identifier),
                                    kv.span(),
                                ));
                            }
                        }
                        swc_ecma_ast::ObjectPatProp::Assign(assign) => {
                            // Simple destructuring: { foo }
                            let identifier = ImportIdentifier {
                                name: assign.key.sym.to_string(),
                                alias: None,
                            };
                            self.imports.push(self.create_import_info(
                                TypeScriptImportType::DestructuredImportOrRequire,
                                source_path.to_string(),
                                Some(identifier),
                                assign.span(),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_standalone_call(&mut self, call: &CallExpr) {
        // Handle standalone require/import calls: require('module'); import('module');
        if let Some(source_path) = self.extract_call_source(call) {
            self.imports.push(self.create_import_info(
                TypeScriptImportType::SideEffectImportOrRequire,
                source_path,
                None,
                call.span(),
            ));
        }
    }
}

impl Visit for SwcImportExtractor {
    fn visit_module_decl(&mut self, decl: &ModuleDecl) {
        match decl {
            ModuleDecl::Import(import) => {
                self.handle_import_decl(import);
            }
            ModuleDecl::ExportAll(_) => {
                // Handle re-exports: export * from 'module'
                // TODO: Add support for re-exports if needed
            }
            ModuleDecl::ExportNamed(named_export) => {
                // Handle re-exports: export { foo } from 'module'
                if let Some(_src) = &named_export.src {
                    // TODO: Add support for named re-exports if needed
                }
            }
            _ => {}
        }

        // Continue visiting children
        decl.visit_children_with(self);
    }

    fn visit_var_decl(&mut self, var_decl: &VarDecl) {
        for declarator in &var_decl.decls {
            self.handle_var_declarator(declarator);
        }

        // Don't visit children as we've already processed all declarators
    }

    fn visit_expr_stmt(&mut self, stmt: &swc_ecma_ast::ExprStmt) {
        // Handle expression statements that might contain standalone calls
        match stmt.expr.as_ref() {
            Expr::Call(call) => {
                self.handle_standalone_call(call);
            }
            Expr::Await(await_expr) => {
                if let Expr::Call(call) = await_expr.arg.as_ref() {
                    self.handle_standalone_call(call);
                }
            }
            _ => {}
        }

        // Continue visiting children to handle nested structures
        stmt.visit_children_with(self);
    }

    // Handle TypeScript import assignment: import foo = require('module')
    fn visit_ts_import_equals_decl(&mut self, import_eq: &swc_ecma_ast::TsImportEqualsDecl) {
        if let swc_ecma_ast::TsModuleRef::TsExternalModuleRef(ext_ref) = &import_eq.module_ref {
            let source_path = self.clean_source_string(&ext_ref.expr.value);
            let identifier = ImportIdentifier {
                name: import_eq.id.sym.to_string(),
                alias: None,
            };

            self.imports.push(self.create_import_info(
                TypeScriptImportType::ImportAndRequire,
                source_path,
                Some(identifier),
                import_eq.span(),
            ));
        }

        // Continue visiting children
        import_eq.visit_children_with(self);
    }
}

#[cfg(test)]
mod swc_import_tests {
    use super::*;
    use crate::typescript::parser::parse_ast;
    use rustc_hash::FxHashSet;

    fn get_imports(code: &str) -> Vec<TypeScriptImportedSymbolInfo> {
        let ast = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        extract_swc_imports(&ast).into_iter().collect()
    }

    fn test_import_extraction(
        code: &str,
        expected_imported_symbols: Vec<(TypeScriptImportType, &str, Option<ImportIdentifier>)>,
        description: &str,
    ) {
        println!("\n=== Testing: {description} ===");
        println!("Code snippet:\n{code}");

        let imports = get_imports(code);

        for symbol in &imports {
            println!("symbol: {symbol:?}");
        }

        assert_eq!(
            imports.len(),
            expected_imported_symbols.len(),
            "Expected {} imported symbols, found {}",
            expected_imported_symbols.len(),
            imports.len()
        );

        let ranges = FxHashSet::from_iter(imports.iter().map(|i| i.range.byte_offset));
        assert_eq!(
            ranges.len(),
            imports.len(),
            "Imported symbols have duplicate ranges"
        );

        println!("Found {} imported symbols:", imports.len());
        for (expected_type, expected_path, expected_identifier) in expected_imported_symbols {
            let _matching_symbol = imports
                .iter()
                .find(|i| {
                    i.import_type == expected_type
                        && i.import_path == expected_path
                        && i.identifier == expected_identifier.clone()
                })
                .unwrap_or_else(|| {
                    panic!(
                        "Could not find: type={:?}, path={}, name={:?}, alias={:?}",
                        expected_type,
                        expected_path,
                        expected_identifier.as_ref().unwrap().name,
                        expected_identifier.as_ref().unwrap().alias
                    )
                });

            if let Some(ident) = &expected_identifier {
                println!(
                    "Found: type={:?}, path={}, name={:?}, alias={:?}",
                    expected_type, expected_path, ident.name, ident.alias
                );
            } else {
                println!(
                    "Found: type={:?}, path={}, name={:?}, alias={:?}",
                    expected_type, expected_path, "None", "None"
                );
            }
        }
        println!("✅ All assertions passed for: {description}\n");
    }

    #[test]
    fn test_import_extraction_typescript_code() -> crate::Result<()> {
        let path = "src/typescript/fixtures/typescript/imports.ts";
        let code = std::fs::read_to_string(path)?;
        let imports = get_imports(&code);
        for symbol in &imports {
            println!(
                "Found: type={:?}, path={}, name={:?}, alias={:?}",
                symbol.import_type, symbol.import_path, "None", "None"
            );
            let range = symbol.range;
            println!("Imported symbol range: {range:?}");
        }

        Ok(())
    }

    #[test]
    fn test_mixed_imports() -> crate::Result<()> {
        let code = r#"
        import React, { useState, useEffect, banana as apple } from 'react';
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::DefaultImport,
                "react",
                Some(ImportIdentifier {
                    name: "React".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "useState".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "useEffect".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::AliasedImport,
                "react",
                Some(ImportIdentifier {
                    name: "banana".to_string(),
                    alias: Some("apple".to_string()),
                }),
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Mixed imports");
        Ok(())
    }

    #[test]
    fn test_sva_require_or_import() -> crate::Result<()> {
        let code = r#"
        const VAR_NAME_1 = await require("hello1");
        const VAR_NAME_2 = require("hello2");
        const VAR_NAME_3 = import("hello3");
        const VAR_NAME_4 = await import("hello4");
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::SvaRequireOrImport,
                "hello1",
                Some(ImportIdentifier {
                    name: "VAR_NAME_1".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::SvaRequireOrImport,
                "hello2",
                Some(ImportIdentifier {
                    name: "VAR_NAME_2".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::SvaRequireOrImport,
                "hello3",
                Some(ImportIdentifier {
                    name: "VAR_NAME_3".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::SvaRequireOrImport,
                "hello4",
                Some(ImportIdentifier {
                    name: "VAR_NAME_4".to_string(),
                    alias: None,
                }),
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "SvaRequireOrImport");
        Ok(())
    }

    #[test]
    fn test_import_with_assignment() -> crate::Result<()> {
        let code = r#"
        import express = require('express');
        "#;
        let expected_imported_symbols = vec![(
            TypeScriptImportType::ImportAndRequire,
            "express",
            Some(ImportIdentifier {
                name: "express".to_string(),
                alias: None,
            }),
        )];
        test_import_extraction(code, expected_imported_symbols, "Import with assignment");
        Ok(())
    }

    #[test]
    fn test_advanced_import_or_require() -> crate::Result<()> {
        // const { NAME: ALIAS } = opt<await> either(require('SOURCE'), import('SOURCE'))
        // const { NAME } = opt<await> either(require('SOURCE'), import('SOURCE'))
        let code = r#"
        const { VAR_NAME_1: ALIAS_1 } = await require("hello1");
        const { VAR_NAME_2: ALIAS_2 } = require("hello2");
        const { VAR_NAME_3: ALIAS_3 } = import("hello3");
        const { VAR_NAME_4: ALIAS_4 } = await import("hello4");
        const { VAR_NAME_5 } = await require("hello5");
        const { VAR_NAME_6 } = require("hello6");
        const { VAR_NAME_7 } = import("hello7");
        const { VAR_NAME_8 } = await import("hello8");
        const { VAR_NAME_9: ALIAS_9, VAR_NAME_10: ALIAS_10 } = await require("hello9");
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello1",
                Some(ImportIdentifier {
                    name: "VAR_NAME_1".to_string(),
                    alias: Some("ALIAS_1".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello2",
                Some(ImportIdentifier {
                    name: "VAR_NAME_2".to_string(),
                    alias: Some("ALIAS_2".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello3",
                Some(ImportIdentifier {
                    name: "VAR_NAME_3".to_string(),
                    alias: Some("ALIAS_3".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello4",
                Some(ImportIdentifier {
                    name: "VAR_NAME_4".to_string(),
                    alias: Some("ALIAS_4".to_string()),
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "hello5",
                Some(ImportIdentifier {
                    name: "VAR_NAME_5".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "hello6",
                Some(ImportIdentifier {
                    name: "VAR_NAME_6".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "hello7",
                Some(ImportIdentifier {
                    name: "VAR_NAME_7".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "hello8",
                Some(ImportIdentifier {
                    name: "VAR_NAME_8".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello9",
                Some(ImportIdentifier {
                    name: "VAR_NAME_9".to_string(),
                    alias: Some("ALIAS_9".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "hello9",
                Some(ImportIdentifier {
                    name: "VAR_NAME_10".to_string(),
                    alias: Some("ALIAS_10".to_string()),
                }),
            ),
        ];
        test_import_extraction(
            code,
            expected_imported_symbols,
            "Advanced import or require",
        );
        Ok(())
    }

    #[test]
    fn test_dynamic_imports() -> crate::Result<()> {
        let code = r#"
        await import('reflect-metadata-3');
        import('reflect-metadata-4');
        await require('reflect-metadata-5');
        require('reflect-metadata-6');
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::SideEffectImportOrRequire,
                "reflect-metadata-3",
                None,
            ),
            (
                TypeScriptImportType::SideEffectImportOrRequire,
                "reflect-metadata-4",
                None,
            ),
            (
                TypeScriptImportType::SideEffectImportOrRequire,
                "reflect-metadata-5",
                None,
            ),
            (
                TypeScriptImportType::SideEffectImportOrRequire,
                "reflect-metadata-6",
                None,
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Dynamic imports");
        Ok(())
    }

    #[test]
    fn test_require_imports_advanced() -> crate::Result<()> {
        let code = r#"
        const APP = require('SOURCE_1');
        const { BigApple } = require('SOURCE_2');
        const { MyApp : MyAppAlias } = require('SOURCE_3');
        const { MyApp : MyAppAlias, BigApple } = require('SOURCE_4');
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::SvaRequireOrImport,
                "SOURCE_1",
                Some(ImportIdentifier {
                    name: "APP".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "SOURCE_2",
                Some(ImportIdentifier {
                    name: "BigApple".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "SOURCE_3",
                Some(ImportIdentifier {
                    name: "MyApp".to_string(),
                    alias: Some("MyAppAlias".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImportOrRequire,
                "SOURCE_4",
                Some(ImportIdentifier {
                    name: "MyApp".to_string(),
                    alias: Some("MyAppAlias".to_string()),
                }),
            ),
            (
                TypeScriptImportType::DestructuredImportOrRequire,
                "SOURCE_4",
                Some(ImportIdentifier {
                    name: "BigApple".to_string(),
                    alias: None,
                }),
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Require imports");
        Ok(())
    }

    #[test]
    fn test_side_effect_imports_all() -> crate::Result<()> {
        let code = r#"
        import 'reflect-metadata';
        require('reflect-metadata-2');
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::SideEffectImport,
                "reflect-metadata",
                None,
            ),
            (
                TypeScriptImportType::SideEffectImportOrRequire,
                "reflect-metadata-2",
                None,
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Side effect imports");
        Ok(())
    }

    #[test]
    fn test_namespace_imports() {
        let code = r#"
        import * as React from 'react';
        import * as fs from 'fs';
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::NamespaceImport,
                "react",
                Some(ImportIdentifier {
                    name: "*".to_string(),
                    alias: Some("React".to_string()),
                }),
            ),
            (
                TypeScriptImportType::NamespaceImport,
                "fs",
                Some(ImportIdentifier {
                    name: "*".to_string(),
                    alias: Some("fs".to_string()),
                }),
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Namespace imports");
    }

    #[test]
    fn test_type_only_imports() {
        let code = r#"
        import type { FC } from 'react';
        import type React from 'react';
        import { type FC2 } from 'react';
        import { type FC3, type FC4 } from 'react';
        import type { FC5, FC6 } from 'react';
        import type { FC7 as FC8 } from 'react';
        import { type FC9 as FC10 } from 'react';
        "#;
        let expected_imported_symbols = vec![
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::DefaultImport,
                "react",
                Some(ImportIdentifier {
                    name: "React".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC2".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC3".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC4".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC5".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::NamedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC6".to_string(),
                    alias: None,
                }),
            ),
            (
                TypeScriptImportType::AliasedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC7".to_string(),
                    alias: Some("FC8".to_string()),
                }),
            ),
            (
                TypeScriptImportType::AliasedImport,
                "react",
                Some(ImportIdentifier {
                    name: "FC9".to_string(),
                    alias: Some("FC10".to_string()),
                }),
            ),
        ];
        test_import_extraction(code, expected_imported_symbols, "Type only imports");
    }
}
