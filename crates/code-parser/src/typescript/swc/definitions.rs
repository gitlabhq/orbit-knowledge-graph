use super::super::types::TypeScriptSwcAst;
use crate::typescript::types::{
    TypeScriptDefinitionInfo, TypeScriptDefinitionType, TypeScriptFqn, TypeScriptFqnPart,
    TypeScriptScopeStack,
};
use crate::utils::{Position, Range};
use smallvec::smallvec;
use std::sync::Arc;
use swc_common::Spanned;
use swc_common::source_map::SmallPos;
use swc_common::{SourceMap, Span, sync::Lrc};
use swc_ecma_ast::{
    ClassDecl, Decl, FnDecl, GetterProp, MethodProp, PrivateMethod, PrivateProp, SetterProp,
    TsEnumDecl, TsInterfaceDecl, TsModuleDecl, TsTypeAliasDecl, VarDecl, VarDeclKind,
    VarDeclarator,
};
use swc_ecma_visit::{Visit, VisitWith};

/// Extracts definitions from an SWC Module AST
pub fn extract_swc_definitions(ast: &TypeScriptSwcAst) -> Vec<TypeScriptDefinitionInfo> {
    let mut extractor = SwcDefinitionExtractor::new(ast.source_map.clone());
    ast.module.visit_with(&mut extractor);
    extractor.definitions
}

/// Visitor that extracts definitions from SWC AST nodes
struct SwcDefinitionExtractor {
    definitions: Vec<TypeScriptDefinitionInfo>,
    scope_stack: TypeScriptScopeStack,
    source_map: Lrc<SourceMap>,
}

impl SwcDefinitionExtractor {
    fn new(source_map: Lrc<SourceMap>) -> Self {
        Self {
            source_map,
            definitions: Vec::new(),
            scope_stack: smallvec![],
        }
    }

    fn span_to_range(&self, span: Span) -> Range {
        let lo = self.source_map.lookup_char_pos_adj(span.lo);
        let hi = self.source_map.lookup_char_pos_adj(span.hi);
        Range::new(
            // SWC uses 1-based line numbers, but the codebase expects 0-based indexing
            Position::new(lo.line.saturating_sub(1), lo.col.to_usize()),
            Position::new(hi.line.saturating_sub(1), hi.col.to_usize()),
            (
                span.lo.to_usize().saturating_sub(1),
                span.hi.to_usize().saturating_sub(1),
            ),
        )
    }

    fn create_fqn(
        &self,
        name: &str,
        def_type: TypeScriptDefinitionType,
        span: Span,
    ) -> TypeScriptFqn {
        let mut fqn_parts = self.scope_stack.clone();
        let range = self.span_to_range(span);
        let new_part = TypeScriptFqnPart::new(def_type, name.to_string(), range);
        fqn_parts.push(new_part);
        Arc::new(fqn_parts)
    }

    fn add_definition(&mut self, name: &str, def_type: TypeScriptDefinitionType, span: Span) {
        let fqn = self.create_fqn(name, def_type, span);
        let definition = TypeScriptDefinitionInfo::new(
            def_type,
            name.to_string(),
            fqn,
            self.span_to_range(span),
        );
        self.definitions.push(definition);
    }

    fn enter_scope(&mut self, name: &str, def_type: TypeScriptDefinitionType, span: Span) {
        let new_part = TypeScriptFqnPart::new(def_type, name.to_string(), self.span_to_range(span));
        self.scope_stack.push(new_part);
    }

    fn exit_scope(&mut self) {
        self.scope_stack.pop();
    }
}

impl Visit for SwcDefinitionExtractor {
    fn visit_decl(&mut self, decl: &Decl) {
        match decl {
            Decl::Class(class_decl) => {
                self.visit_class_decl(class_decl);
            }
            Decl::Fn(fn_decl) => {
                self.visit_fn_decl(fn_decl);
            }
            Decl::Var(var_decl) => {
                self.visit_var_decl(var_decl);
            }
            Decl::TsInterface(interface_decl) => {
                self.visit_ts_interface_decl(interface_decl);
            }
            Decl::TsTypeAlias(type_alias_decl) => {
                self.visit_ts_type_alias_decl(type_alias_decl);
            }
            Decl::TsEnum(enum_decl) => {
                self.visit_ts_enum_decl(enum_decl);
            }
            _ => {
                // Handle other declaration types or continue visiting children
                decl.visit_children_with(self);
            }
        }
    }

    fn visit_class_decl(&mut self, class_decl: &ClassDecl) {
        let name = class_decl.ident.sym.as_ref();
        self.add_definition(name, TypeScriptDefinitionType::Class, class_decl.span());

        // Enter class scope for methods
        self.enter_scope(name, TypeScriptDefinitionType::Class, class_decl.span());

        // Visit class body for methods
        class_decl.class.visit_with(self);

        self.exit_scope();
    }

    fn visit_class_method(&mut self, node: &swc_ecma_ast::ClassMethod) {
        let name = node.key.as_ident();
        if let Some(name) = name {
            let name = name.sym.as_ref();
            self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
        } else {
            tracing::debug!(?node, "No name found for class method");
        }
    }

    fn visit_class_prop(&mut self, node: &swc_ecma_ast::ClassProp) {
        if let Some(name) = node.key.as_ident() {
            let name_str = name.sym.as_ref();

            // Check if this class property is bound to an expression
            if let Some(value) = &node.value {
                match value.as_ref() {
                    swc_ecma_ast::Expr::Arrow(_arrow_expr) => {
                        // Arrow function assignment in class field
                        self.add_definition(
                            name_str,
                            TypeScriptDefinitionType::NamedArrowFunction,
                            node.span(),
                        );
                    }
                    swc_ecma_ast::Expr::Fn(fn_expr) => {
                        // Function expression assignment in class field
                        let is_generator = fn_expr.function.is_generator;
                        let def_type = if is_generator {
                            TypeScriptDefinitionType::NamedGeneratorFunctionExpression
                        } else {
                            TypeScriptDefinitionType::NamedFunctionExpression
                        };
                        self.add_definition(name_str, def_type, node.span());
                    }
                    swc_ecma_ast::Expr::Class(_) => {
                        // Class expression assignment in class field
                        self.add_definition(
                            name_str,
                            TypeScriptDefinitionType::NamedClassExpression,
                            node.span(),
                        );
                    }
                    _ => {
                        // Other expressions - could be regular property assignments
                        // For now, we might not want to track these as definitions
                    }
                }
            }
        }
    }

    fn visit_getter_prop(&mut self, node: &GetterProp) {
        if let Some(name) = node.key.as_ident() {
            let name = name.sym.as_ref();
            self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
        }
    }

    fn visit_setter_prop(&mut self, node: &SetterProp) {
        if let Some(name) = node.key.as_ident() {
            let name = name.sym.as_ref();
            self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
        }
    }

    fn visit_method_prop(&mut self, node: &MethodProp) {
        if let Some(name) = node.key.as_ident() {
            let name = name.sym.as_ref();
            self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
        }
    }

    fn visit_private_method(&mut self, node: &PrivateMethod) {
        let name = node.key.name.as_str();
        self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
    }

    fn visit_constructor(&mut self, node: &swc_ecma_ast::Constructor) {
        let name = "constructor";
        self.add_definition(name, TypeScriptDefinitionType::Method, node.span());
    }

    fn visit_private_prop(&mut self, node: &PrivateProp) {
        let name_str = node.key.name.as_str();

        // Check if this private property is bound to an expression
        if let Some(value) = &node.value {
            match value.as_ref() {
                swc_ecma_ast::Expr::Arrow(_arrow_expr) => {
                    // Arrow function assignment in private property
                    self.add_definition(
                        name_str,
                        TypeScriptDefinitionType::NamedArrowFunction,
                        node.span(),
                    );
                }
                swc_ecma_ast::Expr::Fn(fn_expr) => {
                    // Function expression assignment in private property
                    let is_generator = fn_expr.function.is_generator;
                    let def_type = if is_generator {
                        TypeScriptDefinitionType::NamedGeneratorFunctionExpression
                    } else {
                        TypeScriptDefinitionType::NamedFunctionExpression
                    };
                    self.add_definition(name_str, def_type, node.span());
                }
                swc_ecma_ast::Expr::Class(_) => {
                    // Class expression assignment in private property
                    self.add_definition(
                        name_str,
                        TypeScriptDefinitionType::NamedClassExpression,
                        node.span(),
                    );
                }
                _ => {
                    // Other expressions - could be regular property assignments
                    // For now, we might not want to track these as definitions
                }
            }
        }
    }

    fn visit_fn_decl(&mut self, fn_decl: &FnDecl) {
        let name = fn_decl.ident.sym.as_ref();
        let is_generator = fn_decl.function.is_generator;
        let def_type = if is_generator {
            TypeScriptDefinitionType::NamedGeneratorFunctionDeclaration
        } else {
            TypeScriptDefinitionType::Function
        };

        self.add_definition(name, def_type, fn_decl.span());

        // Enter function scope
        self.enter_scope(name, def_type, fn_decl.span());

        // Visit function body
        fn_decl.function.visit_with(self);

        self.exit_scope();
    }

    fn visit_var_decl(&mut self, var_decl: &VarDecl) {
        for declarator in &var_decl.decls {
            self.visit_var_declarator(declarator, var_decl.kind);
        }
    }

    fn visit_ts_interface_decl(&mut self, interface_decl: &TsInterfaceDecl) {
        let name = interface_decl.id.sym.as_ref();
        self.add_definition(
            name,
            TypeScriptDefinitionType::Interface,
            interface_decl.span(),
        );

        // Enter interface scope
        self.enter_scope(
            name,
            TypeScriptDefinitionType::Interface,
            interface_decl.span(),
        );

        // Visit interface body
        interface_decl.visit_children_with(self);

        self.exit_scope();
    }

    fn visit_ts_type_alias_decl(&mut self, type_alias_decl: &TsTypeAliasDecl) {
        let name = type_alias_decl.id.sym.as_ref();
        self.add_definition(name, TypeScriptDefinitionType::Type, type_alias_decl.span());
    }

    fn visit_ts_enum_decl(&mut self, enum_decl: &TsEnumDecl) {
        let name = enum_decl.id.sym.as_ref();
        self.add_definition(name, TypeScriptDefinitionType::Enum, enum_decl.span());

        // Enter enum scope
        self.enter_scope(name, TypeScriptDefinitionType::Enum, enum_decl.span());

        // Visit enum members
        enum_decl.visit_children_with(self);

        self.exit_scope();
    }

    fn visit_ts_module_decl(&mut self, module_decl: &TsModuleDecl) {
        if let Some(name) = module_decl.id.as_ident() {
            let name_str = name.sym.as_ref();
            self.add_definition(
                name_str,
                TypeScriptDefinitionType::Namespace,
                module_decl.span(),
            );

            // Enter namespace scope
            self.enter_scope(
                name_str,
                TypeScriptDefinitionType::Namespace,
                module_decl.span(),
            );

            // Visit namespace body
            module_decl.visit_children_with(self);

            self.exit_scope();
        }
    }
}

impl SwcDefinitionExtractor {
    fn visit_var_declarator(&mut self, declarator: &VarDeclarator, _kind: VarDeclKind) {
        if let Some(name) = declarator.name.as_ident() {
            let name_str = name.id.sym.as_ref();

            // Check if this is a function assignment
            if let Some(init) = &declarator.init {
                match init.as_ref() {
                    swc_ecma_ast::Expr::Arrow(_arrow_expr) => {
                        // Arrow functions should always be classified as NamedArrowFunction
                        // regardless of what they return (including class expressions)
                        self.add_definition(
                            name_str,
                            TypeScriptDefinitionType::NamedArrowFunction,
                            declarator.span(),
                        );
                    }
                    swc_ecma_ast::Expr::Fn(fn_expr) => {
                        let is_generator = fn_expr.function.is_generator;
                        let def_type = if is_generator {
                            TypeScriptDefinitionType::NamedGeneratorFunctionExpression
                        } else {
                            TypeScriptDefinitionType::NamedFunctionExpression
                        };
                        self.add_definition(name_str, def_type, declarator.span());
                    }
                    swc_ecma_ast::Expr::Class(_) => {
                        self.add_definition(
                            name_str,
                            TypeScriptDefinitionType::NamedClassExpression,
                            declarator.span(),
                        );
                    }
                    _ => {
                        // Regular variable declaration - we might not want to track these
                        // or we could add a Variable definition type
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typescript::parser::parse_ast;
    use crate::typescript::types::TypeScriptDefinitionType;

    #[test]
    fn test_swc_class_definition_extraction() {
        let code = r#"
        class MyClass {
            method() {
                return 42;
            }
        }
        "#;

        let ast = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        for item in &ast.module.body {
            println!("Item: {:?}", item);
        }

        let definitions = extract_swc_definitions(&ast);

        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].name, "MyClass");
        assert_eq!(
            definitions[0].definition_type,
            TypeScriptDefinitionType::Class
        );

        assert_eq!(definitions[1].name, "method");
        assert_eq!(
            definitions[1].definition_type,
            TypeScriptDefinitionType::Method
        );

        for definition in definitions {
            println!("Definition: {:?}", definition);
        }
    }

    #[test]
    fn test_swc_function_definition_extraction() {
        let code = r#"
            function myFunction() {
                return 42;
            }
        "#;

        let module = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        let definitions = extract_swc_definitions(&module);

        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].name, "myFunction");
        assert_eq!(
            definitions[0].definition_type,
            TypeScriptDefinitionType::Function
        );
    }

    #[test]
    fn test_swc_typescript_definitions() {
        let code = r#"
            interface MyInterface {
                prop: string;
            }
            
            type MyType = string | number;
            
            enum MyEnum {
                A, B, C
            }
            
            namespace MyNamespace {
                export const value = 42;
            }
        "#;

        let module = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        let definitions = extract_swc_definitions(&module);

        assert_eq!(definitions.len(), 4);

        let interface_def = definitions
            .iter()
            .find(|d| d.name == "MyInterface")
            .unwrap();
        assert_eq!(
            interface_def.definition_type,
            TypeScriptDefinitionType::Interface
        );

        let type_def = definitions.iter().find(|d| d.name == "MyType").unwrap();
        assert_eq!(type_def.definition_type, TypeScriptDefinitionType::Type);

        let enum_def = definitions.iter().find(|d| d.name == "MyEnum").unwrap();
        assert_eq!(enum_def.definition_type, TypeScriptDefinitionType::Enum);

        let namespace_def = definitions
            .iter()
            .find(|d| d.name == "MyNamespace")
            .unwrap();
        assert_eq!(
            namespace_def.definition_type,
            TypeScriptDefinitionType::Namespace
        );
    }

    #[test]
    fn test_swc_arrow_function_extraction() {
        let code = r#"
            const myArrowFunction = () => {
                return 42;
            };
            
            // Note that call expressions are not supported yet
            const myCallExpression = withSession(async () => {
                return "hello";
            });
        "#;

        let module = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        let definitions = extract_swc_definitions(&module);

        assert_eq!(definitions.len(), 1);

        let arrow_def = definitions
            .iter()
            .find(|d| d.name == "myArrowFunction")
            .unwrap();
        assert_eq!(
            arrow_def.definition_type,
            TypeScriptDefinitionType::NamedArrowFunction
        );
    }

    #[test]
    fn test_anon_class_extraction() {
        let code = r#"
        const myClass = class {
            method() {
                return 42;
            }
        };

        const ClassFactory = (base) => class extends base {
            factory() { return true; }
        };
        "#;

        let module = parse_ast(
            crate::typescript::parser::EcmaDialect::TypeScript,
            "test.ts",
            code,
        )
        .unwrap();

        let definitions = extract_swc_definitions(&module);
        assert_eq!(definitions.len(), 2);
        for definition in definitions {
            println!("Definition: {:?}", definition);
        }
    }
}

#[cfg(test)]
mod tests_swc {
    use super::*;
    use crate::definitions::DefinitionLookup;
    use crate::typescript::analyzer::TypeScriptAnalysisResult;
    use crate::typescript::ast::typescript_fqn_to_string;
    use crate::typescript::parser::TypeScriptParser;
    use crate::typescript::types::{TypeScriptDefinitionInfo, TypeScriptDefinitionType};
    use crate::utils::{Position, Range};

    use std::fs;

    fn get_analysis_result(test_path: &str) -> crate::Result<TypeScriptAnalysisResult> {
        let parser = TypeScriptParser::new();
        let code = fs::read_to_string(test_path).expect("Error in reading JS file");
        let parse_result = parser.parse(&code, Some(test_path))?;
        let defs = extract_swc_definitions(&parse_result.ast);
        Ok(TypeScriptAnalysisResult {
            definitions: defs,
            imports: vec![],
            references: vec![],
        })
    }

    fn print_definitions(result: &Vec<&TypeScriptDefinitionInfo>) {
        for def in result {
            println!("Definition FQN: {:?}", typescript_fqn_to_string(&def.fqn));
        }
        println!("--------------------------------");
    }

    #[test]
    fn test_analyze_simple_js_code() -> crate::Result<()> {
        let test_path = "src/typescript/fixtures/javascript/sample.js";
        let result = get_analysis_result(test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        println!("Total # of Definitions: {:?}", result.definitions.len());
        let mut definitions = result.definitions.clone();
        definitions.sort_by_key(|def| def.range.start.line);
        for def in definitions {
            println!("Definition: {:?}", typescript_fqn_to_string(&def.fqn));
        }
        assert_eq!(result.definitions.len(), 33, "Should find 33 definitions");

        // Check named call expressions
        let named_call_exprs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedCallExpression);
        println!("Named Call Expressions: {:?}", named_call_exprs.len());
        print_definitions(&named_call_exprs);
        // assert_eq!(named_call_exprs.len(), 1, "Should find 1 named call expression");

        // Check specific constructs
        let classes = result.definitions_of_type(&TypeScriptDefinitionType::Class);
        println!("Classes: {:?}", classes.len());
        assert_eq!(classes.len(), 5, "Should find 5 classes");
        print_definitions(&classes);

        // Class FQN tests + class methods
        let class_defs = result.definitions_by_name("BaseAuthController");
        let class_def = class_defs.first().unwrap();
        let class_def_range = class_def.range;
        let class_def_fqn = typescript_fqn_to_string(&class_def.fqn);
        assert_eq!(
            class_def_fqn, "BaseAuthController",
            "Class definition FQN should be BaseAuthController"
        );
        assert_eq!(
            class_def.name, "BaseAuthController",
            "Class definition name should be BaseAuthController"
        );
        assert_eq!(
            class_def_range,
            Range::new(Position::new(11, 0), Position::new(40, 1), (236, 1183)),
            "Class definition range should be {class_def_range:?}"
        );
        assert_eq!(
            class_def.definition_type,
            TypeScriptDefinitionType::Class,
            "Class definition type should be Class"
        );

        let class_defs_2 = result.definitions_by_name("findForGitClient");
        let class_def_2 = class_defs_2.first().unwrap();
        let class_def_2_fqn = typescript_fqn_to_string(&class_def_2.fqn);
        let class_def_2_range = class_def_2.range;
        assert_eq!(
            class_def_2_fqn, "JwtController::findForGitClient",
            "Class definition FQN should be JwtController::findForGitClient"
        );
        assert_eq!(
            class_def_2.name, "findForGitClient",
            "Class definition name should be findForGitClient"
        );
        assert_eq!(
            class_def_2.definition_type,
            TypeScriptDefinitionType::Method,
            "Class definition type should be Method"
        );
        assert_eq!(
            class_def_2_range,
            Range::new(Position::new(146, 4), Position::new(174, 5), (4645, 5540)),
            "Class definition range should be {class_def_2_range:?}"
        );

        let methods = result.definitions_of_type(&TypeScriptDefinitionType::Method);
        println!("Methods: {:?}", methods.len());
        print_definitions(&methods);
        assert_eq!(methods.len(), 25, "Should find 25 methods");

        let functions = result.definitions_of_type(&TypeScriptDefinitionType::Function);
        println!("Functions: {:?}", functions.len());
        assert_eq!(functions.len(), 1, "Should find 1 function");
        print_definitions(&functions);

        let arrow_functions =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedArrowFunction);
        println!("Arrow Functions: {:?}", arrow_functions.len());
        assert_eq!(arrow_functions.len(), 2, "Should find 2 arrow functions");
        print_definitions(&arrow_functions);

        Ok(())
    }

    #[test]
    fn test_js_analyze_edge_cases_functions() -> crate::Result<()> {
        let test_path = "src/typescript/fixtures/javascript/functions.js";
        let result = get_analysis_result(test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        for def in &result.definitions {
            println!("Definition: {:?}", def.name);
        }
        println!("Total # of Definitions: {:?}", result.definitions.len());
        assert_eq!(result.definitions.len(), 40, "Should find 40 definitions");

        // make sure we find all functions
        let functions = result.definitions_of_type(&TypeScriptDefinitionType::Function);
        println!("Functions: {:?}", functions.len());
        print_definitions(&functions);
        assert_eq!(functions.len(), 5, "Should find 5 functions");

        // make sure nested function has correct FQN
        let nested_defs = result.definitions_by_name("nested");
        let nested_def = nested_defs.first().unwrap();
        let nested_def_fqn = typescript_fqn_to_string(&nested_def.fqn);
        let nested_def_range = nested_def.range;
        assert_eq!(
            nested_def_fqn, "withNestedDeclaration::nested",
            "Nested definition FQN should be withNestedDeclaration::nested"
        );
        assert_eq!(
            nested_def.name, "nested",
            "Nested definition name should be nested"
        );
        assert_eq!(
            nested_def.definition_type,
            TypeScriptDefinitionType::Function,
            "Nested definition type should be Function"
        );
        // Nested definition range: Range { start: Position { line: 124, column: 4 }, end: Position { line: 126, column: 5 }, byte_offset: (3877, 3939) }
        assert_eq!(
            nested_def_range,
            Range::new(Position::new(124, 4), Position::new(126, 5), (3877, 3939)),
            "Nested definition range should be {nested_def_range:?}"
        );

        // mixed nesting FQN (making sure anonymous functions are not included)
        let mixed_nesting_defs = result.definitions_by_name("mixedNesting");
        let mixed_nesting_def = mixed_nesting_defs.first().unwrap();
        let mixed_nesting_def_fqn = typescript_fqn_to_string(&mixed_nesting_def.fqn);
        let mixed_nesting_def_range = mixed_nesting_def.range;
        assert_eq!(
            mixed_nesting_def_fqn, "mixedNesting",
            "Mixed nesting definition FQN should be mixedNesting"
        );
        assert_eq!(
            mixed_nesting_def.name, "mixedNesting",
            "Mixed nesting definition name should be mixedNesting"
        );
        assert_eq!(
            mixed_nesting_def.definition_type,
            TypeScriptDefinitionType::NamedFunctionExpression,
            "Mixed nesting definition type should be Function"
        );
        // Mixed nesting definition range: Range { start: Position { line: 114, column: 6 }, end: Position { line: 120, column: 1 }, byte_offset: (3665, 3789) }
        assert_eq!(
            mixed_nesting_def_range,
            Range::new(Position::new(114, 6), Position::new(120, 1), (3665, 3789)),
            "Mixed nesting definition range should be {mixed_nesting_def_range:?}"
        );

        // make sure we find all arrow functions
        let arrow_functions =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedArrowFunction);
        println!("Arrow Functions: {:?}", arrow_functions.len());
        print_definitions(&arrow_functions);
        assert_eq!(arrow_functions.len(), 21, "Should find 21 arrow functions");

        // make sure we find all named function expressions
        let named_functions_exprs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedFunctionExpression);
        println!(
            "Named Functions Expressions: {:?}",
            named_functions_exprs.len()
        );
        print_definitions(&named_functions_exprs);
        assert_eq!(
            named_functions_exprs.len(),
            8,
            "Should find 8 named function expressions"
        );

        // make sure we find all named generator functions
        let named_generator_functions =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedGeneratorFunctionExpression);
        println!(
            "Named Generator Functions: {:?}",
            named_generator_functions.len()
        );
        print_definitions(&named_generator_functions);
        assert_eq!(
            named_generator_functions.len(),
            1,
            "Should find 1 named generator function"
        );

        let named_generator_func_decls = result
            .definitions_of_type(&TypeScriptDefinitionType::NamedGeneratorFunctionDeclaration);
        println!(
            "Named Generator Function Declarations: {:?}",
            named_generator_func_decls.len()
        );
        assert_eq!(
            named_generator_func_decls.len(),
            2,
            "Should find 2 named generator function declarations"
        );
        print_definitions(&named_generator_func_decls);

        // EXTRA: Test getters and setters
        let methods = result.definitions_of_type(&TypeScriptDefinitionType::Method);
        println!("Methods: {:?}", methods.len());
        print_definitions(&methods);
        assert_eq!(methods.len(), 2, "Should find 2 class methods");

        let unique_types = result
            .definitions
            .iter()
            .map(|def| def.definition_type)
            .collect::<std::collections::HashSet<_>>();
        println!("Unique Types: {:?}", unique_types.len());
        assert_eq!(unique_types.len(), 7, "Should find 7 unique types");
        println!("Unique Types: {unique_types:?}");

        Ok(())
    }

    #[test]
    fn test_analyze_classes_js_code() -> crate::Result<()> {
        let test_path = "src/typescript/fixtures/javascript/classes.js";
        let result = get_analysis_result(test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        println!("Total # of Definitions: {:?}", result.definitions.len());
        let mut definitions = result.definitions.clone();
        definitions.sort_by_key(|def| def.range.start.line);
        for def in definitions {
            println!("Definition: {:?}", typescript_fqn_to_string(&def.fqn));
        }
        // assert_eq!(result.definitions.len(), 60, "Should find 60 definitions");

        // Check specific constructs
        let classes = result.definitions_of_type(&TypeScriptDefinitionType::Class);
        println!("Classes: {:?}", classes.len());
        print_definitions(&classes);
        assert_eq!(classes.len(), 12, "Should find 12 classes");

        let class_exprs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedClassExpression);
        println!("Class Expressions: {:?}", class_exprs.len());
        print_definitions(&class_exprs);
        // assert_eq!(class_exprs.len(), 3, "Should find 3 class expressions");

        let methods = result.definitions_of_type(&TypeScriptDefinitionType::Method);
        println!("Class Methods: {:?}", methods.len());
        print_definitions(&methods);
        assert_eq!(methods.len(), 28, "Should find 28 class methods");

        // assigned class fields (arrow funcs, func exprs, generator funcs)
        let class_arrow_funcs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedArrowFunction);
        println!("Class Arrow Functions: {:?}", class_arrow_funcs.len());
        print_definitions(&class_arrow_funcs);
        assert_eq!(
            class_arrow_funcs.len(),
            4,
            "Should find 4 class arrow functions"
        );

        let class_func_exprs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedFunctionExpression);
        println!("Class Function Expressions: {:?}", class_func_exprs.len());
        print_definitions(&class_func_exprs);
        assert_eq!(
            class_func_exprs.len(),
            1,
            "Should find 1 class function expression"
        );

        let class_generator_funcs =
            result.definitions_of_type(&TypeScriptDefinitionType::NamedGeneratorFunctionExpression);
        println!(
            "Class Generator Functions: {:?}",
            class_generator_funcs.len()
        );
        print_definitions(&class_generator_funcs);
        assert_eq!(
            class_generator_funcs.len(),
            1,
            "Should find 1 class generator functions"
        );

        Ok(())
    }

    #[test]
    fn test_analyze_typescript_code() -> crate::Result<()> {
        let test_path = "src/typescript/fixtures/typescript/sample.ts";
        let result = get_analysis_result(test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        for def in &result.definitions {
            println!("Definition: {:?}", def.name);
        }
        println!("Total # of Definitions: {:?}", result.definitions.len());
        // assert_eq!(result.definitions.len(), 86, "Should find 86 definitions");

        // Check specific constructs
        let types = result.definitions_of_type(&TypeScriptDefinitionType::Type);
        println!("Types: {:?}", types.len());
        assert_eq!(types.len(), 45, "Should find 45 types");
        print_definitions(&types);

        // Definition FQN: "MyNamespace::Nested::NestedType"
        let type_defs = result.definitions_by_name("NestedType");
        let type_def = type_defs.first().unwrap();
        let type_def_fqn = typescript_fqn_to_string(&type_def.fqn);
        let type_def_range = type_def.range;
        assert_eq!(
            type_def_fqn, "MyNamespace::Nested::NestedType",
            "Type definition FQN should be MyNamespace::Nested::NestedType"
        );
        assert_eq!(
            type_def.name, "NestedType",
            "Type definition name should be NestedType"
        );
        assert_eq!(
            type_def_range,
            Range::new(Position::new(388, 11), Position::new(388, 36), (9278, 9303)),
            "Type definition range should be {type_def_range:?}"
        );

        let interfaces = result.definitions_of_type(&TypeScriptDefinitionType::Interface);
        println!("Interfaces: {:?}", interfaces.len());
        print_definitions(&interfaces);
        assert_eq!(interfaces.len(), 8, "Should find 8 interfaces");

        // Config definition FQN: "MyNamespace::Config"
        let config_defs = result.definitions_by_name("Config");
        let config_def = config_defs.first().unwrap();
        let config_def_fqn = typescript_fqn_to_string(&config_def.fqn);
        let config_def_range = config_def.range;
        assert_eq!(
            config_def_fqn, "MyNamespace::Config",
            "Config definition FQN should be MyNamespace::Config"
        );
        assert_eq!(
            config_def.name, "Config",
            "Config definition name should be Config"
        );
        assert_eq!(
            config_def_range,
            Range::new(Position::new(379, 9), Position::new(381, 3), (9117, 9160)),
            "Config definition range should be {config_def_range:?}"
        );

        // FQN part range validation
        for part in config_def.fqn.iter() {
            if part.node_type == TypeScriptDefinitionType::Namespace {
                assert_eq!(
                    part.range,
                    Range::new(Position::new(378, 0), Position::new(390, 1), (9084, 9309)),
                    "Config definition range should be {:?}",
                    part.range
                );
            }
            if part.node_type == TypeScriptDefinitionType::Interface {
                assert_eq!(
                    part.range,
                    Range::new(Position::new(379, 9), Position::new(381, 3), (9117, 9160)),
                    "Config definition range should be {:?}",
                    part.range
                );
            }
        }

        let namespaces = result.definitions_of_type(&TypeScriptDefinitionType::Namespace);
        println!("Namespaces: {:?}", namespaces.len());
        print_definitions(&namespaces);
        assert_eq!(namespaces.len(), 2, "Should find 2 namespaces");
        // Namespace definition FQN: "MyNamespace"
        let namespace_defs = result.definitions_by_name("MyNamespace");
        let namespace_def = namespace_defs.first().unwrap();
        let namespace_def_fqn = typescript_fqn_to_string(&namespace_def.fqn);
        let namespace_def_range = namespace_def.range;
        assert_eq!(
            namespace_def_fqn, "MyNamespace",
            "Namespace definition FQN should be MyNamespace"
        );
        assert_eq!(
            namespace_def_range,
            Range::new(Position::new(378, 0), Position::new(390, 1), (9084, 9309)),
            "Namespace definition range should be {namespace_def_range:?}"
        );

        // Check if namespace contains Config
        assert!(
            namespace_def_range.contains(&config_def_range.start),
            "Namespace definition range should contain Config definition range"
        );

        Ok(())
    }

    #[test]
    fn test_analyze_enums_typescript_code() -> crate::Result<()> {
        let test_path = "src/typescript/fixtures/typescript/enums.ts";
        let result = get_analysis_result(test_path)?;

        // Check that we found definitions
        assert!(!result.definitions.is_empty(), "Should find definitions");
        assert_eq!(result.definitions.len(), 23, "Should find 23 definitions");
        println!("Total # of Definitions: {:?}", result.definitions.len());

        // Check specific constructs
        let enums = result.definitions_of_type(&TypeScriptDefinitionType::Enum);
        println!("Enums: {:?}", enums.len());
        print_definitions(&enums);
        assert_eq!(enums.len(), 16, "Should find 16 enums");

        // Computed
        let enum_defs = result.definitions_by_name("Computed");
        let enum_def = enum_defs.first().unwrap();
        let enum_def_fqn = typescript_fqn_to_string(&enum_def.fqn);
        let enum_def_range = enum_def.range;
        assert_eq!(
            enum_def_fqn, "Computed",
            "Enum definition FQN should be Computed"
        );
        assert_eq!(
            enum_def_range,
            Range::new(Position::new(14, 0), Position::new(14, 66), (387, 453)),
            "Enum definition range should be {enum_def_range:?}"
        );

        Ok(())
    }
}
