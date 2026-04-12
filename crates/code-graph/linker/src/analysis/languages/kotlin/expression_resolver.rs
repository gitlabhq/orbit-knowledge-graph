use crate::graph::RelationshipType;
use parser_core::{
    Range,
    kotlin::{
        ast::kotlin_fqn_to_string,
        types::{
            KotlinDefinitionInfo, KotlinKotlinExpression, KotlinExpressionInfo,
            KotlinImportType,
        },
    },
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::RefCell;
use std::collections::VecDeque;
use tracing::{debug, error};

use crate::analysis::{
    languages::kotlin::{
        kotlin_file::{KotlinBinding, KotlinFile},
        types::{KotlinScopeTree, ScopeContext},
        utils::{full_import_path, get_binary_operator_function, get_unary_operator_function},
    },
    types::{
        ConsolidatedRelationship, DefinitionNode, ImportType,
        ImportedSymbolLocation, ImportedSymbolNode,
    },
};
use crate::parse_types::References;

use internment::ArcIntern;

// Standard member functions which should not be added to the function registry because they are already in every
const STD_MEMBER_FUNCTIONS: [&str; 14] = [
    "toString",
    "hashCode",
    "equals",
    "clone",
    "notify",
    "notifyAll",
    "wait",
    "apply",
    "let",
    "run",
    "with",
    "also",
    "takeIf",
    "takeUnless",
];

#[derive(Default, Debug)]
pub(crate) struct Resolutions {
    definition_resolutions: Vec<DefinitionResolution>,
    import_resolutions: Vec<ImportResolution>,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedType {
    Definition(DefinitionResolution),
    Import(ImportResolution),
    Unit,
}

#[derive(Debug, Clone)]
pub(crate) struct DefinitionResolution {
    pub name: String,
    pub fqn: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ImportResolution {
    pub name: Option<String>,
    pub location: ImportedSymbolLocation,
}

#[derive(Debug)]
struct FqnGuard<'a> {
    set: &'a RefCell<FxHashSet<String>>,
    fqn: String,
}

impl<'a> Drop for FqnGuard<'a> {
    fn drop(&mut self) {
        self.set.borrow_mut().remove(&self.fqn);
    }
}

#[derive(Default)]
pub(crate) struct KotlinExpressionResolver {
    /// Package name -> file path
    package_files: FxHashMap<String, Vec<String>>,
    /// Relative file path -> file
    files: FxHashMap<String, KotlinFile>,
    /// FQN -> DefinitionNode
    definition_nodes: FxHashMap<String, DefinitionNode>,
    /// Function registry -> DefinitionNode
    function_registry: FxHashMap<String, Vec<DefinitionNode>>,
    /// Guard set to prevent infinite recursion while resolving from context
    context_resolution_fqns: RefCell<FxHashSet<String>>,
}

impl KotlinExpressionResolver {
    pub fn resolve_expressions(
        &self,
        file_path: &str,
        references: &References,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        debug!("Resolving Kotlin references in file {file_path}.");
        if let Some(iterator) = references.iter_kotlin() {
            for reference in iterator {
                let expression = reference.metadata.as_ref().map(|m| (**m).clone());

                let scope = reference.scope.clone();
                if scope.is_none() {
                    continue;
                }

                let from_definition = match self
                    .definition_nodes
                    .get(&kotlin_fqn_to_string(&scope.unwrap()))
                {
                    Some(definition) => definition,
                    None => continue,
                };

                if let Some(expression) = expression {
                    let mut resolutions = Resolutions::default();
                    debug!("Resolving Kotlin expression {:#?}.", expression);
                    self.resolve_expression(file_path, &expression, &mut resolutions);

                    for resolved_definition in resolutions.definition_resolutions {
                        let to_definition = self.definition_nodes.get(&resolved_definition.fqn);

                        if let Some(to_definition) = to_definition {
                            let mut relationship =
                                ConsolidatedRelationship::definition_to_definition(
                                    from_definition.file_path.clone(),
                                    to_definition.file_path.clone(),
                                );
                            relationship.relationship_type = RelationshipType::Calls;
                            relationship.source_range = ArcIntern::new(from_definition.range);
                            relationship.target_range = ArcIntern::new(to_definition.range);
                            relationships.push(relationship);
                        }
                    }

                    for resolved_import in resolutions.import_resolutions {
                        let mut relationship =
                            ConsolidatedRelationship::definition_to_imported_symbol(
                                from_definition.file_path.clone(),
                                ArcIntern::new(resolved_import.location.file_path.clone()),
                            );
                        relationship.relationship_type = RelationshipType::Calls;
                        relationship.source_range = ArcIntern::new(from_definition.range);
                        relationship.target_range =
                            ArcIntern::new(resolved_import.location.range());
                        relationships.push(relationship);
                    }
                }
            }
        }
    }

    pub fn resolve_expression(
        &self,
        file_path: &str,
        expression: &KotlinExpressionInfo,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        debug!(
            expression_kind = expression.expression.variant_name(),
            "resolving Kotlin expression"
        );

        let remaining_stack = stacker::remaining_stack().unwrap_or(0);
        if remaining_stack < parser_core::MINIMUM_STACK_REMAINING {
            error!(
                remaining_stack,
                expression_kind = expression.expression.variant_name(),
                "stack limit reached, aborting Kotlin expression resolution"
            );
            return None;
        }

        match &expression.expression {
            KotlinExpression::Identifier { name } => {
                self.resolve_identifier_expression(file_path, expression.range, name, resolutions)
            }
            KotlinExpression::Call { name, .. } => {
                self.resolve_call_expression(file_path, expression.range, name, resolutions)
            }
            KotlinExpression::FieldAccess { target, member } => {
                if matches!(target.expression, KotlinExpression::Super) {
                    return self.resolve_super_field_access(file_path, expression.range, member);
                }

                let target = self.resolve_expression(file_path, target, resolutions);

                if let Some(ResolvedType::Definition(target)) = target {
                    if let Some(resolved) = self.resolve_member_type_in_class(&target.fqn, member) {
                        return Some(resolved);
                    } else if let Some(resolved) = self.resolve_extension_field(
                        file_path,
                        &target.name,
                        member,
                        expression.range,
                    ) {
                        return Some(resolved);
                    }
                } else if let Some(ResolvedType::Import(import)) = target {
                    if let Some(imported_symbol_name) = import.name.clone()
                        && let Some(resolved) = self.resolve_extension_field(
                            file_path,
                            &imported_symbol_name,
                            member,
                            expression.range,
                        )
                    {
                        return Some(resolved);
                    } else {
                        resolutions.import_resolutions.push(import);
                    }
                }

                None
            }
            KotlinExpression::MemberFunctionCall { target, member } => {
                if matches!(target.expression, KotlinExpression::Super) {
                    return self.resolve_super_member_function_call(
                        file_path,
                        expression.range,
                        member,
                        resolutions,
                    );
                }

                let target = self.resolve_expression(file_path, target, resolutions);

                if let Some(ResolvedType::Definition(target)) = target {
                    if let Some(resolved) =
                        self.resolve_function_type_in_class(&target.fqn, member, resolutions)
                    {
                        return Some(resolved);
                    } else if let Some(resolved) = self.resolve_extension_function(
                        file_path,
                        &target.name,
                        member,
                        expression.range,
                        resolutions,
                    ) {
                        return Some(resolved);
                    }
                } else if let Some(ResolvedType::Import(import)) = target {
                    if let Some(imported_symbol_name) = import.name.clone()
                        && let Some(resolved) = self.resolve_extension_function(
                            file_path,
                            &imported_symbol_name,
                            member,
                            expression.range,
                            resolutions,
                        )
                    {
                        return Some(resolved);
                    } else {
                        resolutions.import_resolutions.push(import);
                    }
                }

                if let Some(ResolvedType::Definition(target)) = self
                    .resolve_member_type_from_context(
                        file_path,
                        &expression.generics,
                        expression.range,
                        member,
                        resolutions,
                    )
                {
                    return Some(ResolvedType::Definition(target));
                }

                None
            }
            KotlinExpression::MethodReference { target, member } => {
                if let Some(target_expression) = target
                    && matches!(target_expression.expression, KotlinExpression::Super)
                {
                    return self.resolve_super_member_function_call(
                        file_path,
                        expression.range,
                        member,
                        resolutions,
                    );
                }

                let target = match target {
                    Some(target) => self.resolve_expression(file_path, target, resolutions),
                    None => self.resolve_this_expression(file_path, expression.range, None),
                };

                if let Some(ResolvedType::Definition(target)) = target {
                    return self.resolve_function_type_in_class(&target.fqn, member, resolutions);
                } else if let Some(ResolvedType::Import(import)) = target {
                    resolutions.import_resolutions.push(import);
                }

                None
            }
            KotlinExpression::Annotation { name } => {
                debug!("Resolving Kotlin annotation {name}.");

                if let Some(resolution) = self.resolve_type_reference(name, None, file_path) {
                    match resolution {
                        ResolvedType::Definition(definition) => {
                            resolutions.definition_resolutions.push(definition.clone());
                            return Some(ResolvedType::Definition(definition));
                        }
                        ResolvedType::Import(import) => {
                            resolutions.import_resolutions.push(import);
                            return None;
                        }
                        ResolvedType::Unit => {
                            return None;
                        }
                    }
                }

                None
            }
            KotlinExpression::Index { target } => {
                let target = self.resolve_expression(file_path, target, resolutions);

                if let Some(ResolvedType::Definition(target)) = target {
                    return self.resolve_function_type_in_class(&target.fqn, "get", resolutions);
                } else if let Some(ResolvedType::Import(import)) = target {
                    resolutions.import_resolutions.push(import);
                }

                None
            }
            KotlinExpression::This { label } => {
                self.resolve_this_expression(file_path, expression.range, label.clone())
            }
            KotlinExpression::If { bodies } => {
                let mut resolved_types = Vec::new();
                for body in bodies {
                    let body_type = self.resolve_expression(file_path, body, resolutions);
                    if let Some(ResolvedType::Definition(body_type)) = body_type {
                        resolved_types.push(body_type);
                    }
                }

                self.resolve_common_ancestor_type(resolved_types)
            }
            KotlinExpression::Elvis { left, right } => {
                let mut resolved_types = Vec::new();
                if let Some(ResolvedType::Definition(left_type)) =
                    self.resolve_expression(file_path, left, resolutions)
                {
                    resolved_types.push(left_type);
                }
                if let Some(right) = right
                    && let Some(ResolvedType::Definition(right_type)) =
                        self.resolve_expression(file_path, right, resolutions)
                {
                    resolved_types.push(right_type);
                }

                self.resolve_common_ancestor_type(resolved_types)
            }
            KotlinExpression::When { entries } => {
                let mut resolved_types = Vec::new();
                for entry in entries {
                    let entry_type = self.resolve_expression(file_path, entry, resolutions);
                    if let Some(ResolvedType::Definition(entry_type)) = entry_type {
                        resolved_types.push(entry_type);
                    }
                }

                self.resolve_common_ancestor_type(resolved_types)
            }
            KotlinExpression::Try {
                body,
                catch_clauses,
            } => {
                let mut resolved_types = Vec::new();
                if let Some(body) = body
                    && let Some(ResolvedType::Definition(body_type)) =
                        self.resolve_expression(file_path, body, resolutions)
                {
                    resolved_types.push(body_type);
                }

                for catch_clause in catch_clauses {
                    if let Some(ResolvedType::Definition(catch_clause_type)) =
                        self.resolve_expression(file_path, catch_clause, resolutions)
                    {
                        resolved_types.push(catch_clause_type);
                    }
                }

                self.resolve_common_ancestor_type(resolved_types)
            }
            KotlinExpression::Lambda { expression } => {
                self.resolve_expression(file_path, expression, resolutions)
            }
            KotlinExpression::Parenthesized { expression } => {
                self.resolve_expression(file_path, expression, resolutions)
            }
            KotlinExpression::Unary { operator, target } => {
                let target = self.resolve_expression(file_path, target, resolutions);

                if let Some(ResolvedType::Definition(target)) = target
                    && let Some(member) = get_unary_operator_function(operator)
                {
                    return self.resolve_function_type_in_class(&target.fqn, &member, resolutions);
                }

                None
            }
            KotlinExpression::Binary {
                left,
                operator,
                right,
            } => {
                let left = self.resolve_expression(file_path, left, resolutions);
                self.resolve_expression(file_path, right, resolutions);

                if let Some(member) = get_binary_operator_function(operator)
                    && let Some(ResolvedType::Definition(target)) = left
                {
                    return self.resolve_function_type_in_class(&target.fqn, &member, resolutions);
                }

                None
            }
            KotlinExpression::Literal | KotlinExpression::Super => None,
        }
    }

    // If we can't resolve the member type, we can naively resolve it by looking at the generics and the function registry.
    fn resolve_member_type_from_context(
        &self,
        file_path: &str,
        generics: &Vec<String>,
        range: Range,
        function: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;
        let scope = file.get_scope_at_offset(range.byte_offset.0)?;

        // First, look if any of the generics contain the function.
        for generic in generics {
            if let Some(resolved_type) =
                self.resolve_type_reference(generic, Some(scope.fqn.as_str()), file_path)
                && let ResolvedType::Definition(definition) = resolved_type
            {
                let potential_fqn = format!("{}.{}", definition.fqn, function);
                if let Some(definition) = self.definition_nodes.get(&potential_fqn) {
                    resolutions
                        .definition_resolutions
                        .push(DefinitionResolution {
                            name: definition.name().to_string(),
                            fqn: definition.fqn.to_string(),
                        });

                    return Some(ResolvedType::Definition(DefinitionResolution {
                        name: definition.name().to_string(),
                        fqn: definition.fqn.to_string(),
                    }));
                }
            }
        }

        // Then, look if any of the functions in the function registry contain the function.
        if let Some(function_registry) = self.function_registry.get(function) {
            for function_node in function_registry {
                if function_node.fqn.to_string() == scope.fqn {
                    continue;
                }

                // Check all the imported files and look if the function is in there.
                for (_symbol, path) in file.imported_symbols.clone() {
                    if let Some(file_path) = self
                        .package_files
                        .get(&path)
                        .unwrap_or(&vec![])
                        .iter()
                        .next()
                    {
                        let file = self.files.get(file_path)?;
                        if let Some(function) = file.functions.get(&function_node.fqn.to_string()) {
                            resolutions
                                .definition_resolutions
                                .push(DefinitionResolution {
                                    name: function.name.clone(),
                                    fqn: function.fqn.clone(),
                                });

                            if let Some(return_type) = &function.return_type {
                                return self.resolve_type_reference(
                                    return_type,
                                    Some(&function.fqn),
                                    file_path,
                                );
                            } else if let Some(init) = &function.init {
                                if let Some(_guard) = self.enter_fqn_guard(&function.fqn) {
                                    return self.resolve_expression(
                                        file_path,
                                        init,
                                        &mut Resolutions::default(),
                                    );
                                } else {
                                    // Cycle detected; treat as unresolved here to avoid infinite recursion
                                    return Some(ResolvedType::Unit);
                                }
                            }
                        }

                        return Some(ResolvedType::Unit);
                    }
                }

                for path in file.wildcard_imports.clone() {
                    for file_path in self.package_files.get(&path).unwrap_or(&vec![]) {
                        let file = self.files.get(file_path)?;
                        if let Some(function) = file.functions.get(&function_node.fqn.to_string()) {
                            resolutions
                                .definition_resolutions
                                .push(DefinitionResolution {
                                    name: function.name.clone(),
                                    fqn: function.fqn.clone(),
                                });

                            if let Some(return_type) = &function.return_type {
                                return self.resolve_type_reference(
                                    return_type,
                                    Some(&function.fqn),
                                    file_path,
                                );
                            } else if let Some(init) = &function.init {
                                if let Some(_guard) = self.enter_fqn_guard(&function.fqn) {
                                    return self.resolve_expression(
                                        file_path,
                                        init,
                                        &mut Resolutions::default(),
                                    );
                                } else {
                                    // Cycle detected; avoid recursion
                                    return Some(ResolvedType::Unit);
                                }
                            }

                            return Some(ResolvedType::Unit);
                        }
                    }
                }

                // Check all the files in the same package.
                for file_path in self
                    .package_files
                    .get(&file.package_name)
                    .unwrap_or(&vec![])
                {
                    let file = self.files.get(file_path)?;
                    if let Some(function) = file.functions.get(&function_node.fqn.to_string()) {
                        resolutions
                            .definition_resolutions
                            .push(DefinitionResolution {
                                name: function.name.clone(),
                                fqn: function.fqn.clone(),
                            });

                        if let Some(return_type) = &function.return_type {
                            return self.resolve_type_reference(
                                return_type,
                                Some(&function.fqn),
                                file_path,
                            );
                        } else if let Some(init) = &function.init {
                            if let Some(_guard) = self.enter_fqn_guard(&function.fqn) {
                                return self.resolve_expression(
                                    file_path,
                                    init,
                                    &mut Resolutions::default(),
                                );
                            } else {
                                // Cycle detected; avoid recursion
                                return Some(ResolvedType::Unit);
                            }
                        }

                        return Some(ResolvedType::Unit);
                    }
                }
            }
        }
        None
    }

    fn resolve_this_expression(
        &self,
        file_path: &str,
        range: Range,
        label: Option<String>,
    ) -> Option<ResolvedType> {
        debug!(
            "Resolving Kotlin this reference in file {} at range ({}, {}).",
            file_path, range.byte_offset.0, range.byte_offset.1
        );
        let file = self.files.get(file_path)?;
        let label = label.unwrap_or("".to_string());

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            if let Some(function) = file.functions.get(&scope.fqn) {
                // We found an extension function but "this" refers to something else
                if !label.is_empty() && label != function.name {
                    current_scope = file.get_parent_scope(scope);
                    continue;
                }

                if let Some(receiver_type) = &function.receiver_type {
                    return self.resolve_type_reference(
                        receiver_type,
                        Some(&function.fqn),
                        file_path,
                    );
                }
            } else if let Some(class) = file.classes.get(&scope.fqn) {
                if !label.is_empty() && label != class.name {
                    current_scope = file.get_parent_scope(scope);
                    continue;
                }

                return Some(ResolvedType::Definition(DefinitionResolution {
                    name: class.name.clone(),
                    fqn: class.fqn.clone(),
                }));
            }

            current_scope = file.get_parent_scope(scope);
        }

        None
    }

    fn resolve_super_field_access(
        &self,
        file_path: &str,
        range: Range,
        member: &str,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            if let Some(class) = file.classes.get(&scope.fqn) {
                if let Some(super_class) = &class.super_class
                    && let Some(resolved) = self.resolve_member_type_in_super_type(
                        super_class,
                        member,
                        class.fqn.as_str(),
                        file,
                    )
                {
                    return Some(resolved);
                }

                for interface in &class.super_interfaces {
                    if let Some(resolved) = self.resolve_member_type_in_super_type(
                        interface,
                        member,
                        class.fqn.as_str(),
                        file,
                    ) {
                        return Some(resolved);
                    }
                }
            }

            current_scope = file.get_parent_scope(scope);
        }

        None
    }

    fn resolve_super_member_function_call(
        &self,
        file_path: &str,
        range: Range,
        member: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            if let Some(class) = file.classes.get(&scope.fqn) {
                if let Some(super_class) = &class.super_class
                    && let Some(resolved) = self.resolve_function_type_in_super_type(
                        super_class,
                        member,
                        class.fqn.as_str(),
                        file,
                        resolutions,
                    )
                {
                    return Some(resolved);
                }

                for interface in &class.super_interfaces {
                    if let Some(resolved) = self.resolve_function_type_in_super_type(
                        interface,
                        member,
                        class.fqn.as_str(),
                        file,
                        resolutions,
                    ) {
                        return Some(resolved);
                    }
                }
            }

            current_scope = file.get_parent_scope(scope);
        }

        None
    }

    fn resolve_call_expression(
        &self,
        file_path: &str,
        range: Range,
        name: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        debug!("Resolving Kotlin call expression {name}.");
        let file = self.files.get(file_path)?;

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            let potential_fqn = format!("{}.{}", scope.fqn, name);
            if let Some(function) = file.functions.get(&potential_fqn)
                && let Some(_guard) = self.enter_fqn_guard(&function.fqn)
            {
                resolutions
                    .definition_resolutions
                    .push(DefinitionResolution {
                        name: function.name.clone(),
                        fqn: potential_fqn,
                    });

                if let Some(return_type) = &function.return_type {
                    return self.resolve_type_reference(
                        return_type,
                        Some(&function.fqn),
                        file_path,
                    );
                } else if let Some(init) = &function.init {
                    return self.resolve_expression(file_path, init, resolutions);
                }

                return None; // The function returns Unit.
            } else if let Some(class) = file.classes.get(&potential_fqn) {
                // Lookup if there is a constructor for this class.
                let potenrial_constructor_fqn = format!("{}.{}", class.fqn, "<init>");
                if let Some(constructor) = self.definition_nodes.get(&potenrial_constructor_fqn) {
                    resolutions
                        .definition_resolutions
                        .push(DefinitionResolution {
                            name: constructor.name().to_string(),
                            fqn: constructor.fqn.to_string(),
                        });

                    return Some(ResolvedType::Definition(DefinitionResolution {
                        name: class.name.clone(),
                        fqn: class.fqn.clone(),
                    }));
                }

                // Otherwise, resolve the definition class itself.
                resolutions
                    .definition_resolutions
                    .push(DefinitionResolution {
                        name: class.name.clone(),
                        fqn: class.fqn.clone(),
                    });

                return Some(ResolvedType::Definition(DefinitionResolution {
                    name: class.name.clone(),
                    fqn: class.fqn.clone(),
                }));
            }

            let resolved_type_in_scope_context = match file.get_scope_context(scope) {
                ScopeContext::ExtensionFunction(receiver_type) => self
                    .resolve_function_type_in_super_type(
                        receiver_type.as_str(),
                        name,
                        scope.fqn.as_str(),
                        file,
                        resolutions,
                    ),
                ScopeContext::Class => {
                    self.resolve_function_type_in_class(&scope.fqn, name, resolutions)
                }
                _ => None,
            };

            if resolved_type_in_scope_context.is_some() {
                return resolved_type_in_scope_context;
            }

            current_scope = file.get_parent_scope(scope);
        }

        // Check the current package before the imports
        let potential_package_fqn = format!("{}.{}", file.package_name, name);
        if let Some(definition) = self.definition_nodes.get(&potential_package_fqn) {
            return Some(ResolvedType::Definition(DefinitionResolution {
                name: definition.name().to_string(),
                fqn: definition.fqn.to_string(),
            }));
        }

        self.resolve_type_from_imports(file_path, name, resolutions)
    }

    fn resolve_function_type_in_super_type(
        &self,
        super_type: &str,
        name: &str,
        class_fqn: &str,
        file: &KotlinFile,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        if let Some(resolved_type) =
            self.resolve_type_reference(super_type, Some(class_fqn), &file.file_path)
            && let ResolvedType::Definition(definition) = resolved_type
        {
            return self.resolve_function_type_in_class(&definition.fqn, name, resolutions);
        }

        None
    }

    fn resolve_function_type_in_class(
        &self,
        class_fqn: &str,
        name: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        let remaining_stack = stacker::remaining_stack().unwrap_or(0);
        if remaining_stack < parser_core::MINIMUM_STACK_REMAINING {
            error!(
                remaining_stack,
                "stack limit reached, aborting Kotlin function type resolution in class hierarchy"
            );
            return None;
        }

        debug!("Resolving Kotlin method call {name} in target {class_fqn}.");
        let file_path = self.definition_nodes.get(class_fqn)?.file_path.clone();
        let file = self.files.get(file_path.as_ref())?;
        let class = file.classes.get(class_fqn)?;

        // First check if the member is child class of the type
        let potential_init_fqn = format!("{class_fqn}.{name}.<init>");
        if let Some(init) = file.functions.get(&potential_init_fqn) {
            resolutions
                .definition_resolutions
                .push(DefinitionResolution {
                    name: init.name.clone(),
                    fqn: init.fqn.clone(),
                });

            return Some(ResolvedType::Definition(DefinitionResolution {
                name: init.name.clone(),
                fqn: init.fqn.clone(),
            }));
        }

        let potential_fqn = format!("{class_fqn}.{name}");
        if let Some(definition) = file.classes.get(&potential_fqn) {
            resolutions
                .definition_resolutions
                .push(DefinitionResolution {
                    name: definition.name.clone(),
                    fqn: definition.fqn.clone(),
                });

            return Some(ResolvedType::Definition(DefinitionResolution {
                name: definition.name.clone(),
                fqn: definition.fqn.clone(),
            }));
        }

        if let Some(function) = file.functions.get(&potential_fqn)
            && let Some(_guard) = self.enter_fqn_guard(&function.fqn)
        {
            resolutions
                .definition_resolutions
                .push(DefinitionResolution {
                    name: function.name.clone(),
                    fqn: potential_fqn,
                });

            if let Some(return_type) = &function.return_type {
                return self.resolve_type_reference(return_type, Some(&function.fqn), &file_path);
            } else if let Some(init) = &function.init {
                return self.resolve_expression(&file_path, init, &mut Resolutions::default());
            }

            return Some(ResolvedType::Unit); // The function returns Unit.
        }

        if let Some(companion) = &class.companion {
            let companion_fqn = format!("{class_fqn}.{companion}");
            if let Some(resolved) =
                self.resolve_function_type_in_class(&companion_fqn, name, resolutions)
            {
                return Some(resolved);
            }
        }

        if let Some(super_class) = &class.super_class {
            return self.resolve_function_type_in_super_type(
                super_class,
                name,
                class_fqn,
                file,
                resolutions,
            );
        }

        for interface in &class.super_interfaces {
            if let Some(resolved) = self.resolve_function_type_in_super_type(
                interface,
                name,
                class_fqn,
                file,
                resolutions,
            ) {
                return Some(resolved);
            }
        }

        None
    }

    fn resolve_extension_field(
        &self,
        file_path: &str,
        class_name: &str,
        field_name: &str,
        range: Range,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;

        // Look if the field is a constant in the package
        let potential_fqn = format!("{}.{}", file.package_name, field_name);
        if let Some(definition) = self.definition_nodes.get(&potential_fqn) {
            let definition_file = self.files.get(definition.file_path.as_ref())?;
            let field_declaration = definition_file.constants.get(&definition.fqn.to_string())?;

            if field_declaration.is_extension_field(class_name) {
                if let Some(return_type) = &field_declaration.binding_type {
                    return self.resolve_type_reference(
                        return_type,
                        Some(&definition.fqn.to_string()),
                        file_path,
                    );
                } else if let Some(init) = &field_declaration.init {
                    return self.resolve_expression(file_path, init, &mut Resolutions::default());
                }
            }
        }

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            if let Some(binding) = scope.definition_map.unique_definitions.get(field_name)
                && binding.is_extension_field(class_name)
            {
                return self.resolve_binding_type(binding, Some(class_name), file);
            } else if let Some(bindings) =
                scope.definition_map.duplicated_definitions.get(field_name)
            {
                for binding in bindings {
                    if binding.range.0 <= range.byte_offset.0
                        && binding.range.1 >= range.byte_offset.1
                        && binding.is_extension_field(class_name)
                    {
                        return self.resolve_binding_type(binding, Some(class_name), file);
                    }
                }
            }

            current_scope = file.get_parent_scope(scope);
        }

        // Look for the field in the imports
        if let Some(resolved) =
            self.resolve_type_from_imports(file_path, field_name, &mut Resolutions::default())
        {
            return Some(resolved);
        }

        None
    }

    fn resolve_extension_function(
        &self,
        file_path: &str,
        class_name: &str,
        function_name: &str,
        range: Range,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;

        // Look for the function in the scope hierarchy
        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            let potential_fqn = format!("{}.{}", scope.fqn, function_name);
            if let Some(function) = file.functions.get(&potential_fqn)
                && function.is_extension_function(class_name)
            {
                resolutions
                    .definition_resolutions
                    .push(DefinitionResolution {
                        name: function.name.clone(),
                        fqn: potential_fqn,
                    });

                if let Some(return_type) = &function.return_type {
                    return self.resolve_type_reference(
                        return_type,
                        Some(&function.fqn),
                        file_path,
                    );
                } else if let Some(init) = &function.init {
                    return self.resolve_expression(file_path, init, &mut Resolutions::default());
                }
            }

            current_scope = file.get_parent_scope(scope);
        }

        // Look for the function in the imports
        if let Some(resolved) =
            self.resolve_type_from_imports(file_path, function_name, resolutions)
        {
            return Some(resolved);
        }

        // Look for the function in the current package
        let potential_package_fqn = format!("{}.{}", file.package_name, function_name);
        if let Some(definition) = self.definition_nodes.get(&potential_package_fqn) {
            let definition_file = self.files.get(definition.file_path.as_ref())?;
            let function = definition_file.functions.get(&definition.fqn.to_string())?;

            if function.is_extension_function(class_name) {
                resolutions
                    .definition_resolutions
                    .push(DefinitionResolution {
                        name: definition.name().to_string(),
                        fqn: definition.fqn.to_string(),
                    });

                if let Some(return_type) = &function.return_type {
                    return self.resolve_type_reference(
                        return_type,
                        Some(&function.fqn),
                        file_path,
                    );
                } else if let Some(init) = &function.init {
                    return self.resolve_expression(file_path, init, &mut Resolutions::default());
                }
            }
        }

        None
    }

    fn resolve_identifier_expression(
        &self,
        file_path: &str,
        range: Range,
        name: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        debug!("Resolving Kotlin identifier expression {name} in file {file_path}.");
        if let Some(resolved) =
            self.resolve_binding_type_from_scope_hierarchy(file_path, range, name)
        {
            return Some(resolved);
        }

        self.resolve_type_from_imports(file_path, name, resolutions)
    }

    fn resolve_binding_type_from_scope_hierarchy(
        &self,
        file_path: &str,
        range: Range,
        name: &str,
    ) -> Option<ResolvedType> {
        debug!("Resolving Kotlin identifier type {name} in file {file_path} at range.");
        let file = self.files.get(file_path)?;

        let mut current_scope = file.get_scope_at_offset(range.byte_offset.0);
        while let Some(scope) = current_scope {
            if let Some(binding) = scope.definition_map.unique_definitions.get(name) {
                let current_class_fqn = match file.get_scope_context(scope) {
                    ScopeContext::Class => Some(scope.fqn.as_str()),
                    _ => None,
                };

                return self.resolve_binding_type(binding, current_class_fqn, file);
            }

            // Then check duplicated definitions with range matching
            if let Some(bindings) = scope.definition_map.duplicated_definitions.get(name) {
                for binding in bindings {
                    if binding.range.0 <= range.byte_offset.0
                        && binding.range.1 >= range.byte_offset.1
                    {
                        let current_class_fqn = match file.get_scope_context(scope) {
                            ScopeContext::Class => Some(scope.fqn.as_str()),
                            _ => None,
                        };

                        return self.resolve_binding_type(binding, current_class_fqn, file);
                    }
                }
            }

            // Lookup the type in the scope context
            let resolved_type_in_scope_context = match file.get_scope_context(scope) {
                ScopeContext::ExtensionFunction(receiver_type) => self
                    .resolve_member_type_in_super_type(
                        receiver_type.as_str(),
                        name,
                        scope.fqn.as_str(),
                        file,
                    ),
                ScopeContext::Class => {
                    self.resolve_member_type_in_class_hierarchy(scope, name, file)
                }
                _ => None,
            };

            // Then check context-specific resolution
            if let Some(resolved) = resolved_type_in_scope_context {
                return Some(resolved);
            }

            // Move up scope hierarchy
            current_scope = file.get_parent_scope(scope);
        }

        None
    }

    fn resolve_member_type_in_class_hierarchy(
        &self,
        scope: &KotlinScopeTree,
        name: &str,
        file: &KotlinFile,
    ) -> Option<ResolvedType> {
        if let Some(class) = file.classes.get(&scope.fqn) {
            // Check the companion object first
            if let Some(companion_name) = &class.companion {
                let companion_fqn = format!("{}.{}", scope.fqn, companion_name);
                if let Some(companion) = file.classes.get(&companion_fqn) {
                    // Check containing class members
                    if let Some(resolved) = self.resolve_member_type_in_class(&companion_fqn, name)
                    {
                        return Some(resolved);
                    }

                    // Check containing class inheritance
                    if let Some(super_class) = &companion.super_class
                        && let Some(resolved) = self.resolve_member_type_in_super_type(
                            super_class,
                            name,
                            scope.fqn.as_str(),
                            file,
                        )
                    {
                        return Some(resolved);
                    }

                    for interface in &companion.super_interfaces {
                        if let Some(resolved) = self.resolve_member_type_in_super_type(
                            interface,
                            name,
                            scope.fqn.as_str(),
                            file,
                        ) {
                            return Some(resolved);
                        }
                    }
                }
            }

            // Check super class first
            if let Some(super_class) = &class.super_class
                && let Some(resolved) = self.resolve_member_type_in_super_type(
                    super_class,
                    name,
                    scope.fqn.as_str(),
                    file,
                )
            {
                return Some(resolved);
            }

            // Then check super interfaces
            for interface in &class.super_interfaces {
                if let Some(resolved) = self.resolve_member_type_in_super_type(
                    interface,
                    name,
                    scope.fqn.as_str(),
                    file,
                ) {
                    return Some(resolved);
                }
            }
        }

        None
    }

    fn resolve_member_type_in_super_type(
        &self,
        super_type: &str,
        name: &str,
        class_fqn: &str,
        file: &KotlinFile,
    ) -> Option<ResolvedType> {
        if let Some(resolved_type) =
            self.resolve_type_reference(super_type, Some(class_fqn), &file.file_path)
            && let ResolvedType::Definition(definition) = resolved_type
        {
            return self.resolve_member_type_in_class(&definition.fqn, name);
        }

        None
    }

    fn resolve_member_type_in_class(&self, type_fqn: &str, name: &str) -> Option<ResolvedType> {
        debug!(
            "Resolving Kotlin field access {} in target {}.",
            name, type_fqn
        );
        // Find the file containing this type
        let target_file_path = self.definition_nodes.get(type_fqn)?.file_path.clone();
        let target_file = self.files.get(target_file_path.as_ref())?;

        // First check if the member is an enum entry
        let potential_fqn = format!("{type_fqn}.{name}");
        if let Some(enum_fqn) = target_file.enum_entries_by_enum.get(potential_fqn.as_str()) {
            let enum_class = target_file.classes.get(enum_fqn)?;

            return Some(ResolvedType::Definition(DefinitionResolution {
                name: enum_class.name.clone(),
                fqn: enum_class.fqn.clone(),
            }));
        }

        // Then check if the member is child class of the type
        if let Some(definition) = self.definition_nodes.get(&potential_fqn) {
            return Some(ResolvedType::Definition(DefinitionResolution {
                name: definition.name().to_string(),
                fqn: definition.fqn.to_string(),
            }));
        }

        // Look for the member in the type's scope
        if let Some(scope) = target_file.scopes.get(type_fqn) {
            // Check unique definitions
            if let Some(binding) = scope.definition_map.unique_definitions.get(name) {
                let current_class_fqn = match target_file.get_scope_context(scope) {
                    ScopeContext::Class => Some(scope.fqn.as_str()),
                    _ => None,
                };

                return self.resolve_binding_type(binding, current_class_fqn, target_file);
            }

            // Not there, let's check the super types
            if let Some(class) = target_file.classes.get(type_fqn) {
                if let Some(super_class) = &class.super_class
                    && let Some(resolved) = self.resolve_member_type_in_super_type(
                        super_class,
                        name,
                        class.fqn.as_str(),
                        target_file,
                    )
                {
                    return Some(resolved);
                }

                for interface in &class.super_interfaces {
                    if let Some(resolved) = self.resolve_member_type_in_super_type(
                        interface,
                        name,
                        class.fqn.as_str(),
                        target_file,
                    ) {
                        return Some(resolved);
                    }
                }
            }
        }

        None
    }

    fn resolve_type_reference(
        &self,
        type_name: &str,
        class_fqn: Option<&str>,
        file_path: &str,
    ) -> Option<ResolvedType> {
        debug!(
            "Resolving Kotlin type {} in file {} in class {}.",
            type_name,
            file_path,
            class_fqn.unwrap_or("N/A")
        );
        // if type name first letter is a lowercase, it's a FQN.
        if let Some(first_letter) = type_name.chars().next()
            && first_letter.is_lowercase()
        {
            return self.resolve_fully_qualified_type(type_name);
        }

        // attempt to resolve the type in the class hierarchy
        if let Some(class_fqn) = class_fqn {
            let file = self.files.get(file_path)?;

            if let Some(parent_scope) = file.scope_hierarchy.get(class_fqn) {
                let mut current_scope = file.scopes.get(parent_scope);
                while let Some(scope) = current_scope {
                    let potential_fqn = format!("{}.{}", scope.fqn, type_name);
                    // Let's avoid resolving returning the class itself as the type.
                    if potential_fqn != class_fqn
                        && let Some(class) = file.classes.get(&potential_fqn)
                    {
                        return Some(ResolvedType::Definition(DefinitionResolution {
                            name: class.name.clone(),
                            fqn: class.fqn.clone(),
                        }));
                    }

                    if let Some(parent_scope_fqn) = file.scope_hierarchy.get(&scope.fqn) {
                        current_scope = file.scopes.get(parent_scope_fqn);
                    } else {
                        current_scope = None;
                    }
                }
            }
        }

        // if type name first letter is a uppercase, it's a class name
        self.resolve_class_name(type_name, file_path)
    }

    // ex: java.util.List
    fn resolve_fully_qualified_type(&self, type_name: &str) -> Option<ResolvedType> {
        if let Some(definition) = self.definition_nodes.get(type_name) {
            return Some(ResolvedType::Definition(DefinitionResolution {
                name: definition.name().to_string(),
                fqn: definition.fqn.to_string(),
            }));
        }

        None
    }

    // ex: Map, Map.Entry, Map.Entry.Key
    fn resolve_class_name(&self, type_name: &str, file_path: &str) -> Option<ResolvedType> {
        let parts = type_name.split('.').collect::<Vec<&str>>();
        let file = self.files.get(file_path)?;

        let mut parent_symbol_file = None;
        if let Some(parent_symbol) = parts.clone().first() {
            // Check the current package first
            let potential_fqn = format!("{}.{}", file.package_name, parent_symbol);
            if let Some(definition) = self.definition_nodes.get(&potential_fqn) {
                parent_symbol_file = self.files.get(definition.file_path.as_ref());
            }

            // Check imported symbols
            if let Some(import_path) = file.imported_symbols.get(*parent_symbol) {
                if let Some(imported_definition) = self.definition_nodes.get(import_path)
                    && let Some(file) = self.files.get(imported_definition.file_path.as_ref())
                {
                    parent_symbol_file = Some(file);
                } else {
                    if let Some(imported_symbol_node) = file.import_nodes.get(import_path) {
                        return Some(ResolvedType::Import(ImportResolution {
                            name: imported_symbol_node
                                .identifier
                                .as_ref()
                                .map(|id| id.name.clone()),
                            location: imported_symbol_node.location.clone(),
                        }));
                    }

                    return None;
                }
            }

            // Check wildcard imports
            for wildcard_import in &file.wildcard_imports {
                let full_import_path = format!("{wildcard_import}.{parent_symbol}");
                if let Some(definition) = self.definition_nodes.get(&full_import_path) {
                    parent_symbol_file = self.files.get(definition.file_path.as_ref());
                }
            }
        }

        if let Some(parent_symbol_file) = parent_symbol_file {
            let potential_fqn = format!("{}.{}", parent_symbol_file.package_name, type_name);
            if let Some(definition) = self.definition_nodes.get(&potential_fqn) {
                return Some(ResolvedType::Definition(DefinitionResolution {
                    name: definition.name().to_string(),
                    fqn: definition.fqn.to_string(),
                }));
            }
        }

        None
    }

    pub fn resolve_binding_type(
        &self,
        binding: &KotlinBinding,
        class_fqn: Option<&str>,
        file: &KotlinFile,
    ) -> Option<ResolvedType> {
        if let Some(binding_type) = &binding.binding_type
            && let Some(resolved) =
                self.resolve_type_reference(binding_type, class_fqn, &file.file_path)
        {
            return Some(resolved);
        } else if let Some(init) = &binding.init {
            return self.resolve_expression(&file.file_path, init, &mut Resolutions::default());
        }

        None
    }

    fn resolve_type_from_imports(
        &self,
        file_path: &str,
        name: &str,
        resolutions: &mut Resolutions,
    ) -> Option<ResolvedType> {
        let file = self.files.get(file_path)?;

        // First look at the imported symbols
        if let Some(import_path) = file.imported_symbols.get(name) {
            if let Some(definition) = self.definition_nodes.get(import_path) {
                // If the definition is a property, resolve the type of the property.

                let definition_file = self.files.get(definition.file_path.as_ref())?;
                if matches!(definition.kind, code_graph_types::DefKind::Property) {
                    if let Some(binding) = definition_file.constants.get(import_path) {
                        return self.resolve_binding_type(binding, None, definition_file);
                    }
                } else if matches!(definition.kind, code_graph_types::DefKind::Function) {
                    if let Some(function) = definition_file.functions.get(import_path) {
                        resolutions
                            .definition_resolutions
                            .push(DefinitionResolution {
                                name: function.name.clone(),
                                fqn: function.fqn.clone(),
                            });

                        if let Some(return_type) = &function.return_type {
                            return self.resolve_type_reference(
                                return_type,
                                Some(&function.fqn),
                                file_path,
                            );
                        } else if let Some(init) = &function.init {
                            return self.resolve_expression(
                                &definition_file.file_path,
                                init,
                                &mut Resolutions::default(),
                            );
                        }

                        return None; // The function returns Unit.
                    }
                } else {
                    // Otherwise, resolve the definition constructor directly or the definition class itself.
                    let potential_constructor_fqn = format!("{}.{}", definition.fqn, "<init>");
                    if let Some(constructor) = self.definition_nodes.get(&potential_constructor_fqn)
                    {
                        resolutions
                            .definition_resolutions
                            .push(DefinitionResolution {
                                name: constructor.name().to_string(),
                                fqn: constructor.fqn.to_string(),
                            });

                        // Resolve to the definition class itself.
                        return Some(ResolvedType::Definition(DefinitionResolution {
                            name: definition.name().to_string(),
                            fqn: definition.fqn.to_string(),
                        }));
                    }

                    resolutions
                        .definition_resolutions
                        .push(DefinitionResolution {
                            name: definition.name().to_string(),
                            fqn: definition.fqn.to_string(),
                        });

                    return Some(ResolvedType::Definition(DefinitionResolution {
                        name: definition.name().to_string(),
                        fqn: definition.fqn.to_string(),
                    }));
                }
            } else if let Some(imported_symbol) = file.import_nodes.get(import_path) {
                let name = imported_symbol
                    .identifier
                    .as_ref()
                    .map(|id| id.name.clone());
                resolutions.import_resolutions.push(ImportResolution {
                    name: name.clone(),
                    location: imported_symbol.location.clone(),
                });

                return Some(ResolvedType::Import(ImportResolution {
                    name,
                    location: imported_symbol.location.clone(),
                }));
            }
        }

        // Then look at the wildcard imports
        for wildcard_import in file.wildcard_imports.iter() {
            let full_import_path = format!("{wildcard_import}.{name}");
            if let Some(definition) = self.definition_nodes.get(&full_import_path) {
                // If the definition is a property, resolve the type of the property.
                let definition_file = self.files.get(definition.file_path.as_ref())?;
                if matches!(definition.kind, code_graph_types::DefKind::Property) {
                    if let Some(binding) = definition_file.constants.get(&full_import_path) {
                        return self.resolve_binding_type(binding, None, definition_file);
                    }
                } else if matches!(definition.kind, code_graph_types::DefKind::Function) {
                    if let Some(function) = definition_file.functions.get(&full_import_path) {
                        resolutions
                            .definition_resolutions
                            .push(DefinitionResolution {
                                name: function.name.clone(),
                                fqn: function.fqn.clone(),
                            });

                        if let Some(return_type) = &function.return_type {
                            return self.resolve_type_reference(
                                return_type,
                                Some(&function.fqn),
                                file_path,
                            );
                        } else if let Some(init) = &function.init {
                            return self.resolve_expression(
                                &definition_file.file_path,
                                init,
                                &mut Resolutions::default(),
                            );
                        }

                        return Some(ResolvedType::Unit); // The function returns Unit.
                    }
                } else {
                    // Otherwise, resolve the definition directly.
                    return Some(ResolvedType::Definition(DefinitionResolution {
                        name: definition.name().to_string(),
                        fqn: definition.fqn.to_string(),
                    }));
                }
            }
        }

        None
    }

    fn resolve_common_ancestor_type(
        &self,
        types: Vec<DefinitionResolution>,
    ) -> Option<ResolvedType> {
        if types.is_empty() {
            return None;
        }

        if types.len() == 1 {
            let only = types.first().unwrap().clone();
            return Some(ResolvedType::Definition(only));
        }

        let first = &types[0];
        let first_chain = self.collect_ancestors_in_order(&first.fqn);

        // Build ancestor sets for the remaining types for quick membership tests
        let mut other_sets: Vec<FxHashSet<String>> = Vec::new();
        for t in types.iter().skip(1) {
            let set: FxHashSet<String> = self
                .collect_ancestors_in_order(&t.fqn)
                .into_iter()
                .collect();
            other_sets.push(set);
        }

        for candidate_fqn in first_chain {
            if other_sets.iter().all(|set| set.contains(&candidate_fqn))
                && let Some(def_node) = self.definition_nodes.get(&candidate_fqn)
            {
                return Some(ResolvedType::Definition(DefinitionResolution {
                    name: def_node.name().to_string(),
                    fqn: def_node.fqn.to_string(),
                }));
            }
        }

        None
    }

    fn collect_ancestors_in_order(&self, start_fqn: &str) -> Vec<String> {
        let mut order: Vec<String> = Vec::new();
        let mut visited: FxHashSet<String> = FxHashSet::default();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(start_fqn.to_string());

        while let Some(current_fqn) = queue.pop_front() {
            if !visited.insert(current_fqn.clone()) {
                continue;
            }

            order.push(current_fqn.clone());

            // Lookup class info to traverse its super types
            let def_node = match self.definition_nodes.get(&current_fqn) {
                Some(node) => node,
                None => continue,
            };

            let file_path = def_node.file_path.clone();
            let file = match self.files.get(file_path.as_ref()) {
                Some(f) => f,
                None => continue,
            };

            let class = match file.classes.get(&current_fqn) {
                Some(c) => c,
                None => continue,
            };

            // Enqueue superclass first (if any), then interfaces
            if let Some(super_class_name) = &class.super_class
                && let Some(ResolvedType::Definition(res)) = self.resolve_type_reference(
                    super_class_name,
                    Some(class.fqn.as_str()),
                    &file_path,
                )
                && !visited.contains(&res.fqn)
            {
                queue.push_back(res.fqn.clone());
            }

            for interface_name in &class.super_interfaces {
                if let Some(ResolvedType::Definition(res)) = self.resolve_type_reference(
                    interface_name,
                    Some(class.fqn.as_str()),
                    &file_path,
                ) && !visited.contains(&res.fqn)
                {
                    queue.push_back(res.fqn.clone());
                }
            }
        }

        order
    }

    fn enter_fqn_guard(&self, fqn: &str) -> Option<FqnGuard<'_>> {
        let mut set = self.context_resolution_fqns.borrow_mut();
        if !set.insert(fqn.to_string()) {
            return None;
        }
        Some(FqnGuard {
            set: &self.context_resolution_fqns,
            fqn: fqn.to_string(),
        })
    }

    pub fn add_file(&mut self, package_name: String, file_path: String) {
        if !self.files.contains_key(&file_path) {
            self.files.insert(
                file_path.clone(),
                KotlinFile::new(package_name.clone(), file_path.clone()),
            );
        } else {
            self.files.get_mut(&file_path).unwrap().package_name = package_name.clone();
        }

        self.package_files
            .entry(package_name.clone())
            .or_default()
            .push(file_path.clone());
    }

    pub fn add_definition(
        &mut self,
        file_path: String,
        definition: KotlinDefinitionInfo,
        definition_node: DefinitionNode,
    ) {
        if !self.files.contains_key(&file_path) {
            self.files.insert(
                file_path.clone(),
                KotlinFile::new_in_unknown_package(file_path.clone()),
            );
        }

        let fqn = kotlin_fqn_to_string(&definition.fqn);
        match definition.definition_type {
            KotlinDefinitionType::Class
            | KotlinDefinitionType::ValueClass
            | KotlinDefinitionType::AnnotationClass
            | KotlinDefinitionType::DataClass
            | KotlinDefinitionType::Enum
            | KotlinDefinitionType::EnumEntry
            | KotlinDefinitionType::Interface
            | KotlinDefinitionType::Object
            | KotlinDefinitionType::CompanionObject
            | KotlinDefinitionType::Constructor
            | KotlinDefinitionType::Property
            | KotlinDefinitionType::Lambda => {
                self.definition_nodes.insert(fqn.clone(), definition_node);
            }
            KotlinDefinitionType::Function => {
                if !STD_MEMBER_FUNCTIONS.contains(&definition.name.as_str()) {
                    self.function_registry
                        .entry(definition.name.clone())
                        .or_default()
                        .push(definition_node.clone());
                }

                self.definition_nodes.insert(fqn.clone(), definition_node);
            }
            _ => {}
        }

        self.files
            .get_mut(&file_path)
            .unwrap()
            .index_definition(&definition);
    }

    pub fn add_import(&mut self, file_path: String, imported_symbol: &ImportedSymbolNode) {
        if !self.files.contains_key(&file_path) {
            self.files.insert(
                file_path.clone(),
                KotlinFile::new_in_unknown_package(file_path.clone()),
            );
        }

        let file = self.files.get_mut(&file_path).unwrap();

        if matches!(
            imported_symbol.import_type,
            ImportType::Kotlin(KotlinImportType::WildcardImport)
        ) {
            file.wildcard_imports
                .insert(imported_symbol.import_path.clone());
            file.import_nodes
                .insert(imported_symbol.import_path.clone(), imported_symbol.clone());
        } else {
            let (name, import_path) = full_import_path(imported_symbol);
            file.imported_symbols.insert(name, import_path.clone());
            file.import_nodes
                .insert(import_path, imported_symbol.clone());
        }
    }
}
