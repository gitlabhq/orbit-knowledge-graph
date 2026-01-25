use parser_core::java::{
    ast::java_fqn_to_string,
    types::{
        JavaDefinitionInfo, JavaDefinitionMetadata, JavaDefinitionType, JavaExpression, JavaFqn,
    },
};

use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;

use crate::analysis::{languages::java::types::ScopeTree, types::ImportedSymbolNode};

#[derive(Debug, Clone)]
pub(crate) struct JavaBinding {
    pub range: (u64, u64),
    pub java_type: Option<String>,
    pub init: Option<JavaExpression>,
}

#[derive(Debug, Clone)]
pub(crate) struct JavaMethod {
    pub name: String,
    pub return_type: String,
}

#[derive(Debug, Clone)]

pub(crate) struct JavaClass {
    pub name: String,
    pub fqn: String,
    pub super_types: FxHashSet<String>,
}

pub(crate) struct JavaFile {
    pub package_name: String,
    pub file_path: String,
    /// Full import path -> ImportedSymbolNode
    pub import_nodes: FxHashMap<String, ImportedSymbolNode>,
    /// Imported symbol -> Import path (e.g. "Outer" -> "com.example.util.Outer")
    pub imported_symbols: FxHashMap<String, String>,
    /// Imported packages (e.g. "com.example.util.*")
    pub wildcard_imports: FxHashSet<String>,
    /// Class FQN -> Class
    pub classes: FxHashMap<String, JavaClass>,
    /// Enum name -> set of enum constant names declared in that enum (in this file)
    pub enum_constants_by_enum: FxHashMap<String, FxHashSet<String>>,
    /// Method FQN -> Method
    pub methods: FxHashMap<String, JavaMethod>,
    /// FQN -> Scope
    pub scopes: FxHashMap<String, ScopeTree>,
    /// Scope FQN -> parent_scope
    pub scope_hierarchy: FxHashMap<String, String>,
}

impl JavaFile {
    pub fn new(package_name: String, file_path: String) -> Self {
        Self {
            package_name,
            file_path,
            import_nodes: FxHashMap::default(),
            imported_symbols: FxHashMap::default(),
            wildcard_imports: FxHashSet::default(),
            classes: FxHashMap::default(),
            enum_constants_by_enum: FxHashMap::default(),
            methods: FxHashMap::default(),
            scopes: FxHashMap::default(),
            scope_hierarchy: FxHashMap::default(),
        }
    }

    pub fn new_in_unknown_package(file_path: String) -> Self {
        Self {
            file_path,
            package_name: String::new(),
            import_nodes: FxHashMap::default(),
            imported_symbols: FxHashMap::default(),
            wildcard_imports: FxHashSet::default(),
            classes: FxHashMap::default(),
            enum_constants_by_enum: FxHashMap::default(),
            methods: FxHashMap::default(),
            scopes: FxHashMap::default(),
            scope_hierarchy: FxHashMap::default(),
        }
    }

    pub fn index_definition(&mut self, definition: &JavaDefinitionInfo) {
        match definition.definition_type {
            JavaDefinitionType::Class
            | JavaDefinitionType::Record
            | JavaDefinitionType::Interface
            | JavaDefinitionType::Annotation
            | JavaDefinitionType::Enum => self.index_class(definition),
            JavaDefinitionType::EnumConstant => self.index_enum_constant(definition),
            JavaDefinitionType::Method
            | JavaDefinitionType::Lambda
            | JavaDefinitionType::AnnotationDeclaration
            | JavaDefinitionType::Constructor => self.index_method(definition),
            JavaDefinitionType::Field
            | JavaDefinitionType::LocalVariable
            | JavaDefinitionType::Parameter => self.index_binding(definition),
            JavaDefinitionType::Package => {}
        }
    }

    pub fn index_class(&mut self, definition: &JavaDefinitionInfo) {
        let super_types = match &definition.metadata {
            Some(JavaDefinitionMetadata::Class { super_types }) => {
                super_types.iter().map(|s| s.name.clone()).collect()
            }
            _ => FxHashSet::default(),
        };

        let class = JavaClass {
            name: definition.name.clone(),
            fqn: java_fqn_to_string(&definition.fqn),
            super_types,
        };

        self.index_scope(definition.fqn.clone(), true);
        self.classes.insert(class.fqn.clone(), class);
    }

    pub fn index_method(&mut self, definition: &JavaDefinitionInfo) {
        let return_type = match &definition.metadata {
            Some(JavaDefinitionMetadata::Method { return_type }) => return_type.clone(),
            _ => return,
        };

        let method = JavaMethod {
            name: definition.name.clone(),
            return_type: return_type.name.clone(),
        };

        self.index_scope(definition.fqn.clone(), true);
        self.methods
            .insert(java_fqn_to_string(&definition.fqn), method);
    }

    pub fn index_enum_constant(&mut self, definition: &JavaDefinitionInfo) {
        // Ensure the parent enum entry exists and record the constant name
        if definition.fqn.len() >= 2 {
            let parent_enum_name = definition.fqn[definition.fqn.len() - 2].node_name.clone();

            self.enum_constants_by_enum
                .entry(parent_enum_name)
                .or_default()
                .insert(definition.name.clone());
        }

        self.index_scope(definition.fqn.clone(), false);
    }

    pub fn index_binding(&mut self, definition: &JavaDefinitionInfo) {
        let (java_type, init) = match &definition.metadata {
            Some(JavaDefinitionMetadata::Field { field_type }) => (Some(field_type.clone()), None),
            Some(JavaDefinitionMetadata::LocalVariable {
                variable_type,
                init,
            }) => (variable_type.clone(), init.clone()),
            Some(JavaDefinitionMetadata::Parameter { parameter_type }) => {
                (Some(parameter_type.clone()), None)
            }
            _ => return,
        };

        let binding = JavaBinding {
            range: (
                definition.range.byte_offset.0 as u64,
                definition.range.byte_offset.1 as u64,
            ),
            java_type: java_type.map(|t| t.name.clone()),
            init,
        };

        // Ensure the parent scope is indexed.
        self.index_scope(definition.fqn.clone(), false);
        if let Some(scope) = self.get_enclosing_scope_mut(definition.fqn.clone()) {
            scope.add_binding(definition.name.clone(), binding);
        }
    }

    pub fn get_class_at_offset(&self, offset: u64) -> Option<&JavaClass> {
        let mut result: Option<&JavaClass> = None;
        let mut current_range_distance = u64::MAX;

        for scope in self.scopes.values() {
            if !self.classes.contains_key(&scope.fqn) {
                continue;
            }

            let class = self.classes.get(&scope.fqn).unwrap();
            if scope.range.0 <= offset && scope.range.1 >= offset {
                let range_distance = offset - scope.range.0;
                if range_distance < current_range_distance {
                    current_range_distance = range_distance;
                    result = Some(class);
                }
            }
        }

        result
    }

    pub fn get_scope_at_offset(&self, offset: u64) -> Option<&ScopeTree> {
        let mut result: Option<&ScopeTree> = None;
        let mut current_range_distance = u64::MAX;

        for scope in self.scopes.values() {
            if scope.range.0 <= offset && scope.range.1 >= offset {
                let range_distance = offset - scope.range.0;
                if range_distance < current_range_distance {
                    current_range_distance = range_distance;
                    result = Some(scope);
                }
            }
        }

        result
    }

    pub fn get_enclosing_scope_mut(&mut self, fqn: JavaFqn) -> Option<&mut ScopeTree> {
        let mut scope_name = String::new();
        let mut last_known_scope = None;
        for i in 0..fqn.len() {
            if i != 0 {
                scope_name.push('.');
            }

            scope_name.push_str(&fqn[i].node_name);
            if !self.scopes.contains_key(&scope_name) {
                break;
            }

            last_known_scope = Some(scope_name.clone());
        }

        match last_known_scope {
            Some(scope_name) => self.scopes.get_mut(&scope_name),
            None => None,
        }
    }

    pub fn index_scope(&mut self, fqn: JavaFqn, include_self: bool) {
        let mut parent_scope: Option<String> = None;
        let mut scope_name = String::new();

        for i in 0..(if include_self {
            fqn.len()
        } else {
            fqn.len() - 1
        }) {
            let part = &fqn[i];
            scope_name.push_str(&part.node_name);

            if !self.scopes.contains_key(&scope_name) {
                let scope = ScopeTree::new(
                    scope_name.clone(),
                    (
                        part.range.byte_offset.0 as u64,
                        part.range.byte_offset.1 as u64,
                    ),
                );

                self.scopes.insert(scope_name.clone(), scope);
            }

            if let Some(parent_scope) = parent_scope {
                self.scope_hierarchy
                    .insert(scope_name.clone(), parent_scope.clone());
            }

            parent_scope = Some(scope_name.clone());
            scope_name.push('.');
        }
    }
}
