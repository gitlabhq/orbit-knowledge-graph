use parser_core::kotlin::ast::kotlin_fqn_to_string;
use parser_core::kotlin::types::KotlinDefinitionInfo;
use parser_core::kotlin::types::KotlinDefinitionMetadata;
use parser_core::kotlin::types::KotlinDefinitionType;
use parser_core::kotlin::types::KotlinExpressionInfo;
use parser_core::kotlin::types::KotlinFqn;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;

use crate::analysis::languages::kotlin::types::KotlinScopeTree;
use crate::analysis::languages::kotlin::types::ScopeContext;
use crate::analysis::types::ImportedSymbolNode;

#[derive(Debug, Clone)]
pub(crate) struct KotlinBinding {
    pub range: (usize, usize),
    pub receiver_type: Option<String>,
    pub binding_type: Option<String>,
    pub init: Option<KotlinExpressionInfo>,
}

impl KotlinBinding {
    pub fn is_extension_field(&self, class_name: &str) -> bool {
        if let Some(receiver_type) = &self.receiver_type {
            return receiver_type == class_name;
        }

        false
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KotlinFunction {
    pub name: String,
    pub fqn: String,
    pub receiver_type: Option<String>,
    pub return_type: Option<String>,
    pub init: Option<KotlinExpressionInfo>,
}

impl KotlinFunction {
    pub fn is_extension_function(&self, class_name: &str) -> bool {
        if let Some(receiver_type) = &self.receiver_type {
            return receiver_type == class_name;
        }

        false
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KotlinClass {
    pub name: String,
    pub fqn: String,
    pub companion: Option<String>,
    pub super_class: Option<String>,
    pub super_interfaces: FxHashSet<String>,
}

pub(crate) struct KotlinFile {
    pub package_name: String,
    pub file_path: String,
    /// Full import path -> ImportedSymbolNode
    pub import_nodes: FxHashMap<String, ImportedSymbolNode>,
    /// Imported symbol -> Full import path (e.g. "Outer" -> "com.example.util.Outer")
    pub imported_symbols: FxHashMap<String, String>,
    /// Imported packages (e.g. "com.example.util.*")
    pub wildcard_imports: FxHashSet<String>,
    /// FQN -> class (Includes enum constants)
    pub classes: FxHashMap<String, KotlinClass>,
    /// FQN -> functions
    pub functions: FxHashMap<String, KotlinFunction>,
    /// FQN -> constants
    pub constants: FxHashMap<String, KotlinBinding>,
    /// FQN -> enum FQN
    pub enum_entries_by_enum: FxHashMap<String, String>,
    /// FQN -> Scope
    pub scopes: FxHashMap<String, KotlinScopeTree>,
    /// FQN -> parent_scope
    pub scope_hierarchy: FxHashMap<String, String>,
}

impl KotlinFile {
    pub fn new(package_name: String, file_path: String) -> Self {
        Self {
            package_name,
            file_path,
            import_nodes: FxHashMap::default(),
            imported_symbols: FxHashMap::default(),
            wildcard_imports: FxHashSet::default(),
            classes: FxHashMap::default(),
            functions: FxHashMap::default(),
            constants: FxHashMap::default(),
            enum_entries_by_enum: FxHashMap::default(),
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
            functions: FxHashMap::default(),
            constants: FxHashMap::default(),
            enum_entries_by_enum: FxHashMap::default(),
            scopes: FxHashMap::default(),
            scope_hierarchy: FxHashMap::default(),
        }
    }

    pub fn index_definition(&mut self, definition: &KotlinDefinitionInfo) {
        match definition.definition_type {
            KotlinDefinitionType::Class
            | KotlinDefinitionType::DataClass
            | KotlinDefinitionType::Enum
            | KotlinDefinitionType::Object
            | KotlinDefinitionType::Interface
            | KotlinDefinitionType::AnnotationClass
            | KotlinDefinitionType::ValueClass => self.index_class(definition),
            KotlinDefinitionType::EnumEntry => self.index_enum_entry(definition),
            KotlinDefinitionType::CompanionObject => self.index_companion_object(definition),
            KotlinDefinitionType::Function
            | KotlinDefinitionType::Constructor
            | KotlinDefinitionType::Lambda => self.index_function(definition),
            KotlinDefinitionType::Property
            | KotlinDefinitionType::LocalVariable
            | KotlinDefinitionType::Parameter => self.index_binding(definition),
            _ => (),
        }
    }

    fn index_enum_entry(&mut self, definition: &KotlinDefinitionInfo) {
        let mut parent_class_fqn = "".to_string();
        for i in 0..definition.fqn.len() - 1 {
            if i != 0 {
                parent_class_fqn.push('.');
            }
            parent_class_fqn.push_str(&definition.fqn[i].node_name);
        }

        self.enum_entries_by_enum
            .insert(kotlin_fqn_to_string(&definition.fqn), parent_class_fqn);
        self.index_scope(definition.fqn.clone(), false);
    }

    fn index_companion_object(&mut self, definition: &KotlinDefinitionInfo) {
        let mut parent_class_fqn = "".to_string();
        for i in 0..definition.fqn.len() - 1 {
            if i != 0 {
                parent_class_fqn.push('.');
            }
            parent_class_fqn.push_str(&definition.fqn[i].node_name);
        }

        if let Some(parent_class) = self.classes.get_mut(&parent_class_fqn) {
            parent_class.companion = Some(definition.name.clone());
        }

        self.index_class(definition);
    }

    fn index_class(&mut self, definition: &KotlinDefinitionInfo) {
        let (super_class, super_interfaces) = match &definition.metadata {
            Some(KotlinDefinitionMetadata::Class {
                super_class,
                super_interfaces,
            }) => (super_class, super_interfaces),
            _ => (&None, &Vec::new()),
        };

        let class = KotlinClass {
            name: definition.name.clone(),
            fqn: kotlin_fqn_to_string(&definition.fqn.clone()),
            companion: None,
            super_class: super_class.clone(),
            super_interfaces: super_interfaces.iter().cloned().collect(),
        };

        self.index_scope(definition.fqn.clone(), true);
        self.classes
            .insert(kotlin_fqn_to_string(&definition.fqn), class);
    }

    fn index_function(&mut self, definition: &KotlinDefinitionInfo) {
        let (receiver_type, return_type, init) = match &definition.metadata {
            Some(KotlinDefinitionMetadata::Function {
                receiver,
                return_type,
                init,
            }) => (receiver, return_type, init),
            _ => (&None, &None, &None),
        };

        let function = KotlinFunction {
            name: definition.name.clone(),
            fqn: kotlin_fqn_to_string(&definition.fqn),
            receiver_type: receiver_type.clone(),
            return_type: return_type.clone(),
            init: init.clone(),
        };

        self.index_scope(definition.fqn.clone(), true);
        self.functions
            .insert(kotlin_fqn_to_string(&definition.fqn), function);
    }

    fn index_binding(&mut self, definition: &KotlinDefinitionInfo) {
        let (receiver_type, binding_type, init, range) = match &definition.metadata {
            Some(KotlinDefinitionMetadata::Field {
                receiver,
                field_type,
                init,
                range,
            }) => (receiver, field_type, init, range),
            Some(KotlinDefinitionMetadata::Parameter {
                parameter_type,
                range,
            }) => (&None, &Some(parameter_type.clone()), &None, range),
            _ => return,
        };

        let binding = KotlinBinding {
            range: (range.byte_offset.0, range.byte_offset.1),
            receiver_type: receiver_type.clone(),
            binding_type: binding_type.clone(),
            init: init.clone(),
        };

        self.index_scope(definition.fqn.clone(), false);

        if let Some(scope) = self.get_enclosing_scope_mut(definition.fqn.clone()) {
            scope.add_binding(definition.name.clone(), binding.clone());

            if matches!(definition.definition_type, KotlinDefinitionType::Property) {
                let parent_scope = scope.fqn.clone();
                let property_scope = format!("{}.{}", parent_scope, definition.name);

                let range = (
                    definition.fqn.last().unwrap().range.byte_offset.0,
                    definition.fqn.last().unwrap().range.byte_offset.1,
                );

                self.scopes.insert(
                    property_scope.clone(),
                    KotlinScopeTree::new(property_scope.clone(), range),
                );
                self.scope_hierarchy
                    .insert(property_scope.clone(), parent_scope.clone());

                self.constants
                    .insert(kotlin_fqn_to_string(&definition.fqn), binding.clone());
            }
        }
    }

    fn index_scope(&mut self, fqn: KotlinFqn, include_self: bool) {
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
                let scope = KotlinScopeTree::new(
                    scope_name.clone(),
                    (part.range.byte_offset.0, part.range.byte_offset.1),
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

    fn get_enclosing_scope_mut(&mut self, fqn: KotlinFqn) -> Option<&mut KotlinScopeTree> {
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

        if let Some(last_known_scope) = last_known_scope {
            self.scopes.get_mut(&last_known_scope)
        } else {
            None
        }
    }

    pub fn get_scope_at_offset(&self, offset: usize) -> Option<&KotlinScopeTree> {
        let mut result: Option<&KotlinScopeTree> = None;
        let mut current_range_distance = usize::MAX;

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

    // FIXME: Fold in scope creation
    pub fn get_scope_context(&self, scope: &KotlinScopeTree) -> ScopeContext {
        // Check if this scope corresponds to a function with receiver type
        if let Some(function) = self.functions.get(&scope.fqn) {
            if let Some(receiver_type) = &function.receiver_type {
                return ScopeContext::ExtensionFunction(receiver_type.clone());
            } else {
                return ScopeContext::Function;
            }
        }

        // Check if this scope corresponds to a class
        if self.classes.contains_key(&scope.fqn) {
            return ScopeContext::Class;
        }

        ScopeContext::Other
    }

    pub fn get_parent_scope(&self, scope: &KotlinScopeTree) -> Option<&KotlinScopeTree> {
        if let Some(parent_scope_name) = self.scope_hierarchy.get(&scope.fqn) {
            self.scopes.get(parent_scope_name)
        } else {
            None
        }
    }
}
