use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, IntoEnumIterator};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter, AsRefStr)]
pub enum RelationshipType {
    // Directory relationships
    #[strum(serialize = "DIR_CONTAINS_DIR")]
    DirContainsDir,
    #[strum(serialize = "DIR_CONTAINS_FILE")]
    DirContainsFile,
    // File relationships
    #[strum(serialize = "FILE_DEFINES")]
    FileDefines,
    #[strum(serialize = "FILE_IMPORTS")]
    FileImports,
    // Definition-imported-symbol relationships
    #[strum(serialize = "DEFINES_IMPORTED_SYMBOL")]
    DefinesImportedSymbol,
    // Definition relationships - Module
    #[strum(serialize = "MODULE_TO_METHOD")]
    ModuleToMethod,
    #[strum(serialize = "MODULE_TO_SINGLETON_METHOD")]
    ModuleToSingletonMethod,
    #[strum(serialize = "MODULE_TO_CLASS")]
    ModuleToClass,
    #[strum(serialize = "MODULE_TO_MODULE")]
    ModuleToModule,
    // Definition relationships - Class
    #[strum(serialize = "CLASS_TO_METHOD")]
    ClassToMethod,
    #[strum(serialize = "CLASS_TO_SINGLETON_METHOD")]
    ClassToSingletonMethod,
    #[strum(serialize = "CLASS_TO_CLASS")]
    ClassToClass,
    #[strum(serialize = "CLASS_TO_LAMBDA")]
    ClassToLambda,
    #[strum(serialize = "CLASS_TO_PROC")]
    ClassToProc,
    #[strum(serialize = "CLASS_TO_INTERFACE")]
    ClassToInterface,
    #[strum(serialize = "CLASS_TO_PROPERTY")]
    ClassToProperty,
    #[strum(serialize = "CLASS_TO_CONSTRUCTOR")]
    ClassToConstructor,
    #[strum(serialize = "CLASS_TO_ENUM_ENTRY")]
    ClassToEnumEntry,
    // Definition relationships - Function
    #[strum(serialize = "FUNCTION_TO_FUNCTION")]
    FunctionToFunction,
    #[strum(serialize = "FUNCTION_TO_CLASS")]
    FunctionToClass,
    #[strum(serialize = "FUNCTION_TO_LAMBDA")]
    FunctionToLambda,
    #[strum(serialize = "FUNCTION_TO_PROC")]
    FunctionToProc,
    // Definition relationships - Lambda
    #[strum(serialize = "LAMBDA_TO_LAMBDA")]
    LambdaToLambda,
    #[strum(serialize = "LAMBDA_TO_CLASS")]
    LambdaToClass,
    #[strum(serialize = "LAMBDA_TO_FUNCTION")]
    LambdaToFunction,
    #[strum(serialize = "LAMBDA_TO_PROC")]
    LambdaToProc,
    #[strum(serialize = "LAMBDA_TO_METHOD")]
    LambdaToMethod,
    #[strum(serialize = "LAMBDA_TO_PROPERTY")]
    LambdaToProperty,
    #[strum(serialize = "LAMBDA_TO_INTERFACE")]
    LambdaToInterface,
    // Definition relationships - Method
    #[strum(serialize = "METHOD_TO_METHOD")]
    MethodToMethod,
    #[strum(serialize = "METHOD_TO_CLASS")]
    MethodToClass,
    #[strum(serialize = "METHOD_TO_FUNCTION")]
    MethodToFunction,
    #[strum(serialize = "METHOD_TO_LAMBDA")]
    MethodToLambda,
    #[strum(serialize = "METHOD_TO_PROC")]
    MethodToProc,
    #[strum(serialize = "METHOD_TO_PROPERTY")]
    MethodToProperty,
    #[strum(serialize = "METHOD_TO_INTERFACE")]
    MethodToInterface,
    // Interface relationships
    #[strum(serialize = "INTERFACE_TO_INTERFACE")]
    InterfaceToInterface,
    #[strum(serialize = "INTERFACE_TO_CLASS")]
    InterfaceToClass,
    #[strum(serialize = "INTERFACE_TO_METHOD")]
    InterfaceToMethod,
    #[strum(serialize = "INTERFACE_TO_FUNCTION")]
    InterfaceToFunction,
    #[strum(serialize = "INTERFACE_TO_PROPERTY")]
    InterfaceToProperty,
    #[strum(serialize = "INTERFACE_TO_LAMBDA")]
    InterfaceToLambda,
    // Reference relationships
    #[strum(serialize = "CALLS")]
    Calls,
    #[strum(serialize = "AMBIGUOUSLY_CALLS")]
    AmbiguouslyCalls,
    #[strum(serialize = "PROPERTY_REFERENCE")]
    PropertyReference,
    // Imported symbol relationships
    #[strum(serialize = "IMPORTED_SYMBOL_TO_IMPORTED_SYMBOL")]
    ImportedSymbolToImportedSymbol,
    #[strum(serialize = "IMPORTED_SYMBOL_TO_DEFINITION")]
    ImportedSymbolToDefinition,
    #[strum(serialize = "IMPORTED_SYMBOL_TO_FILE")]
    ImportedSymbolToFile,
    #[strum(serialize = "EMPTY")]
    Empty,
}

impl RelationshipType {
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    pub fn as_string(&self) -> String {
        self.as_str().to_string()
    }

    pub fn all_types() -> Vec<RelationshipType> {
        RelationshipType::iter().collect()
    }

    /// Maps a fine-grained relationship type to the ontology edge label
    /// stored in the `relationship_kind` column of `gl_edge`.
    pub fn edge_kind(&self) -> &'static str {
        match self {
            Self::DirContainsDir | Self::DirContainsFile => "CONTAINS",

            Self::FileDefines
            | Self::DefinesImportedSymbol
            | Self::ModuleToMethod
            | Self::ModuleToSingletonMethod
            | Self::ModuleToClass
            | Self::ModuleToModule
            | Self::ClassToMethod
            | Self::ClassToSingletonMethod
            | Self::ClassToClass
            | Self::ClassToLambda
            | Self::ClassToProc
            | Self::ClassToInterface
            | Self::ClassToProperty
            | Self::ClassToConstructor
            | Self::ClassToEnumEntry
            | Self::FunctionToFunction
            | Self::FunctionToClass
            | Self::FunctionToLambda
            | Self::FunctionToProc
            | Self::LambdaToLambda
            | Self::LambdaToClass
            | Self::LambdaToFunction
            | Self::LambdaToProc
            | Self::LambdaToMethod
            | Self::LambdaToProperty
            | Self::LambdaToInterface
            | Self::MethodToMethod
            | Self::MethodToClass
            | Self::MethodToFunction
            | Self::MethodToLambda
            | Self::MethodToProc
            | Self::MethodToProperty
            | Self::MethodToInterface
            | Self::InterfaceToInterface
            | Self::InterfaceToClass
            | Self::InterfaceToMethod
            | Self::InterfaceToFunction
            | Self::InterfaceToProperty
            | Self::InterfaceToLambda => "DEFINES",

            Self::FileImports
            | Self::ImportedSymbolToImportedSymbol
            | Self::ImportedSymbolToDefinition
            | Self::ImportedSymbolToFile => "IMPORTS",

            Self::Calls | Self::AmbiguouslyCalls | Self::PropertyReference => "CALLS",

            Self::Empty => "EMPTY",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RelationshipKind {
    DirectoryToDirectory,
    DirectoryToFile,
    FileToDefinition,
    FileToImportedSymbol,
    DefinitionToDefinition,
    DefinitionToImportedSymbol,
    ImportedSymbolToImportedSymbol,
    ImportedSymbolToDefinition,
    ImportedSymbolToFile,
    #[default]
    Empty,
}

impl RelationshipKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::DirectoryToDirectory => "DIR_CONTAINS_DIR",
            Self::DirectoryToFile => "DIR_CONTAINS_FILE",
            Self::FileToDefinition => "FILE_DEFINES",
            Self::FileToImportedSymbol => "FILE_IMPORTS",
            Self::DefinitionToDefinition => "DEFINES_DEFINITION",
            Self::DefinitionToImportedSymbol => "DEFINES_IMPORTED_SYMBOL",
            Self::ImportedSymbolToImportedSymbol => "IMPORTED_SYMBOL_TO_IMPORTED_SYMBOL",
            Self::ImportedSymbolToDefinition => "IMPORTED_SYMBOL_TO_DEFINITION",
            Self::ImportedSymbolToFile => "IMPORTED_SYMBOL_TO_FILE",
            Self::Empty => "EMPTY",
        }
    }

    /// Returns the ontology node type names for the source and target of this relationship.
    pub fn source_target_kinds(&self) -> (&'static str, &'static str) {
        match self {
            Self::DirectoryToDirectory => ("Directory", "Directory"),
            Self::DirectoryToFile => ("Directory", "File"),
            Self::FileToDefinition => ("File", "Definition"),
            Self::FileToImportedSymbol => ("File", "ImportedSymbol"),
            Self::DefinitionToDefinition => ("Definition", "Definition"),
            Self::DefinitionToImportedSymbol => ("Definition", "ImportedSymbol"),
            Self::ImportedSymbolToImportedSymbol => ("ImportedSymbol", "ImportedSymbol"),
            Self::ImportedSymbolToDefinition => ("ImportedSymbol", "Definition"),
            Self::ImportedSymbolToFile => ("ImportedSymbol", "File"),
            Self::Empty => ("Unknown", "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relationship_type_mapping_iteration() {
        let mapping = RelationshipType::all_types();
        assert!(!mapping.is_empty());
        assert!(mapping.contains(&RelationshipType::DirContainsFile));
    }

    #[test]
    fn edge_kind_covers_all_variants() {
        for rt in RelationshipType::iter() {
            let kind = rt.edge_kind();
            assert!(
                ["CONTAINS", "DEFINES", "IMPORTS", "CALLS", "EMPTY"].contains(&kind),
                "{:?} mapped to unexpected edge kind: {}",
                rt,
                kind
            );
        }
    }

    #[test]
    fn source_target_kinds_covers_all_variants() {
        for rk in [
            RelationshipKind::DirectoryToDirectory,
            RelationshipKind::DirectoryToFile,
            RelationshipKind::FileToDefinition,
            RelationshipKind::FileToImportedSymbol,
            RelationshipKind::DefinitionToDefinition,
            RelationshipKind::DefinitionToImportedSymbol,
            RelationshipKind::ImportedSymbolToImportedSymbol,
            RelationshipKind::ImportedSymbolToDefinition,
            RelationshipKind::ImportedSymbolToFile,
            RelationshipKind::Empty,
        ] {
            let (src, tgt) = rk.source_target_kinds();
            assert!(!src.is_empty());
            assert!(!tgt.is_empty());
        }
    }
}
