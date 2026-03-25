use crate::{
    definitions::{DefinitionInfo, DefinitionTypeInfo},
    imports::{ImportTypeInfo, ImportedSymbolInfo},
    utils::Range,
};
use serde::{Deserialize, Serialize};

/// Types of Go definitions that can be extracted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GoDefinitionType {
    /// Function declaration (e.g., `func Add(a, b int) int`)
    Function,
    /// Method declaration (e.g., `func (p *Person) Greet() string`)
    Method,
    /// Struct type (e.g., `type Person struct { Name string }`)
    Struct,
    /// Interface type (e.g., `type Reader interface { Read() }`)
    Interface,
    /// Type alias or type definition (e.g., `type MyInt int`)
    Type,
    /// Constant declaration (e.g., `const Pi = 3.14`)
    Constant,
    /// Variable declaration (e.g., `var count int`)
    Variable,
}

impl DefinitionTypeInfo for GoDefinitionType {
    fn as_str(&self) -> &str {
        match self {
            GoDefinitionType::Function => "function",
            GoDefinitionType::Method => "method",
            GoDefinitionType::Struct => "struct",
            GoDefinitionType::Interface => "interface",
            GoDefinitionType::Type => "type",
            GoDefinitionType::Constant => "constant",
            GoDefinitionType::Variable => "variable",
        }
    }
}

/// Fully Qualified Name for Go definitions
///
/// Go FQN format:
/// - Functions: `package.FunctionName`
/// - Methods: `package.ReceiverType.MethodName`
/// - Structs/Interfaces: `package.TypeName`
/// - Constants/Variables: `package.Name`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GoFqn {
    /// Package name (e.g., "main", "net/http")
    pub package: Option<String>,
    /// Receiver type for methods (e.g., "Person", "*User")
    pub receiver: Option<String>,
    /// Name of the definition
    pub name: String,
}

/// Types of Go imports
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GoImportType {
    /// Standard library import (e.g., "fmt", "net/http")
    Standard,
    /// External/third-party package (e.g., "github.com/user/repo")
    External,
    /// Local project package (relative imports)
    Local,
}

impl ImportTypeInfo for GoImportType {
    fn as_str(&self) -> &str {
        match self {
            GoImportType::Standard => "standard",
            GoImportType::External => "external",
            GoImportType::Local => "local",
        }
    }
}

/// Parameter information for functions and methods
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoParameter {
    /// Parameter name (may be empty for unnamed parameters)
    pub name: String,
    /// Parameter type (e.g., "int", "string", "*User")
    pub param_type: String,
    /// Whether this is a variadic parameter (e.g., `...args`)
    pub is_variadic: bool,
}

/// Return type information for functions and methods
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoReturnType {
    /// Return type (e.g., "int", "error", "(int, error)")
    pub type_name: String,
    /// Optional name for named return values (e.g., `(result int, err error)`)
    pub name: Option<String>,
}

/// Type parameter for generics (Go 1.18+)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoTypeParameter {
    /// Type parameter name (e.g., "T", "K", "V")
    pub name: String,
    /// Type constraint (e.g., "any", "comparable", "CustomConstraint")
    pub constraint: String,
}

/// Struct field information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoStructField {
    /// Field name (empty for embedded types)
    pub name: String,
    /// Field type (e.g., "string", "*User", "[]int")
    pub field_type: String,
    /// Struct tag (e.g., `json:"name,omitempty"`)
    pub tag: Option<String>,
    /// Whether this is an embedded field
    pub is_embedded: bool,
}

/// Interface method specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoInterfaceMethod {
    /// Method name
    pub name: String,
    /// Method parameters
    pub parameters: Vec<GoParameter>,
    /// Return types
    pub return_types: Vec<GoReturnType>,
    /// Type parameters for generic methods
    pub type_parameters: Vec<GoTypeParameter>,
}

/// Function/method signature information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoSignature {
    /// Function/method parameters
    pub parameters: Vec<GoParameter>,
    /// Return types
    pub return_types: Vec<GoReturnType>,
    /// Type parameters for generic functions/methods (Go 1.18+)
    pub type_parameters: Vec<GoTypeParameter>,
}

/// Metadata for Go definitions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoDefinitionMetadata {
    /// Documentation comment (docstring) for the definition
    pub docstring: Option<String>,
    /// Function/method signature (for functions and methods)
    pub signature: Option<GoSignature>,
    /// Struct fields (for struct types)
    pub struct_fields: Option<Vec<GoStructField>>,
    /// Interface methods (for interface types)
    pub interface_methods: Option<Vec<GoInterfaceMethod>>,
    /// Type parameters (for generic types)
    pub type_parameters: Option<Vec<GoTypeParameter>>,
}

/// Type alias for Go definition information
pub type GoDefinitionInfo = DefinitionInfo<GoDefinitionType, GoFqn, GoDefinitionMetadata>;

/// Type alias for Go imported symbol information
pub type GoImportedSymbolInfo = ImportedSymbolInfo<GoImportType, GoFqn>;

/// Information about a Go reference (function call, variable usage, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoReferenceInfo {
    /// Name of the referenced symbol
    pub name: String,
    /// Location in the source code
    pub range: Range,
    /// Type of reference
    pub reference_type: GoReferenceType,
}

/// Types of Go references
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GoReferenceType {
    /// Function call (e.g., `fmt.Println()`)
    FunctionCall,
    /// Method call (e.g., `user.Greet()`)
    MethodCall,
    /// Struct instantiation (e.g., `Person{Name: "Alice"}`)
    StructInstantiation,
}
