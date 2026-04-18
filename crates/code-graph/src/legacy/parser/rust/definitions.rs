use crate::legacy::parser::definitions::DefinitionInfo;
use crate::legacy::parser::rust::types::{RustDefinitionType, RustFqn, RustFqnPartType};
use crate::utils::Range;
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// Represents a Rust definition found in the code
/// This is now a type alias using the generic DefinitionInfo with Rust-specific types
pub type RustDefinitionInfo = DefinitionInfo<RustDefinitionType, RustFqn>;

/// Map that stores definitions by their node ranges
/// This is populated during FQN traversal and used by the analyzer
pub type RustDefinitionsMap = FxHashMap<Range, RustDefinitionInfo>;

/// Create definition info from FQN data
/// This is called during FQN traversal when we encounter nodes that represent definitions
pub fn create_definition_from_fqn<'a>(
    _node: &Node<'a, StrDoc<SupportLang>>,
    fqn_part_type: RustFqnPartType,
    name: String,
    fqn: RustFqn,
    range: Range,
) -> Option<RustDefinitionInfo> {
    // Only create definitions for FQN part types that represent actual definitions
    if let Ok(definition_type) = RustDefinitionType::try_from(fqn_part_type) {
        Some(RustDefinitionInfo::new(definition_type, name, fqn, range))
    } else {
        None
    }
}

/// Extract definitions from the definitions map
/// This is called by the analyzer to get all definitions found during FQN traversal
pub fn extract_definitions_from_map(
    definitions_map: &RustDefinitionsMap,
) -> Vec<RustDefinitionInfo> {
    definitions_map.values().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::parser::parser::{GenericParser, LanguageParser, SupportedLanguage};
    use crate::legacy::parser::rust::fqn::{build_fqn_and_node_indices, rust_fqn_to_string};

    #[test]
    fn test_module_definitions_captured() {
        let rust_code = r#"
mod network {
    pub mod tcp {
        pub struct Connection;
    }
}

mod utils;
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let module_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Module)
            .collect();

        let module_names: Vec<&String> = module_defs.iter().map(|d| &d.name).collect();
        assert!(module_names.contains(&&"network".to_string()));
        assert!(module_names.contains(&&"tcp".to_string()));
    }

    #[test]
    fn test_struct_definitions_captured() {
        let rust_code = r#"
pub struct Point {
    x: f64,
    y: f64,
}

struct Rectangle {
    width: u32,
    height: u32,
}

#[derive(Debug)]
struct User {
    name: String,
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let struct_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Struct)
            .collect();

        let struct_names: Vec<&String> = struct_defs.iter().map(|d| &d.name).collect();
        assert!(struct_names.contains(&&"Point".to_string()));
        assert!(struct_names.contains(&&"Rectangle".to_string()));
        assert!(struct_names.contains(&&"User".to_string()));
    }

    #[test]
    fn test_enum_and_variant_definitions_captured() {
        let rust_code = r#"
pub enum Color {
    Red,
    Green,
    Blue,
}

enum Result<T, E> {
    Ok(T),
    Err(E),
}

#[derive(Debug)]
enum Message {
    Quit,
    Move { x: i32, y: i32 },
    Write(String),
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let enum_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Enum)
            .collect();

        let variant_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Variant)
            .collect();

        let enum_names: Vec<&String> = enum_defs.iter().map(|d| &d.name).collect();
        assert!(enum_names.contains(&&"Color".to_string()));
        assert!(enum_names.contains(&&"Result".to_string()));
        assert!(enum_names.contains(&&"Message".to_string()));

        let variant_names: Vec<&String> = variant_defs.iter().map(|d| &d.name).collect();
        assert!(variant_names.contains(&&"Red".to_string()));
        assert!(variant_names.contains(&&"Green".to_string()));
        assert!(variant_names.contains(&&"Blue".to_string()));
        assert!(variant_names.contains(&&"Ok".to_string()));
        assert!(variant_names.contains(&&"Err".to_string()));
        assert!(variant_names.contains(&&"Quit".to_string()));
        assert!(variant_names.contains(&&"Move".to_string()));
        assert!(variant_names.contains(&&"Write".to_string()));
    }

    #[test]
    fn test_trait_definitions_captured() {
        let rust_code = r#"
pub trait Display {
    fn fmt(&self) -> String;
}

trait Iterator<Item> {
    type IntoIter;
    fn next(&mut self) -> Option<Item>;
    fn collect(self) -> Vec<Item> where Self: Sized;
}

pub trait Clone {
    fn clone(&self) -> Self;
    fn clone_from(&mut self, source: &Self) {
        *self = source.clone();
    }
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let trait_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Trait)
            .collect();

        let trait_names: Vec<&String> = trait_defs.iter().map(|d| &d.name).collect();
        assert!(trait_names.contains(&&"Display".to_string()));
        assert!(trait_names.contains(&&"Iterator".to_string()));
        assert!(trait_names.contains(&&"Clone".to_string()));

        let method_defs: Vec<_> = definitions
            .iter()
            .filter(|d| {
                matches!(
                    d.definition_type,
                    RustDefinitionType::Method | RustDefinitionType::AssociatedFunction
                )
            })
            .collect();

        let method_names: Vec<&String> = method_defs.iter().map(|d| &d.name).collect();
        assert!(method_names.contains(&&"fmt".to_string()));
        assert!(method_names.contains(&&"next".to_string()));
        assert!(method_names.contains(&&"collect".to_string()));
        assert!(method_names.contains(&&"clone".to_string()));
    }

    #[test]
    fn test_function_method_associated_function_definitions() {
        let rust_code = r#"
struct Calculator {
    value: i32,
}

impl Calculator {
    // Associated function (no self)
    pub fn new(value: i32) -> Self {
        Self { value }
    }

    // Method (has &self)
    pub fn get_value(&self) -> i32 {
        self.value
    }

    // Mutable method (has &mut self)
    pub fn set_value(&mut self, value: i32) {
        self.value = value;
    }

    // Method with owned self
    pub fn consume(self) -> i32 {
        self.value
    }
}

// Free function
pub fn helper_function() -> i32 {
    42
}

// Async function
pub async fn async_function() -> Result<(), std::io::Error> {
    Ok(())
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let associated_function_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::AssociatedFunction)
            .collect();

        let method_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Method)
            .collect();

        let function_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Function)
            .collect();

        // Verify associated functions (no self parameter)
        let assoc_names: Vec<&String> = associated_function_defs.iter().map(|d| &d.name).collect();
        assert!(assoc_names.contains(&&"new".to_string()));

        // Verify methods (have self parameter)
        let method_names: Vec<&String> = method_defs.iter().map(|d| &d.name).collect();
        assert!(method_names.contains(&&"get_value".to_string()));
        assert!(method_names.contains(&&"set_value".to_string()));
        assert!(method_names.contains(&&"consume".to_string()));

        // Verify free functions
        let function_names: Vec<&String> = function_defs.iter().map(|d| &d.name).collect();
        assert!(function_names.contains(&&"helper_function".to_string()));
        assert!(function_names.contains(&&"async_function".to_string()));
    }

    #[test]
    fn test_impl_definitions_captured() {
        let rust_code = r#"
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    fn distance(&self, other: &Point) -> f64 {
        0.0
    }
}

trait Display {
    fn display(&self) -> String;
}

impl Display for Point {
    fn display(&self) -> String {
        format!("({}, {})", self.x, self.y)
    }
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let impl_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Impl)
            .collect();

        // Check that impl block names match the types they implement
        let impl_names: Vec<&String> = impl_defs.iter().map(|d| &d.name).collect();
        assert!(impl_names.contains(&&"Point".to_string())); // impl Point
        assert!(impl_names.contains(&&"Display".to_string())); // impl Display for Point
    }

    #[test]
    fn test_macro_definitions_captured() {
        let rust_code = r#"
macro_rules! vec {
    ($($x:expr),*) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push($x);
            )*
            temp_vec
        }
    };
}

macro_rules! debug_print {
    ($msg:expr) => {
        println!("[DEBUG] {}", $msg);
    };
}

pub fn test_macros() {
    vec![1, 2, 3];
    debug_print!("Hello, world!");
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let macro_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Macro)
            .collect();

        let macro_call_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::MacroCall)
            .collect();

        let macro_names: Vec<&String> = macro_defs.iter().map(|d| &d.name).collect();
        assert!(macro_names.contains(&&"vec".to_string()));
        assert!(macro_names.contains(&&"debug_print".to_string()));

        let macro_call_names: Vec<&String> = macro_call_defs.iter().map(|d| &d.name).collect();
        assert!(macro_call_names.contains(&&"vec".to_string()));
        assert!(macro_call_names.contains(&&"debug_print".to_string()));
    }

    #[test]
    fn test_closure_definitions_captured() {
        let rust_code = r#"
pub fn test_closures() {
    let add_one = |x| x + 1;
    let multiply = |a, b| a * b;
    
    let complex_closure = |data: Vec<i32>| -> i32 {
        data.iter().sum()
    };
    
    // Anonymous closure (should not be captured)
    [1, 2, 3].iter().map(|x| x * 2).collect::<Vec<_>>();
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let closure_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Closure)
            .collect();

        let closure_names: Vec<&String> = closure_defs.iter().map(|d| &d.name).collect();
        assert!(closure_names.contains(&&"add_one".to_string()));
        assert!(closure_names.contains(&&"multiply".to_string()));
        assert!(closure_names.contains(&&"complex_closure".to_string()));
    }

    #[test]
    fn test_constant_static_type_alias_definitions() {
        let rust_code = r#"
pub const PI: f64 = 3.14159;
pub const MAX_SIZE: usize = 1000;

pub static GLOBAL_COUNTER: std::sync::atomic::AtomicUsize = 
    std::sync::atomic::AtomicUsize::new(0);

static mut GLOBAL_STATE: i32 = 0;

pub type StringMap = std::collections::HashMap<String, String>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Container {
    pub size: usize,
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let const_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Constant)
            .collect();

        let static_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Static)
            .collect();

        let type_alias_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::TypeAlias)
            .collect();

        let field_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Field)
            .collect();

        let const_names: Vec<&String> = const_defs.iter().map(|d| &d.name).collect();
        assert!(const_names.contains(&&"PI".to_string()));
        assert!(const_names.contains(&&"MAX_SIZE".to_string()));

        let static_names: Vec<&String> = static_defs.iter().map(|d| &d.name).collect();
        assert!(static_names.contains(&&"GLOBAL_COUNTER".to_string()));
        assert!(static_names.contains(&&"GLOBAL_STATE".to_string()));

        let type_alias_names: Vec<&String> = type_alias_defs.iter().map(|d| &d.name).collect();
        assert!(type_alias_names.contains(&&"StringMap".to_string()));
        assert!(type_alias_names.contains(&&"Result".to_string()));

        let field_names: Vec<&String> = field_defs.iter().map(|d| &d.name).collect();
        assert!(field_names.contains(&&"size".to_string()));
    }

    #[test]
    fn test_union_definitions_captured() {
        let rust_code = r#"
pub union FloatOrInt {
    f: f32,
    i: i32,
}

union Value {
    integer: i64,
    float: f64,
    boolean: bool,
}

impl FloatOrInt {
    pub unsafe fn as_float(&self) -> f32 {
        self.f
    }
    
    pub unsafe fn as_int(&self) -> i32 {
        self.i
    }
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        let union_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Union)
            .collect();

        let field_defs: Vec<_> = definitions
            .iter()
            .filter(|d| d.definition_type == RustDefinitionType::Field)
            .collect();

        let union_names: Vec<&String> = union_defs.iter().map(|d| &d.name).collect();
        assert!(union_names.contains(&&"FloatOrInt".to_string()));
        assert!(union_names.contains(&&"Value".to_string()));

        // Verify union fields are captured
        let field_names: Vec<&String> = field_defs.iter().map(|d| &d.name).collect();
        assert!(field_names.contains(&&"f".to_string()));
        assert!(field_names.contains(&&"i".to_string()));
        assert!(field_names.contains(&&"integer".to_string()));
        assert!(field_names.contains(&&"float".to_string()));
        assert!(field_names.contains(&&"boolean".to_string()));
    }

    #[test]
    fn test_nested_definitions_with_correct_fqns() {
        let rust_code = r#"
mod outer {
    pub mod inner {
        pub struct Point {
            x: f64,
            y: f64,
        }
        
        impl Point {
            pub fn new(x: f64, y: f64) -> Self {
                Point { x, y }
            }
        }
        
        pub enum Color {
            Red,
            Blue,
        }
        
        pub const DEFAULT_POINT: Point = Point { x: 0.0, y: 0.0 };
    }
    
    pub trait Display {
        fn display(&self) -> String;
    }
}
"#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (_, _, definitions_map, _) = build_fqn_and_node_indices(&parse_result.ast);
        let definitions = extract_definitions_from_map(&definitions_map);

        // Verify all definition types are found
        let def_by_name: std::collections::HashMap<String, &RustDefinitionInfo> =
            definitions.iter().map(|d| (d.name.clone(), d)).collect();

        // Verify FQNs are correctly nested
        let point_def = def_by_name.get("Point").unwrap();
        assert_eq!(rust_fqn_to_string(&point_def.fqn), "outer::inner::Point");

        let new_def = def_by_name.get("new").unwrap();
        assert_eq!(rust_fqn_to_string(&new_def.fqn), "outer::inner::Point::new");

        let red_def = def_by_name.get("Red").unwrap();
        assert_eq!(rust_fqn_to_string(&red_def.fqn), "outer::inner::Color::Red");

        let display_def = def_by_name.get("Display").unwrap();
        assert_eq!(rust_fqn_to_string(&display_def.fqn), "outer::Display");
    }
}
