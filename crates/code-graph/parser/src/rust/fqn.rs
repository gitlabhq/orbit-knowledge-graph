use crate::rust::definitions::{RustDefinitionsMap, create_definition_from_fqn};
use crate::rust::imports::{RustImportedSymbolInfo, detect_import_declaration};
use crate::rust::types::{RustFqn, RustFqnPart, RustFqnPartType, RustNodeFqnMap, node_types};
use crate::utils::{Range, node_to_range};
use rustc_hash::FxHashMap;
use smallvec::{SmallVec, smallvec};
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

/// Use SmallVec for scope stack since Rust nesting is typically shallow
type ScopeStack = SmallVec<[RustFqnPart; 8]>;

/// Stack entry for iterative processing
struct StackEntry<'a> {
    node: Node<'a, StrDoc<SupportLang>>,
    scope_depth: usize,
    is_exiting: bool, // is true when we're exiting the node
}

/// Index a node in the node index map for later lookup
fn index_node<'a>(node: &Node<'a, StrDoc<SupportLang>>, node_index_map: &mut NodeIndexMap<'a>) {
    let range = node_to_range(node);
    node_index_map.insert(range, node.clone());
}

/// Store FQN mapping for a specific node and create a definition if applicable
fn store_fqn_mapping<'a>(
    name_node: Node<'a, StrDoc<SupportLang>>,
    fqn_parts: SmallVec<[RustFqnPart; 8]>,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) {
    let range = node_to_range(&name_node);

    // Create a definition if this FQN part represents a definition
    if let Some(last_part) = fqn_parts.last()
        && last_part.node_type.is_definition()
    {
        let fqn = RustFqn::new(fqn_parts.clone());
        if let Some(definition) = create_definition_from_fqn(
            &name_node,
            last_part.node_type,
            last_part.node_name().to_string(),
            fqn,
            range,
        ) {
            definitions_map.insert(range, definition);
        }
    }

    // Store the FQN mapping after potentially using fqn_parts
    node_fqn_map.insert(range, (name_node.clone(), Arc::new(fqn_parts)));
}

/// Create an FQN part from the given information
fn create_fqn_part(
    fqn_part_type: RustFqnPartType,
    name: String,
    definition_node: &Node<StrDoc<SupportLang>>,
) -> RustFqnPart {
    let range = node_to_range(definition_node);
    RustFqnPart::new(fqn_part_type, name, range)
}

/// Result of FQN processing for a node
struct FQNResult {
    creates_scope: bool,
    new_scope_part: Option<RustFqnPart>,
}

impl FQNResult {
    fn no_scope() -> Self {
        Self {
            creates_scope: false,
            new_scope_part: None,
        }
    }

    fn with_scope(scope_part: RustFqnPart) -> Self {
        Self {
            creates_scope: true,
            new_scope_part: Some(scope_part),
        }
    }
}

/// Main iterative FQN computation function
fn compute_fqns_and_index_iterative<'a>(
    root: Node<'a, StrDoc<SupportLang>>,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    node_index_map: &mut NodeIndexMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
    discovered_imports: &mut Vec<RustImportedSymbolInfo>,
) {
    let mut stack = Vec::with_capacity(256);
    let mut current_scope: ScopeStack = smallvec![];

    // Start with the root node
    stack.push(StackEntry {
        node: root,
        scope_depth: 0,
        is_exiting: false,
    });

    while let Some(entry) = stack.pop() {
        if entry.is_exiting {
            while current_scope.len() > entry.scope_depth {
                current_scope.pop();
            }
            continue;
        }

        index_node(&entry.node, node_index_map);

        // Detect imports first
        let detected_imports = detect_import_declaration(&entry.node, &current_scope);
        discovered_imports.extend(detected_imports);

        let fqn_result = process_node(&entry.node, &current_scope, node_fqn_map, definitions_map);

        // If this node creates a new scope, handle it BEFORE adding children
        if fqn_result.creates_scope
            && let Some(scope_part) = fqn_result.new_scope_part
        {
            current_scope.push(scope_part);

            // Add an exit marker to the stack
            stack.push(StackEntry {
                node: entry.node.clone(),
                scope_depth: current_scope.len() - 1, // Exit to the depth before this scope
                is_exiting: true,
            });
        }

        // Add children to the stack in reverse order for depth-first traversal
        let children: Vec<_> = entry.node.children().collect();
        for child in children.into_iter().rev() {
            stack.push(StackEntry {
                node: child,
                scope_depth: current_scope.len(),
                is_exiting: false,
            });
        }
    }
}

/// Process a single node for FQN computation
fn process_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) -> FQNResult {
    let node_kind = node.kind();

    match node_kind.as_ref() {
        // Scope-creating items
        node_types::MODULE
        | node_types::STRUCT
        | node_types::ENUM
        | node_types::TRAIT
        | node_types::IMPL
        | node_types::FUNCTION
        | node_types::MACRO_DEFINITION
        | node_types::UNION => {
            process_scope_creating_node(node, current_scope, node_fqn_map, definitions_map)
        }

        // Non-scope-creating but FQN-tracked items
        node_types::CONST
        | node_types::STATIC
        | node_types::TYPE_ALIAS
        | node_types::VARIANT
        | node_types::FIELD => {
            process_non_scope_creating_node(node, current_scope, node_fqn_map, definitions_map);
            FQNResult::no_scope()
        }

        node_types::FUNCTION_SIGNATURE => {
            process_trait_method_signature(node, current_scope, node_fqn_map, definitions_map);
            FQNResult::no_scope()
        }

        // Closures and macro calls (special handling)
        node_types::CLOSURE => {
            process_closure_node(node, current_scope, node_fqn_map, definitions_map);
            FQNResult::no_scope()
        }

        node_types::MACRO_INVOCATION => {
            process_macro_call_node(node, current_scope, node_fqn_map, definitions_map);
            FQNResult::no_scope()
        }

        _ => FQNResult::no_scope(),
    }
}

/// Process nodes that create new scopes (modules, structs, traits, functions, etc.)
fn process_scope_creating_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) -> FQNResult {
    let node_kind = node.kind();

    // Extract the name and determine the FQN part type
    let (name, fqn_part_type) = match node_kind.as_ref() {
        node_types::MODULE => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Module)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::STRUCT => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Struct)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::ENUM => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Enum)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::TRAIT => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Trait)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::IMPL => {
            // For impl blocks, we extract the type being implemented
            if let Some(impl_type) = extract_impl_type_name(node) {
                (impl_type, RustFqnPartType::Impl)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::FUNCTION => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                // Determine if this is a method, associated function, or regular function
                let fqn_part_type = if is_in_impl_block(node) {
                    let has_self = has_self_parameter(node);
                    if has_self {
                        RustFqnPartType::Method
                    } else {
                        RustFqnPartType::AssociatedFunction
                    }
                } else {
                    RustFqnPartType::Function
                };
                (name, fqn_part_type)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::MACRO_DEFINITION => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Macro)
            } else {
                return FQNResult::no_scope();
            }
        }
        node_types::UNION => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Union)
            } else {
                return FQNResult::no_scope();
            }
        }
        _ => return FQNResult::no_scope(),
    };

    // Create the FQN part
    let fqn_part = create_fqn_part(fqn_part_type, name, node);

    // Build the complete FQN path including this node
    let mut fqn_parts = current_scope.clone();
    fqn_parts.push(fqn_part.clone());

    // Store the FQN mapping for this node
    store_fqn_mapping(node.clone(), fqn_parts, node_fqn_map, definitions_map);

    FQNResult::with_scope(fqn_part)
}

/// Process nodes that don't create scopes but should be tracked in FQNs
fn process_non_scope_creating_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) {
    let node_kind = node.kind();

    let (name, fqn_part_type) = match node_kind.as_ref() {
        node_types::CONST => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Constant)
            } else {
                return;
            }
        }
        node_types::STATIC => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Static)
            } else {
                return;
            }
        }
        node_types::TYPE_ALIAS => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::TypeAlias)
            } else {
                return;
            }
        }
        node_types::VARIANT => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Variant)
            } else {
                return;
            }
        }
        node_types::FIELD => {
            if let Some(name) = extract_identifier_from_node(node, "name") {
                (name, RustFqnPartType::Field)
            } else {
                return;
            }
        }
        _ => return,
    };

    // Create the FQN part
    let fqn_part = create_fqn_part(fqn_part_type, name, node);

    // Build the complete FQN path (include this item but don't add as scope)
    let mut fqn_parts = current_scope.clone();
    fqn_parts.push(fqn_part);

    // Store the FQN mapping
    store_fqn_mapping(node.clone(), fqn_parts, node_fqn_map, definitions_map);
}

/// Process closure expressions
fn process_closure_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) {
    // Only capture closures that are assigned to variables
    if let Some(closure_name) = extract_closure_variable_name(node) {
        let fqn_part = create_fqn_part(RustFqnPartType::Closure, closure_name, node);

        let mut fqn_parts = current_scope.clone();
        fqn_parts.push(fqn_part);

        store_fqn_mapping(node.clone(), fqn_parts, node_fqn_map, definitions_map);
    }
    // Anonymous closures (not assigned to variables) are not captured at all
}

/// Process macro invocations
fn process_macro_call_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) {
    if let Some(macro_name) = extract_macro_name(node) {
        let fqn_part = create_fqn_part(RustFqnPartType::MacroCall, macro_name, node);

        let mut fqn_parts = current_scope.clone();
        fqn_parts.push(fqn_part);

        store_fqn_mapping(node.clone(), fqn_parts, node_fqn_map, definitions_map);
    }
}

/// Process trait method signatures (function_signature_item nodes)
fn process_trait_method_signature<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
    node_fqn_map: &mut RustNodeFqnMap<'a>,
    definitions_map: &mut RustDefinitionsMap,
) {
    // For function_signature_item, the identifier is a direct child, not a field
    let name = node
        .children()
        .find(|child| child.kind().as_ref() == node_types::IDENTIFIER)
        .map(|name_node| name_node.text().to_string());

    if let Some(name) = name {
        // Determine if this is a method or associated function based on self parameter
        let fqn_part_type = if has_self_parameter(node) {
            RustFqnPartType::Method
        } else {
            RustFqnPartType::AssociatedFunction
        };

        let fqn_part = create_fqn_part(fqn_part_type, name, node);

        let mut fqn_parts = current_scope.clone();
        fqn_parts.push(fqn_part);

        store_fqn_mapping(node.clone(), fqn_parts, node_fqn_map, definitions_map);
    }
}

/// Extract the variable name that a closure is assigned to
/// This traverses up the AST to find patterns like:
/// - `let variable_name = |...| ...;`
/// - `let variable_name: Type = |...| ...;`
/// - `variable_name = |...| ...;`
fn extract_closure_variable_name(closure_node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    let mut current = closure_node.parent();

    while let Some(parent) = current {
        match parent.kind().as_ref() {
            // let variable = closure;
            "let_declaration" => {
                if let Some(pattern) = parent.field("pattern") {
                    return extract_pattern_identifier(&pattern);
                }
            }
            // variable = closure; (assignment expression)
            "assignment_expression" => {
                if let Some(left) = parent.field("left")
                    && left.kind().as_ref() == node_types::IDENTIFIER
                {
                    return Some(left.text().to_string());
                }
            }
            // Break on certain parent types that indicate we've gone too far
            "block" | "function_item" | "impl_item" | "mod_item" => break,
            _ => {}
        }
        current = parent.parent();
    }

    None
}

/// Extract identifier from a pattern node (handles destructuring patterns)
fn extract_pattern_identifier(pattern_node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    match pattern_node.kind().as_ref() {
        node_types::IDENTIFIER => Some(pattern_node.text().to_string()),
        "mutable_pattern" => {
            // For `mut variable_name` patterns
            pattern_node
                .children()
                .find(|child| child.kind().as_ref() == node_types::IDENTIFIER)
                .map(|id| id.text().to_string())
        }
        "tuple_pattern" | "struct_pattern" | "slice_pattern" => {
            // For destructuring patterns, we could extract the first identifier
            // but for simplicity, we'll skip these for now
            None
        }
        _ => None,
    }
}

/// Extract identifier name from a node with a specific field name
fn extract_identifier_from_node(
    node: &Node<StrDoc<SupportLang>>,
    field_name: &str,
) -> Option<String> {
    node.field(field_name)
        .map(|name_node| name_node.text().to_string())
}

/// Extract the scope name from an impl block
/// For trait implementations (impl Trait for Type), uses the trait name
/// For regular implementations (impl Type), uses the type name
fn extract_impl_type_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    // Check if this is a trait implementation first
    if let Some(trait_node) = node.field("trait") {
        // For trait implementations (impl Trait for Type), use the trait name
        extract_type_name_from_node(&trait_node)
    } else if let Some(type_node) = node.field("type") {
        // For regular implementations (impl Type), use the type name
        extract_type_name_from_node(&type_node)
    } else {
        None
    }
}

/// Extract a type name from a type node, handling various patterns
fn extract_type_name_from_node(type_node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    match type_node.kind().as_ref() {
        node_types::TYPE_IDENTIFIER | node_types::IDENTIFIER => Some(type_node.text().to_string()),
        node_types::SCOPED_IDENTIFIER => {
            // For scoped identifiers, take the last part
            if let Some(name_node) = type_node.field("name") {
                Some(name_node.text().to_string())
            } else {
                Some(type_node.text().to_string())
            }
        }
        "generic_type" => {
            // For generic types like Container<T>, extract just the base type name
            if let Some(type_name_node) = type_node.field("type") {
                extract_type_name_from_node(&type_name_node)
            } else {
                // Fallback: try to extract from the first child
                type_node
                    .children()
                    .find(|child| matches!(child.kind().as_ref(), "type_identifier" | "identifier"))
                    .map(|child| child.text().to_string())
            }
        }
        _ => {
            // For other complex types, try to extract identifier children
            if let Some(identifier) = type_node
                .children()
                .find(|child| matches!(child.kind().as_ref(), "type_identifier" | "identifier"))
            {
                Some(identifier.text().to_string())
            } else {
                // Final fallback: use the entire text
                // TODO: this is ugly, we should find a better way to do this
                // Complex types are out of scope for now
                Some(type_node.text().to_string())
            }
        }
    }
}

/// Extract macro name from a macro invocation
fn extract_macro_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    node.field("macro")
        .map(|macro_node| match macro_node.kind().as_ref() {
            node_types::IDENTIFIER => macro_node.text().to_string(),
            node_types::SCOPED_IDENTIFIER => {
                if let Some(name_node) = macro_node.field("name") {
                    name_node.text().to_string()
                } else {
                    macro_node.text().to_string()
                }
            }
            _ => macro_node.text().to_string(),
        })
}

/// Check if a function node is inside an impl block
fn is_in_impl_block(node: &Node<StrDoc<SupportLang>>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind().as_ref() == node_types::IMPL {
            return true;
        }
        current = parent.parent();
    }
    false
}

/// Check if a function has a self parameter
fn has_self_parameter(node: &Node<StrDoc<SupportLang>>) -> bool {
    // Look for parameters and check if the first one is 'self'
    if let Some(params_node) = node.field("parameters") {
        for param in params_node.children() {
            // Check for both 'parameter' and 'self_parameter' kinds
            if param.kind().as_ref() == "self_parameter" {
                return true;
            } else if param.kind().as_ref() == "parameter" {
                if let Some(pattern) = param.field("pattern") {
                    let pattern_text = pattern.text();
                    if pattern_text == "self"
                        || pattern_text.starts_with("&self")
                        || pattern_text.starts_with("mut self")
                    {
                        return true;
                    }
                }
                // Only check the first parameter of type 'parameter'
                break;
            }
        }
    }
    false
}

/// Convert Rust FQN to string representation
pub fn rust_fqn_to_string(fqn: &RustFqn) -> String {
    fqn.parts
        .iter()
        .map(|part| part.node_name().to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// Parse an FQN string back into parts (utility function)
pub fn fqn_from_rust_string(fqn_str: &str) -> crate::fqn::Fqn<String> {
    let parts: Vec<String> = fqn_str.split("::").map(|s| s.to_string()).collect();
    crate::fqn::Fqn::new(parts)
}

/// Type alias for node index map
pub type NodeIndexMap<'a> = FxHashMap<Range, Node<'a, StrDoc<SupportLang>>>;

/// Build FQN and node indices for the entire AST, also collecting definitions
///
/// This function processes the AST in a single pass and builds:
/// 1. A map from ranges to nodes with their FQNs
/// 2. A map from ranges to AST nodes for efficient lookup
/// 3. A map from ranges to definitions found during FQN traversal
/// 4. A vector of discovered imports during traversal
///
/// Returns a tuple of (RustNodeFqnMap, NodeIndexMap, RustDefinitionsMap, Vec<RustImportedSymbolInfo>)
pub fn build_fqn_and_node_indices<'a>(
    ast: &'a Root<StrDoc<SupportLang>>,
) -> (
    RustNodeFqnMap<'a>,
    NodeIndexMap<'a>,
    RustDefinitionsMap,
    Vec<RustImportedSymbolInfo>,
) {
    let mut node_fqn_map = FxHashMap::with_capacity_and_hasher(512, Default::default());
    let mut node_index_map = FxHashMap::with_capacity_and_hasher(512, Default::default());
    let mut definitions_map = RustDefinitionsMap::default();
    let mut discovered_imports = Vec::new();

    compute_fqns_and_index_iterative(
        ast.root(),
        &mut node_fqn_map,
        &mut node_index_map,
        &mut definitions_map,
        &mut discovered_imports,
    );

    (
        node_fqn_map,
        node_index_map,
        definitions_map,
        discovered_imports,
    )
}

/// Find FQN for a node by looking up its range in the FQN map
pub fn find_fqn_for_node<'a>(range: Range, node_fqn_map: &RustNodeFqnMap<'a>) -> Option<RustFqn> {
    node_fqn_map
        .get(&range)
        .map(|(_, fqn_parts)| RustFqn::new((**fqn_parts).clone()))
}

/// Find Rust FQN for a node by looking up its range in the Rust FQN map
pub fn find_rust_fqn_for_node<'a>(
    range: Range,
    rust_node_fqn_map: &RustNodeFqnMap<'a>,
) -> Option<RustFqn> {
    rust_node_fqn_map
        .get(&range)
        .map(|(_, fqn_parts)| RustFqn::new((**fqn_parts).clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{GenericParser, Language, LanguageParser};
    #[test]
    fn test_rust_fqn_computation_with_sample_code() {
        let rust_code = r#"
mod utils {
    pub struct Calculator {
        value: i32,
    }
    
    impl Calculator {
        pub fn new(value: i32) -> Self {
            Calculator { value }
        }
        
        pub fn add(&mut self, other: i32) {
            self.value += other;
        }
        
        pub fn create_default() -> Self {
            Self::new(0)
        }
    }
    
    pub fn helper_function() {}
}

pub enum Color {
    Red,
    Green,
    Blue,
}

pub trait Display {
    fn display(&self) -> String;
}

impl Display for Color {
    fn display(&self) -> String {
        match self {
            Color::Red => "Red".to_string(),
            Color::Green => "Green".to_string(),
            Color::Blue => "Blue".to_string(),
        }
    }
}

pub const MAX_SIZE: usize = 1000;
pub static GLOBAL_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

type StringMap = std::collections::HashMap<String, String>;

macro_rules! debug_print {
    ($msg:expr) => {
        println!("[DEBUG] {}", $msg);
    };
}

pub fn top_level_function() {
    let closure = |x: i32| x * 2;
    debug_print!("Testing macro");
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("test.rs")).unwrap();
        let (rust_node_fqn_map, _node_index_map, _, _) =
            build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        assert!(fqn_strings.contains(&"utils".to_string()));
        assert!(fqn_strings.contains(&"utils::Calculator".to_string()));
        assert!(fqn_strings.contains(&"utils::Calculator::new".to_string()));
        assert!(fqn_strings.contains(&"utils::Calculator::add".to_string()));
        assert!(fqn_strings.contains(&"utils::Calculator::create_default".to_string()));
        assert!(fqn_strings.contains(&"utils::helper_function".to_string()));
        assert!(fqn_strings.contains(&"Color".to_string()));
        assert!(fqn_strings.contains(&"Color::Red".to_string()));
        assert!(fqn_strings.contains(&"Color::Green".to_string()));
        assert!(fqn_strings.contains(&"Color::Blue".to_string()));
        assert!(fqn_strings.contains(&"Display".to_string()));
        assert!(fqn_strings.contains(&"Display::display".to_string()));
        assert!(fqn_strings.contains(&"MAX_SIZE".to_string()));
        assert!(fqn_strings.contains(&"GLOBAL_COUNTER".to_string()));
        assert!(fqn_strings.contains(&"StringMap".to_string()));
        assert!(fqn_strings.contains(&"debug_print".to_string()));
        assert!(fqn_strings.contains(&"top_level_function".to_string()));
    }

    #[test]
    fn test_rust_fqn_with_nested_modules() {
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
            
            pub fn distance(&self, other: &Point) -> f64 {
                let dx = self.x - other.x;
                let dy = self.y - other.y;
                (dx * dx + dy * dy).sqrt()
            }
        }
        
        pub mod geometry {
            use super::Point;
            
            pub fn calculate_area(points: &[Point]) -> f64 {
                // Simple triangle area calculation
                0.0
            }
        }
    }
    
    pub use inner::Point;
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("nested.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Nested module FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify deeply nested FQNs
        assert!(fqn_strings.contains(&"outer".to_string()));
        assert!(fqn_strings.contains(&"outer::inner".to_string()));
        assert!(fqn_strings.contains(&"outer::inner::Point".to_string()));
        assert!(fqn_strings.contains(&"outer::inner::Point::new".to_string()));
        assert!(fqn_strings.contains(&"outer::inner::Point::distance".to_string()));
        assert!(fqn_strings.contains(&"outer::inner::geometry".to_string()));
        assert!(fqn_strings.contains(&"outer::inner::geometry::calculate_area".to_string()));
    }

    #[test]
    fn test_rust_fqn_with_generics_and_lifetimes() {
        let rust_code = r#"
pub struct Container<T> {
    data: Vec<T>,
}

impl<T> Container<T> {
    pub fn new() -> Self {
        Container { data: Vec::new() }
    }
    
    pub fn push(&mut self, item: T) {
        self.data.push(item);
    }
}

impl<T: Clone> Container<T> {
    pub fn duplicate_last(&mut self) {
        if let Some(last) = self.data.last() {
            self.data.push(last.clone());
        }
    }
}

pub trait Iterator<'a, T> {
    fn next(&mut self) -> Option<&'a T>;
}

impl<'a, T> Iterator<'a, T> for Container<T> {
    fn next(&mut self) -> Option<&'a T> {
        None
    }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("generics.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Generic types FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify FQNs for generic types
        assert!(fqn_strings.contains(&"Container".to_string()));
        assert!(fqn_strings.contains(&"Container::new".to_string()));
        assert!(fqn_strings.contains(&"Container::push".to_string()));
        assert!(fqn_strings.contains(&"Container::duplicate_last".to_string()));
        assert!(fqn_strings.contains(&"Iterator".to_string()));
        assert!(fqn_strings.contains(&"Iterator::next".to_string()));
    }

    #[test]
    fn test_rust_fqn_metadata_types() {
        let rust_code = r#"
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    fn distance(&self, other: &Point) -> f64 {
        0.0
    }
    
    fn create() -> Point {
        Point { x: 0.0, y: 0.0 }
    }
}

fn standalone_function() {}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("metadata.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        // Check that we correctly distinguish between methods and associated functions
        let fqns_with_types: Vec<_> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| {
                let fqn = RustFqn::new((**fqn_parts).clone());
                let last_part = fqn.parts.last().unwrap();
                (rust_fqn_to_string(&fqn), last_part.node_type)
            })
            .collect();

        let distance_fqn = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "Point::distance")
            .unwrap();
        assert_eq!(distance_fqn.1, RustFqnPartType::Method);

        let create_fqn = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "Point::create")
            .unwrap();
        assert_eq!(create_fqn.1, RustFqnPartType::AssociatedFunction);

        let standalone_fqn = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "standalone_function")
            .unwrap();
        assert_eq!(standalone_fqn.1, RustFqnPartType::Function);
    }

    #[test]
    fn test_rust_fqn_with_closures_and_macros() {
        let rust_code = r#"
macro_rules! my_macro {
    ($x:expr) => { $x * 2 };
}

fn main() {
    let multiplier = |x: i32| x * 3;
    let result = my_macro!(5);
    
    let nested_closure = || {
        let inner = |y: i32| y + 1;
        inner(10)
    };
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("closures.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Closures and macros FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify we track macros and closures
        assert!(fqn_strings.contains(&"my_macro".to_string()));
        assert!(fqn_strings.contains(&"main".to_string()));

        // Check for closure FQNs (they should use variable names)
        assert!(fqn_strings.contains(&"main::multiplier".to_string()));
        assert!(fqn_strings.contains(&"main::nested_closure".to_string()));
        assert!(fqn_strings.contains(&"main::inner".to_string()));

        // Verify we have at least 3 closures with variable names
        let closure_count = fqn_strings
            .iter()
            .filter(|fqn| {
                fqn == &"main::multiplier"
                    || fqn == &"main::nested_closure"
                    || fqn == &"main::inner"
            })
            .count();
        assert_eq!(
            closure_count, 3,
            "Expected 3 named closures, found {closure_count}"
        );
    }

    #[test]
    fn test_rust_fqn_closure_variable_name_extraction() {
        let rust_code = r#"
fn closure_tests() {
    // Simple let binding
    let simple_closure = |x: i32| x * 2;
    
    // Mutable binding
    let mut mutable_closure = |x: i32| x + 1;
    
    // Type annotation
    let typed_closure: Box<dyn Fn(i32) -> i32> = Box::new(|x: i32| x * 3);
    
    // Assignment (not declaration)
    let mut reassigned;
    reassigned = |x: i32| x - 1;
    
    // Closure without variable name (anonymous, should fallback to position)
    [1, 2, 3].iter().map(|x| x * 2).collect::<Vec<_>>();
    
    // Nested scope
    {
        let nested_scope_closure = || println!("nested");
        nested_scope_closure();
    }
}

mod test_mod {
    pub fn mod_closure_test() {
        let mod_closure = |a, b| a + b;
    }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("closure_names.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Closure variable names FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify named closures are captured with variable names
        assert!(fqn_strings.contains(&"closure_tests::simple_closure".to_string()));
        assert!(fqn_strings.contains(&"closure_tests::mutable_closure".to_string()));
        assert!(fqn_strings.contains(&"closure_tests::typed_closure".to_string()));
        assert!(fqn_strings.contains(&"closure_tests::reassigned".to_string()));
        assert!(fqn_strings.contains(&"closure_tests::nested_scope_closure".to_string()));
        assert!(fqn_strings.contains(&"test_mod::mod_closure_test::mod_closure".to_string()));

        // Anonymous closures should NOT be captured
        let anonymous_closures: Vec<_> = fqn_strings
            .iter()
            .filter(|fqn| fqn.contains("<closure@"))
            .collect();
        assert_eq!(
            anonymous_closures.len(),
            0,
            "Anonymous closures should not be captured, but found: {anonymous_closures:?}"
        );
    }

    #[test]
    fn test_rust_fqn_with_async_and_unsafe() {
        let rust_code = r#"
mod network {
    pub async fn fetch_data(url: &str) -> Result<String, reqwest::Error> {
        reqwest::get(url).await?.text().await
    }
    
    pub unsafe fn raw_memory_access(ptr: *mut u8) {
        *ptr = 42;
    }
    
    pub async unsafe fn dangerous_async_operation() {
        // Combining async and unsafe
    }
}

struct AsyncStruct;

impl AsyncStruct {
    async fn async_method(&self) -> i32 {
        42
    }
    
    unsafe fn unsafe_method(&mut self) {
        // unsafe operations
    }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("async_unsafe.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Async and unsafe FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify async and unsafe functions are tracked
        assert!(fqn_strings.contains(&"network".to_string()));
        assert!(fqn_strings.contains(&"network::fetch_data".to_string()));
        assert!(fqn_strings.contains(&"network::raw_memory_access".to_string()));
        assert!(fqn_strings.contains(&"network::dangerous_async_operation".to_string()));
        assert!(fqn_strings.contains(&"AsyncStruct".to_string()));
        assert!(fqn_strings.contains(&"AsyncStruct::async_method".to_string()));
        assert!(fqn_strings.contains(&"AsyncStruct::unsafe_method".to_string()));
    }

    #[test]
    fn test_rust_fqn_with_complex_generics() {
        let rust_code = r#"
pub trait Iterator<Item> {
    type IntoIter: Iterator<Item = Item>;
    
    fn into_iter(self) -> Self::IntoIter;
}

pub struct GenericContainer<T, E = std::io::Error> 
where 
    T: Clone + Send + Sync,
    E: std::error::Error,
{
    data: Vec<T>,
    error_handler: Option<Box<dyn Fn(E) -> ()>>,
}

impl<T, E> GenericContainer<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            error_handler: None,
        }
    }
    
    pub async fn process_async<F, Fut>(&self, processor: F) -> Result<(), E>
    where
        F: Fn(&T) -> Fut,
        Fut: std::future::Future<Output = Result<(), E>>,
    {
        Ok(())
    }
}

pub enum Result<T, E = Box<dyn std::error::Error>> {
    Ok(T),
    Err(E),
}

pub type StringResult<T> = Result<T, String>;
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser
            .parse(rust_code, Some("complex_generics.rs"))
            .unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Complex generics FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        // Verify complex generic structures are tracked
        assert!(fqn_strings.contains(&"Iterator".to_string()));
        assert!(fqn_strings.contains(&"Iterator::into_iter".to_string()));
        assert!(fqn_strings.contains(&"GenericContainer".to_string()));
        assert!(fqn_strings.contains(&"GenericContainer::new".to_string()));
        assert!(fqn_strings.contains(&"GenericContainer::process_async".to_string()));
        assert!(fqn_strings.contains(&"Result".to_string()));
        assert!(fqn_strings.contains(&"Result::Ok".to_string()));
        assert!(fqn_strings.contains(&"Result::Err".to_string()));
        assert!(fqn_strings.contains(&"StringResult".to_string()));
    }

    #[test]
    fn test_rust_fqn_with_self_parameter_variations() {
        let rust_code = r#"
struct SelfVariations {
    data: i32,
}

impl SelfVariations {
    // Different self parameter patterns
    fn method_self(self) -> i32 { self.data }
    fn method_ref_self(&self) -> &i32 { &self.data }
    fn method_mut_self(&mut self) -> &mut i32 { &mut self.data }
    fn method_box_self(self: Box<Self>) -> i32 { self.data }
    fn method_rc_self(self: std::rc::Rc<Self>) -> i32 { self.data }
    fn method_pin_self(self: std::pin::Pin<&mut Self>) -> &i32 { &self.data }
    
    // Associated function (no self)
    fn associated_function() -> Self {
        Self { data: 0 }
    }
    
    // Associated function with Self type
    fn create_instance() -> Box<Self> {
        Box::new(Self { data: 42 })
    }
}

trait TraitWithSelfVariations {
    fn trait_method(&self) -> i32;
    fn trait_mut_method(&mut self);
    fn trait_owned_method(self) -> Self;
    fn trait_associated() -> Self;
}

impl TraitWithSelfVariations for SelfVariations {
    fn trait_method(&self) -> i32 { self.data }
    fn trait_mut_method(&mut self) { self.data += 1; }
    fn trait_owned_method(self) -> Self { self }
    fn trait_associated() -> Self { Self { data: 0 } }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("self_variations.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        // Check that methods and associated functions are properly distinguished
        let fqns_with_types: Vec<_> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| {
                let fqn = RustFqn::new((**fqn_parts).clone());
                let last_part = fqn.parts.last().unwrap();
                (rust_fqn_to_string(&fqn), last_part.node_type)
            })
            .collect();

        // Methods (have self prameter)
        let method_self = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "SelfVariations::method_self")
            .unwrap();
        assert_eq!(method_self.1, RustFqnPartType::Method);

        let method_ref_self = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "SelfVariations::method_ref_self")
            .unwrap();
        assert_eq!(method_ref_self.1, RustFqnPartType::Method);

        let method_box_self = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "SelfVariations::method_box_self")
            .unwrap();
        assert_eq!(method_box_self.1, RustFqnPartType::Method);

        // Associated functions (no self parameter)
        let associated_function = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "SelfVariations::associated_function")
            .unwrap();
        assert_eq!(associated_function.1, RustFqnPartType::AssociatedFunction);

        let create_instance = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "SelfVariations::create_instance")
            .unwrap();
        assert_eq!(create_instance.1, RustFqnPartType::AssociatedFunction);

        // Trait methods should also be correctly classified
        let trait_method = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "TraitWithSelfVariations::trait_method")
            .unwrap();
        assert_eq!(trait_method.1, RustFqnPartType::Method);

        let trait_associated = fqns_with_types
            .iter()
            .find(|(fqn, _)| fqn == "TraitWithSelfVariations::trait_associated")
            .unwrap();
        assert_eq!(trait_associated.1, RustFqnPartType::AssociatedFunction);
    }

    #[test]
    fn test_rust_fqn_with_trait_impls_and_associated_types() {
        let rust_code = r#"
pub trait DatabaseConnection {
    type Error: std::error::Error;
    type Transaction<'a>: Send where Self: 'a;
    
    async fn connect(&self) -> Result<(), Self::Error>;
    fn begin_transaction(&mut self) -> Self::Transaction<'_>;
}

pub struct PostgresConnection {
    url: String,
}

impl DatabaseConnection for PostgresConnection {
    type Error = sqlx::Error;
    type Transaction<'a> = PostgresTransaction<'a>;
    
    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    
    fn begin_transaction(&mut self) -> Self::Transaction<'_> {
        PostgresTransaction::new(self)
    }
}

pub struct PostgresTransaction<'a> {
    conn: &'a mut PostgresConnection,
}

impl<'a> PostgresTransaction<'a> {
    fn new(conn: &'a mut PostgresConnection) -> Self {
        Self { conn }
    }
    
    async fn commit(self) -> Result<(), sqlx::Error> {
        Ok(())
    }
}

// Multiple trait bounds
impl<T, U> std::fmt::Display for GenericPair<T, U>
where
    T: std::fmt::Display,
    U: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

pub struct GenericPair<T, U>(T, U);
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("trait_impls.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Trait implementations and associated types FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        assert!(fqn_strings.contains(&"DatabaseConnection".to_string()));
        assert!(fqn_strings.contains(&"DatabaseConnection::connect".to_string()));
        assert!(fqn_strings.contains(&"DatabaseConnection::begin_transaction".to_string()));

        assert!(fqn_strings.contains(&"PostgresConnection".to_string()));
        assert!(fqn_strings.contains(&"DatabaseConnection::connect".to_string())); // Impl method
        assert!(fqn_strings.contains(&"DatabaseConnection::begin_transaction".to_string())); // Impl method

        assert!(fqn_strings.contains(&"PostgresTransaction".to_string()));
        assert!(fqn_strings.contains(&"PostgresTransaction::new".to_string()));
        assert!(fqn_strings.contains(&"PostgresTransaction::commit".to_string()));

        assert!(fqn_strings.contains(&"GenericPair".to_string()));
    }

    #[test]
    fn test_rust_fqn_edge_cases_and_error_handling() {
        let rust_code = r#"
// Anonymous constants
const BUFFER_SIZE: usize = const { 1024 * 1024 };

// Inline modules
mod inline_mod {
    pub mod inner {
        pub const VALUE: i32 = 42;
    }
}

// Procedural macros (attribute macros)
#[derive(Debug, Clone)]
pub struct DerivedStruct {
    field: String,
}

// Macro with complex patterns
macro_rules! complex_macro {
    ($(#[$attr:meta])* $vis:vis struct $name:ident { $($field:ident: $ty:ty),* }) => {
        $(#[$attr])*
        $vis struct $name {
            $($field: $ty,)*
        }
    };
}

// Union types
pub union FloatUnion {
    f: f32,
    i: u32,
}

impl FloatUnion {
    pub unsafe fn as_float(&self) -> f32 {
        self.f
    }
    
    pub unsafe fn as_int(&self) -> u32 {
        self.i
    }
}

// Extern functions
extern "C" {
    fn malloc(size: usize) -> *mut std::ffi::c_void;
    fn free(ptr: *mut std::ffi::c_void);
}

// Function pointers and closures as fields
pub struct CallbackContainer {
    callback: Box<dyn Fn(i32) -> i32>,
    function_ptr: fn(i32) -> i32,
}

impl CallbackContainer {
    pub fn new<F>(callback: F, function_ptr: fn(i32) -> i32) -> Self
    where
        F: Fn(i32) -> i32 + 'static,
    {
        Self {
            callback: Box::new(callback),
            function_ptr,
        }
    }
}

// Deeply nested scopes
mod level1 {
    pub mod level2 {
        pub mod level3 {
            pub mod level4 {
                pub struct DeepStruct;
                
                impl DeepStruct {
                    pub fn deep_method() -> Self {
                        Self
                    }
                }
            }
        }
    }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("edge_cases.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let mut fqn_strings: Vec<String> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| rust_fqn_to_string(&RustFqn::new((**fqn_parts).clone())))
            .collect();
        fqn_strings.sort();

        println!("Edge cases FQNs:");
        for fqn in &fqn_strings {
            println!("  {fqn}");
        }

        assert!(fqn_strings.contains(&"BUFFER_SIZE".to_string()));
        assert!(fqn_strings.contains(&"inline_mod".to_string()));
        assert!(fqn_strings.contains(&"inline_mod::inner".to_string()));
        assert!(fqn_strings.contains(&"inline_mod::inner::VALUE".to_string()));
        assert!(fqn_strings.contains(&"DerivedStruct".to_string()));
        assert!(fqn_strings.contains(&"complex_macro".to_string()));
        assert!(fqn_strings.contains(&"FloatUnion".to_string()));
        assert!(fqn_strings.contains(&"FloatUnion::as_float".to_string()));
        assert!(fqn_strings.contains(&"FloatUnion::as_int".to_string()));
        assert!(fqn_strings.contains(&"CallbackContainer".to_string()));
        assert!(fqn_strings.contains(&"CallbackContainer::new".to_string()));

        assert!(fqn_strings.contains(&"level1".to_string()));
        assert!(fqn_strings.contains(&"level1::level2".to_string()));
        assert!(fqn_strings.contains(&"level1::level2::level3".to_string()));
        assert!(fqn_strings.contains(&"level1::level2::level3::level4".to_string()));
        assert!(fqn_strings.contains(&"level1::level2::level3::level4::DeepStruct".to_string()));
        assert!(
            fqn_strings
                .contains(&"level1::level2::level3::level4::DeepStruct::deep_method".to_string())
        );
    }

    #[test]
    fn test_rust_fqn_complex_self_parameter_edge_cases() {
        let rust_code = r#"
struct ComplexSelf;

impl ComplexSelf {
    // Edge cases for self parameter detection
    fn method_self_colon_type(self: ComplexSelf) -> i32 { 42 }
    fn method_self_box(self: Box<ComplexSelf>) -> i32 { 42 }
    fn method_self_rc(self: std::rc::Rc<ComplexSelf>) -> i32 { 42 }
    fn method_self_arc(self: std::sync::Arc<ComplexSelf>) -> i32 { 42 }
    fn method_self_pin(self: std::pin::Pin<Box<ComplexSelf>>) -> i32 { 42 }
    fn method_self_pin_mut(self: std::pin::Pin<&mut ComplexSelf>) -> i32 { 42 }
    
    // Complex pattern that might confuse the parser
    fn method_with_self_in_param(not_self: &ComplexSelf, self_param: &str) -> i32 { 42 }
    fn method_self_generic<T>(self: Box<T>) -> i32 where T: Clone { 42 }
    
    // Destructured self (edge case)
    fn method_destructured_self(ComplexSelf: ComplexSelf) -> i32 { 42 }
    
    // Associated functions that definitely don't have self
    fn new() -> ComplexSelf { ComplexSelf }
    fn with_param(param: i32) -> ComplexSelf { ComplexSelf }
}
"#;

        let parser = GenericParser::default_for_language(Language::Rust);
        let parse_result = parser.parse(rust_code, Some("complex_self.rs")).unwrap();
        let (rust_node_fqn_map, _, _, _) = build_fqn_and_node_indices(&parse_result.ast);

        let fqns_with_types: Vec<_> = rust_node_fqn_map
            .values()
            .map(|(_, fqn_parts)| {
                let fqn = RustFqn::new((**fqn_parts).clone());
                let last_part = fqn.parts.last().unwrap();
                (rust_fqn_to_string(&fqn), last_part.node_type)
            })
            .collect();

        println!("Complex self parameter FQNs:");
        for (fqn, fqn_type) in &fqns_with_types {
            println!("  {fqn} -> {fqn_type:?}");
        }

        // All the self variations should be detected as methods
        let self_methods = [
            "ComplexSelf::method_self_colon_type",
            "ComplexSelf::method_self_box",
            "ComplexSelf::method_self_rc",
            "ComplexSelf::method_self_arc",
            "ComplexSelf::method_self_pin",
            "ComplexSelf::method_self_pin_mut",
            "ComplexSelf::method_self_generic",
        ];

        for method_name in &self_methods {
            let method_fqn = fqns_with_types.iter().find(|(fqn, _)| fqn == method_name);
            assert!(method_fqn.is_some(), "Should find method {method_name}");
            assert_eq!(
                method_fqn.unwrap().1,
                RustFqnPartType::Method,
                "Method {method_name} should be classified as Method"
            );
        }

        // These should be associated functions (no self)
        let associated_functions = [
            "ComplexSelf::new",
            "ComplexSelf::with_param",
            "ComplexSelf::method_with_self_in_param", // self in non-first param doesn't count
            "ComplexSelf::method_destructured_self",  // destructured doesn't count as self
        ];

        for func_name in &associated_functions {
            let func_fqn = fqns_with_types.iter().find(|(fqn, _)| fqn == func_name);
            assert!(func_fqn.is_some(), "Should find function {func_name}");
            assert_eq!(
                func_fqn.unwrap().1,
                RustFqnPartType::AssociatedFunction,
                "Function {func_name} should be classified as AssociatedFunction"
            );
        }
    }
}
