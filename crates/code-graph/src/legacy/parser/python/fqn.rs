use crate::legacy::parser::python::types::{
    PythonDefinitionInfo, PythonDefinitionType, PythonFqn, PythonFqnPart, PythonNodeFqnMap,
};
use crate::utils::{Range, node_to_range};
use rustc_hash::FxHashMap;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

type ScopeStack = Vec<PythonFqnPart>;

/// Type alias for the list of definitions extracted during FQN traversal
pub type PythonDefinitions = Vec<PythonDefinitionInfo>;

/// Returns the FQN with metadata as a string, joined by '.'
pub fn python_fqn_to_string(fqn: &PythonFqn) -> String {
    fqn.parts
        .iter()
        .map(|part| part.node_name().replace('.', "#"))
        .collect::<Vec<_>>()
        .join(".")
}

fn get_class_definition_type<'a>(node: &Node<'a, StrDoc<SupportLang>>) -> PythonDefinitionType {
    if has_decorators(node) {
        PythonDefinitionType::DecoratedClass
    } else {
        PythonDefinitionType::Class
    }
}

fn get_function_definition_type<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> PythonDefinitionType {
    let is_async = node.children().any(|child| child.kind() == "async");
    let has_decorator = has_decorators(node);
    let is_method = is_in_class_scope(current_scope);

    match (is_method, is_async, has_decorator) {
        (true, true, true) => PythonDefinitionType::DecoratedAsyncMethod,
        (true, true, false) => PythonDefinitionType::AsyncMethod,
        (true, false, true) => PythonDefinitionType::DecoratedMethod,
        (true, false, false) => PythonDefinitionType::Method,
        (false, true, true) => PythonDefinitionType::DecoratedAsyncFunction,
        (false, true, false) => PythonDefinitionType::AsyncFunction,
        (false, false, true) => PythonDefinitionType::DecoratedFunction,
        (false, false, false) => PythonDefinitionType::Function,
    }
}

// Process a node and returns the FQN part if the node creates a scope,
// and a definition if this node represents one
fn process_node<'a>(
    node: Node<'a, StrDoc<SupportLang>>,
    current_scope: &mut ScopeStack,
    node_fqn_map: &mut FxHashMap<Range, (Node<'a, StrDoc<SupportLang>>, PythonFqn)>,
) -> Option<(PythonDefinitionInfo, Option<PythonFqnPart>)> {
    let node_kind = node.kind();
    if node_kind == "class_definition" {
        if let Some(name_node) = node.field("name") {
            let name = name_node.text().to_string();
            let range = node_to_range(&name_node);
            let definition_type = get_class_definition_type(&node);

            let mut fqn_parts = current_scope.clone();
            let fqn_part = PythonFqnPart::new(definition_type, name.clone(), range);
            fqn_parts.push(fqn_part.clone());
            let fqn = PythonFqn::new(fqn_parts);
            node_fqn_map.insert(range, (name_node.clone(), fqn.clone()));

            // Get the full range of the class definition for the definition info
            let def_range = node_to_range(&node);
            let definition = PythonDefinitionInfo::new(definition_type, name, fqn, def_range);

            return Some((definition, Some(fqn_part)));
        }
    } else if node_kind == "function_definition" {
        if let Some(name_node) = node.field("name") {
            let name = name_node.text().to_string();
            let range = node_to_range(&name_node);
            let definition_type = get_function_definition_type(&node, current_scope);

            let mut fqn_parts = current_scope.clone();
            let fqn_part = PythonFqnPart::new(definition_type, name.clone(), range);
            fqn_parts.push(fqn_part.clone());
            let fqn = PythonFqn::new(fqn_parts);
            node_fqn_map.insert(range, (name_node.clone(), fqn.clone()));

            // Get the full range - for decorated functions, get the decorated_definition range
            let def_range = if let Some(parent) = node.parent()
                && parent.kind() == "decorated_definition"
            {
                node_to_range(&parent)
            } else {
                node_to_range(&node)
            };
            let definition = PythonDefinitionInfo::new(definition_type, name, fqn, def_range);

            return Some((definition, Some(fqn_part)));
        }
    } else if node_kind == "assignment" {
        if let (Some(left_node), Some(right_node)) = (node.field("left"), node.field("right")) {
            let left_kind = left_node.kind();
            if ["identifier", "attribute"].contains(&&*left_kind)
                && is_lambda_assignment(&right_node)
            {
                let name = if left_kind == "attribute" {
                    extract_attribute_path(&left_node)
                } else {
                    left_node.text().to_string()
                };
                let range = node_to_range(&left_node);
                let definition_type = PythonDefinitionType::Lambda;

                let mut fqn_parts = current_scope.clone();
                let fqn_part = PythonFqnPart::new(definition_type, name.clone(), range);
                fqn_parts.push(fqn_part.clone());
                let fqn = PythonFqn::new(fqn_parts);
                node_fqn_map.insert(range, (node.clone(), fqn.clone()));

                // Lambda definitions use the assignment range
                let def_range = node_to_range(&node);
                let definition = PythonDefinitionInfo::new(definition_type, name, fqn, def_range);

                // Lambdas don't create a scope (we don't descend into them for FQN purposes)
                return Some((definition, None));
            }
        }
    } else if [
        "import_statement",
        "import_from_statement",
        "future_import_statement",
    ]
    .contains(&&*node_kind)
    {
        let fqn_parts = current_scope.clone();
        node_fqn_map.insert(
            node_to_range(&node),
            (node.clone(), PythonFqn::new(fqn_parts)),
        );
    }

    None
}

/// Traverses AST and builds map of byte ranges to (Node, FQN), also collecting definitions
///
/// Returns a tuple of (PythonNodeFqnMap, PythonDefinitions)
pub fn build_fqn_index<'a>(
    ast: &'a Root<StrDoc<SupportLang>>,
) -> (PythonNodeFqnMap<'a>, PythonDefinitions) {
    let mut node_fqn_map = FxHashMap::with_capacity_and_hasher(128, Default::default());
    let mut definitions = Vec::with_capacity(64);
    let mut current_scope: ScopeStack = vec![]; // Stack of FQN parts

    // Stack of nodes to process
    let mut stack: Vec<Option<Node<StrDoc<SupportLang>>>> = vec![];
    stack.push(Some(ast.root()));

    while let Some(node_option) = stack.pop() {
        if let Some(node) = node_option {
            let children: Vec<_> = node.children().collect();
            stack.reserve(children.len());

            if let Some((def, scope_part)) =
                process_node(node, &mut current_scope, &mut node_fqn_map)
            {
                definitions.push(def);
                if let Some(part) = scope_part {
                    current_scope.push(part);
                    // Sentinel indicating end of scope
                    stack.push(None);
                }
            }

            for child in children.into_iter().rev() {
                stack.push(Some(child));
            }
        } else {
            current_scope.pop();
        }
    }

    (node_fqn_map, definitions)
}

/// Find Python FQN with metadata
pub fn find_python_fqn_for_node<'a>(
    range: Range,
    node_fqn_map: &PythonNodeFqnMap<'a>,
) -> Option<PythonFqn> {
    node_fqn_map.get(&range).map(|(_, fqn)| fqn.clone())
}

/// Helper function to check if we're in a class scope
fn is_in_class_scope(current_scope: &ScopeStack) -> bool {
    current_scope
        .last()
        .map(|part| {
            part.node_type == PythonDefinitionType::Class
                || part.node_type == PythonDefinitionType::DecoratedClass
        })
        .unwrap_or(false)
}

/// Helper function to check if a node has decorators
fn has_decorators(node: &Node<StrDoc<SupportLang>>) -> bool {
    if let Some(parent) = node.parent()
        && parent.kind() == "decorated_definition"
    {
        return true;
    }

    false
}

/// Check if a node represents a lambda function assignment
fn is_lambda_assignment(node: &Node<StrDoc<SupportLang>>) -> bool {
    let node_kind = node.kind();

    if node_kind == "call" {
        // Handles cases like my_var = (lambda x: x)(1)
        false
    } else if node_kind == "lambda" {
        true
    } else if node_kind == "parenthesized_expression" {
        // Handles cases like my_fn = (lambda x: x)
        if let Some(inner) = node.child(0) {
            is_lambda_assignment(&inner)
        } else {
            false
        }
    } else {
        false
    }
}

/// Extract attribute path (used for attributes in lambda assignments)
fn extract_attribute_path(node: &Node<StrDoc<SupportLang>>) -> String {
    let node_kind = node.kind();
    if node_kind == "attribute" {
        let mut parts = Vec::new();

        // Get the object part (left side of the dot)
        if let Some(object) = node.field("object") {
            parts.push(extract_attribute_path(&object));
        }

        // Get the attribute part (right side of the dot)
        if let Some(attribute) = node.field("attribute") {
            parts.push(attribute.text().to_string());
        }

        parts.join(".")
    } else {
        node.text().to_string()
    }
}
