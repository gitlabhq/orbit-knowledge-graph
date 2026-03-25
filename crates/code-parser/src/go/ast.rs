use std::collections::HashSet;

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::go::imports::extract_imports;
use crate::go::types::{
    GoDefinitionInfo, GoDefinitionMetadata, GoDefinitionType, GoFqn, GoImportedSymbolInfo,
    GoInterfaceMethod, GoParameter, GoReferenceInfo, GoReferenceType, GoReturnType, GoSignature,
    GoStructField, GoTypeParameter,
};
use crate::utils::node_to_range;

use super::analyzer::GoAnalyzerResult;

/// Convert a GoFqn to its string representation
///
/// Format:
/// - Functions: `package.FunctionName`
/// - Methods: `package.ReceiverType.MethodName`
/// - Types: `package.TypeName`
pub fn go_fqn_to_string(fqn: &GoFqn) -> String {
    match (&fqn.package, &fqn.receiver) {
        (Some(pkg), Some(recv)) => {
            let clean_recv = recv.trim_start_matches('*');
            format!("{}.{}.{}", pkg, clean_recv, fqn.name)
        }
        (Some(pkg), None) => format!("{}.{}", pkg, fqn.name),
        (None, Some(recv)) => {
            let clean_recv = recv.trim_start_matches('*');
            format!("{}.{}", clean_recv, fqn.name)
        }
        (None, None) => fqn.name.clone(),
    }
}

/// Parse a Go AST and extract all definitions, imports, and references
pub fn parse_ast(ast: &Root<StrDoc<SupportLang>>) -> GoAnalyzerResult {
    let root = ast.root();

    let mut definitions = Vec::new();
    let mut imports = Vec::new();
    let mut references = Vec::new();

    let package_name = extract_package_name(&root);
    extract_imports(&root, &mut imports);

    let known_packages = build_known_packages(&imports);

    extract_definitions(&root, &mut definitions, package_name.as_deref());
    extract_references(&root, &mut references, &known_packages);

    GoAnalyzerResult {
        definitions,
        imports,
        references,
    }
}

fn extract_package_name(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    for child in node.children() {
        if child.kind().as_ref() == "package_clause" {
            for package_child in child.children() {
                if package_child.kind().as_ref() == "package_identifier" {
                    return Some(package_child.text().to_string());
                }
            }
        }
    }
    None
}

fn extract_definitions(
    node: &Node<StrDoc<SupportLang>>,
    definitions: &mut Vec<GoDefinitionInfo>,
    current_package: Option<&str>,
) {
    match node.kind().as_ref() {
        "function_declaration" => {
            if let Some(def) = extract_function(node, current_package) {
                definitions.push(def);
            }
            return;
        }
        "method_declaration" => {
            if let Some(def) = extract_method(node, current_package) {
                definitions.push(def);
            }
            return;
        }
        "func_literal" => {
            return;
        }
        "type_declaration" => {
            extract_type_definitions(node, definitions, current_package);
        }
        "const_declaration" => {
            extract_const_declarations(node, definitions, current_package);
        }
        "var_declaration" => {
            extract_var_declarations(node, definitions, current_package);
        }
        _ => {}
    }

    for child in node.children() {
        extract_definitions(&child, definitions, current_package);
    }
}

fn extract_function(
    node: &Node<StrDoc<SupportLang>>,
    package: Option<&str>,
) -> Option<GoDefinitionInfo> {
    let name_node = node.field("name")?;
    let name = name_node.text().to_string();

    let range = node_to_range(node);
    let docstring = extract_docstring(node);
    let signature = extract_signature(node);
    let type_parameters = extract_type_parameters(node);

    Some(GoDefinitionInfo {
        name: name.clone(),
        definition_type: GoDefinitionType::Function,
        range,
        fqn: GoFqn {
            package: package.map(String::from),
            receiver: None,
            name,
        },
        metadata: Some(GoDefinitionMetadata {
            docstring,
            signature: Some(signature),
            struct_fields: None,
            interface_methods: None,
            type_parameters: if type_parameters.is_empty() {
                None
            } else {
                Some(type_parameters)
            },
        }),
    })
}

fn extract_method(
    node: &Node<StrDoc<SupportLang>>,
    package: Option<&str>,
) -> Option<GoDefinitionInfo> {
    let name_node = node.field("name")?;
    let name = name_node.text().to_string();

    let receiver = node
        .field("receiver")
        .and_then(|r| extract_receiver_type(&r));

    let range = node_to_range(node);
    let docstring = extract_docstring(node);
    let signature = extract_signature(node);
    let type_parameters = extract_type_parameters(node);

    Some(GoDefinitionInfo {
        name: name.clone(),
        definition_type: GoDefinitionType::Method,
        range,
        fqn: GoFqn {
            package: package.map(String::from),
            receiver,
            name,
        },
        metadata: Some(GoDefinitionMetadata {
            docstring,
            signature: Some(signature),
            struct_fields: None,
            interface_methods: None,
            type_parameters: if type_parameters.is_empty() {
                None
            } else {
                Some(type_parameters)
            },
        }),
    })
}

fn extract_type_definitions(
    node: &Node<StrDoc<SupportLang>>,
    definitions: &mut Vec<GoDefinitionInfo>,
    package: Option<&str>,
) {
    for child in node.children() {
        match child.kind().as_ref() {
            "type_spec" => {
                if let Some(def) = extract_type_spec(&child, package) {
                    definitions.push(def);
                }
            }
            "type_spec_list" => {
                for spec_child in child.children() {
                    if spec_child.kind().as_ref() == "type_spec"
                        && let Some(def) = extract_type_spec(&spec_child, package)
                    {
                        definitions.push(def);
                    }
                }
            }
            _ => {}
        }
    }
}

fn extract_type_spec(
    node: &Node<StrDoc<SupportLang>>,
    package: Option<&str>,
) -> Option<GoDefinitionInfo> {
    let name_node = node.field("name")?;
    let name = name_node.text().to_string();

    let type_node = node.field("type")?;
    let (definition_type, struct_fields, interface_methods) = match type_node.kind().as_ref() {
        "struct_type" => {
            let fields = extract_struct_fields(&type_node);
            (GoDefinitionType::Struct, Some(fields), None)
        }
        "interface_type" => {
            let methods = extract_interface_methods(&type_node);
            (GoDefinitionType::Interface, None, Some(methods))
        }
        _ => (GoDefinitionType::Type, None, None),
    };

    let range = node_to_range(node);
    let docstring = extract_docstring(node);

    let type_parameters = node
        .field("type_parameters")
        .map(|tp_node| extract_type_parameter_list(&tp_node))
        .filter(|tp| !tp.is_empty());

    Some(GoDefinitionInfo {
        name: name.clone(),
        definition_type,
        range,
        fqn: GoFqn {
            package: package.map(String::from),
            receiver: None,
            name,
        },
        metadata: Some(GoDefinitionMetadata {
            docstring,
            signature: None,
            struct_fields,
            interface_methods,
            type_parameters,
        }),
    })
}

fn extract_const_declarations(
    node: &Node<StrDoc<SupportLang>>,
    definitions: &mut Vec<GoDefinitionInfo>,
    package: Option<&str>,
) {
    for child in node.children() {
        match child.kind().as_ref() {
            "const_spec" => {
                extract_const_or_var_spec(&child, GoDefinitionType::Constant, package, definitions);
            }
            "const_spec_list" => {
                for spec_child in child.children() {
                    if spec_child.kind().as_ref() == "const_spec" {
                        extract_const_or_var_spec(
                            &spec_child,
                            GoDefinitionType::Constant,
                            package,
                            definitions,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

fn extract_var_declarations(
    node: &Node<StrDoc<SupportLang>>,
    definitions: &mut Vec<GoDefinitionInfo>,
    package: Option<&str>,
) {
    for child in node.children() {
        match child.kind().as_ref() {
            "var_spec" => {
                extract_const_or_var_spec(&child, GoDefinitionType::Variable, package, definitions);
            }
            "var_spec_list" => {
                for spec_child in child.children() {
                    if spec_child.kind().as_ref() == "var_spec" {
                        extract_const_or_var_spec(
                            &spec_child,
                            GoDefinitionType::Variable,
                            package,
                            definitions,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

fn extract_const_or_var_spec(
    node: &Node<StrDoc<SupportLang>>,
    def_type: GoDefinitionType,
    package: Option<&str>,
    definitions: &mut Vec<GoDefinitionInfo>,
) {
    for child in node.children() {
        if child.kind().as_ref() == "identifier" {
            let name = child.text().to_string();
            let range = node_to_range(node);
            let docstring = extract_docstring(node);

            definitions.push(GoDefinitionInfo {
                name: name.clone(),
                definition_type: def_type,
                range,
                fqn: GoFqn {
                    package: package.map(String::from),
                    receiver: None,
                    name,
                },
                metadata: Some(GoDefinitionMetadata {
                    docstring,
                    signature: None,
                    struct_fields: None,
                    interface_methods: None,
                    type_parameters: None,
                }),
            });
        }
    }
}

fn build_known_packages(imports: &[GoImportedSymbolInfo]) -> HashSet<String> {
    let mut known = HashSet::new();
    for import in imports {
        if let Some(ref id) = import.identifier {
            let name = &id.name;
            if name != "_" && name != "." {
                known.insert(name.clone());
            }
            if let Some(ref alias) = id.alias
                && alias != "_"
                && alias != "."
            {
                known.insert(alias.clone());
            }
        }
    }
    known
}

fn extract_references(
    node: &Node<StrDoc<SupportLang>>,
    references: &mut Vec<GoReferenceInfo>,
    known_packages: &HashSet<String>,
) {
    match node.kind().as_ref() {
        "call_expression" => {
            extract_call_reference(node, references, known_packages);
        }
        "composite_literal" => {
            extract_struct_instantiation(node, references);
        }
        _ => {}
    }

    for child in node.children() {
        extract_references(&child, references, known_packages);
    }
}

fn extract_call_reference(
    node: &Node<StrDoc<SupportLang>>,
    references: &mut Vec<GoReferenceInfo>,
    known_packages: &HashSet<String>,
) {
    if let Some(function_node) = node.field("function") {
        match function_node.kind().as_ref() {
            "selector_expression" => {
                if let Some(field_node) = function_node.field("field") {
                    let is_method_call = if let Some(operand) = function_node.field("operand") {
                        match operand.kind().as_ref() {
                            "identifier" => {
                                let operand_text = operand.text();
                                !known_packages.contains(operand_text.as_ref())
                            }
                            "call_expression" | "selector_expression" | "composite_literal" => true,
                            _ => false,
                        }
                    } else {
                        false
                    };

                    let ref_type = if is_method_call {
                        GoReferenceType::MethodCall
                    } else {
                        GoReferenceType::FunctionCall
                    };

                    references.push(GoReferenceInfo {
                        name: field_node.text().to_string(),
                        range: node_to_range(&field_node),
                        reference_type: ref_type,
                    });
                }
            }
            "identifier" => {
                references.push(GoReferenceInfo {
                    name: function_node.text().to_string(),
                    range: node_to_range(&function_node),
                    reference_type: GoReferenceType::FunctionCall,
                });
            }
            _ => {}
        }
    }
}

fn extract_struct_instantiation(
    node: &Node<StrDoc<SupportLang>>,
    references: &mut Vec<GoReferenceInfo>,
) {
    if let Some(type_node) = node.field("type") {
        let type_name = match type_node.kind().as_ref() {
            "type_identifier" => Some(type_node.text().to_string()),
            "qualified_type" => type_node
                .field("name")
                .map(|n| n.text().to_string())
                .or_else(|| Some(type_node.text().to_string())),
            _ => None,
        };

        if let Some(name) = type_name {
            references.push(GoReferenceInfo {
                name,
                range: node_to_range(&type_node),
                reference_type: GoReferenceType::StructInstantiation,
            });
        }
    }
}

fn extract_receiver_type(receiver_node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    for child in receiver_node.children() {
        if child.kind().as_ref() == "parameter_declaration"
            && let Some(type_node) = child.field("type")
        {
            let raw = type_node.text().to_string();
            let without_ptr = raw.trim_start_matches('*');
            let clean = if let Some(bracket_pos) = without_ptr.find('[') {
                without_ptr[..bracket_pos].to_string()
            } else {
                without_ptr.to_string()
            };
            return Some(clean);
        }
    }
    None
}

fn extract_signature(node: &Node<StrDoc<SupportLang>>) -> GoSignature {
    let parameters = node
        .field("parameters")
        .map(|p| extract_parameters(&p))
        .unwrap_or_default();

    let return_types = node
        .field("result")
        .map(|r| extract_return_types(&r))
        .unwrap_or_default();

    let type_parameters = node
        .field("type_parameters")
        .map(|tp| extract_type_parameter_list(&tp))
        .unwrap_or_default();

    GoSignature {
        parameters,
        return_types,
        type_parameters,
    }
}

fn extract_parameters(params_node: &Node<StrDoc<SupportLang>>) -> Vec<GoParameter> {
    let mut parameters = Vec::new();

    for child in params_node.children() {
        match child.kind().as_ref() {
            "parameter_declaration" => {
                let mut names: Vec<String> = Vec::new();
                let mut is_variadic = false;

                for param_child in child.children() {
                    if param_child.kind().as_ref() == "identifier" {
                        names.push(param_child.text().to_string());
                    }
                }

                let param_type = if let Some(type_node) = child.field("type") {
                    let type_text = type_node.text().to_string();
                    if type_text.starts_with("...") {
                        is_variadic = true;
                        type_text.trim_start_matches("...").to_string()
                    } else {
                        type_text
                    }
                } else {
                    String::new()
                };

                if names.is_empty() {
                    if !param_type.is_empty() {
                        parameters.push(GoParameter {
                            name: String::new(),
                            param_type,
                            is_variadic,
                        });
                    }
                } else {
                    for name in names {
                        parameters.push(GoParameter {
                            name,
                            param_type: param_type.clone(),
                            is_variadic,
                        });
                    }
                }
            }
            "variadic_parameter_declaration" => {
                let name = child
                    .field("name")
                    .map(|n| n.text().to_string())
                    .unwrap_or_default();
                let param_type = child
                    .field("type")
                    .map(|t| t.text().to_string())
                    .unwrap_or_default();
                parameters.push(GoParameter {
                    name,
                    param_type,
                    is_variadic: true,
                });
            }
            _ => {}
        }
    }

    parameters
}

fn extract_return_types(result_node: &Node<StrDoc<SupportLang>>) -> Vec<GoReturnType> {
    let mut return_types = Vec::new();

    match result_node.kind().as_ref() {
        "parameter_list" => {
            for child in result_node.children() {
                if child.kind().as_ref() == "parameter_declaration" {
                    let mut names: Vec<String> = Vec::new();
                    for param_child in child.children() {
                        if param_child.kind().as_ref() == "identifier" {
                            names.push(param_child.text().to_string());
                        }
                    }
                    let type_name = child
                        .field("type")
                        .map(|t| t.text().to_string())
                        .unwrap_or_default();

                    if names.is_empty() {
                        return_types.push(GoReturnType {
                            type_name,
                            name: None,
                        });
                    } else {
                        for name in names {
                            return_types.push(GoReturnType {
                                type_name: type_name.clone(),
                                name: Some(name),
                            });
                        }
                    }
                }
            }
        }
        _ => {
            return_types.push(GoReturnType {
                type_name: result_node.text().to_string(),
                name: None,
            });
        }
    }

    return_types
}

fn extract_type_parameters(node: &Node<StrDoc<SupportLang>>) -> Vec<GoTypeParameter> {
    node.field("type_parameters")
        .map(|tp_node| extract_type_parameter_list(&tp_node))
        .unwrap_or_default()
}

fn extract_type_parameter_list(tp_node: &Node<StrDoc<SupportLang>>) -> Vec<GoTypeParameter> {
    let mut type_params = Vec::new();

    for child in tp_node.children() {
        if child.kind().as_ref() == "type_parameter_declaration" {
            let constraint = child
                .field("type")
                .map(|t| t.text().to_string())
                .unwrap_or_else(|| "any".to_string());

            let names: Vec<String> = {
                let mut ns = Vec::new();
                for pc in child.children() {
                    let k = pc.kind();
                    if k.as_ref() == "identifier" || k.as_ref() == "type_identifier" {
                        ns.push(pc.text().to_string());
                    }
                }
                ns
            };

            for name in names {
                type_params.push(GoTypeParameter {
                    name,
                    constraint: constraint.clone(),
                });
            }
        }
    }

    type_params
}

fn extract_struct_fields(struct_node: &Node<StrDoc<SupportLang>>) -> Vec<GoStructField> {
    let mut fields = Vec::new();

    for child in struct_node.children() {
        if child.kind().as_ref() == "field_declaration_list" {
            for field_child in child.children() {
                if field_child.kind().as_ref() == "field_declaration" {
                    extract_field_declaration(&field_child, &mut fields);
                }
            }
        }
    }

    fields
}

fn extract_field_declaration(node: &Node<StrDoc<SupportLang>>, fields: &mut Vec<GoStructField>) {
    let has_names = node
        .children()
        .any(|c| c.kind().as_ref() == "field_identifier");

    if has_names {
        let field_type = node
            .field("type")
            .map(|t| t.text().to_string())
            .unwrap_or_default();

        let tag = node
            .children()
            .find(|c| {
                c.kind().as_ref() == "raw_string_literal"
                    || c.kind().as_ref() == "interpreted_string_literal"
            })
            .map(|t| t.text().to_string());

        for child in node.children() {
            if child.kind().as_ref() == "field_identifier" {
                fields.push(GoStructField {
                    name: child.text().to_string(),
                    field_type: field_type.clone(),
                    tag: tag.clone(),
                    is_embedded: false,
                });
            }
        }
    } else if let Some(type_node) = node.field("type") {
        fields.push(GoStructField {
            name: String::new(),
            field_type: type_node.text().to_string(),
            tag: None,
            is_embedded: true,
        });
    }
}

fn extract_interface_methods(interface_node: &Node<StrDoc<SupportLang>>) -> Vec<GoInterfaceMethod> {
    let mut methods = Vec::new();

    for child in interface_node.children() {
        match child.kind().as_ref() {
            "method_elem" | "method_spec" => {
                if let Some(method) = extract_interface_method(&child) {
                    methods.push(method);
                }
            }
            _ => {}
        }
    }

    methods
}

fn extract_interface_method(method_node: &Node<StrDoc<SupportLang>>) -> Option<GoInterfaceMethod> {
    let name_node = method_node.field("name")?;
    let name = name_node.text().to_string();

    let parameters = method_node
        .field("parameters")
        .map(|p| extract_parameters(&p))
        .unwrap_or_default();

    let return_types = method_node
        .field("result")
        .map(|r| extract_return_types(&r))
        .unwrap_or_default();

    let type_parameters = method_node
        .field("type_parameters")
        .map(|tp| extract_type_parameter_list(&tp))
        .unwrap_or_default();

    Some(GoInterfaceMethod {
        name,
        parameters,
        return_types,
        type_parameters,
    })
}

fn clean_comment(raw: &str) -> Option<String> {
    let cleaned = raw
        .lines()
        .map(|line| {
            let line = line.trim();
            if line.starts_with("//") {
                line.trim_start_matches("//").trim().to_string()
            } else if line.starts_with("/*") {
                line.trim_start_matches("/*").trim().to_string()
            } else if line.ends_with("*/") {
                line.trim_end_matches("*/").trim().to_string()
            } else if line.starts_with('*') {
                line.trim_start_matches('*').trim().to_string()
            } else {
                line.to_string()
            }
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn extract_docstring(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    let mut lines_rev: Vec<String> = Vec::new();

    let mut cursor = node.prev();
    while let Some(sibling) = cursor {
        if sibling.kind().as_ref() == "comment" {
            if let Some(cleaned) = clean_comment(sibling.text().as_ref()) {
                lines_rev.push(cleaned);
            }
            cursor = sibling.prev();
        } else {
            break;
        }
    }

    if lines_rev.is_empty() {
        return None;
    }

    lines_rev.reverse();
    Some(lines_rev.join("\n"))
}
