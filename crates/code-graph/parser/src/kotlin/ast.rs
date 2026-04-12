use std::cmp::{max, min};
use std::collections::HashSet;
use std::sync::Arc;

use tracing::error;

use smallvec::{SmallVec, smallvec};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::kotlin::analyzer::KotlinAnalyzerResult;
use crate::kotlin::imports::parse_import_node;
use crate::kotlin::types::{
    AstNode, AstRootNode, KotlinDefinitionInfo, KotlinDefinitionMetadata, KotlinDefinitionType,
    KotlinDefinitions, KotlinExpression, KotlinExpressionInfo, KotlinFqn, KotlinFqnPart,
    KotlinFqnPartType, KotlinImports, KotlinNodeFqnMap, KotlinReferenceInfo, KotlinReferenceType,
    KotlinReferences, node_types,
};
use crate::kotlin::utils::{get_child_by_any_kind, get_child_by_kind, get_children_by_kind};
use crate::references::ReferenceTarget;
use crate::utils::{Range, node_to_range};

const LAMBDA_TYPES: &[&str] = &[
    node_types::LAMBDA_LITERAL,
    node_types::ANONYMOUS_FUNCTION,
    node_types::CALLABLE_REFERENCE,
];

const REFERENCE_PARENT_TYPES: &[&str] = &[
    node_types::JUMP_EXPRESSION,
    node_types::PROPERTY,
    node_types::FUNCTION_VALUE_PARAMETERS,
    node_types::FUNCTION_BODY,
    node_types::STATEMENTS,
    node_types::PREFIX_EXPRESSION,
    node_types::VALUE_ARGUMENT,
    node_types::MULTIPLICATIVE_EXPRESSION,
    node_types::ADDITIVE_EXPRESSION,
    node_types::WHEN_SUBJECT,
    node_types::WHEN_ENTRY,
    node_types::WHEN_CONDITION,
    node_types::CONTROL_STRUCTURE_BODY,
    node_types::POSTFIX_EXPRESSION,
    node_types::EQUALITY_EXPRESSION,
    node_types::ASSIGNMENT,
    node_types::CONJUNCTION_EXPRESSION,
    node_types::ELVIS_EXPRESSION,
    node_types::INDEXING_SUFFIX,
    node_types::CHECK_EXPRESSION,
    node_types::IF_EXPRESSION,
    node_types::FOR_STATEMENT,
    node_types::WHILE_STATEMENT,
    node_types::DO_WHILE_STATEMENT,
    node_types::RANGE_EXPRESSION,
    node_types::COMPARISON_EXPRESSION,
    node_types::INTERPOLATED_EXPRESSION,
    node_types::AS_EXPRESSION,
    node_types::SOURCE_FILE,
    node_types::STRING_LITERAL,
    node_types::DISJUNCTION_EXPRESSION,
    node_types::PARENTHESIZED_EXPRESSION,
    node_types::ASSIGNMENT,
];

pub mod node_names {
    pub const COMPANION_OBJECT: &str = "Companion";
    pub const CONSTRUCTOR: &str = "<init>";
}

struct LexicalScope {
    range: Range,
    created_by_definition: bool,
    top_level: bool,
}

impl LexicalScope {
    fn created_by_definition(range: Range) -> Self {
        Self {
            range,
            created_by_definition: true,
            top_level: false,
        }
    }

    fn created_by_block(range: Range) -> Self {
        Self {
            range,
            created_by_definition: false,
            top_level: false,
        }
    }

    fn created_by_file(range: Range) -> Self {
        Self {
            range,
            created_by_definition: false,
            top_level: true,
        }
    }
}

type ScopeStack = SmallVec<[KotlinFqnPart; 8]>;
type LexicalScopeStack = SmallVec<[LexicalScope; 8]>;

pub(in crate::kotlin) struct AstParseResult {
    definitions: KotlinDefinitions,
    imports: KotlinImports,
    references: KotlinReferences,
    current_scope: ScopeStack, // The current definition scope
    current_lexical_scope: LexicalScopeStack, // The current lexical scope
    visited_nodes: HashSet<usize>,
}

impl Default for AstParseResult {
    fn default() -> Self {
        Self {
            definitions: Vec::new(),
            imports: Vec::new(),
            references: Vec::new(),
            current_scope: smallvec![],
            current_lexical_scope: smallvec![],
            visited_nodes: HashSet::new(),
        }
    }
}

fn parse_simple_node<'a>(
    node: AstNode<'a>,
    name_node: AstNode<'a>,
    node_type: KotlinFqnPartType,
    parse_result: &mut AstParseResult,
) -> bool {
    let range = node_to_range(&node);

    let fqn_part = KotlinFqnPart::new(node_type, name_node.text().to_string(), range);

    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(range));

    let definition = KotlinDefinitionInfo::new(
        KotlinDefinitionType::from_fqn_part_type(&fqn_part.node_type).unwrap(),
        fqn_part.node_name().to_string(),
        Arc::new(parse_result.current_scope.clone()),
        range,
    );

    parse_result.definitions.push(definition);
    true
}

fn parse_property_declaration_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let mut receiver_type = None;
    let mut simple_identifier = None;
    let mut property_type = None;
    let mut init_node = None;

    for child in node.children() {
        parse_result.visited_nodes.insert(child.node_id());

        if !child.is_named() {
            continue;
        }

        if child.kind() == node_types::MODIFIERS {
            continue;
        } else if child.kind() == node_types::VARIABLE_DECLARATION {
            for variable_child in child.children() {
                parse_result.visited_nodes.insert(variable_child.node_id());

                if variable_child.kind() == node_types::SIMPLE_IDENTIFIER {
                    simple_identifier = Some(variable_child);
                    continue;
                }

                property_type = parse_type_node(&variable_child);
            }
        } else if simple_identifier.is_none() {
            receiver_type = parse_type_node(&child);
        } else {
            if LAMBDA_TYPES.contains(&child.kind().as_ref()) {
                return parse_simple_node(
                    child.clone(),
                    #[allow(clippy::unnecessary_unwrap)]
                    simple_identifier.unwrap(), // False positive
                    KotlinFqnPartType::Lambda,
                    parse_result,
                );
            }
            init_node = Some(child);
        }
    }

    if simple_identifier.is_none() {
        return false;
    }

    // Getters and setters are parsed as separate nodes, so we need to merge them into the property node.
    let mut getter = None;
    let mut setter = None;

    if let Some(next_node) = node.next() {
        if next_node.kind() == node_types::GETTER {
            getter = Some(next_node.clone());

            if let Some(setter_node) = next_node.next()
                && setter_node.kind() == node_types::SETTER
            {
                setter = Some(setter_node);
            }
        }

        if next_node.kind() == node_types::SETTER {
            setter = Some(next_node.clone());
        }
    }

    let node_range =
        calculate_combined_range(node_to_range(node), getter.as_ref(), setter.as_ref());
    let node_name = simple_identifier.unwrap().text().to_string();
    let node_type = match parse_result.current_scope.last() {
        Some(last_fqn_part) => match last_fqn_part.node_type {
            KotlinFqnPartType::Function => KotlinFqnPartType::LocalVariable,
            _ => KotlinFqnPartType::Property,
        },
        None => KotlinFqnPartType::Property,
    };

    let fqn_part = KotlinFqnPart::new(node_type.clone(), node_name.clone(), node_range);

    let creates_scope = !matches!(node_type, KotlinFqnPartType::LocalVariable);
    if creates_scope {
        parse_result
            .current_lexical_scope
            .push(LexicalScope::created_by_definition(node_range));

        parse_result.current_scope.push(fqn_part.clone());
    }

    let mut init = match init_node {
        Some(n) => parse_reference_expression_node(&n, parse_result),
        None => None,
    };

    if property_type.is_none()
        && init.is_none()
        && getter.is_some()
        && let Some(getter_expression) =
            get_child_by_kind(&getter.unwrap(), node_types::FUNCTION_BODY)
    {
        for child in getter_expression.children() {
            if child.is_named() {
                init = parse_reference_expression_node(&child, parse_result);
                break;
            }
        }
    }

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::from_fqn_part_type(&node_type).unwrap(),
        node_name,
        Arc::new(parse_result.current_scope.clone()),
        node_range,
        KotlinDefinitionMetadata::Field {
            receiver: receiver_type,
            field_type: property_type,
            init,
            range: parse_result
                .current_lexical_scope
                .last()
                .map(|l| l.range)
                .unwrap_or(node_range),
        },
    );

    parse_result.definitions.push(definition);
    creates_scope
}

fn parse_when_expression_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> KotlinExpressionInfo {
    let mut when_entries = vec![];

    for child in node.children() {
        if child.kind().as_ref() == node_types::WHEN_ENTRY {
            parse_result.visited_nodes.insert(child.node_id());
            if let Some(expression) = parse_when_entry_node(&child, parse_result) {
                when_entries.push(expression);
            }
        }
    }

    KotlinExpressionInfo {
        range: node_to_range(node),
        generics: when_entries
            .iter()
            .flat_map(|e| e.generics.clone())
            .collect(),
        expression: KotlinExpression::When {
            entries: when_entries,
        },
    }
}

fn parse_when_subject_node<'a>(node: &AstNode<'a>, parse_result: &mut AstParseResult) {
    let mut simple_identifier = None;
    let mut variable_type = None;
    let mut init = None;

    for child in node.children() {
        if child.kind().as_ref() == node_types::VARIABLE_DECLARATION {
            for variable_child in child.children() {
                if variable_child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
                    simple_identifier = Some(variable_child);
                    continue;
                }

                variable_type = parse_type_node(&variable_child);
            }
            continue;
        }

        if child.is_named() {
            init = parse_reference_expression_node(&child, parse_result);
            break;
        }
    }

    if simple_identifier.is_none() || variable_type.is_none() {
        return;
    }

    let node_range = node_to_range(node);

    let mut fqn_parts = parse_result.current_scope.clone();
    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Parameter,
        simple_identifier.unwrap().text().to_string(),
        node_range,
    );
    fqn_parts.push(fqn_part.clone());

    parse_result
        .definitions
        .push(KotlinDefinitionInfo::new_with_metadata(
            KotlinDefinitionType::Parameter,
            fqn_part.node_name().to_string(),
            Arc::new(fqn_parts),
            node_range,
            KotlinDefinitionMetadata::Field {
                receiver: None,
                field_type: variable_type,
                init,
                range: parse_result.current_lexical_scope.last().unwrap().range,
            },
        ));
}

fn parse_when_entry_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> Option<KotlinExpressionInfo> {
    let mut last_statement = None;

    for child in node.children() {
        if child.kind().as_ref() == node_types::CONTROL_STRUCTURE_BODY {
            for body_child in child.children() {
                if body_child.kind().as_ref() == node_types::STATEMENTS {
                    for statement_child in body_child.children() {
                        if statement_child.is_named() {
                            last_statement = Some(statement_child);
                        }
                    }
                } else if body_child.is_named() {
                    return parse_reference_expression_node(&body_child, parse_result);
                }
            }
        }
    }

    if let Some(last_statement) = last_statement {
        return parse_reference_expression_node(&last_statement, parse_result);
    }

    None
}

fn parse_class_declaration_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let mut class_type = KotlinFqnPartType::Class;

    if let Some(modifiers) = get_child_by_kind(node, node_types::MODIFIERS)
        && let Some(class_modifier) = get_child_by_kind(&modifiers, node_types::CLASS_MODIFIER)
    {
        if class_modifier.text() == "data" {
            class_type = KotlinFqnPartType::DataClass;
        } else if class_modifier.text() == "value" {
            class_type = KotlinFqnPartType::ValueClass;
        } else if class_modifier.text() == "annotation" {
            class_type = KotlinFqnPartType::AnnotationClass;
        }
    }

    if get_child_by_kind(node, node_types::ENUM_CLASS_BODY).is_some() {
        class_type = KotlinFqnPartType::Enum;
    }

    let class_name_node = match get_child_by_kind(node, node_types::TYPE_IDENTIFIER) {
        Some(n) => n,
        None => return false,
    };

    let class_keyword_length = class_name_node.range().start - node.range().start;
    let class_keyword = node.text()[0..class_keyword_length].to_string();

    // The interface keyword is not a modifier, so we need to handle it separately.
    if matches!(class_type, KotlinFqnPartType::Class) && class_keyword.contains("interface") {
        class_type = KotlinFqnPartType::Interface;
    }

    let class_name = class_name_node.text().to_string();
    let class_range = node_to_range(node);

    let fqn_part = KotlinFqnPart::new(class_type, class_name.clone(), class_range);
    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(class_range));

    let (super_class, super_interfaces) = parse_super_types_node(node);

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::from_fqn_part_type(fqn_part.node_type()).unwrap(),
        fqn_part.node_name().to_string(),
        Arc::new(parse_result.current_scope.clone()),
        class_range,
        KotlinDefinitionMetadata::Class {
            super_class,
            super_interfaces,
        },
    );

    parse_result.definitions.push(definition);

    true
}

fn parse_class_parameter_node<'a>(node: &AstNode<'a>, parse_result: &mut AstParseResult) -> bool {
    if get_child_by_any_kind(node, LAMBDA_TYPES).is_some() {
        return parse_simple_node(
            node.clone(),
            get_child_by_kind(node, node_types::SIMPLE_IDENTIFIER).unwrap(),
            KotlinFqnPartType::Lambda,
            parse_result,
        );
    }

    let mut name_node = None;
    let mut parameter_type = None;
    for child in node.children() {
        if child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
            name_node = Some(child);
            break;
        }

        parameter_type = parse_type_node(&child);
    }

    let name_node = match name_node {
        Some(n) => n,
        None => return false,
    };

    let node_range = node_to_range(node);

    let mut fqn_parts = parse_result.current_scope.clone();
    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Property,
        name_node.text().to_string(),
        node_range,
    );
    fqn_parts.push(fqn_part.clone());

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::Property,
        fqn_part.node_name().to_string(),
        Arc::new(fqn_parts),
        node_range,
        KotlinDefinitionMetadata::Field {
            receiver: None,
            field_type: parameter_type,
            init: None,
            range: parse_result.current_lexical_scope.last().unwrap().range,
        },
    );

    parse_result.definitions.push(definition);
    false
}

fn parse_object_declaration_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let object_name_node = match get_child_by_kind(node, node_types::TYPE_IDENTIFIER) {
        Some(n) => n,
        None => return false,
    };

    let object_name = object_name_node.text().to_string();
    let object_range = node_to_range(node);
    let (super_class, super_interfaces) = parse_super_types_node(node);

    let fqn_part = KotlinFqnPart::new(KotlinFqnPartType::Object, object_name.clone(), object_range);
    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(object_range));

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::Object,
        fqn_part.node_name().to_string(),
        Arc::new(parse_result.current_scope.clone()),
        object_range,
        KotlinDefinitionMetadata::Class {
            super_class,
            super_interfaces,
        },
    );

    parse_result.definitions.push(definition);

    true
}

fn parse_companion_object_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let companion_object_name = get_child_by_kind(node, node_types::TYPE_IDENTIFIER)
        .map(|n| n.text().to_string())
        .unwrap_or_else(|| node_names::COMPANION_OBJECT.to_string());

    let companion_object_range = node_to_range(node);

    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::CompanionObject,
        companion_object_name,
        companion_object_range,
    );
    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(companion_object_range));

    let (super_class, super_interfaces) = parse_super_types_node(node);

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::CompanionObject,
        fqn_part.node_name().to_string(),
        Arc::new(parse_result.current_scope.clone()),
        companion_object_range,
        KotlinDefinitionMetadata::Class {
            super_class,
            super_interfaces,
        },
    );

    parse_result.definitions.push(definition);

    true
}

fn parse_function_declaration_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let function_name_node = match get_child_by_kind(node, node_types::SIMPLE_IDENTIFIER) {
        Some(n) => n,
        None => return false,
    };

    let function_name = function_name_node.text().to_string();
    let function_range = node_to_range(node);

    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Function,
        function_name.clone(),
        function_range,
    );
    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(function_range));

    // Parse function parameters, extension type and return type.
    let mut extension_type: Option<String> = None;
    let mut return_type: Option<String> = None;
    let mut init: Option<KotlinExpressionInfo> = None;
    let mut passed_parameters = false;
    for child in node.children() {
        parse_result.visited_nodes.insert(child.node_id());

        if !child.is_named() {
            continue;
        }

        if child.kind().as_ref() == node_types::FUNCTION_VALUE_PARAMETERS {
            parse_function_value_parameters_node(&child, parse_result);
            passed_parameters = true;
            continue;
        } else if child.kind().as_ref() == node_types::FUNCTION_BODY && return_type.is_none() {
            for body_child in child.children() {
                if body_child.is_named() {
                    init = parse_reference_expression_node(&body_child, parse_result);
                    break;
                }
            }
            continue;
        }

        let extracted_type = parse_type_node(&child);
        if let Some(type_str) = extracted_type {
            if !passed_parameters {
                extension_type = Some(type_str);
            } else {
                return_type = Some(type_str);
            }
        }
    }

    let definition = KotlinDefinitionInfo::new_with_metadata(
        KotlinDefinitionType::Function,
        fqn_part.node_name.clone(),
        Arc::new(parse_result.current_scope.clone()),
        function_range,
        KotlinDefinitionMetadata::Function {
            receiver: extension_type,
            return_type,
            init,
        },
    );

    parse_result.definitions.push(definition);

    true
}

fn parse_function_value_parameters_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) {
    for parameter in get_children_by_kind(node, node_types::PARAMETER) {
        parse_result.visited_nodes.insert(parameter.node_id());

        let parameter_name = match get_child_by_kind(&parameter, node_types::SIMPLE_IDENTIFIER) {
            Some(node) => node.text().to_string(),
            None => continue,
        };

        let resolve_parameter_type = match parse_type_from_children(&parameter) {
            Some(type_name) => type_name,
            None => continue, // Parameters should always have a type.
        };

        let parameter_range = node_to_range(&parameter);

        let mut fqn_parts = parse_result.current_scope.clone();
        let fqn_part = KotlinFqnPart::new(
            KotlinFqnPartType::Parameter,
            parameter_name.clone(),
            parameter_range,
        );
        fqn_parts.push(fqn_part.clone());

        parse_result
            .definitions
            .push(KotlinDefinitionInfo::new_with_metadata(
                KotlinDefinitionType::Parameter,
                parameter_name,
                Arc::new(fqn_parts),
                parameter_range,
                KotlinDefinitionMetadata::Parameter {
                    parameter_type: resolve_parameter_type,
                    range: parse_result.current_lexical_scope.last().unwrap().range,
                },
            ));
    }
}

fn parse_secondary_constructor_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let constructor_name = node_names::CONSTRUCTOR;
    let constructor_range = node_to_range(node);

    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Constructor,
        constructor_name.to_string(),
        constructor_range,
    );
    parse_result.current_scope.push(fqn_part.clone());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(constructor_range));

    let definition = KotlinDefinitionInfo::new(
        KotlinDefinitionType::Constructor,
        fqn_part.node_name().to_string(),
        Arc::new(parse_result.current_scope.clone()),
        constructor_range,
    );

    parse_result.definitions.push(definition);

    true
}

fn parse_primary_constructor_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> bool {
    let constructor_name = node_names::CONSTRUCTOR;
    let constructor_range = node_to_range(node);

    let mut fqn_parts = parse_result.current_scope.clone();
    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Constructor,
        constructor_name.to_string(),
        constructor_range,
    );
    fqn_parts.push(fqn_part.clone());

    let definition = KotlinDefinitionInfo::new(
        KotlinDefinitionType::Constructor,
        fqn_part.node_name().to_string(),
        Arc::new(fqn_parts),
        constructor_range,
    );

    parse_result.definitions.push(definition);

    false
}

fn parse_super_types_node(node: &Node<StrDoc<SupportLang>>) -> (Option<String>, Vec<String>) {
    let mut super_class: Option<String> = None;
    let mut super_interfaces = Vec::new();

    for child in node.children() {
        if child.kind().as_ref() == node_types::DELEGATION_SPECIFIER {
            // Extract super interfaces
            for user_type in get_children_by_kind(&child, node_types::USER_TYPE) {
                let mut indentifier = String::new();
                for user_type_child in user_type.children() {
                    if user_type_child.kind().as_ref() == node_types::TYPE_IDENTIFIER {
                        if !indentifier.is_empty() {
                            indentifier.push('.');
                        }

                        indentifier.push_str(user_type_child.text().as_ref());
                    }
                }

                if !indentifier.is_empty() {
                    super_interfaces.push(indentifier);
                }
            }

            // Extract super class
            for constructor in get_children_by_kind(&child, node_types::CONSTRUCTOR_INVOCATION) {
                let user_type = match get_child_by_kind(&constructor, node_types::USER_TYPE) {
                    Some(node) => node,
                    None => continue,
                };

                let mut indentifier = String::new();
                for user_type_child in user_type.children() {
                    if user_type_child.kind().as_ref() == node_types::TYPE_IDENTIFIER {
                        if !indentifier.is_empty() {
                            indentifier.push('.');
                        }

                        indentifier.push_str(user_type_child.text().as_ref());
                    }
                }

                if !indentifier.is_empty() {
                    super_class = Some(indentifier);
                }
            }
        }
    }

    (super_class, super_interfaces)
}

fn parse_type_from_children(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    if let Some(function_type) = get_child_by_kind(node, node_types::FUNCTION_TYPE) {
        return Some(function_type.text().to_string()); // FIXME: Properly handle lambda types.
    }

    let type_node = match get_child_by_kind(node, node_types::NULLABLE_TYPE) {
        Some(nullable_type) => nullable_type,
        None => node.clone(),
    };

    let user_type = get_child_by_kind(&type_node, node_types::USER_TYPE)?;

    let mut indentifier = String::new();
    for child in user_type.children() {
        if child.kind().as_ref() == node_types::TYPE_IDENTIFIER {
            if !indentifier.is_empty() {
                indentifier.push('.');
            }

            indentifier.push_str(child.text().as_ref());
        }
    }

    if !indentifier.is_empty() {
        Some(indentifier)
    } else {
        None
    }
}

fn parse_type_node(node: &Node<StrDoc<SupportLang>>) -> Option<String> {
    match node.kind().as_ref() {
        node_types::TYPE_IDENTIFIER => Some(node.text().to_string()),
        node_types::USER_TYPE => {
            let mut indentifier = String::new();
            for child in node.children() {
                if child.kind().as_ref() == node_types::TYPE_IDENTIFIER {
                    if !indentifier.is_empty() {
                        indentifier.push('.');
                    }

                    indentifier.push_str(child.text().as_ref());
                }
            }

            if !indentifier.is_empty() {
                Some(indentifier)
            } else {
                None
            }
        }
        node_types::NULLABLE_TYPE => {
            if let Some(user_type) = get_child_by_kind(node, node_types::USER_TYPE) {
                parse_type_node(&user_type)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn parse_expression_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> Option<KotlinExpressionInfo> {
    let remaining_stack = stacker::remaining_stack().unwrap_or(0);
    if remaining_stack < crate::MINIMUM_STACK_REMAINING {
        error!(
            remaining_stack,
            node_kind = node.kind().as_ref(),
            "stack limit reached, aborting Kotlin expression parsing"
        );
        return None;
    }

    match node.kind().as_ref() {
        node_types::SIMPLE_IDENTIFIER => {
            parse_result.visited_nodes.insert(node.node_id());
            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: vec![],
                expression: KotlinExpression::Identifier {
                    name: node.text().to_string(),
                },
            })
        }
        node_types::CALL_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());

            let mut call_range = None;
            let mut call_generics = vec![];
            let mut call_name = None;
            let mut call_target = None;

            for child in node.children() {
                if child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
                    parse_result.visited_nodes.insert(child.node_id());
                    call_range = Some(node_to_range(&child));
                    call_name = Some(child.text().to_string());
                } else if child.kind().as_ref() == node_types::NAVIGATION_EXPRESSION {
                    parse_result.visited_nodes.insert(child.node_id());
                    if let Some((target, _, name)) =
                        parse_navigation_expression(&child, parse_result)
                    {
                        call_target = Some(target);
                        call_name = Some(name);
                        call_range = Some(node_to_range(&child));
                    }
                } else if child.kind().as_ref() == node_types::CALL_SUFFIX {
                    for suffix_child in child.children() {
                        if suffix_child.kind().as_ref() == node_types::TYPE_ARGUMENTS {
                            for type_argument in suffix_child.children() {
                                if type_argument.kind().as_ref() == node_types::TYPE_PROJECTION {
                                    call_generics.push(type_argument.text().to_string());
                                }
                            }
                        }
                    }
                }
            }

            if call_range.is_none() || call_name.is_none() {
                return None;
            }

            if let Some(target) = call_target {
                call_generics.extend(target.generics.clone());

                Some(KotlinExpressionInfo {
                    range: call_range.unwrap(),
                    generics: call_generics,
                    expression: KotlinExpression::MemberFunctionCall {
                        target: Box::new(target),
                        member: call_name.unwrap(),
                    },
                })
            } else {
                Some(KotlinExpressionInfo {
                    range: call_range.unwrap(),
                    generics: call_generics,
                    expression: KotlinExpression::Call {
                        name: call_name.unwrap(),
                    },
                })
            }
        }
        node_types::INFIX_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            let left = node.child(0)?;
            parse_result.visited_nodes.insert(left.node_id());

            let function = node.child(1)?;
            parse_result.visited_nodes.insert(function.node_id());

            let right = node.child(2)?;
            parse_result.visited_nodes.insert(right.node_id());

            let target = parse_expression_node(&left, parse_result)?;
            let member = function.text().to_string();

            // Add the argument as a reference
            parse_reference_expression_node(&right, parse_result);

            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: target.generics.clone(),
                expression: KotlinExpression::MemberFunctionCall {
                    target: Box::new(target),
                    member,
                },
            })
        }
        node_types::INDEXING_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            for child in node.children() {
                if child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
                    parse_result.visited_nodes.insert(child.node_id());
                    return Some(KotlinExpressionInfo {
                        range: node_to_range(&child),
                        generics: vec![],
                        expression: KotlinExpression::Index {
                            target: Box::new(KotlinExpressionInfo {
                                range: node_to_range(&child),
                                generics: vec![],
                                expression: KotlinExpression::Identifier {
                                    name: child.text().to_string(),
                                },
                            }),
                        },
                    });
                } else if child.kind().as_ref() == node_types::NAVIGATION_EXPRESSION {
                    parse_result.visited_nodes.insert(child.node_id());
                    if let Some((target, _, _)) = parse_navigation_expression(&child, parse_result)
                    {
                        return Some(KotlinExpressionInfo {
                            range: node_to_range(&child),
                            generics: target.generics.clone(),
                            expression: KotlinExpression::Index {
                                target: Box::new(target),
                            },
                        });
                    }
                }
            }
            None
        }
        node_types::NAVIGATION_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            if let Some((target, operator, name)) = parse_navigation_expression(node, parse_result)
            {
                if operator == "." {
                    return Some(KotlinExpressionInfo {
                        range: node_to_range(node),
                        generics: target.generics.clone(),
                        expression: KotlinExpression::FieldAccess {
                            target: Box::new(target),
                            member: name,
                        },
                    });
                } else {
                    return Some(KotlinExpressionInfo {
                        range: node_to_range(node),
                        generics: target.generics.clone(),
                        expression: KotlinExpression::MethodReference {
                            target: Some(Box::new(target)),
                            member: name,
                        },
                    });
                }
            }
            None
        }
        node_types::CALLABLE_REFERENCE => {
            parse_result.visited_nodes.insert(node.node_id());
            let identifier = get_child_by_kind(node, node_types::SIMPLE_IDENTIFIER)?;
            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: vec![],
                expression: KotlinExpression::MethodReference {
                    target: None,
                    member: identifier.text().to_string(),
                },
            })
        }
        node_types::ELVIS_EXPRESSION => {
            let left: Node<'a, StrDoc<SupportLang>> = node.child(0)?;
            parse_result.visited_nodes.insert(left.node_id());
            let right = node.child(2)?;
            parse_result.visited_nodes.insert(right.node_id());

            let left = parse_expression_node(&left, parse_result)?;
            let right = parse_expression_node(&right, parse_result);

            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: left.generics.clone(),
                expression: KotlinExpression::Elvis {
                    left: Box::new(left),
                    right: right.map(Box::new),
                },
            })
        }
        node_types::WHEN_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            Some(parse_when_expression_node(node, parse_result))
        }
        node_types::TRY_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            Some(parse_try_expression_node(node, parse_result))
        }
        node_types::IF_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            Some(parse_if_expression_node(node, parse_result))
        }
        node_types::LAMBDA_LITERAL => {
            parse_result.visited_nodes.insert(node.node_id());
            let mut last_statement = None;
            for child in node.children() {
                if child.kind().as_ref() == node_types::STATEMENTS {
                    for statement in child.children() {
                        if statement.is_named() {
                            last_statement = Some(statement);
                        }
                    }
                    break;
                }
            }

            if let Some(last_statement) = last_statement {
                return Some(KotlinExpressionInfo {
                    range: node_to_range(&last_statement),
                    generics: vec![],
                    expression: KotlinExpression::Lambda {
                        expression: Box::new(parse_reference_expression_node(
                            &last_statement,
                            parse_result,
                        )?),
                    },
                });
            }

            None
        }
        node_types::PARENTHESIZED_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            for child in node.children() {
                if child.is_named() {
                    parse_result.visited_nodes.insert(child.node_id());
                    if let Some(expression) = parse_expression_node(&child, parse_result) {
                        return Some(KotlinExpressionInfo {
                            range: node_to_range(&child),
                            generics: expression.generics.clone(),
                            expression: KotlinExpression::Parenthesized {
                                expression: Box::new(expression),
                            },
                        });
                    }
                }
            }
            None
        }
        node_types::ADDITIVE_EXPRESSION
        | node_types::MULTIPLICATIVE_EXPRESSION
        | node_types::CONJUNCTION_EXPRESSION
        | node_types::DISJUNCTION_EXPRESSION
        | node_types::EQUALITY_EXPRESSION
        | node_types::RANGE_EXPRESSION
        | node_types::COMPARISON_EXPRESSION
        | node_types::CHECK_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            let left = node.child(0)?;
            parse_result.visited_nodes.insert(left.node_id());

            let operator = node.child(1)?.text().to_string();
            parse_result.visited_nodes.insert(node.child(1)?.node_id());

            let right = node.child(2)?;
            parse_result.visited_nodes.insert(right.node_id());

            let left = parse_reference_expression_node(&left, parse_result)?;
            let right = parse_reference_expression_node(&right, parse_result)?;

            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: left.generics.clone(), // Left and right should the same generics
                expression: KotlinExpression::Binary {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                },
            })
        }
        node_types::PREFIX_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            let operator = node.child(0)?;
            parse_result.visited_nodes.insert(operator.node_id());

            let right = node.child(1)?;
            parse_result.visited_nodes.insert(right.node_id());

            let right = parse_reference_expression_node(&right, parse_result)?;

            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: right.generics.clone(),
                expression: KotlinExpression::Unary {
                    operator: operator.text().to_string(),
                    target: Box::new(right),
                },
            })
        }
        node_types::POSTFIX_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            let left = node.child(0)?;
            parse_result.visited_nodes.insert(left.node_id());

            let operator = node.child(1)?;
            parse_result.visited_nodes.insert(operator.node_id());

            let left = parse_reference_expression_node(&left, parse_result)?;

            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: left.generics.clone(),
                expression: KotlinExpression::Unary {
                    operator: operator.text().to_string(),
                    target: Box::new(left),
                },
            })
        }
        node_types::THIS_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            let mut label = None;
            for child in node.children() {
                if child.is_named() {
                    label = parse_type_node(&child);
                    break;
                }
            }
            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: vec![],
                expression: KotlinExpression::This { label },
            })
        }
        node_types::SUPER_EXPRESSION => {
            parse_result.visited_nodes.insert(node.node_id());
            Some(KotlinExpressionInfo {
                range: node_to_range(node),
                generics: vec![],
                expression: KotlinExpression::Super,
            })
        }
        _ => None,
    }
}

/// Returns the target expression, the operator (. or ::) and the target name.
fn parse_navigation_expression<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> Option<(KotlinExpressionInfo, String, String)> {
    let remaining_stack = stacker::remaining_stack().unwrap_or(0);
    if remaining_stack < crate::MINIMUM_STACK_REMAINING {
        error!(
            remaining_stack,
            "stack limit reached, aborting Kotlin navigation expression parsing"
        );
        return None;
    }

    let mut target = None;

    for child in node.children() {
        if child.is_named() && target.is_none() {
            target = parse_expression_node(&child, parse_result);
            continue;
        }

        if child.kind().as_ref() == node_types::NAVIGATION_SUFFIX {
            let operator = child.child(0)?.text().to_string();
            let member = child.child(1)?.text().to_string();

            if let Some(target) = target {
                return Some((target, operator, member));
            }
            break;
        }
    }

    None
}

fn parse_try_expression_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> KotlinExpressionInfo {
    let mut last_try_statement = None;
    let mut catch_statements = Vec::new();

    for child in node.children() {
        if child.kind().as_ref() == node_types::STATEMENTS {
            for statement in child.children() {
                if statement.is_named() {
                    last_try_statement = Some(statement);
                }
            }
        } else if child.kind().as_ref() == node_types::CATCH_BLOCK {
            let mut catch_statement = None;
            for catch_child in child.children() {
                if catch_child.kind().as_ref() == node_types::STATEMENTS {
                    for statement in catch_child.children() {
                        if statement.is_named() {
                            catch_statement = Some(statement);
                        }
                    }
                }
            }

            if let Some(catch_statement) = catch_statement {
                catch_statements.push(catch_statement);
            }
        }
    }

    let try_expression = last_try_statement.and_then(|statement| {
        parse_reference_expression_node(&statement, parse_result).map(Box::new)
    });

    let mut generics = vec![];
    if let Some(try_expression) = &try_expression {
        generics.extend(try_expression.generics.clone());
    }

    let catch_clauses = catch_statements
        .into_iter()
        .filter_map(|statement| parse_reference_expression_node(&statement, parse_result))
        .collect::<Vec<KotlinExpressionInfo>>();

    generics.extend(catch_clauses.iter().flat_map(|c| c.generics.clone()));

    KotlinExpressionInfo {
        range: node_to_range(node),
        generics,
        expression: KotlinExpression::Try {
            body: try_expression,
            catch_clauses,
        },
    }
}

fn parse_if_expression_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> KotlinExpressionInfo {
    let mut control_structure_bodies = vec![];
    for child in node.children() {
        if child.kind().as_ref() == node_types::CONTROL_STRUCTURE_BODY
            && let Some(body) = parse_control_structure_body(&child, parse_result)
        {
            control_structure_bodies.push(body);
        }
    }

    KotlinExpressionInfo {
        range: node_to_range(node),
        generics: control_structure_bodies
            .iter()
            .flat_map(|b| b.generics.clone())
            .collect(),
        expression: KotlinExpression::If {
            bodies: control_structure_bodies,
        },
    }
}

fn parse_control_structure_body<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> Option<KotlinExpressionInfo> {
    let mut last_statement = None;

    for body_child in node.children() {
        if body_child.kind().as_ref() == node_types::STATEMENTS {
            for statement_child in body_child.children() {
                if statement_child.is_named() {
                    last_statement = Some(statement_child);
                    break;
                }
            }
        } else if body_child.is_named() {
            last_statement = Some(body_child);
        }
    }

    if let Some(last_statement) = last_statement {
        return parse_reference_expression_node(&last_statement, parse_result);
    }

    None
}

fn parse_catch_block_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) {
    let mut catch_variable_identifier = None;
    let mut catch_variable_type = None;
    for child in node.children() {
        if child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
            parse_result.visited_nodes.insert(child.node_id());
            catch_variable_identifier = Some(child.text().to_string());
        }

        if let Some(kotlin_type) = parse_type_node(&child) {
            parse_result.visited_nodes.insert(child.node_id());
            catch_variable_type = Some(kotlin_type);
            break;
        }
    }

    if catch_variable_identifier.is_none() || catch_variable_type.is_none() {
        return;
    }

    let catch_variable_identifier = catch_variable_identifier.unwrap();
    let catch_variable_range = node_to_range(node);
    let catch_variable_type = catch_variable_type.unwrap();

    let mut fqn_parts = parse_result.current_scope.clone();
    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Parameter,
        catch_variable_identifier.clone(),
        catch_variable_range,
    );
    fqn_parts.push(fqn_part.clone());

    parse_result
        .definitions
        .push(KotlinDefinitionInfo::new_with_metadata(
            KotlinDefinitionType::Parameter,
            catch_variable_identifier,
            Arc::new(fqn_parts),
            catch_variable_range,
            KotlinDefinitionMetadata::Parameter {
                parameter_type: catch_variable_type,
                range: parse_result.current_lexical_scope.last().unwrap().range,
            },
        ));
}

fn parse_for_statement_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) {
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_block(node_to_range(node)));
    let mut simple_identifier = None;
    let mut variable_type = None;
    let mut init = None;

    for child in node.children() {
        if child.kind().as_ref() == node_types::VARIABLE_DECLARATION {
            for variable_child in child.children() {
                if variable_child.kind().as_ref() == node_types::SIMPLE_IDENTIFIER {
                    simple_identifier = Some(variable_child);
                    continue;
                }

                variable_type = parse_type_node(&variable_child);
            }
            continue;
        }

        if child.is_named() {
            init = parse_reference_expression_node(&child, parse_result);
            break;
        }
    }

    if simple_identifier.is_none() || variable_type.is_none() {
        return;
    }

    let simple_identifier = simple_identifier.unwrap();
    let node_range = node_to_range(node);

    let mut fqn_parts = parse_result.current_scope.clone();
    let fqn_part = KotlinFqnPart::new(
        KotlinFqnPartType::Property,
        simple_identifier.text().to_string(),
        node_range,
    );
    fqn_parts.push(fqn_part.clone());

    parse_result
        .definitions
        .push(KotlinDefinitionInfo::new_with_metadata(
            KotlinDefinitionType::Property,
            fqn_part.node_name().to_string(),
            Arc::new(fqn_parts),
            node_range,
            KotlinDefinitionMetadata::Field {
                receiver: None,
                field_type: variable_type,
                init,
                range: parse_result.current_lexical_scope.last().unwrap().range,
            },
        ));
}

fn parse_reference_expression_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) -> Option<KotlinExpressionInfo> {
    if let Some(expression) = parse_expression_node(node, parse_result) {
        if matches!(
            &expression.expression,
            KotlinExpression::MemberFunctionCall { .. }
                | KotlinExpression::MethodReference { .. }
                | KotlinExpression::Call { .. }
        ) {
            parse_result.references.push(KotlinReferenceInfo {
                name: "".to_string(),
                range: node_to_range(node),
                target: ReferenceTarget::Unresolved(),
                reference_type: KotlinReferenceType::Call,
                metadata: Some(Box::new(expression.clone())),
                scope: Some(Arc::new(parse_result.current_scope.clone())),
            });
        }

        return Some(expression);
    }

    None
}

fn parse_annotation_reference_node<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    parse_result: &mut AstParseResult,
) {
    if !node.is_named() {
        return;
    }

    let mut identifier_node = None;
    for child in node.children() {
        if child.kind().as_ref() == node_types::CONSTRUCTOR_INVOCATION {
            for constructor_child in child.children() {
                if let Some(type_node) = parse_type_node(&constructor_child) {
                    identifier_node = Some(type_node);
                    break;
                }
            }
        } else if child.is_named() {
            identifier_node = parse_type_node(&child);
            break;
        }
    }

    if identifier_node.is_none() {
        return;
    }

    let annotation_name = identifier_node.unwrap();
    parse_result.references.push(KotlinReferenceInfo {
        name: "".to_string(),
        range: node_to_range(node),
        target: ReferenceTarget::Unresolved(),
        reference_type: KotlinReferenceType::Call,
        metadata: Some(Box::new(KotlinExpressionInfo {
            range: node_to_range(node),
            generics: vec![],
            expression: KotlinExpression::Annotation {
                name: annotation_name,
            },
        })),
        scope: Some(Arc::new(parse_result.current_scope.clone())),
    });
}

// Process a node and returns the FQN part if the node creates a scope
// Passes the current scope and the node_fqn_map because some node may create multiple fqn parts (ex: destructuring declarations)
fn parse_node<'a>(node: &Node<'a, StrDoc<SupportLang>>, parse_result: &mut AstParseResult) -> bool {
    match node.kind().as_ref() {
        node_types::CLASS => parse_class_declaration_node(node, parse_result),
        node_types::OBJECT => parse_object_declaration_node(node, parse_result),
        node_types::COMPANION_OBJECT => parse_companion_object_node(node, parse_result),
        node_types::ENUM_ENTRY => {
            parse_simple_node(
                node.clone(),
                get_child_by_kind(node, node_types::SIMPLE_IDENTIFIER).unwrap(),
                KotlinFqnPartType::EnumEntry,
                parse_result,
            )
        }
        node_types::FUNCTION => parse_function_declaration_node(node, parse_result),
        node_types::CLASS_PARAMETER => parse_class_parameter_node(node, parse_result),
        node_types::PROPERTY => parse_property_declaration_node(node, parse_result),
        node_types::PRIMARY_CONSTRUCTOR => parse_primary_constructor_node(node, parse_result),
        node_types::SECONDARY_CONSTRUCTOR => parse_secondary_constructor_node(
            node,
            parse_result,
        ),
        node_types::FUNCTION_VALUE_PARAMETERS => {
            parse_function_value_parameters_node(node, parse_result);
            false
        }
        node_types::CATCH_BLOCK => {
            parse_catch_block_node(node, parse_result);
            false
        },
        node_types::WHEN_SUBJECT => {
            parse_when_subject_node(node, parse_result);
            false
        },
        node_types::FOR_STATEMENT => {
            parse_for_statement_node(node, parse_result);
            true
        }
        node_types::WHEN_EXPRESSION // when expression can contain a variable declaration before the when block.
        | node_types::STATEMENTS
        | node_types::CONTROL_STRUCTURE_BODY => {
            parse_result
                .current_lexical_scope
                .push(LexicalScope::created_by_block(node_to_range(node)));
            true
        }
        node_types::CALL_EXPRESSION
        | node_types::SIMPLE_IDENTIFIER
        | node_types::NAVIGATION_EXPRESSION
        | node_types::INDEXING_EXPRESSION
        | node_types::INFIX_EXPRESSION
        | node_types::INTERPOLATED_IDENTIFIER => {
            if REFERENCE_PARENT_TYPES.contains(&node.parent().unwrap().kind().as_ref()) {
                parse_reference_expression_node(node, parse_result);
            }
            false
        }
        node_types::ANNOTATION => {
            parse_annotation_reference_node(node, parse_result);
            false
        }
        _ => false,
    }
}

pub(in crate::kotlin) fn parse_ast(ast: &AstRootNode) -> KotlinAnalyzerResult {
    let mut parse_result = AstParseResult::default();

    // Ensure the file always has a lexical scope
    let file_range = node_to_range(&ast.root());
    parse_result
        .current_lexical_scope
        .push(LexicalScope::created_by_file(file_range));

    if let Some(package_declaration) = get_child_by_kind(&ast.root(), node_types::PACKAGE)
        && let Some(package_name) = get_child_by_kind(&package_declaration, node_types::IDENTIFIER)
    {
        parse_result.current_scope.push(KotlinFqnPart::new(
            KotlinFqnPartType::Package,
            package_name.text().to_string(),
            node_to_range(&package_name),
        ));

        parse_result.definitions.push(KotlinDefinitionInfo::new(
            KotlinDefinitionType::Package,
            package_name.text().to_string(),
            Arc::new(parse_result.current_scope.clone()),
            node_to_range(&package_name),
        ));
    }

    // Stack of nodes to process
    let mut stack: Vec<Option<Node<StrDoc<SupportLang>>>> = Vec::with_capacity(128);
    stack.push(Some(ast.root()));

    while let Some(node_option) = stack.pop() {
        if let Some(node) = node_option {
            if !parse_result.visited_nodes.insert(node.node_id()) {
                push_children_reverse(node.children().collect(), &mut stack);
                continue;
            }

            let node_kind = node.kind();
            if node_kind == node_types::IMPORT_HEADER {
                if let Some(import) = parse_import_node(&node) {
                    parse_result.imports.push(import);
                }
                continue;
            }

            if node_kind == node_types::GETTER || node_kind == node_types::SETTER {
                stack.push(None);
                push_children_reverse(node.children().collect(), &mut stack);
                continue;
            }

            if parse_node(&node, &mut parse_result) {
                stack.push(None);
            }

            push_children_reverse(node.children().collect(), &mut stack);
        } else {
            if let Some(next) = stack.last()
                && let Some(next_node) = next
            {
                // If the next node is a getter or setter, we don't want to pop the scope.
                if next_node.kind() == node_types::GETTER || next_node.kind() == node_types::SETTER
                {
                    continue;
                }
            }

            // If the ast is invalid we could get into a situation where scopes are closed too early.
            // This is a workaround to prevent the top level scope from being closed too early.
            if let Some(popped_scope) = parse_result.current_lexical_scope.last()
                && popped_scope.top_level
            {
                continue;
            }

            // None indicates the end of a scope
            if let Some(popped_scope) = parse_result.current_lexical_scope.pop()
                && popped_scope.created_by_definition
            {
                parse_result.current_scope.pop();
            }
        }
    }

    KotlinAnalyzerResult {
        definitions: parse_result.definitions,
        imports: parse_result.imports,
        references: parse_result.references,
    }
}

/// Helper function to add children to stack in reverse order
fn push_children_reverse<'a>(
    children: Vec<Node<'a, StrDoc<SupportLang>>>,
    stack: &mut Vec<Option<Node<'a, StrDoc<SupportLang>>>>,
) {
    for child in children.into_iter().rev() {
        stack.push(Some(child));
    }
}

/// Calculate the combined range of a property node, taking into account the range of the getter and setter nodes
fn calculate_combined_range(
    base_range: Range,
    getter: Option<&Node<StrDoc<SupportLang>>>,
    setter: Option<&Node<StrDoc<SupportLang>>>,
) -> Range {
    let mut combined = base_range;

    if let Some(getter) = getter {
        let getter_range = node_to_range(getter);
        combined.byte_offset.0 = min(combined.byte_offset.0, getter_range.byte_offset.0);
        combined.byte_offset.1 = max(combined.byte_offset.1, getter_range.byte_offset.1);
    }

    if let Some(setter) = setter {
        let setter_range = node_to_range(setter);
        combined.byte_offset.0 = min(combined.byte_offset.0, setter_range.byte_offset.0);
        combined.byte_offset.1 = max(combined.byte_offset.1, setter_range.byte_offset.1);
    }

    combined
}

/// Find FQN for a node by looking up its range in the FQN map
pub fn find_kotlin_fqn_for_node<'a>(
    range: Range,
    node_fqn_map: &KotlinNodeFqnMap<'a>,
) -> Option<KotlinFqn> {
    node_fqn_map.get(&range).map(|(_, fqn)| fqn.clone())
}

/// Convert a Kotlin FQN to its string representation
/// The parts are joined by '.' to form the full FQN string
pub fn kotlin_fqn_to_string(fqn: &KotlinFqn) -> String {
    fqn.iter()
        .map(|part| part.node_name.clone())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kotlin::types::{KotlinDefinitions, KotlinImportType, KotlinImports},
        parser::{GenericParser, Language, LanguageParser},
    };

    #[test]
    fn test_kotlin_code_outside_a_package() {
        let kotlin_code = r#"
        class MyClass {
            val myProperty = 1;

            fun myMethod() {
                println("Hello, World!")
            }
        }

        fun main() {
            MyClass().myMethod()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 4);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myProperty",
            "MyClass.myProperty",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.myMethod",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(&definitions, "main", "main", KotlinDefinitionType::Function);
    }

    #[test]
    fn test_kotlin_code_in_a_package_are_included_in_definitions() {
        let kotlin_code = r#"
        package com.example.test;

        const val MY_CONSTANT = 1;
        val myFileProperty = 2;

        class MyClass(
            val myConstructorProperty1: Int = 1,
            val myConstructorProperty2: Int
        ) {
            val myProperty = 1;

            fun myMethod() {
                println("Hello, World!")
            }
        }

        fun main() {
            MyClass().myMethod()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 10);

        validate_definition_exists(
            &definitions,
            "com.example.test",
            "com.example.test",
            KotlinDefinitionType::Package,
        );
        validate_definition_exists(
            &definitions,
            "MyClass",
            "com.example.test.MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "<init>",
            "com.example.test.MyClass.<init>",
            KotlinDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "myConstructorProperty1",
            "com.example.test.MyClass.myConstructorProperty1",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "myConstructorProperty2",
            "com.example.test.MyClass.myConstructorProperty2",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "myProperty",
            "com.example.test.MyClass.myProperty",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "com.example.test.MyClass.myMethod",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "main",
            "com.example.test.main",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "MY_CONSTANT",
            "com.example.test.MY_CONSTANT",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "myFileProperty",
            "com.example.test.myFileProperty",
            KotlinDefinitionType::Property,
        );
    }

    #[test]
    fn test_includes_declarations_inside_functions_are_included_in_definitions() {
        let kotlin_code = r#"
        fun main() {
            val myProperty = 1;

            fun myMethod() {
                val myMethodProperty = 1;
            }

            class MyClass {
                val myClassProperty = 1;
            }

            println(myProperty)
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 6);

        validate_definition_exists(&definitions, "main", "main", KotlinDefinitionType::Function);
        validate_definition_exists(
            &definitions,
            "myProperty",
            "main",
            KotlinDefinitionType::LocalVariable,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "main.myMethod",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "myMethodProperty",
            "main.myMethod",
            KotlinDefinitionType::LocalVariable,
        );
        validate_definition_exists(
            &definitions,
            "MyClass",
            "main.MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myClassProperty",
            "main.MyClass.myClassProperty",
            KotlinDefinitionType::Property,
        );
    }

    #[test]
    fn test_nested_classes_are_included_in_definitions() {
        let kotlin_code = r#"
        class MyClass {
            class MyNestedClass {
                fun myMethod() {
                    println("Hello, World!")
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "MyNestedClass",
            "MyClass.MyNestedClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.MyNestedClass.myMethod",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn test_companion_objects_are_included_in_definitions() {
        let kotlin_code = r#"
        class MyClass {
            companion object {
                fun myMethod() {
                    println("Hello, World!")
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "Companion",
            "MyClass.Companion",
            KotlinDefinitionType::CompanionObject,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.Companion.myMethod",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn test_operator_functions_are_included_in_definitions() {
        let kotlin_code = r#"
        class MyClass {
            operator fun plus(other: MyClass) = MyClass()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "plus",
            "MyClass.plus",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "other",
            "MyClass.plus.other",
            KotlinDefinitionType::Parameter,
        );
    }

    #[test]
    fn test_function_overloading_have_the_same_definitions() {
        let kotlin_code = r#"
        class MyClass {
            fun myMethod(a: Int) = 1
            fun myMethod(a: String) = 2
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.myMethod",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "a",
            "MyClass.myMethod.a",
            KotlinDefinitionType::Parameter,
        );
    }

    #[test]
    fn test_secondary_constructors_are_included_in_definitions() {
        let kotlin_code = r#"
        class MyClass {
            constructor(a: Int) = This()
            constructor(a: String) = This()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "<init>",
            "MyClass.<init>",
            KotlinDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "a",
            "MyClass.<init>.a",
            KotlinDefinitionType::Parameter,
        );
    }

    #[test]
    fn test_handles_class_with_modifiers() {
        let kotlin_code = r#"
        data class MyClass(
            val myProperty: Int
        )
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::DataClass,
        );
        validate_definition_exists(
            &definitions,
            "myProperty",
            "MyClass.myProperty",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "<init>",
            "MyClass.<init>",
            KotlinDefinitionType::Constructor,
        );
    }

    #[test]
    fn test_interface_definitions_are_included_in_definitions() {
        let kotlin_code = r#"
        interface Repository<T> {
            fun findById(id: String): T
            fun save(entity: T): T
        }

        class UserRepository : Repository<User> {
            override fun findById(id: String): User {
                return User(id)
            }

            override fun save(entity: User): User {
                return entity
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 10);

        validate_definition_exists(
            &definitions,
            "Repository",
            "Repository",
            KotlinDefinitionType::Interface,
        );
        validate_definition_exists(
            &definitions,
            "findById",
            "Repository.findById",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "save",
            "Repository.save",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "UserRepository",
            "UserRepository",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "findById",
            "UserRepository.findById",
            KotlinDefinitionType::Function,
        );
        validate_definition_exists(
            &definitions,
            "save",
            "UserRepository.save",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn object_declarations_are_included_in_definitions() {
        let kotlin_code = r#"
        object MyObject {
            fun myMethod() {
                println("Hello, World!")
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 2);

        validate_definition_exists(
            &definitions,
            "MyObject",
            "MyObject",
            KotlinDefinitionType::Object,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyObject.myMethod",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn annotation_classes_are_included_in_definitions() {
        let kotlin_code = r#"
        @Target(AnnotationTarget.CLASS)
        annotation class MyAnnotation(
            val myProperty: Int
        )

        @MyAnnotation(1)
        class MyClass {
            fun myMethod() {
                println("Hello, World!")
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(
            &definitions,
            "MyAnnotation",
            "MyAnnotation",
            KotlinDefinitionType::AnnotationClass,
        );
        validate_definition_exists(
            &definitions,
            "myProperty",
            "MyAnnotation.myProperty",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "<init>",
            "MyAnnotation.<init>",
            KotlinDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            KotlinDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.myMethod",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn enum_entries_are_included_in_fqn() {
        let kotlin_code = r#"
        enum class MyEnum(val myProperty: Int) {
            ENTRY1(1),
            ENTRY2(2)
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(&definitions, "MyEnum", "MyEnum", KotlinDefinitionType::Enum);
        validate_definition_exists(
            &definitions,
            "<init>",
            "MyEnum.<init>",
            KotlinDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "myProperty",
            "MyEnum.myProperty",
            KotlinDefinitionType::Property,
        );
        validate_definition_exists(
            &definitions,
            "ENTRY1",
            "MyEnum.ENTRY1",
            KotlinDefinitionType::EnumEntry,
        );
        validate_definition_exists(
            &definitions,
            "ENTRY2",
            "MyEnum.ENTRY2",
            KotlinDefinitionType::EnumEntry,
        );
    }

    #[test]
    fn extension_properties_are_included_in_fqn() {
        let kotlin_code = r#"
        val String.count: Int
            get() = length
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 1);

        validate_definition_exists(
            &definitions,
            "count",
            "count",
            KotlinDefinitionType::Property,
        );
    }

    #[test]
    fn extension_methods_are_included_in_definitions() {
        let kotlin_code = r#"
        fun String.capitalizeFirst(): String {
            return this.replaceFirstChar { it.uppercase() }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 1);

        validate_definition_exists(
            &definitions,
            "capitalizeFirst",
            "capitalizeFirst",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn test_lambda_declarations_are_included_in_definitions() {
        let kotlin_code = r#"
        val declaredLambda = { a, b -> 
            val result = a + b
            println(result)
        }
        val referencedLambda = ::println
        val anonymousFunction = fun (a: Int) { println(a) }

        data class LambdaAsClassParameter(
            val anonymousClassFunction: () -> Unit = {}
        )
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 8);

        validate_definition_exists(
            &definitions,
            "declaredLambda",
            "declaredLambda",
            KotlinDefinitionType::Lambda,
        );
        validate_definition_exists(
            &definitions,
            "referencedLambda",
            "referencedLambda",
            KotlinDefinitionType::Lambda,
        );
        validate_definition_exists(
            &definitions,
            "anonymousFunction",
            "anonymousFunction",
            KotlinDefinitionType::Lambda,
        );
        validate_definition_exists(
            &definitions,
            "a",
            "anonymousFunction.a",
            KotlinDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "LambdaAsClassParameter",
            "LambdaAsClassParameter",
            KotlinDefinitionType::DataClass,
        );
        validate_definition_exists(
            &definitions,
            "<init>",
            "LambdaAsClassParameter.<init>",
            KotlinDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "anonymousClassFunction",
            "LambdaAsClassParameter.anonymousClassFunction",
            KotlinDefinitionType::Lambda,
        );
    }

    #[test]
    fn test_value_classes_are_included_in_definitions() {
        let kotlin_code = r#"
        @JvmInline
        value class MyValueClass(val value: Int)
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyValueClass",
            "MyValueClass",
            KotlinDefinitionType::ValueClass,
        );

        validate_definition_exists(
            &definitions,
            "<init>",
            "MyValueClass.<init>",
            KotlinDefinitionType::Constructor,
        );

        validate_definition_exists(
            &definitions,
            "value",
            "MyValueClass.value",
            KotlinDefinitionType::Property,
        );
    }

    #[test]
    fn test_nested_functions_are_included_in_definitions() {
        let kotlin_code = r#"
        fun main() {
            fun nestedFunction() {
                println("Hello, World!")
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        assert_eq!(definitions.len(), 2);

        validate_definition_exists(&definitions, "main", "main", KotlinDefinitionType::Function);

        validate_definition_exists(
            &definitions,
            "nestedFunction",
            "main.nestedFunction",
            KotlinDefinitionType::Function,
        );
    }

    #[test]
    fn test_imports_are_extracted_correctly() {
        let kotlin_code = r#"
        import kotlin.io.println
        import kotlin.io.File as DiskFile
        import kotlin.mysql.*
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { imports, .. } = parse_ast(&parse_result.ast);

        assert_eq!(imports.len(), 3);

        validate_import_exists(
            &imports,
            "kotlin.io",
            "println",
            None,
            KotlinImportType::Import,
        );
        validate_import_exists(
            &imports,
            "kotlin.io",
            "File",
            Some("DiskFile"),
            KotlinImportType::AliasedImport,
        );
        validate_import_exists(
            &imports,
            "kotlin.mysql",
            "*",
            None,
            KotlinImportType::WildcardImport,
        );
    }

    #[test]
    #[allow(clippy::get_first)]
    fn test_call_expressions_are_included_in_references() {
        let kotlin_code = r#"
        annotation class Traceable

        open class Application {
            fun run() {}
        }

        class Executor {
            fun execute(f: () -> Unit) {}
            companion object {
                fun executeFn() {}
            }
        }

        class Foo {
            val executor = Executor()
            fun bar(): Bar { return Bar() }
        }

        class Bar { fun baz() {} }

        class Main : Application() {
            lateinit var myParameter: Foo

            constructor() {
                myParameter = Foo()
            }

            @Traceable
            fun main() {
                val bar: Bar = this.myParameter.bar() as Bar
                bar.baz()

                myParameter.executor.execute(Executor::executeFn)
                await({ super.run() })
            }
        }

        fun await(block: () -> Unit) { block() }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let references = parse_ast(&parse_result.ast).references;
        assert_eq!(references.len(), 10);

        let executor_creation_reference = &references
            .get(0)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match executor_creation_reference {
            KotlinExpression::Call { name } => assert_eq!(name, "Executor"),
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let bar_creation_reference = &references
            .get(1)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match bar_creation_reference {
            KotlinExpression::Call { name } => assert_eq!(name, "Bar"),
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let foo_creation_reference = &references
            .get(2)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match foo_creation_reference {
            KotlinExpression::Call { name } => {
                assert_eq!(name, "Foo");
            }
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let traceable_annotation_reference = &references
            .get(3)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match traceable_annotation_reference {
            KotlinExpression::Annotation { name } => {
                assert_eq!(name, "Traceable");
            }
            _ => panic!("Expected KotlinExpression::Annotation"),
        }

        let bar_call_reference = &references
            .get(4)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match bar_call_reference {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::FieldAccess { target, member } => {
                        match &target.as_ref().expression {
                            KotlinExpression::This { .. } => {}
                            _ => panic!("Expected KotlinExpression::This"),
                        }
                        assert_eq!(member, "myParameter");
                    }
                    _ => panic!("Expected KotlinExpression::FieldAccess"),
                }
                assert_eq!(member, "bar");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }

        let baz_call_reference = &references
            .get(5)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match baz_call_reference {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::Identifier { name } => {
                        assert_eq!(name, "bar");
                    }
                    _ => panic!("Expected KotlinExpression::Identifier"),
                }
                assert_eq!(member, "baz");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }

        let execute_reference = &references
            .get(6)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match execute_reference {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::FieldAccess { target, member } => {
                        match &target.as_ref().expression {
                            KotlinExpression::Identifier { name } => {
                                assert_eq!(name, "myParameter");
                            }
                            _ => panic!("Expected KotlinExpression::Identifier"),
                        }
                        assert_eq!(member, "executor");
                    }
                    _ => panic!("Expected KotlinExpression::FieldAccess"),
                }
                assert_eq!(member, "execute");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }

        let await_reference = &references
            .get(7)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match await_reference {
            KotlinExpression::Call { name } => {
                assert_eq!(name, "await");
            }
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let run_reference = &references
            .get(8)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match run_reference {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::Super => {}
                    _ => panic!("Expected KotlinExpression::Super"),
                }
                assert_eq!(member, "run");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }
    }

    #[test]
    #[allow(clippy::get_first)]
    fn test_when_expressions_are_included_in_references() {
        let kotlin_code = r#"
        class Foo { fun baz() {} }
        fun printOne() {}

        fun main(x: Int, foo: Foo) {
            when (x) {
                1 -> printOne()
                2 -> foo.baz()
                else -> println("other")
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let references = parse_ast(&parse_result.ast).references;
        assert_eq!(references.len(), 3);

        let print_one_ref = &references
            .get(0)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match print_one_ref {
            KotlinExpression::Call { name } => assert_eq!(name, "printOne"),
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let baz_ref = &references
            .get(1)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match baz_ref {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::Identifier { name } => assert_eq!(name, "foo"),
                    _ => panic!("Expected KotlinExpression::Identifier"),
                }
                assert_eq!(member, "baz");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }

        let println_ref = &references
            .get(2)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match println_ref {
            KotlinExpression::Call { name } => assert_eq!(name, "println"),
            _ => panic!("Expected KotlinExpression::Call"),
        }
    }

    #[test]
    #[allow(clippy::get_first)]
    fn test_if_expressions_are_included_in_references() {
        let kotlin_code = r#"
        class Foo { fun baz() {} }
        fun printOne() {}

        fun main(flag: Boolean, foo: Foo) {
            val a = if (flag) foo.baz() else printOne()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let references = parse_ast(&parse_result.ast).references;
        assert_eq!(references.len(), 2);

        let baz_ref = &references
            .get(0)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match baz_ref {
            KotlinExpression::MemberFunctionCall { target, member } => {
                match &target.as_ref().expression {
                    KotlinExpression::Identifier { name } => assert_eq!(name, "foo"),
                    _ => panic!("Expected KotlinExpression::Identifier"),
                }
                assert_eq!(member, "baz");
            }
            _ => panic!("Expected KotlinExpression::MemberFunctionCall"),
        }

        let print_one_ref = &references
            .get(1)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match print_one_ref {
            KotlinExpression::Call { name } => assert_eq!(name, "printOne"),
            _ => panic!("Expected KotlinExpression::Call"),
        }
    }

    #[test]
    #[allow(clippy::get_first)]
    fn test_try_expressions_are_included_in_references() {
        let kotlin_code = r#"
        fun mayThrow() {}
        fun onError() {}

        fun main() {
            try {
                mayThrow()
            } catch (e: Exception) {
                onError()
            }
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let references = parse_ast(&parse_result.ast).references;
        assert_eq!(references.len(), 2);

        let try_ref = &references
            .get(0)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match try_ref {
            KotlinExpression::Call { name } => assert_eq!(name, "mayThrow"),
            _ => panic!("Expected KotlinExpression::Call"),
        }

        let catch_ref = &references
            .get(1)
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .expression;
        match catch_ref {
            KotlinExpression::Call { name } => assert_eq!(name, "onError"),
            _ => panic!("Expected KotlinExpression::Call"),
        }
    }

    #[test]
    fn test_property_init_when_expression_is_captured() {
        let kotlin_code = r#"
        fun foo() {}

        val x = when (1) {
            1 -> foo()
            else -> 2
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        let x_definition = definitions.iter().find(|d| d.name == "x").unwrap();
        assert_eq!(x_definition.definition_type, KotlinDefinitionType::Property);

        match x_definition.metadata.as_ref().unwrap() {
            crate::kotlin::types::KotlinDefinitionMetadata::Field { init, .. } => {
                let init = init
                    .as_ref()
                    .expect("init should be present for property initializer");
                match &init.expression {
                    KotlinExpression::When { entries } => {
                        assert!(!entries.is_empty());
                        let mut found_call = false;
                        for e in entries {
                            if let KotlinExpression::Call { name } = &e.expression
                                && name == "foo"
                            {
                                found_call = true;
                                break;
                            }
                        }
                        assert!(found_call, "Expected when entry calling foo()");
                    }
                    _ => panic!("Expected KotlinExpression::When"),
                }
            }
            _ => panic!("Expected KotlinDefinitionMetadata::Field for property"),
        }
    }

    #[test]
    fn test_function_init_try_expression_is_captured() {
        let kotlin_code = r#"
        fun mayThrow() { throw Exception() }
        fun onError() {}

        fun a() = try {
            mayThrow()
        } catch (e: Exception) {
            onError()
        }
        "#;

        let parser = GenericParser::default_for_language(Language::Kotlin);
        let parse_result = parser.parse(kotlin_code, Some("test.kt")).unwrap();

        let KotlinAnalyzerResult { definitions, .. } = parse_ast(&parse_result.ast);

        let a_definition = definitions.iter().find(|d| d.name == "a").unwrap();
        assert_eq!(a_definition.definition_type, KotlinDefinitionType::Function);

        match a_definition.metadata.as_ref().unwrap() {
            crate::kotlin::types::KotlinDefinitionMetadata::Function { init, .. } => {
                let init = init
                    .as_ref()
                    .expect("init should be present for expression-bodied function");
                match &init.expression {
                    KotlinExpression::Try {
                        body,
                        catch_clauses,
                    } => {
                        // body should be mayThrow()
                        match body.as_ref().expect("try body expected").expression.clone() {
                            KotlinExpression::Call { name } => assert_eq!(name, "mayThrow"),
                            _ => panic!("Expected KotlinExpression::Call in try body"),
                        }

                        // one of catch clauses should call onError()
                        let mut found_catch_call = false;
                        for c in catch_clauses {
                            if let KotlinExpression::Call { name } = &c.expression
                                && name == "onError"
                            {
                                found_catch_call = true;
                                break;
                            }
                        }
                        assert!(found_catch_call, "Expected catch clause calling onError()");
                    }
                    _ => panic!("Expected KotlinExpression::Try"),
                }
            }
            _ => panic!("Expected KotlinDefinitionMetadata::Function for function"),
        }
    }

    fn validate_definition_exists(
        definitions: &KotlinDefinitions,
        name: &str,
        fqn: &str,
        expected_type: KotlinDefinitionType,
    ) {
        let definition = definitions.iter().find(|definition| {
            definition.name == name && kotlin_fqn_to_string(&definition.fqn) == fqn
        });
        assert!(
            definition.is_some(),
            "Definition with name {name} and FQN {fqn} not found"
        );

        let definition = definition.unwrap();
        let definition_type = definition.definition_type;

        assert_eq!(
            definition_type, expected_type,
            "Definition type for definition {name} does not match expected value"
        );
    }

    fn validate_import_exists(
        imports: &KotlinImports,
        path: &str,
        symbol: &str,
        alias: Option<&str>,
        expected_type: KotlinImportType,
    ) {
        let import = imports.iter().find(|import| {
            import.import_path == path
                && import.identifier.as_ref().unwrap().name == symbol
                && import.identifier.as_ref().unwrap().alias == alias.map(|a| a.to_string())
        });
        assert!(
            import.is_some(),
            "Import with path {path} and symbol {symbol} not found"
        );

        let import = import.unwrap();
        let import_type = import.import_type;
        assert_eq!(
            import_type, expected_type,
            "Import type for import {path} does not match expected value"
        );
    }
}
