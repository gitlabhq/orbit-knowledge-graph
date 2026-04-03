use crate::{
    Range,
    java::{
        analyzer::JavaAnalyzerResult,
        imports::extract_import_declaration,
        types::{
            AstRootNode, JavaDefinitionInfo, JavaDefinitionMetadata, JavaDefinitionType,
            JavaDefinitions, JavaExpression, JavaFqn, JavaFqnPart, JavaFqnPartType, JavaImports,
            JavaReferenceInfo, JavaReferenceType, JavaReferences, JavaType,
        },
        utils::{get_child_by_kind, node_types},
    },
    references::ReferenceTarget,
    utils::node_to_range,
};
use smallvec::{SmallVec, smallvec};
use std::{collections::HashSet, sync::Arc};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type ScopeStack = SmallVec<[JavaFqnPart; 8]>;
type LexicalScopeStack = SmallVec<[LexicalScope; 8]>;

const REFERENCE_PARENT_TYPES: &[&str] = &[
    node_types::RETURN_STATEMENT,
    node_types::TERNARY_EXPRESSION,
    node_types::BINARY_EXPRESSION,
    node_types::UNARY_EXPRESSION,
    node_types::UPDATE_EXPRESSION,
    node_types::PARENTHESIZED_EXPRESSION,
    node_types::EXPRESSION_STATEMENT,
    node_types::ARGUMENT_LIST,
    node_types::THROW_STATEMENT,
    node_types::VARIABLE_DECLARATOR,
    node_types::LAMBDA_EXPRESSION,
    node_types::ANNOTATION_ARGUMENT_LIST,
];

const CALLABLE_EXPRESSIONS: &[&str] = &[
    node_types::OBJECT_CREATION_EXPRESSION,
    node_types::ARRAY_CREATION_EXPRESSION,
    node_types::THIS,
    node_types::SUPER,
    node_types::METHOD_INVOCATION,
    node_types::METHOD_REFERENCE,
];

struct LexicalScope {
    range: Range,
    created_by_definition: bool,
}

impl LexicalScope {
    fn created_by_definition(range: Range) -> Self {
        Self {
            range,
            created_by_definition: true,
        }
    }

    fn created_by_block(range: Range) -> Self {
        Self {
            range,
            created_by_definition: false,
        }
    }
}

struct JavaParseResult {
    definitions: JavaDefinitions,
    imports: JavaImports,
    references: JavaReferences,
    current_scope: ScopeStack, // The current definition scope
    current_lexical_scope: LexicalScopeStack, // The current lexical scope
    visited_nodes: HashSet<usize>,
}

impl Default for JavaParseResult {
    fn default() -> Self {
        Self {
            definitions: Vec::with_capacity(128),
            imports: Vec::with_capacity(32),
            references: Vec::with_capacity(128),
            current_scope: smallvec![],
            current_lexical_scope: smallvec![],
            visited_nodes: HashSet::new(),
        }
    }
}

fn parse_simple_node(
    node: &Node<StrDoc<SupportLang>>,
    node_type: JavaFqnPartType,
    parser_result: &mut JavaParseResult,
) -> bool {
    let identifier_node = node.field("name");
    if identifier_node.is_none() {
        return false;
    }

    let identifier_node = identifier_node.unwrap();
    let name = identifier_node.text().into_owned();
    let range = node_to_range(node);

    let fqn_part = JavaFqnPart::new(node_type, name.clone(), range);

    parser_result.current_scope.push(fqn_part.clone());
    parser_result.definitions.push(JavaDefinitionInfo::new(
        JavaDefinitionType::from_fqn_part_type(&fqn_part.node_type).unwrap(),
        name,
        Arc::new(parser_result.current_scope.clone()),
        range,
    ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(range));

    true
}

fn parse_class_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let class_name = node.field("name");
    if class_name.is_none() {
        return false;
    }

    let mut super_types = Vec::new();
    if let Some(super_class_node) = node.field("superclass") {
        for type_node in super_class_node.children() {
            if type_node.text() == "extends" {
                continue;
            }

            if let Some(super_class_type) = parse_type_node(&type_node) {
                super_types.push(super_class_type.clone());
            }
        }
    }

    if let Some(interfaces_node) = node.field("interfaces")
        && let Some(type_list_node) = get_child_by_kind(&interfaces_node, node_types::TYPE_LIST)
    {
        for type_node in type_list_node.children() {
            if type_node.text() == "," {
                continue;
            }

            if let Some(interface_type) = parse_type_node(&type_node) {
                super_types.push(interface_type);
            }
        }
    }

    let class_name = class_name.unwrap().text().into_owned();
    let class_range = node_to_range(node);

    let fqn_part = JavaFqnPart::new(JavaFqnPartType::Class, class_name.clone(), class_range);

    parser_result.current_scope.push(fqn_part.clone());
    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::Class,
            class_name,
            Arc::new(parser_result.current_scope.clone()),
            class_range,
            JavaDefinitionMetadata::Class { super_types },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(class_range));

    true
}

fn parse_interface_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let class_name = node.field("name");
    if class_name.is_none() {
        return false;
    }

    let mut super_types = Vec::new();
    if let Some(interfaces_node) = get_child_by_kind(node, node_types::EXTENDS_INTERFACES)
        && let Some(type_list_node) = get_child_by_kind(&interfaces_node, node_types::TYPE_LIST)
    {
        for type_node in type_list_node.children() {
            if type_node.text() == "," {
                continue;
            }

            if let Some(interface_type) = parse_type_node(&type_node) {
                super_types.push(interface_type);
            }
        }
    }

    let class_name = class_name.unwrap().text().into_owned();
    let class_range = node_to_range(node);

    let fqn_part = JavaFqnPart::new(JavaFqnPartType::Interface, class_name.clone(), class_range);

    parser_result.current_scope.push(fqn_part.clone());
    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::Interface,
            class_name,
            Arc::new(parser_result.current_scope.clone()),
            class_range,
            JavaDefinitionMetadata::Class { super_types },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(class_range));

    true
}

fn parse_enum_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let enum_name = node.field("name");
    if enum_name.is_none() {
        return false;
    }

    let enum_name = enum_name.unwrap().text().into_owned();
    let enum_range = node_to_range(node);

    let mut super_types = Vec::new();
    if let Some(super_interface_node) = node.field("interfaces") {
        let super_interface_type_node =
            get_child_by_kind(&super_interface_node, node_types::TYPE_LIST);
        if let Some(super_interface_type_node) = super_interface_type_node
            && let Some(super_interface_type) = parse_type_node(&super_interface_type_node)
        {
            super_types.push(super_interface_type);
        }
    }

    let fqn_part = JavaFqnPart::new(JavaFqnPartType::Enum, enum_name.clone(), enum_range);

    parser_result.current_scope.push(fqn_part.clone());

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::Enum,
            enum_name,
            Arc::new(parser_result.current_scope.clone()),
            enum_range,
            JavaDefinitionMetadata::Class { super_types },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(enum_range));

    true
}

fn parse_record_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let record_name = node.field("name");
    if record_name.is_none() {
        return false;
    }

    let record_name = record_name.unwrap().text().into_owned();
    let record_range = node_to_range(node);

    let mut super_types = Vec::new();
    if let Some(interfaces_node) = node.field("interfaces")
        && let Some(type_list_node) = get_child_by_kind(&interfaces_node, node_types::TYPE_LIST)
    {
        for type_node in type_list_node.children() {
            if type_node.text() == "," {
                continue;
            }

            if let Some(interface_type) = parse_type_node(&type_node) {
                super_types.push(interface_type);
            }
        }
    }

    let fqn_part = JavaFqnPart::new(JavaFqnPartType::Record, record_name.clone(), record_range);

    parser_result.current_scope.push(fqn_part.clone());
    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::Record,
            record_name,
            Arc::new(parser_result.current_scope.clone()),
            record_range,
            JavaDefinitionMetadata::Class { super_types },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(record_range));

    true
}

fn parse_method_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let method_name = node.field("name");
    let method_return_type = node.field("type");
    if method_name.is_none() || method_return_type.is_none() {
        return false;
    }

    let method_return_type = parse_type_node(&method_return_type.unwrap());
    if method_return_type.is_none() {
        return false;
    }

    let method_name = method_name.unwrap().text().into_owned();
    let method_range = node_to_range(node);

    let fqn_part = JavaFqnPart::new(JavaFqnPartType::Method, method_name.clone(), method_range);

    parser_result.current_scope.push(fqn_part.clone());
    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::from_fqn_part_type(&fqn_part.node_type).unwrap(),
            method_name,
            Arc::new(parser_result.current_scope.clone()),
            method_range,
            JavaDefinitionMetadata::Method {
                return_type: method_return_type.unwrap(),
            },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(method_range));

    true
}

fn parse_field_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let variable_type = node.field("type");
    let variable_declarator = node.field("declarator");
    let variable_range = node_to_range(node);

    if variable_type.is_none() || variable_declarator.is_none() {
        return false;
    }

    let variable_declarator = variable_declarator.unwrap();
    let variable_name = variable_declarator
        .field("name")
        .map(|t| t.text().into_owned());
    if variable_name.is_none() {
        return false;
    }

    let variable_name = variable_name.unwrap();

    // If the field is not a lambda, we just need the binding and thats it.
    let is_lambda =
        get_child_by_kind(&variable_declarator, node_types::LAMBDA_EXPRESSION).is_some();
    if !is_lambda {
        let fqn_part = JavaFqnPart::new(
            JavaFqnPartType::Field,
            variable_name.clone(),
            variable_range,
        );

        let mut fqn = parser_result.current_scope.clone();
        fqn.push(fqn_part);

        parser_result
            .definitions
            .push(JavaDefinitionInfo::new_with_metadata(
                JavaDefinitionType::Field,
                variable_name,
                Arc::new(fqn),
                parser_result.current_lexical_scope.last().unwrap().range,
                JavaDefinitionMetadata::Field {
                    field_type: parse_type_node(&variable_type.unwrap()).unwrap(),
                },
            ));

        return false;
    }

    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::Lambda,
        variable_name.clone(),
        variable_range,
    );

    parser_result.current_scope.push(fqn_part.clone());
    parser_result.definitions.push(JavaDefinitionInfo::new(
        JavaDefinitionType::from_fqn_part_type(&fqn_part.node_type).unwrap(),
        variable_name,
        Arc::new(parser_result.current_scope.clone()),
        variable_range,
    ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(variable_range));

    true
}

fn parse_local_variable_declaration_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let variable_type = node.field("type");
    let variable_declarator = node.field("declarator");
    let variable_range = node_to_range(node);

    if variable_type.is_none() || variable_declarator.is_none() {
        return false;
    }

    let variable_declarator = variable_declarator.unwrap();
    let variable_name = variable_declarator
        .field("name")
        .map(|t| t.text().into_owned());
    let variable_value = variable_declarator.field("value");

    if variable_name.is_none() {
        return false;
    }

    let variable_name = variable_name.unwrap();
    let variable_java_type = parse_type_node(&variable_type.unwrap());

    let init = match variable_java_type {
        None => {
            if let Some(value) = variable_value.clone() {
                parse_reference_expression_node(&value, parser_result)
            } else {
                None
            }
        }
        _ => None, // No initialization required if we already know the type.
    };

    // If the variable is not a lambda, we just need the binding and thats it.
    let is_lambda =
        variable_value.is_some() && variable_value.unwrap().kind() == node_types::LAMBDA_EXPRESSION;
    if !is_lambda {
        let fqn_part = JavaFqnPart::new(
            JavaFqnPartType::LocalVariable,
            variable_name.clone(),
            variable_range,
        );

        let mut fqn = parser_result.current_scope.clone();
        fqn.push(fqn_part);

        parser_result
            .definitions
            .push(JavaDefinitionInfo::new_with_metadata(
                JavaDefinitionType::LocalVariable,
                variable_name,
                Arc::new(fqn),
                parser_result.current_lexical_scope.last().unwrap().range,
                JavaDefinitionMetadata::LocalVariable {
                    variable_type: variable_java_type,
                    init,
                },
            ));
        return false;
    }

    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::Lambda,
        variable_name.clone(),
        variable_range,
    );

    parser_result.current_scope.push(fqn_part.clone());
    parser_result.definitions.push(JavaDefinitionInfo::new(
        JavaDefinitionType::from_fqn_part_type(&fqn_part.node_type).unwrap(),
        variable_name,
        Arc::new(parser_result.current_scope.clone()),
        variable_range,
    ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_definition(variable_range));

    true
}

fn parse_formal_parameters_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    if let Some(current_scope) = parser_result.current_scope.last() {
        // In this function we're only interested in parsing a record constructor.
        if !matches!(current_scope.node_type, JavaFqnPartType::Record) {
            return false;
        }

        let fqn_part = JavaFqnPart::new(
            JavaFqnPartType::Constructor,
            current_scope.node_name().to_string(),
            node_to_range(node),
        );

        let mut fqn = parser_result.current_scope.clone();
        fqn.push(fqn_part);

        parser_result.definitions.push(JavaDefinitionInfo::new(
            JavaDefinitionType::Constructor,
            current_scope.node_name().to_string(),
            Arc::new(fqn),
            node_to_range(node),
        ));
    }

    for parameter_node in node.children() {
        if parameter_node.kind() == node_types::FORMAL_PARAMETER {
            parser_result.visited_nodes.insert(parameter_node.node_id());

            let parameter_type = parameter_node.field("type");
            let parameter_name = parameter_node.field("name");

            if parameter_type.is_none() || parameter_name.is_none() {
                continue;
            }

            let parameter_java_type = parse_type_node(&parameter_type.unwrap());
            if parameter_java_type.is_none() {
                continue;
            }

            let parameter_name = parameter_name.unwrap().text().into_owned();

            // Create the field
            let fqn_part = JavaFqnPart::new(
                JavaFqnPartType::Field,
                parameter_name.clone(),
                node_to_range(&parameter_node),
            );

            let mut fqn = parser_result.current_scope.clone();
            fqn.push(fqn_part);

            parser_result
                .definitions
                .push(JavaDefinitionInfo::new_with_metadata(
                    JavaDefinitionType::Field,
                    parameter_name.clone(),
                    Arc::new(fqn.clone()),
                    node_to_range(&parameter_node),
                    JavaDefinitionMetadata::Field {
                        field_type: parameter_java_type.clone().unwrap(),
                    },
                ));

            // Create the access method
            fqn.pop();
            fqn.push(JavaFqnPart::new(
                JavaFqnPartType::Method,
                parameter_name.clone(),
                node_to_range(&parameter_node),
            ));

            parser_result
                .definitions
                .push(JavaDefinitionInfo::new_with_metadata(
                    JavaDefinitionType::Method,
                    parameter_name,
                    Arc::new(fqn),
                    node_to_range(&parameter_node),
                    JavaDefinitionMetadata::Method {
                        return_type: parameter_java_type.unwrap(),
                    },
                ));
        }
    }

    false
}

fn parse_formal_parameter_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) {
    let parameter_type = node.field("type");
    let parameter_name = node.field("name");

    if parameter_type.is_none() || parameter_name.is_none() {
        return;
    }

    let parameter_java_type = parse_type_node(&parameter_type.unwrap());
    if parameter_java_type.is_none() {
        return;
    }

    let parameter_name = parameter_name.unwrap().text().into_owned();
    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::Parameter,
        parameter_name.clone(),
        node_to_range(node),
    );

    let mut fqn = parser_result.current_scope.clone();
    fqn.push(fqn_part);

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::Parameter,
            parameter_name,
            Arc::new(fqn),
            parser_result.current_lexical_scope.last().unwrap().range,
            JavaDefinitionMetadata::Parameter {
                parameter_type: parameter_java_type.unwrap(),
            },
        ));
}

fn parse_enhanced_for_statement_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> bool {
    let for_variable_type = node.field("type");
    let for_variable_name = node.field("name");
    let for_variable_value = node.field("value");
    let for_range = node_to_range(node);

    if for_variable_type.is_none() || for_variable_name.is_none() || for_variable_value.is_none() {
        return false;
    }

    let for_variable_name = for_variable_name.unwrap().text().into_owned();
    let for_variable_type = parse_type_node(&for_variable_type.unwrap());
    let init = match for_variable_type {
        None => parse_reference_expression_node(&for_variable_value.unwrap(), parser_result),
        _ => None, // No initialization required if we already know the type.
    };

    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::LocalVariable,
        for_variable_name.clone(),
        for_range,
    );

    let mut fqn = parser_result.current_scope.clone();
    fqn.push(fqn_part);

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::LocalVariable,
            for_variable_name,
            Arc::new(fqn),
            for_range,
            JavaDefinitionMetadata::LocalVariable {
                variable_type: for_variable_type,
                init,
            },
        ));

    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_block(for_range));

    true
}

fn parse_resource_node(node: &Node<StrDoc<SupportLang>>, parser_result: &mut JavaParseResult) {
    let resource_type = node.field("type");
    let resource_name = node.field("name");
    let resource_value = node.field("value");

    if resource_type.is_none() || resource_name.is_none() || resource_value.is_none() {
        return;
    }

    let binding_type = parse_type_node(&resource_type.unwrap());
    let init = match binding_type {
        None => parse_reference_expression_node(&resource_value.unwrap(), parser_result),
        _ => None, // No initialization required if we already know the type.
    };

    let resource_name = resource_name.unwrap().text().into_owned();
    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::LocalVariable,
        resource_name.clone(),
        node_to_range(node),
    );

    let mut fqn = parser_result.current_scope.clone();
    fqn.push(fqn_part);

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::LocalVariable,
            resource_name,
            Arc::new(fqn),
            parser_result.current_lexical_scope.last().unwrap().range,
            JavaDefinitionMetadata::LocalVariable {
                variable_type: binding_type,
                init,
            },
        ));
}

fn parse_expression_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> Option<JavaExpression> {
    enum ExpressionPart {
        FieldAccess { member: String },
        MemberMethodCall { member: String },
        ArrayAccess,
        MethodReference { member: String },
    }

    fn build_expression(
        mut expression: JavaExpression,
        parts: &mut Vec<ExpressionPart>,
    ) -> JavaExpression {
        while let Some(part) = parts.pop() {
            expression = match part {
                ExpressionPart::FieldAccess { member } => JavaExpression::FieldAccess {
                    target: Box::new(expression),
                    member,
                },
                ExpressionPart::MemberMethodCall { member } => JavaExpression::MemberMethodCall {
                    target: Box::new(expression),
                    member,
                },
                ExpressionPart::ArrayAccess => JavaExpression::ArrayAccess {
                    target: Box::new(expression),
                },
                ExpressionPart::MethodReference { member } => JavaExpression::MethodReference {
                    target: Box::new(expression),
                    member,
                },
            };
        }

        expression
    }

    let mut parts = Vec::new();
    let mut current = node.clone();

    loop {
        parser_result.visited_nodes.insert(current.node_id());

        match current.kind().as_ref() {
            node_types::IDENTIFIER => {
                let expression = JavaExpression::Identifier {
                    name: current.text().into_owned(),
                };

                return Some(build_expression(expression, &mut parts));
            }
            node_types::THIS => {
                return Some(build_expression(JavaExpression::This, &mut parts));
            }
            node_types::SUPER => {
                return Some(build_expression(JavaExpression::Super, &mut parts));
            }
            node_types::FIELD_ACCESS => {
                let member = current
                    .field("field")
                    .map(|n| n.text().into_owned())
                    .unwrap_or_default();

                parts.push(ExpressionPart::FieldAccess { member });

                if let Some(object_node) = current.field("object") {
                    current = object_node;
                    continue;
                }

                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
            node_types::METHOD_INVOCATION => {
                if let Some(target_node) = current.field("object") {
                    let member = current
                        .field("name")
                        .map(|n| n.text().into_owned())
                        .unwrap_or_default();

                    parts.push(ExpressionPart::MemberMethodCall { member });
                    current = target_node;
                    continue;
                }

                let name = current
                    .field("name")
                    .map(|n| n.text().into_owned())
                    .unwrap_or_else(|| current.text().into_owned());

                return Some(build_expression(
                    JavaExpression::MethodCall { name },
                    &mut parts,
                ));
            }
            node_types::ARRAY_ACCESS => {
                parts.push(ExpressionPart::ArrayAccess);

                if let Some(array_node) = current.field("array") {
                    current = array_node;
                    continue;
                }

                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
            node_types::METHOD_REFERENCE => {
                let member = current
                    .child(2) // Child 1 is the '::'
                    .map(|n| n.text().into_owned())
                    .unwrap_or_default();

                parts.push(ExpressionPart::MethodReference { member });

                if let Some(target_node) = current.child(0) {
                    current = target_node;
                    continue;
                }

                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
            node_types::OBJECT_CREATION_EXPRESSION => {
                if let Some(type_node) = current.field("type")
                    && let Some(java_type) = parse_type_node(&type_node)
                {
                    return Some(build_expression(
                        JavaExpression::ObjectCreation {
                            target: Box::new(java_type),
                        },
                        &mut parts,
                    ));
                }

                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
            node_types::ARRAY_CREATION_EXPRESSION => {
                if let Some(type_node) = current.field("type")
                    && let Some(java_type) = parse_type_node(&type_node)
                {
                    return Some(build_expression(
                        JavaExpression::ArrayCreation {
                            target: Box::new(java_type),
                        },
                        &mut parts,
                    ));
                }

                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
            _ => {
                return Some(build_expression(JavaExpression::Literal, &mut parts));
            }
        }
    }
}

fn parse_reference_expression_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) -> Option<JavaExpression> {
    let expression = parse_expression_node(node, parser_result);
    if let Some(expression) = expression {
        parser_result.references.push(JavaReferenceInfo {
            name: "".to_string(),
            range: node_to_range(node),
            target: ReferenceTarget::Unresolved(),
            reference_type: JavaReferenceType::Call,
            metadata: Some(Box::new(expression.clone())),
            scope: Some(Arc::new(parser_result.current_scope.clone())),
        });

        return Some(expression);
    }

    None
}

fn parse_assignment_expression_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) {
    let right = node.field("right");
    if right.is_none() {
        return;
    }

    parse_reference_expression_node(&right.unwrap(), parser_result);
}

fn parse_instance_of_expression_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) {
    let left = node.field("left");

    if let Some(left) = left
        && CALLABLE_EXPRESSIONS.contains(&left.kind().as_ref())
    {
        parse_reference_expression_node(&left, parser_result);
    }

    let right = node.field("right");
    let name = node.field("name");
    if name.is_none() || right.is_none() {
        return;
    }

    let name = name.unwrap().text().into_owned();
    let java_type = parse_type_node(&right.unwrap());

    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::Parameter,
        name.clone(),
        node_to_range(node),
    );

    let mut fqn = parser_result.current_scope.clone();
    fqn.push(fqn_part);

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::LocalVariable, // Let's count pattern variables as local variables
            name,
            Arc::new(fqn),
            parser_result.current_lexical_scope.last().unwrap().range,
            JavaDefinitionMetadata::LocalVariable {
                variable_type: java_type,
                init: None,
            },
        ));
}

fn parse_pattern_node(node: &Node<StrDoc<SupportLang>>, parser_result: &mut JavaParseResult) {
    let pattern_node = get_child_by_kind(node, node_types::TYPE_PATTERN);
    if pattern_node.is_none() {
        return;
    }

    let pattern_node = pattern_node.unwrap();
    let type_node = pattern_node.child(0);
    let name_node = pattern_node.child(1);

    if type_node.is_none() || name_node.is_none() {
        return;
    }

    let name = name_node.unwrap().text().to_string();
    let java_type = parse_type_node(&type_node.unwrap());

    let fqn_part = JavaFqnPart::new(
        JavaFqnPartType::LocalVariable,
        name.clone(),
        node_to_range(node),
    );

    let mut fqn = parser_result.current_scope.clone();
    fqn.push(fqn_part);

    parser_result
        .definitions
        .push(JavaDefinitionInfo::new_with_metadata(
            JavaDefinitionType::LocalVariable,
            name,
            Arc::new(fqn),
            parser_result.current_lexical_scope.last().unwrap().range,
            JavaDefinitionMetadata::LocalVariable {
                variable_type: java_type,
                init: None,
            },
        ));
}

fn parse_element_value_pair_node(
    node: &Node<StrDoc<SupportLang>>,
    parser_result: &mut JavaParseResult,
) {
    let value = node.field("value");

    if let Some(value) = value
        && CALLABLE_EXPRESSIONS.contains(&value.kind().as_ref())
    {
        parse_reference_expression_node(&value, parser_result);
    }
}

fn parse_annotation_node(node: &Node<StrDoc<SupportLang>>, parser_result: &mut JavaParseResult) {
    let name = node.field("name");
    if name.is_none() {
        return;
    }

    let name = name.unwrap().text().to_string();
    parser_result.references.push(JavaReferenceInfo {
        name: "".to_string(),
        range: node_to_range(node),
        target: ReferenceTarget::Unresolved(),
        reference_type: JavaReferenceType::Call,
        metadata: Some(Box::new(JavaExpression::Annotation { name: name.clone() })),
        scope: Some(Arc::new(parser_result.current_scope.clone())),
    });
}

fn parse_type_node(node: &Node<StrDoc<SupportLang>>) -> Option<JavaType> {
    match node.kind().as_ref() {
        node_types::ARRAY_TYPE => parse_type_node(&node.child(0)?),
        node_types::GENERIC_TYPE => parse_type_node(&node.child(0)?),
        _ => {
            let name = node.text().into_owned();
            if name == "var" {
                return None;
            }

            Some(JavaType { name })
        }
    }
}

fn parse_node(node: &Node<StrDoc<SupportLang>>, parser_result: &mut JavaParseResult) -> bool {
    match node.kind().as_ref() {
        // Definitions
        node_types::CLASS => parse_class_declaration_node(node, parser_result),
        node_types::INTERFACE => parse_interface_declaration_node(node, parser_result),
        node_types::ENUM => parse_enum_declaration_node(node, parser_result),
        node_types::ENUM_CONSTANT => {
            parse_simple_node(node, JavaFqnPartType::EnumConstant, parser_result)
        }
        node_types::RECORD => parse_record_declaration_node(node, parser_result),
        node_types::ANNOTATION_DECLARATION => {
            parse_simple_node(node, JavaFqnPartType::Annotation, parser_result)
        }
        node_types::ANNOTATION_ELEMENT_DECLARATION => {
            parse_simple_node(node, JavaFqnPartType::AnnotationDeclaration, parser_result)
        }
        node_types::CONSTRUCTOR_DECLARATION => {
            parse_simple_node(node, JavaFqnPartType::Constructor, parser_result)
        }
        node_types::FORMAL_PARAMETERS => parse_formal_parameters_node(node, parser_result),
        node_types::METHOD => parse_method_declaration_node(node, parser_result),
        // Lexical Scopes,
        node_types::BLOCK
        | node_types::IF_STATEMENT
        | node_types::SWITCH_RULE
        | node_types::TRY_WITH_RESOURCES_STATEMENT
        | node_types::CATCH_CLAUSE
        | node_types::FOR_STATEMENT => {
            parser_result
                .current_lexical_scope
                .push(LexicalScope::created_by_block(node_to_range(node)));
            true
        }
        node_types::ENHANCED_FOR_STATEMENT => {
            parse_enhanced_for_statement_node(node, parser_result)
        }
        // Bindings / Definitions
        node_types::FIELD_DECLARATION => parse_field_declaration_node(node, parser_result),
        node_types::LOCAL_VARIABLE_DECLARATION => {
            parse_local_variable_declaration_node(node, parser_result)
        }
        node_types::FORMAL_PARAMETER => {
            parse_formal_parameter_node(node, parser_result);
            false
        }
        node_types::RESOURCE => {
            parse_resource_node(node, parser_result);
            false
        }
        // Expressions
        node_types::OBJECT_CREATION_EXPRESSION
        | node_types::ARRAY_CREATION_EXPRESSION
        | node_types::THIS
        | node_types::SUPER
        | node_types::METHOD_INVOCATION
        | node_types::METHOD_REFERENCE => {
            if REFERENCE_PARENT_TYPES.contains(&node.parent().unwrap().kind().as_ref()) {
                parse_reference_expression_node(node, parser_result);
            }
            false
        }
        node_types::ASSIGNMENT_EXPRESSION => {
            parse_assignment_expression_node(node, parser_result);
            false
        }
        node_types::INSTANCE_OF_EXPRESSION => {
            parse_instance_of_expression_node(node, parser_result);
            false
        }
        node_types::ELEMENT_VALUE_PAIR => {
            parse_element_value_pair_node(node, parser_result);
            false
        }
        node_types::PATTERN => {
            parse_pattern_node(node, parser_result);
            false
        }
        node_types::ANNOTATION | node_types::MARKER_ANNOTATION => {
            parse_annotation_node(node, parser_result);
            false
        }
        _ => false,
    }
}

// FIXME: Should use a visitor pattern instead of a stack
pub fn parse_ast(ast: &AstRootNode) -> JavaAnalyzerResult {
    let mut parser_result = JavaParseResult::default();

    // Ensure the file always has a lexical scope
    let file_range = node_to_range(&ast.root());
    parser_result
        .current_lexical_scope
        .push(LexicalScope::created_by_block(file_range));

    if let Some(package_declaration) = get_child_by_kind(&ast.root(), node_types::PACKAGE)
        && let Some(package_name) =
            get_child_by_kind(&package_declaration, node_types::SCOPED_IDENTIFIER)
    {
        parser_result.current_scope.push(JavaFqnPart::new(
            JavaFqnPartType::Package,
            package_name.text().to_string(),
            node_to_range(&package_declaration),
        ));

        parser_result.definitions.push(JavaDefinitionInfo::new(
            JavaDefinitionType::Package,
            package_name.text().to_string(),
            Arc::new(parser_result.current_scope.clone()),
            node_to_range(&package_declaration),
        ));
    }

    let mut stack: Vec<Option<Node<StrDoc<SupportLang>>>> = Vec::with_capacity(128);
    stack.push(Some(ast.root()));

    while let Some(node_option) = stack.pop() {
        if let Some(node) = node_option {
            if !parser_result.visited_nodes.insert(node.node_id()) {
                // add all children to the stack in reverse order, some of them may have not been visited yet.
                push_children_reverse(&node, &mut stack);
                continue;
            }

            let node_kind = node.kind();
            if node_kind == node_types::IMPORT_DECLARATION {
                if let Some(import) = extract_import_declaration(&node) {
                    parser_result.imports.push(import);
                }

                continue;
            } else if parse_node(&node, &mut parser_result) {
                stack.push(None);
            }

            // add all children to the stack in reverse order
            push_children_reverse(&node, &mut stack);
        } else {
            // pop the current scope
            if let Some(popped_scope) = parser_result.current_lexical_scope.pop()
                && popped_scope.created_by_definition
            {
                parser_result.current_scope.pop();
            }
        }
    }

    JavaAnalyzerResult {
        definitions: parser_result.definitions,
        imports: parser_result.imports,
        references: parser_result.references,
    }
}

/// Helper function to add children to stack in reverse order
fn push_children_reverse<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    stack: &mut Vec<Option<Node<'a, StrDoc<SupportLang>>>>,
) {
    let children: Vec<_> = node.children().collect();
    stack.reserve(children.len());
    for child in children.into_iter().rev() {
        stack.push(Some(child));
    }
}

/// Convert a Java FQN to its string representation
/// The parts are joined by '.' to form the full FQN string
pub fn java_fqn_to_string(fqn: &JavaFqn) -> String {
    fqn.iter()
        .map(|part| part.node_name().to_string())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        java::types::JavaImportType,
        parser::{GenericParser, LanguageParser, SupportedLanguage},
    };

    #[test]
    fn test_java_code_outside_a_package() {
        let java_code = r#"
        public class MyClass {
            private int myField = 1;

            public void myMethod() {
                System.out.println("Hello, World!");
            }
        }

        public class Main {
            public static void main(String[] args) {
                MyClass obj = new MyClass();
                obj.myMethod();
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 7);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myField",
            "MyClass.myField", // FQN is used as the scope of the fields
            JavaDefinitionType::Field,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.myMethod",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "args",
            "Main.main.args", // FQN is used as the scope of the parameters
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "obj",
            "Main.main.obj", // FQN is used as the scope of the local variables
            JavaDefinitionType::LocalVariable,
        );
        validate_definition_exists(&definitions, "Main", "Main", JavaDefinitionType::Class);
        validate_definition_exists(
            &definitions,
            "main",
            "Main.main",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_java_code_in_a_package() {
        let java_code = r#"
        package com.example.test;

        public class MyClass {
            private int myField = 1;

            public MyClass() {
                // constructor
            }

            public void myMethod() {
                System.out.println("Hello, World!");
            }
        }

        public class Main {
            public static void main(String[] args) {
                var obj = new MyClass();
                obj.myMethod();
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;
        assert_eq!(definitions.len(), 9);

        validate_definition_exists(
            &definitions,
            "com.example.test",
            "com.example.test",
            JavaDefinitionType::Package,
        );
        validate_definition_exists(
            &definitions,
            "MyClass",
            "com.example.test.MyClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "MyClass",
            "com.example.test.MyClass.MyClass",
            JavaDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "myField",
            "com.example.test.MyClass.myField",
            JavaDefinitionType::Field,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "com.example.test.MyClass.myMethod",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "Main",
            "com.example.test.Main",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "args",
            "com.example.test.Main.main.args",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "obj",
            "com.example.test.Main.main.obj",
            JavaDefinitionType::LocalVariable,
        );
        validate_definition_exists(
            &definitions,
            "main",
            "com.example.test.Main.main",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_includes_declarations_inside_methods() {
        let java_code = r#"
        public class Main {
            public static void main(String[] args) {
                int myLocalVariable = 1;

                class LocalClass {
                    private int localClassField = 1;
                }

                System.out.println(myLocalVariable);
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 6);

        validate_definition_exists(&definitions, "Main", "Main", JavaDefinitionType::Class);
        validate_definition_exists(
            &definitions,
            "main",
            "Main.main",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "args",
            "Main.main.args",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "myLocalVariable",
            "Main.main.myLocalVariable",
            JavaDefinitionType::LocalVariable,
        );
        validate_definition_exists(
            &definitions,
            "LocalClass",
            "Main.main.LocalClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "localClassField",
            "Main.main.LocalClass.localClassField",
            JavaDefinitionType::Field,
        );
    }

    #[test]
    fn test_nested_classes_are_included_in_fqn() {
        let java_code = r#"
        public class OuterClass {
            public class InnerClass {
                public void innerMethod() {
                    System.out.println("Hello from inner class!");
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "OuterClass",
            "OuterClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "InnerClass",
            "OuterClass.InnerClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "innerMethod",
            "OuterClass.InnerClass.innerMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_interface_definitions_are_included_in_fqn() {
        let java_code = r#"
        public interface Repository<T> {
            T findById(String id);
            T save(T entity);
        }

        public class UserRepository implements Repository<User> {
            @Override
            public User findById(String id) {
                return new User(id);
            }

            @Override
            public User save(User entity) {
                return entity;
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 10);

        validate_definition_exists(
            &definitions,
            "Repository",
            "Repository",
            JavaDefinitionType::Interface,
        );
        validate_definition_exists(
            &definitions,
            "findById",
            "Repository.findById",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "id",
            "Repository.findById.id",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "save",
            "Repository.save",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "entity",
            "Repository.save.entity",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "UserRepository",
            "UserRepository",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "findById",
            "UserRepository.findById",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "id",
            "UserRepository.findById.id",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "save",
            "UserRepository.save",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "entity",
            "UserRepository.save.entity",
            JavaDefinitionType::Parameter,
        );
    }

    #[test]
    fn test_enum_definitions_are_included_in_fqn() {
        let java_code = r#"
        public enum Status {
            ACTIVE("active"),
            INACTIVE("inactive");

            private final String value;

            Status(String value) {
                this.value = value;
            }

            public String getValue() {
                return value;
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 7);

        validate_definition_exists(&definitions, "Status", "Status", JavaDefinitionType::Enum);
        validate_definition_exists(
            &definitions,
            "ACTIVE",
            "Status.ACTIVE",
            JavaDefinitionType::EnumConstant,
        );
        validate_definition_exists(
            &definitions,
            "INACTIVE",
            "Status.INACTIVE",
            JavaDefinitionType::EnumConstant,
        );
        validate_definition_exists(
            &definitions,
            "Status",
            "Status.Status",
            JavaDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "value",
            "Status.Status.value",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "value",
            "Status.value",
            JavaDefinitionType::Field,
        );
        validate_definition_exists(
            &definitions,
            "getValue",
            "Status.getValue",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_record_definitions_are_included_in_fqn() {
        let java_code = r#"
        public record Person(String name, int age) {
            public Person {
                if (age < 0) {
                    throw new IllegalArgumentException("Age cannot be negative");
                }
            }

            public String getDisplayName() {
                return name + " (" + age + ")";
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let analysis_result = parse_ast(&parse_result.ast);
        let definitions = analysis_result.definitions;

        assert_eq!(definitions.len(), 7);

        validate_definition_exists(&definitions, "Person", "Person", JavaDefinitionType::Record);
        validate_definition_exists(
            &definitions,
            "Person",
            "Person.Person",
            JavaDefinitionType::Constructor,
        );
        validate_definition_exists(
            &definitions,
            "name",
            "Person.name",
            JavaDefinitionType::Field,
        );
        validate_definition_exists(
            &definitions,
            "name",
            "Person.name",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(&definitions, "age", "Person.age", JavaDefinitionType::Field);
        validate_definition_exists(
            &definitions,
            "age",
            "Person.age",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "getDisplayName",
            "Person.getDisplayName",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_annotation_definitions_are_included_in_fqn() {
        let java_code = r#"
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        public @interface MyAnnotation {
            String value() default "";
            int count() default 0;
        }

        @MyAnnotation(value = "test", count = 5)
        public class AnnotatedClass {
            public void myMethod() {
                System.out.println("Hello, World!");
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(
            &definitions,
            "MyAnnotation",
            "MyAnnotation",
            JavaDefinitionType::Annotation,
        );
        validate_definition_exists(
            &definitions,
            "value",
            "MyAnnotation.value",
            JavaDefinitionType::AnnotationDeclaration,
        );
        validate_definition_exists(
            &definitions,
            "count",
            "MyAnnotation.count",
            JavaDefinitionType::AnnotationDeclaration,
        );
        validate_definition_exists(
            &definitions,
            "AnnotatedClass",
            "AnnotatedClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "AnnotatedClass.myMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_method_overloading_have_the_same_fqn() {
        let java_code = r#"
        public class MyClass {
            public void myMethod(int a) {
                System.out.println("int: " + a);
            }

            public void myMethod(String a) {
                System.out.println("String: " + a);
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "a",
            "MyClass.myMethod.a",
            JavaDefinitionType::Parameter,
        );
        validate_definition_exists(
            &definitions,
            "myMethod",
            "MyClass.myMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_handles_class_with_modifiers() {
        let java_code = r#"
        public final class MyClass {
            private static final int MY_CONSTANT = 42;
            public String myField = "test";
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "MY_CONSTANT",
            "MyClass.MY_CONSTANT",
            JavaDefinitionType::Field,
        );
        validate_definition_exists(
            &definitions,
            "myField",
            "MyClass.myField",
            JavaDefinitionType::Field,
        );
    }

    #[test]
    fn test_static_nested_classes_are_included_in_fqn() {
        let java_code = r#"
        public class OuterClass {
            public static class StaticInnerClass {
                public void staticInnerMethod() {
                    System.out.println("Hello from static inner class!");
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "OuterClass",
            "OuterClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "StaticInnerClass",
            "OuterClass.StaticInnerClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "staticInnerMethod",
            "OuterClass.StaticInnerClass.staticInnerMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_abstract_classes_are_included_in_fqn() {
        let java_code = r#"
        public abstract class AbstractClass {
            protected abstract void abstractMethod();
            
            public void concreteMethod() {
                System.out.println("Concrete method");
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 3);

        validate_definition_exists(
            &definitions,
            "AbstractClass",
            "AbstractClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "abstractMethod",
            "AbstractClass.abstractMethod",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "concreteMethod",
            "AbstractClass.concreteMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_lambda_expressions_are_included_in_fqn() {
        let java_code = r#"
        public class Main {
            static Function<Void> STATIC_CALLABLE = () -> {};
            Runnable fieldCallable = () -> {};
            
            public void main() {
                BiFunction<Integer, String, Void> mainCallable = (i, s) -> {};
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        assert_eq!(definitions.len(), 5);

        validate_definition_exists(&definitions, "Main", "Main", JavaDefinitionType::Class);
        validate_definition_exists(
            &definitions,
            "fieldCallable",
            "Main.fieldCallable",
            JavaDefinitionType::Lambda,
        );
        validate_definition_exists(
            &definitions,
            "STATIC_CALLABLE",
            "Main.STATIC_CALLABLE",
            JavaDefinitionType::Lambda,
        );
        validate_definition_exists(
            &definitions,
            "main",
            "Main.main",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "mainCallable",
            "Main.main.mainCallable",
            JavaDefinitionType::Lambda,
        );
    }

    #[test]
    fn test_imports_are_included_in_fqn() {
        let java_code = r#"
        import java.util.List;
        import static java.util.List.of;
        import java.io.*;
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let imports = parse_ast(&parse_result.ast).imports;

        validate_import_exists(&imports, "java.util", "List", JavaImportType::Import);
        validate_import_exists(
            &imports,
            "java.util.List",
            "of",
            JavaImportType::StaticImport,
        );
        validate_import_exists(&imports, "java.io", "*", JavaImportType::WildcardImport);
    }

    #[test]
    fn test_creating_lexical_scope_exit_doesnt_always_exit_the_fqn_scope() {
        let java_code = r#"
        package com.example.test;

        public class McpSchema {
        	public record Tool(String name) {}

            private static Map<String, Object> schemaToMap(String schema) {
                try {
                    return OBJECT_MAPPER.readValue(schema, MAP_TYPE_REF);
                }
                catch (IOException e) {
                    throw new IllegalArgumentException("Invalid schema: " + schema, e);
                }
            }

            private static JsonSchema parseSchema(String schema) {
                try {
                    return OBJECT_MAPPER.readValue(schema, JsonSchema.class);
                }
                catch (IOException e) {
                    throw new IllegalArgumentException("Invalid schema: " + schema, e);
                }
            }

            public record Resource() {}
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        validate_definition_exists(
            &definitions,
            "McpSchema",
            "com.example.test.McpSchema",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "Tool",
            "com.example.test.McpSchema.Tool",
            JavaDefinitionType::Record,
        );
        validate_definition_exists(
            &definitions,
            "schemaToMap",
            "com.example.test.McpSchema.schemaToMap",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "parseSchema",
            "com.example.test.McpSchema.parseSchema",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "Resource",
            "com.example.test.McpSchema.Resource",
            JavaDefinitionType::Record,
        );
    }

    #[test]
    fn test_super_types_are_included_in_declaration() {
        let java_code = r#"
        class MyClass extends MySuperClass implements MyInterface {
        }

        interface MyInterface extends MySuperInterface {
        }

        record MyRecord() implements MyInterface {
        }

        enum MyEnum implements MyInterface {
            A,
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        let my_class = definitions
            .iter()
            .find(|definition| definition.name == "MyClass")
            .unwrap();
        assert_eq!(my_class.definition_type, JavaDefinitionType::Class);
        match my_class.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::Class { super_types } => {
                assert_eq!(super_types[0].name, "MySuperClass");
                assert_eq!(super_types[1].name, "MyInterface");
            }
            _ => panic!("Expected JavaDefinitionMetadata::Class"),
        }

        let my_interface = definitions
            .iter()
            .find(|definition| definition.name == "MyInterface")
            .unwrap();
        assert_eq!(my_interface.definition_type, JavaDefinitionType::Interface);
        match my_interface.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::Class { super_types } => {
                assert_eq!(super_types[0].name, "MySuperInterface");
            }
            _ => panic!("Expected JavaDefinitionMetadata::Class"),
        }

        let my_record = definitions
            .iter()
            .find(|definition| definition.name == "MyRecord")
            .unwrap();
        assert_eq!(my_record.definition_type, JavaDefinitionType::Record);
        match my_record.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::Class { super_types } => {
                assert_eq!(super_types[0].name, "MyInterface");
            }
            _ => panic!("Expected JavaDefinitionMetadata::Record"),
        }

        let my_enum = definitions
            .iter()
            .find(|definition| definition.name == "MyEnum")
            .unwrap();
        assert_eq!(my_enum.definition_type, JavaDefinitionType::Enum);
        match my_enum.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::Class { super_types } => {
                assert_eq!(super_types[0].name, "MyInterface");
            }
            _ => panic!("Expected JavaDefinitionMetadata::Enum"),
        }
    }

    #[test]
    fn test_local_bindings_range_last_the_current_lexical_scope() {
        let java_code = r#"
        class MyClass {
            public void myMethod(int z) {
                int x = 1;
                if (true) {
                    int y = 2;
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let result = parse_ast(&parse_result.ast);

        let x_definition = result
            .definitions
            .iter()
            .find(|definition| definition.name == "x");
        assert!(x_definition.is_some());
        let x_definition = x_definition.unwrap();
        assert_eq!(
            x_definition.definition_type,
            JavaDefinitionType::LocalVariable
        );
        assert_eq!(x_definition.range.start.line, 2); // Range is all of the method
        assert_eq!(x_definition.range.end.line, 7);

        let z_definition = result
            .definitions
            .iter()
            .find(|definition| definition.name == "z");
        assert!(z_definition.is_some());
        let z_definition = z_definition.unwrap();
        assert_eq!(z_definition.definition_type, JavaDefinitionType::Parameter);
        assert_eq!(z_definition.range.start.line, 2); // Range is all of the method
        assert_eq!(z_definition.range.end.line, 7);

        let y_definition = result
            .definitions
            .iter()
            .find(|definition| definition.name == "y");
        assert!(y_definition.is_some());
        let y_definition = y_definition.unwrap();
        assert_eq!(
            y_definition.definition_type,
            JavaDefinitionType::LocalVariable
        );
        assert_eq!(y_definition.range.start.line, 4); // Range is all of the if block
        assert_eq!(y_definition.range.end.line, 6);
    }

    #[test]
    fn test_pattern_bindings_are_included_in_references() {
        let java_code = r#"
        public class Main {
            Foo myParameter;

            public void main() {
                switch (myParameter.randomType()) {
                    case Foo foo -> foo.foo();
                    case Bar bar -> bar.bar();
                    default -> throw new IllegalArgumentException("Invalid type");
                }

                if (myParameter instanceof Foo ifFoo) {
                    ifFoo.foo();
                }
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        let foo_definition = definitions
            .iter()
            .find(|definition| definition.name == "foo");
        assert!(foo_definition.is_some());
        let foo_definition = foo_definition.unwrap();
        assert_eq!(
            foo_definition.definition_type,
            JavaDefinitionType::LocalVariable
        );
        assert_eq!(foo_definition.range.start.line, 6);
        assert_eq!(foo_definition.range.end.line, 6);
        match foo_definition.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::LocalVariable {
                variable_type,
                init,
            } => {
                assert_eq!(variable_type.as_ref().unwrap().name, "Foo");
                assert!(init.is_none());
            }
            _ => panic!("Expected JavaDefinitionMetadata::LocalVariable"),
        }

        let bar_definition = definitions
            .iter()
            .find(|definition| definition.name == "bar");
        assert!(bar_definition.is_some());
        let bar_definition = bar_definition.unwrap();
        assert_eq!(
            bar_definition.definition_type,
            JavaDefinitionType::LocalVariable
        );
        assert_eq!(bar_definition.range.start.line, 7);
        assert_eq!(bar_definition.range.end.line, 7);
        match bar_definition.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::LocalVariable {
                variable_type,
                init,
            } => {
                assert_eq!(variable_type.as_ref().unwrap().name, "Bar");
                assert!(init.is_none());
            }
            _ => panic!("Expected JavaDefinitionMetadata::LocalVariable"),
        }

        let if_foo_definition = definitions
            .iter()
            .find(|definition| definition.name == "ifFoo");
        assert!(if_foo_definition.is_some());
        let if_foo_definition = if_foo_definition.unwrap();
        assert_eq!(
            if_foo_definition.definition_type,
            JavaDefinitionType::LocalVariable
        );
        assert_eq!(if_foo_definition.range.start.line, 11);
        assert_eq!(if_foo_definition.range.end.line, 13);
        match if_foo_definition.metadata.as_ref().unwrap() {
            JavaDefinitionMetadata::LocalVariable {
                variable_type,
                init,
            } => {
                assert_eq!(variable_type.as_ref().unwrap().name, "Foo");
                assert!(init.is_none());
            }
            _ => panic!("Expected JavaDefinitionMetadata::LocalVariable"),
        }
    }

    #[test]
    #[allow(clippy::get_first)]
    fn test_call_expressions_are_included_in_references() {
        let java_code = r#"
        public class Main extends Application {
            Foo myParameter;

            public Main() {
                myParameter = new Foo();
            }

            @Traceable
            public void main() {
                if (this.myParameter.bar() instanceof Bar bar) {
                    bar.baz();
                }

                myParameter.executor.execute(Executor::executeFn);
                await(() -> super.run());
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let references = parse_ast(&parse_result.ast).references;
        assert_eq!(references.len(), 8);

        let foo_creation_reference = references.get(0).unwrap().metadata.as_ref().unwrap();
        match &**foo_creation_reference {
            JavaExpression::ObjectCreation { target } => {
                assert_eq!(target.name, "Foo");
            }
            _ => panic!("Expected JavaExpression::ObjectCreation"),
        }

        let traceable_annotation_reference = references.get(1).unwrap().metadata.as_ref().unwrap();
        match &**traceable_annotation_reference {
            JavaExpression::Annotation { name } => {
                assert_eq!(name, "Traceable");
            }
            _ => panic!("Expected JavaExpression::Annotation"),
        }

        let bar_reference = references.get(2).unwrap().metadata.as_ref().unwrap();
        match &**bar_reference {
            JavaExpression::MemberMethodCall { target, member } => {
                match target.as_ref() {
                    JavaExpression::FieldAccess { target, member } => {
                        assert!(matches!(target.as_ref(), JavaExpression::This));
                        assert_eq!(member, "myParameter");
                    }
                    _ => panic!("Expected JavaExpression::FieldAccess"),
                }
                assert_eq!(member, "bar");
            }
            _ => panic!("Expected JavaExpression::MethodCall"),
        }

        let baz_reference = references.get(3).unwrap().metadata.as_ref().unwrap();
        match &**baz_reference {
            JavaExpression::MemberMethodCall { target, member } => {
                match target.as_ref() {
                    JavaExpression::Identifier { name } => {
                        assert_eq!(name, "bar");
                    }
                    _ => panic!("Expected JavaExpression::Identifier"),
                }
                assert_eq!(member, "baz");
            }
            _ => panic!("Expected JavaExpression::MethodCall"),
        }

        let execute_reference = references.get(4).unwrap().metadata.as_ref().unwrap();
        match &**execute_reference {
            JavaExpression::MemberMethodCall { target, member } => {
                match target.as_ref() {
                    JavaExpression::FieldAccess { target, member } => {
                        match target.as_ref() {
                            JavaExpression::Identifier { name } => {
                                assert_eq!(name, "myParameter");
                            }
                            _ => panic!("Expected JavaExpression::Identifier"),
                        }
                        assert_eq!(member, "executor");
                    }
                    _ => panic!("Expected JavaExpression::FieldAccess"),
                }
                assert_eq!(member, "execute");
            }
            _ => panic!("Expected JavaExpression::MethodCall"),
        }

        let executor_reference = references.get(5).unwrap().metadata.as_ref().unwrap();
        match &**executor_reference {
            JavaExpression::MethodReference { target, member } => {
                match target.as_ref() {
                    JavaExpression::Identifier { name } => {
                        assert_eq!(name, "Executor");
                    }
                    _ => panic!("Expected JavaExpression::Identifier"),
                }
                assert_eq!(member, "executeFn");
            }
            _ => panic!("Expected JavaExpression::MethodReference"),
        }

        let await_reference = references.get(6).unwrap().metadata.as_ref().unwrap();
        match &**await_reference {
            JavaExpression::MethodCall { name } => {
                assert_eq!(name, "await");
            }
            _ => panic!("Expected JavaExpression::MethodCall"),
        }

        let run_reference = references.get(7).unwrap().metadata.as_ref().unwrap();
        match &**run_reference {
            JavaExpression::MemberMethodCall { target, member } => {
                assert!(matches!(target.as_ref(), JavaExpression::Super));
                assert_eq!(member, "run");
            }
            _ => panic!("Expected JavaExpression::MethodCall"),
        }
    }

    #[test]
    fn test_should_not_create_scope_for_resource_in_try_with_resource() {
        let java_code = r#"
        class MyClass {
            void methodWithTryWithResource() {
                try(var resource = 1) {}
            }

            void otherMethod() {
            }
        }
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        validate_definition_exists(
            &definitions,
            "MyClass",
            "MyClass",
            JavaDefinitionType::Class,
        );
        validate_definition_exists(
            &definitions,
            "methodWithTryWithResource",
            "MyClass.methodWithTryWithResource",
            JavaDefinitionType::Method,
        );
        validate_definition_exists(
            &definitions,
            "resource",
            "MyClass.methodWithTryWithResource.resource",
            JavaDefinitionType::LocalVariable,
        );
        validate_definition_exists(
            &definitions,
            "otherMethod",
            "MyClass.otherMethod",
            JavaDefinitionType::Method,
        );
    }

    #[test]
    fn test_creating_an_example_declaration_file_does_not_panic() {
        let java_code = r#"
            var x = 1;
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::Java);
        let parse_result = parser.parse(java_code, Some("test.java")).unwrap();

        let definitions = parse_ast(&parse_result.ast).definitions;

        validate_definition_exists(&definitions, "x", "x", JavaDefinitionType::LocalVariable);
    }

    fn validate_definition_exists(
        definitions: &JavaDefinitions,
        name: &str,
        fqn: &str,
        expected_type: JavaDefinitionType,
    ) {
        let definition = definitions.iter().find(|definition| {
            definition.name == name
                && java_fqn_to_string(&definition.fqn) == fqn
                && definition.definition_type == expected_type
        });
        assert!(
            definition.is_some(),
            "Definition with name {name}, FQN {fqn} and type {expected_type:?} not found"
        );
    }

    fn validate_import_exists(
        imports: &JavaImports,
        path: &str,
        symbol: &str,
        expected_type: JavaImportType,
    ) {
        let import = imports.iter().find(|import| {
            import.import_path == path && import.identifier.as_ref().unwrap().name == symbol
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
