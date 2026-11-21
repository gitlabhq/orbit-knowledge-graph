use crate::intern::Lang;
use crate::tree::{Cursor, Node, Storage, Tree};

use super::types::Tf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JavaTf {
    Signature,
    DeclaredType,
    ArgTypes,
}

impl JavaTf {
    pub fn from_name(name: &str) -> Option<Tf> {
        Some(Tf::Java(match name {
            "java_signature" => Self::Signature,
            "java_declared_type" => Self::DeclaredType,
            "java_arg_types" => Self::ArgTypes,
            _ => return None,
        }))
    }

    pub(crate) fn apply<S: Storage<Node = Node>>(self, t: &Tree<S>, lang: &Lang, id: u32) -> u32 {
        let reader = Reader { t, lang };
        let node = t.cursor(id);
        let text = match self {
            Self::Signature => reader.signature(node),
            Self::DeclaredType => reader.declared_type(node),
            Self::ArgTypes => reader.arg_types(node),
        };
        match text.as_deref() {
            None | Some("") => 0,
            Some(text) => lang.syms.intern(text),
        }
    }
}

struct Reader<'a, S: Storage<Node = Node>> {
    t: &'a Tree<S>,
    lang: &'a Lang,
}

impl<'a, S: Storage<Node = Node>> Reader<'a, S> {
    fn kind(&self, node: Cursor<'a, S>) -> &'a str {
        self.lang.kind_name(node.kind())
    }

    fn text(&self, node: Cursor<'a, S>) -> &'a str {
        self.t.text_at(node.index(), self.lang)
    }

    fn field(&self, node: Cursor<'a, S>, name: &str) -> Option<Cursor<'a, S>> {
        node.children()
            .find(|child| self.lang.field_name(child.field()) == name)
    }

    fn is_annotation(&self, node: Cursor<'a, S>) -> bool {
        matches!(self.kind(node), "annotation" | "marker_annotation")
    }

    fn is_comment(&self, node: Cursor<'a, S>) -> bool {
        matches!(self.kind(node), "line_comment" | "block_comment")
    }

    fn bracket_pairs(&self, node: Option<Cursor<'a, S>>) -> usize {
        node.map_or(0, |node| {
            node.children().filter(|c| self.kind(*c) == "[").count()
        })
    }

    fn has_error(&self, node: Cursor<'a, S>) -> bool {
        std::iter::once(node)
            .chain(node.descendants())
            .any(|n| n.named() && self.kind(n).is_empty())
    }

    fn erased_type(&self, node: Cursor<'a, S>) -> String {
        match self.kind(node) {
            "array_type" => {
                let element = self
                    .field(node, "element")
                    .map(|element| self.erased_type(element))
                    .unwrap_or_default();
                let dimensions = self.bracket_pairs(self.field(node, "dimensions"));
                format!("{element}{}", "[]".repeat(dimensions))
            }
            "generic_type" | "scoped_type_identifier" => node
                .children()
                .filter(|c| {
                    c.named() && !self.is_annotation(*c) && self.kind(*c) != "type_arguments"
                })
                .map(|c| self.erased_type(c))
                .collect::<Vec<_>>()
                .join("."),
            "annotated_type" => node
                .children()
                .find(|c| c.named() && !self.is_annotation(*c))
                .map(|c| self.erased_type(c))
                .unwrap_or_default(),
            _ => self.text(node).to_string(),
        }
    }

    fn spread_type(&self, param: Cursor<'a, S>) -> Option<String> {
        let element = param
            .children()
            .take_while(|c| self.kind(*c) != "...")
            .filter(|c| c.named() && self.kind(*c) != "modifiers" && !self.is_annotation(*c))
            .last()?;
        Some(self.erased_type(element))
    }

    fn signature(&self, node: Cursor<'a, S>) -> Option<String> {
        let parameters = self.field(node, "parameters")?;
        if self.has_error(parameters) {
            return None;
        }
        let types: Vec<String> = parameters
            .children()
            .filter_map(|param| match self.kind(param) {
                "formal_parameter" => {
                    let ty = self.erased_type(self.field(param, "type")?);
                    let dims = self.bracket_pairs(self.field(param, "dimensions"));
                    Some(format!("{ty}{}", "[]".repeat(dims)))
                }
                "spread_parameter" => Some(format!("{}...", self.spread_type(param)?)),
                _ => None,
            })
            .collect();
        Some(format!("({})", types.join(", ")))
    }

    fn declared_type(&self, node: Cursor<'a, S>) -> Option<String> {
        if self.kind(node) == "spread_parameter" {
            return Some(format!("{}[]", self.spread_type(node)?));
        }
        let ty = self
            .field(node, "type")
            .or_else(|| self.field(node, "right"))?;
        let declarator = self.field(node, "declarator");
        let dimensions = self
            .field(node, "dimensions")
            .or_else(|| declarator.and_then(|d| self.field(d, "dimensions")));
        Some(format!(
            "{}{}",
            self.erased_type(ty),
            "[]".repeat(self.bracket_pairs(dimensions))
        ))
    }

    fn arg_types(&self, list: Cursor<'a, S>) -> Option<String> {
        let tokens: Vec<String> = list
            .children()
            .filter(|c| c.named() && !self.is_comment(*c))
            .map(|arg| self.arg_token(arg))
            .collect();
        Some(format!("({})", tokens.join(",")))
    }

    fn arg_token(&self, arg: Cursor<'a, S>) -> String {
        let unknown = || "?".to_string();
        match self.kind(arg) {
            "decimal_integer_literal"
            | "hex_integer_literal"
            | "octal_integer_literal"
            | "binary_integer_literal" => format!("#int:{}", self.text(arg)),
            "decimal_floating_point_literal" | "hex_floating_point_literal" => {
                format!("#fp:{}", self.text(arg))
            }
            "character_literal" => "char".into(),
            "string_literal" => "String".into(),
            "true" | "false" => "boolean".into(),
            "null_literal" => "null".into(),
            "identifier" => {
                let name = self.text(arg);
                if self.in_anonymous_body(arg) {
                    return unknown();
                }
                let sigil = if self.declared_in_members(arg, name) {
                    '~'
                } else {
                    '^'
                };
                format!("{sigil}{name}")
            }
            "this" => "this".into(),
            "object_creation_expression" => self
                .field(arg, "type")
                .map(|ty| self.erased_type(ty))
                .unwrap_or_else(unknown),
            "cast_expression" => self
                .field(arg, "type")
                .filter(|ty| self.kind(*ty) != "intersection_type")
                .map(|ty| self.erased_type(ty))
                .unwrap_or_else(unknown),
            "parenthesized_expression" => arg
                .children()
                .find(|c| c.named() && !self.is_comment(*c))
                .map(|inner| self.arg_token(inner))
                .unwrap_or_else(unknown),
            "unary_expression" => self.unary_token(arg).unwrap_or_else(unknown),
            _ => unknown(),
        }
    }

    fn in_anonymous_body(&self, node: Cursor<'a, S>) -> bool {
        node.ancestors()
            .find(|up| self.kind(*up) == "class_body")
            .and_then(|body| body.parent())
            .is_some_and(|owner| {
                matches!(
                    self.kind(owner),
                    "object_creation_expression" | "enum_constant"
                )
            })
    }

    fn declared_in_members(&self, node: Cursor<'a, S>, name: &str) -> bool {
        node.ancestors()
            .filter(|member| {
                member.parent().is_some_and(|parent| {
                    matches!(
                        self.kind(parent),
                        "class_body"
                            | "interface_body"
                            | "enum_body_declarations"
                            | "annotation_type_body"
                    )
                })
            })
            .any(|member| {
                std::iter::once(member)
                    .chain(member.descendants())
                    .any(|id| self.declares(id, name))
            })
    }

    fn declares(&self, id: Cursor<'a, S>, name: &str) -> bool {
        if self.kind(id) != "identifier" || self.text(id) != name {
            return false;
        }
        let Some(parent) = id.parent() else {
            return false;
        };
        match self.kind(parent) {
            "inferred_parameters" | "type_pattern" | "record_pattern_component" => true,
            "variable_declarator"
            | "formal_parameter"
            | "catch_formal_parameter"
            | "resource"
            | "enhanced_for_statement" => self.lang.field_name(id.field()) == "name",
            "lambda_expression" => self.lang.field_name(id.field()) == "parameters",
            _ => false,
        }
    }

    fn unary_token(&self, arg: Cursor<'a, S>) -> Option<String> {
        let operand = self.field(arg, "operand")?;
        let operator = self.field(arg, "operator").map(|op| self.kind(op))?;
        match operator {
            "!" => Some("boolean".into()),
            "-" | "+" => Some(self.arg_token(operand)).filter(|token| token.starts_with('#')),
            _ => None,
        }
    }
}
