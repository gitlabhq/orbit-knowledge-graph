use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{DefKind, Fqn};
use treesitter_visit::extract::{child_of_kind, text};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolutionRules};
use treesitter_visit::Axis;
use treesitter_visit::Match;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// Extract the text of the Nth `string_lit` child's inner `template_literal`.
fn nth_label(n: isize) -> treesitter_visit::extract::Extract {
    text()
        .nth(Axis::Child, Match::Kind("string_lit"), n)
        .child_of_kind("template_literal")
}

#[derive(Default)]
pub struct HclDsl;

impl DslLanguage for HclDsl {
    fn name() -> &'static str {
        "hcl"
    }

    fn language() -> Language {
        Language::Hcl
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            // resource "type" "name" { ... }
            // Creates a scope named by the resource type (first label).
            // The on_scope hook injects a child def for the resource name (second label).
            scope("block", "Resource")
                .def_kind(DefKind::Class)
                .when(has_child_text("resource"))
                .name_from(nth_label(0)),
            // data "type" "name" { ... }
            scope("block", "DataSource")
                .def_kind(DefKind::Class)
                .when(has_child_text("data"))
                .name_from(nth_label(0)),
            // variable "name" { ... }
            scope("block", "Variable")
                .def_kind(DefKind::Property)
                .when(has_child_text("variable"))
                .no_scope()
                .name_from(nth_label(0)),
            // output "name" { ... }
            scope("block", "Output")
                .def_kind(DefKind::Property)
                .when(has_child_text("output"))
                .no_scope()
                .name_from(nth_label(0)),
            // module "name" { ... }
            scope("block", "Module")
                .def_kind(DefKind::Module)
                .when(has_child_text("module"))
                .no_scope()
                .name_from(nth_label(0)),
            // locals { key = value; ... }
            scope("block", "Locals")
                .def_kind(DefKind::Other)
                .when(has_child_text("locals"))
                .name_from(child_of_kind("identifier")),
            // provider "name" { ... }
            scope("block", "Provider")
                .def_kind(DefKind::Other)
                .when(has_child_text("provider"))
                .no_scope()
                .name_from(nth_label(0)),
            // Attributes inside a locals block become property definitions.
            scope("attribute", "Local")
                .def_kind(DefKind::Property)
                .when(ancestor_is(&["block"]))
                .no_scope()
                .name_from(child_of_kind("identifier")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Function calls: merge(...), file(...), toset(...), etc.
            reference("function_call").name_from(child_of_kind("identifier")),
            // Bare variable references: aws_vpc, var, local, module, data, etc.
            // These are the base of dot-separated reference chains.
            reference("variable_expr").name_from(child_of_kind("identifier")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn bindings() -> Vec<BindingRule> {
        vec![]
    }

    fn branches() -> Vec<BranchRule> {
        vec![branch("conditional").branches(&["true_expr", "false_expr"])]
    }

    fn loops() -> Vec<LoopRule> {
        vec![]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_scope: Some(hcl_on_scope),
            ref_name_rewrite: Some(hcl_rewrite_ref),
            ..LanguageHooks::default()
        }
    }

    fn chain_config() -> Option<ChainConfig> {
        None
    }
}

/// For resource and data blocks with two labels, inject a child definition
/// named by the second label. This produces FQNs like `aws_instance.web`
/// (scope "aws_instance" + child def "web").
fn hcl_on_scope(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if node.kind().as_ref() != "block" {
        return false;
    }

    let block_type = node
        .children()
        .find(|c| c.kind().as_ref() == "identifier")
        .map(|c| c.text().to_string());

    match block_type.as_deref() {
        Some("resource") | Some("data") => {
            let labels: Vec<_> = node
                .children()
                .filter(|c| c.kind().as_ref() == "string_lit")
                .collect();

            if labels.len() >= 2 {
                let name = labels[1]
                    .children()
                    .find(|c| c.kind().as_ref() == "template_literal")
                    .map(|c| c.text().to_string());

                if let Some(name) = name {
                    let fqn = Fqn::from_scope(scope_stack, &name, sep);
                    defs.push(crate::v2::types::CanonicalDefinition {
                        definition_type: "Resource",
                        kind: DefKind::Class,
                        name,
                        fqn,
                        range: crate::v2::types::Range::empty(),
                        is_top_level: false,
                        metadata: None,
                    });
                }
            }
        }
        _ => {}
    }
    false
}

/// Terraform built-in namespaces that aren't user-defined references.
const TF_BUILTIN_NAMESPACES: &[&str] = &["each", "self", "count", "path", "terraform"];

/// Rewrite `variable_expr` references to include the first `get_attr`
/// sibling, producing dot-separated names like `aws_vpc.main` that
/// match resource FQNs. For `var` and `local` prefixes, emits just
/// the attribute name since variables and locals are flat definitions.
fn hcl_rewrite_ref(node: &N<'_>, name: &str) -> Option<String> {
    if node.kind().as_ref() != "variable_expr" {
        return None;
    }
    if TF_BUILTIN_NAMESPACES.contains(&name) {
        return None;
    }

    let parent = node.parent()?;
    if parent.kind().as_ref() != "expression" {
        return None;
    }

    // Find the first get_attr sibling after the variable_expr.
    let attr_name = parent
        .children()
        .filter(|c| c.kind().as_ref() == "get_attr")
        .next()?
        .children()
        .find(|c| c.kind().as_ref() == "identifier")
        .map(|c| c.text().to_string())?;

    match name {
        // var.x → "x" (variables are flat defs named by their label)
        "var" => Some(attr_name),
        // local.x → "locals.x" (locals scope is named "locals")
        "local" => Some(format!("locals.{attr_name}")),
        // module.x → "x" (modules are flat defs named by their label)
        "module" => Some(attr_name),
        // data.type.name → need type + name, skip for now (bare "data" isn't useful)
        "data" => None,
        // resource refs: aws_vpc.main → "aws_vpc.main"
        _ => Some(format!("{name}.{attr_name}")),
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct HclRules;

impl HasRules for HclRules {
    fn rules() -> ResolutionRules {
        let spec = HclDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "hcl",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName,
            ],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::default(),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::config::Language;
    use crate::v2::trace::Tracer;

    fn parse_defs(code: &str) -> Vec<(String, String)> {
        let result = HclDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.tf",
                Language::Hcl,
                &Tracer::new(false),
            )
            .unwrap();
        result
            .definitions
            .iter()
            .map(|d| (d.name.clone(), d.fqn.as_str().to_string()))
            .collect()
    }

    #[test]
    fn resource_block_produces_type_scope_and_named_def() {
        let defs = parse_defs(
            r#"
resource "aws_instance" "web" {
  ami = "ami-123"
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"aws_instance"),
            "expected type scope: {names:?}"
        );
        assert!(
            names.contains(&"web"),
            "expected resource name def: {names:?}"
        );

        let web_fqn = defs
            .iter()
            .find(|(n, _)| n == "web")
            .map(|(_, f)| f.as_str());
        assert_eq!(web_fqn, Some("aws_instance.web"));
    }

    #[test]
    fn variable_block_produces_definition() {
        let defs = parse_defs(
            r#"
variable "instance_type" {
  type    = string
  default = "t3.micro"
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"instance_type"),
            "expected variable def: {names:?}"
        );
    }

    #[test]
    fn output_block_produces_definition() {
        let defs = parse_defs(
            r#"
output "vpc_id" {
  value = "test"
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"vpc_id"), "expected output def: {names:?}");
    }

    #[test]
    fn module_block_produces_definition() {
        let defs = parse_defs(
            r#"
module "security_group" {
  source = "terraform-aws-modules/security-group/aws"
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"security_group"),
            "expected module def: {names:?}"
        );
    }

    #[test]
    fn locals_block_produces_attribute_defs() {
        let defs = parse_defs(
            r#"
locals {
  common_tags   = { Environment = "prod" }
  instance_name = "web-1"
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"common_tags"),
            "expected local def: {names:?}"
        );
        assert!(
            names.contains(&"instance_name"),
            "expected local def: {names:?}"
        );
    }

    fn parse_refs(code: &str) -> Vec<String> {
        let result = HclDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.tf",
                Language::Hcl,
                &Tracer::new(false),
            )
            .unwrap();
        result.refs.iter().map(|r| r.name.to_string()).collect()
    }

    #[test]
    fn resource_ref_rewrites_to_type_dot_name() {
        let refs = parse_refs(
            r#"
resource "aws_instance" "web" {
  subnet_id = aws_subnet.public.id
}
"#,
        );
        assert!(
            refs.contains(&"aws_subnet.public".to_string()),
            "expected rewritten resource ref: {refs:?}"
        );
    }

    #[test]
    fn var_ref_rewrites_to_variable_name() {
        let refs = parse_refs(
            r#"
resource "aws_instance" "web" {
  instance_type = var.instance_type
}
"#,
        );
        assert!(
            refs.contains(&"instance_type".to_string()),
            "expected var ref rewritten to bare name: {refs:?}"
        );
    }

    #[test]
    fn local_ref_rewrites_to_locals_scope() {
        let refs = parse_refs(
            r#"
resource "aws_instance" "web" {
  tags = local.common_tags
}
"#,
        );
        assert!(
            refs.contains(&"locals.common_tags".to_string()),
            "expected local ref rewritten to locals scope: {refs:?}"
        );
    }

    #[test]
    fn data_source_produces_type_scope_and_named_def() {
        let defs = parse_defs(
            r#"
data "aws_ami" "ubuntu" {
  most_recent = true
}
"#,
        );
        let names: Vec<&str> = defs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"aws_ami"), "expected type scope: {names:?}");
        assert!(
            names.contains(&"ubuntu"),
            "expected data name def: {names:?}"
        );

        let ubuntu_fqn = defs
            .iter()
            .find(|(n, _)| n == "ubuntu")
            .map(|(_, f)| f.as_str());
        assert_eq!(ubuntu_fqn, Some("aws_ami.ubuntu"));
    }
}
