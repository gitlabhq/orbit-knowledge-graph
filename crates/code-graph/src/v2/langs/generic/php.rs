use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::Node;
use treesitter_visit::extract::{Emit, Extract, child_of_kind, default_name, field, text};
use treesitter_visit::predicate::has_child_text;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolutionRules, ResolveSettings};

type N<'a> = Node<'a, SyntaxTree>;

// PHP scalar/pseudo types, never user-defined classes.
const PHP_PRIMITIVE_TYPES: &[&str] = &[
    "int", "float", "string", "bool", "void", "array", "object", "mixed", "null", "false", "true",
    "callable", "iterable", "never", "self", "static", "parent",
];

const PHP_TYPE_NAME_KINDS: &[&str] = &["name", "qualified_name"];

#[derive(Default)]
pub struct PhpDsl;

fn rewrite_php(tree: &mut SyntaxTree) {
    let mut supertypes: Vec<(u32, String)> = Vec::new();

    let class_kinds = [
        "class_declaration",
        "interface_declaration",
        "trait_declaration",
        "enum_declaration",
    ];
    for kind in class_kinds {
        for cls in tree.nodes_of_kind(kind).collect::<Vec<_>>() {
            for &child in tree.children(cls) {
                let k = tree.kind(child);
                if k == "base_clause" || k == "class_interface_clause" {
                    for &t in tree.children(child) {
                        if PHP_TYPE_NAME_KINDS.contains(&tree.kind(t)) {
                            supertypes
                                .push((cls, tree.text(t).trim_start_matches('\\').to_string()));
                        }
                    }
                }
            }
            if let Some(body) = tree.field(cls, "body") {
                for ud in tree
                    .children_of_kind(body, "use_declaration")
                    .collect::<Vec<_>>()
                {
                    for &t in tree.children(ud) {
                        if PHP_TYPE_NAME_KINDS.contains(&tree.kind(t)) {
                            supertypes
                                .push((cls, tree.text(t).trim_start_matches('\\').to_string()));
                        }
                    }
                }
            }
        }
    }

    for (cls, text) in supertypes {
        tree.insert_child(cls, "__supertype", &text);
    }
}

impl DslLanguage for PhpDsl {
    fn name() -> &'static str {
        "php"
    }

    fn language() -> Language {
        Language::Php
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            adopt_sibling_refs: &["attribute_list"],
            on_scope: Some(php_on_scope),
            on_import: Some(php_extract_use),
            ref_name_rewrite: Some(php_rewrite_ref_name),
            ..LanguageHooks::default()
        }
    }

    fn rewrite(tree: &mut SyntaxTree) {
        rewrite_php(tree);
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || {
            metadata().super_types(|n: &Node<'_, SyntaxTree>| {
                n.children()
                    .filter(|c| c.kind().as_ref() == "__supertype")
                    .map(|c| c.text().to_string())
                    .collect()
            })
        };

        vec![
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("interface_declaration", "Interface")
                .def_kind(DefKind::Interface)
                .metadata(class_meta()),
            scope("trait_declaration", "Trait")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("enum_declaration", "Enum")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(metadata().return_type(field("return_type").descendant("name"))),
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .metadata(metadata().return_type(field("return_type").descendant("name"))),
            // Index the bare member name so `$this->id` resolves.
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(
                    child_of_kind("property_element")
                        .field("name")
                        .child_of_kind("name"),
                )
                .metadata(metadata().type_annotation(field("type").descendant("name"))),
            // Constructor property promotion (PHP 8.0); php_on_scope re-anchors the FQN to the class.
            scope("property_promotion_parameter", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(field("name").child_of_kind("name"))
                .metadata(metadata().type_annotation(field("type").descendant("name"))),
            scope("const_declaration", "Constant")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(child_of_kind("const_element").child_of_kind("name")),
            scope("enum_case", "EnumCase")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // $obj->method()
            reference("member_call_expression")
                .name_from(field("name"))
                .receiver("object"),
            // $obj?->method() (PHP 8.0 nullsafe)
            reference("nullsafe_member_call_expression")
                .name_from(field("name"))
                .receiver("object"),
            // Foo::method(), self::method(), parent::method(), static::method()
            reference("scoped_call_expression")
                .name_from(field("name"))
                .receiver_via(field("scope")),
            // Foo::class is the class-name fetch, not a member: reference the class itself
            // (first named child), not the `class` keyword. ref_name_rewrite maps self/static/parent.
            reference("class_constant_access_expression")
                .when(has_child_text("class"))
                .name_from(Extract::one(Child, Named)),
            // Foo::CONST / self::VERSION / EnumType::Case: scope is the first named child, name the last.
            reference("class_constant_access_expression")
                .name_from(Extract::terminal(Emit::Text).nth(Child, Named, -1))
                .receiver_via(Extract::one(Child, Named)),
            // foo()
            reference("function_call_expression").name_from(field("function")),
            // new Foo()
            reference("object_creation_expression")
                .name_from(Extract::one(Child, AnyKind(&["name", "qualified_name"]))),
            // Attribute application: #[Route], #[ORM\Entity] (PHP 8.0)
            reference("attribute")
                .name_from(Extract::one(Child, AnyKind(&["name", "qualified_name"]))),
            // Bare type references: param/return/property types, instanceof, catch.
            reference("named_type").name_from(text()),
            // $x instanceof Foo: the right operand is the class being tested.
            reference("binary_expression")
                .when(has_child_text("instanceof"))
                .name_from(field("right")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        // Handled entirely by the `php_extract_use` hook.
        vec![]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            // $this/$repo are variable_name; self/parent/static are relative_scope; qualified_name is one class FQN.
            ident_kinds: &["name", "variable_name", "relative_scope", "qualified_name"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![
                FieldAccessEntry {
                    kind: "member_access_expression",
                    object: field("object"),
                    member: field("name"),
                },
                // $a?->b chains (PHP 8.0 nullsafe property access)
                FieldAccessEntry {
                    kind: "nullsafe_member_access_expression",
                    object: field("object"),
                    member: field("name"),
                },
            ],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        // `namespace App\Models;` becomes the FQN prefix (default_name reads the namespace_name).
        Some(("namespace_definition", default_name()))
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("assignment_expression", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        // $x = new \Vendor\Foo(): resolve_type_name strips the leading `\`.
                        field("right")
                            .where_(Kind("object_creation_expression"))
                            .child_of_kind("qualified_name"),
                        // $x = new Foo(): resolves via import_map or module_prefix.
                        field("right")
                            .where_(Kind("object_creation_expression"))
                            .child_of_kind("name"),
                    ],
                    PHP_PRIMITIVE_TYPES,
                )
                .instance_attrs(&["$this->"]),
            binding("simple_parameter", BindingKind::Parameter)
                .name_from(&["name"])
                .typed(
                    vec![
                        field("type").child_of_kind("qualified_name"),
                        field("type").descendant("name"),
                    ],
                    PHP_PRIMITIVE_TYPES,
                )
                .no_value(),
            binding("property_promotion_parameter", BindingKind::Parameter)
                .name_from(&["name"])
                .typed(
                    vec![
                        field("type").child_of_kind("qualified_name"),
                        field("type").descendant("name"),
                    ],
                    PHP_PRIMITIVE_TYPES,
                )
                .no_value(),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["compound_statement", "else_if_clause", "else_clause"])
                .condition("condition")
                .catch_all("else_clause"),
            branch("try_statement").branches(&[
                "compound_statement",
                "catch_clause",
                "finally_clause",
            ]),
            branch("switch_block").branches(&["case_statement", "default_statement"]),
            branch("match_block")
                .branches(&["match_conditional_expression", "match_default_expression"]),
            branch("conditional_expression")
                .branches(&["consequence", "alternative"])
                .catch_all("alternative"),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement"),
            loop_rule("while_statement"),
            loop_rule("foreach_statement"),
            loop_rule("do_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["$this", "self", "static"],
            super_name: Some("parent"),
            ..Default::default()
        }
    }
}

/// On-scope hook: re-anchor a promoted property's FQN to the enclosing class,
/// and rewrite a method's self/static/parent return type to a concrete class FQN.
#[expect(
    clippy::ptr_arg,
    reason = "signature must match the on_scope hook fn pointer"
)]
fn php_on_scope(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    let nk = node.kind();
    let nk_ref = nk.as_ref();

    if nk_ref == "property_promotion_parameter" {
        // The lexical scope adds the trailing `__construct`; drop it so the
        // promoted property is class-scoped.
        if let Some(last) = defs.last_mut()
            && last.kind == DefKind::Property
        {
            let class_scope = match scope_stack.last().map(|s| s.as_ref()) {
                Some("__construct") => &scope_stack[..scope_stack.len() - 1],
                _ => scope_stack,
            };
            let name = last.name.clone();
            last.fqn = crate::v2::types::Fqn::from_scope(class_scope, &name, sep);
        }
        return false;
    }

    if nk_ref == "method_declaration" || nk_ref == "function_definition" {
        let is_parent = match defs
            .last()
            .and_then(|d| d.metadata.as_ref())
            .and_then(|m| m.return_type.as_deref())
        {
            Some("self") | Some("static") => false,
            Some("parent") => true,
            _ => return false,
        };
        if scope_stack.len() < 2 {
            return false;
        }
        let class_fqn: String = scope_stack[..scope_stack.len() - 1]
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>()
            .join(sep);
        let new_rt = if is_parent {
            // `parent` returns the declaring class's first super (already `\`-stripped).
            let parent = defs
                .iter()
                .rev()
                .find(|d| d.kind.is_type_container() && d.fqn.as_str() == class_fqn)
                .and_then(|d| d.metadata.as_ref())
                .and_then(|m| m.super_types.first().cloned());
            match parent {
                Some(p) => p,
                None => return false,
            }
        } else {
            class_fqn
        };
        if let Some(meta) = defs.last_mut().and_then(|d| d.metadata.as_mut()) {
            meta.return_type = Some(new_rt);
        }
    }
    false
}

/// Rewrite a reference name: strip the leading `\` of an absolute name, and resolve
/// `new self/static/parent` and `self/static/parent::class` to the enclosing concrete type.
fn php_rewrite_ref_name(node: &N<'_>, name: &str) -> Option<String> {
    if let Some(stripped) = name.strip_prefix('\\') {
        return Some(stripped.to_string());
    }
    if !matches!(name, "self" | "static" | "parent") {
        return None;
    }
    if !matches!(
        node.kind().as_ref(),
        "object_creation_expression" | "class_constant_access_expression"
    ) {
        return None;
    }
    // Walk up to the enclosing class/enum. Traits/interfaces are skipped: `new self`
    // there is ambiguous (the resolver has no concrete type to anchor to).
    let mut cur = node.parent();
    while let Some(p) = cur {
        match p.kind().as_ref() {
            "class_declaration" => {
                if name == "parent" {
                    let base = p.children().find(|c| c.kind().as_ref() == "base_clause")?;
                    let sup = base
                        .children()
                        .find(|c| matches!(c.kind().as_ref(), "name" | "qualified_name"))?;
                    return Some(sup.text().trim_start_matches('\\').to_string());
                }
                return p.field("name").map(|n| n.text().to_string());
            }
            // Enums cannot extend; `new parent` inside one is unresolvable by design.
            "enum_declaration" => {
                return (name != "parent")
                    .then(|| p.field("name").map(|n| n.text().to_string()))
                    .flatten();
            }
            _ => {}
        }
        cur = p.parent();
    }
    None
}

/// Extract `use` imports: single, aliased, grouped, and `use function`/`use const`.
fn php_extract_use(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "namespace_use_declaration" {
        return false;
    }

    if let Some(group) = node
        .children()
        .find(|c| c.kind().as_ref() == "namespace_use_group")
    {
        let prefix = node
            .children()
            .find(|c| c.kind().as_ref() == "namespace_name")
            .map(|n| n.text().to_string());
        for clause in group
            .children()
            .filter(|c| c.kind().as_ref() == "namespace_use_clause")
        {
            push_use_clause(&clause, prefix.as_deref(), imports);
        }
        return true;
    }

    for clause in node
        .children()
        .filter(|c| c.kind().as_ref() == "namespace_use_clause")
    {
        push_use_clause(&clause, None, imports);
    }
    true
}

fn push_use_clause(clause: &N<'_>, group_prefix: Option<&str>, imports: &mut Vec<CanonicalImport>) {
    let Some(target) = clause
        .children()
        .find(|c| matches!(c.kind().as_ref(), "qualified_name" | "name"))
    else {
        return;
    };

    let mut full = target.text().to_string();
    if let Some(prefix) = group_prefix {
        full = format!("{prefix}\\{full}");
    }
    let full = full.trim_start_matches('\\').to_string();
    let alias = clause.field("alias").map(|a| a.text().to_string());

    // Split into namespace prefix + symbol so the engine rejoins the full FQN.
    let (path, name) = match full.rsplit_once('\\') {
        Some((p, n)) => (p.to_string(), n.to_string()),
        None => (String::new(), full),
    };

    imports.push(CanonicalImport {
        import_type: if alias.is_some() {
            "AliasedImport"
        } else {
            "Import"
        },
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name: Some(name),
        alias,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
}

// ── Resolution rules ────────────────────────────────────────────

pub struct PhpRules;

impl HasRules for PhpRules {
    fn rules() -> ResolutionRules {
        let spec = PhpDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "php",
            scopes,
            spec,
            // No ImplicitMember/chain_fallback: in PHP they only mis-routed bare names (and were O(n^2)).
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
                // Last resort for global-namespace classes used without `use` (capped).
                ImportStrategy::GlobalName,
            ],
            ReceiverMode::Keyword,
            "\\",
            &["$this", "self", "static"],
            Some("parent"),
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            ..Default::default()
        })
        .with_settings(ResolveSettings {
            chain_fallback: false,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        PhpDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.php",
                crate::v2::config::Language::Php,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse("Test.php", format!("Parse error: {e:?}"))
            })
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            "<?php\nclass Calculator {\n    public function add(int $a, int $b): int { return $a + $b; }\n}\n",
        )
        .unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"Calculator"), "should have class");
        assert!(names.contains(&"add"), "should have method");
    }

    #[test]
    fn namespace_scoping() {
        let result = parse(
            "<?php\nnamespace App\\Models;\nclass Service {\n    public function run(): void {}\n}\n",
        )
        .unwrap();
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "App\\Models\\Service");
    }

    #[test]
    fn interface_trait_enum() {
        let result = parse(
            "<?php\ninterface Repo { public function find(int $id); }\ntrait T { public function touch(): void {} }\nenum Status: string { case Active = 'active'; }\n",
        )
        .unwrap();
        let repo = result
            .definitions
            .iter()
            .find(|d| d.name == "Repo")
            .unwrap();
        assert_eq!(repo.kind, DefKind::Interface);
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"T"), "should have trait");
        assert!(names.contains(&"Status"), "should have enum");
        assert!(names.contains(&"Active"), "should have enum case");
    }

    #[test]
    fn super_types_extracted() {
        let result =
            parse("<?php\nclass Dog extends Animal implements Runnable, Loud {\n}\n").unwrap();
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("Dog should have metadata");
        assert!(meta.super_types.iter().any(|s| s.ends_with("Animal")));
        assert!(meta.super_types.iter().any(|s| s.ends_with("Runnable")));
        assert!(meta.super_types.iter().any(|s| s.ends_with("Loud")));
    }

    #[test]
    fn trait_use_is_super_type() {
        let result =
            parse("<?php\ntrait Timestamps { public function touch(): void {} }\nclass User {\n    use Timestamps;\n}\n").unwrap();
        let user = result
            .definitions
            .iter()
            .find(|d| d.name == "User")
            .unwrap();
        let meta = user.metadata.as_ref().expect("User should have metadata");
        assert!(
            meta.super_types.iter().any(|s| s.ends_with("Timestamps")),
            "trait use should be recorded as a super type, got {:?}",
            meta.super_types
        );
    }

    #[test]
    fn imports_extracted() {
        let result = parse(
            "<?php\nnamespace App;\nuse App\\Support\\Logger;\nuse App\\Support\\Cache as C;\n\nclass Test {}\n",
        )
        .unwrap();
        assert!(
            result.imports.len() >= 2,
            "expected >= 2 imports, got {}",
            result.imports.len()
        );
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.iter().any(|p| p.contains("Support")));
        let aliased = result
            .imports
            .iter()
            .find(|i| i.alias.as_deref() == Some("C"))
            .expect("aliased import should be captured");
        assert_eq!(aliased.name.as_deref(), Some("Cache"));
    }

    #[test]
    fn properties_and_constants() {
        let result = parse(
            "<?php\nclass Model {\n    const TABLE = 'models';\n    public int $id = 0;\n    protected ?string $name = null;\n}\n",
        )
        .unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"TABLE"), "should have constant");
        assert!(
            names.contains(&"id"),
            "property should be bare 'id', not '$id'"
        );
        assert!(names.contains(&"name"), "should have property name");
    }

    #[test]
    fn function_definition() {
        let result = parse("<?php\nfunction helper(int $x): int { return $x; }\n").unwrap();
        let helper = result
            .definitions
            .iter()
            .find(|d| d.name == "helper")
            .unwrap();
        assert_eq!(helper.kind, DefKind::Function);
    }
}
