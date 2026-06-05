use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{Emit, Extract, child_of_kind, default_name, field, text};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{CodeGraph, HasRules, ResolutionRules, ResolveSettings};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

// PHP scalar/pseudo types that are never user-defined classes, so they
// must not be recorded as variable types or super types.
const PHP_PRIMITIVE_TYPES: &[&str] = &[
    "int", "float", "string", "bool", "void", "array", "object", "mixed", "null", "false", "true",
    "callable", "iterable", "never", "self", "static", "parent",
];

#[derive(Default)]
pub struct PhpDsl;

/// Collect a class/interface/enum's parents from `extends` (`base_clause`),
/// `implements` (`class_interface_clause`), and body-level `use TraitName;`
/// declarations. Trait uses are treated as supertypes so the trait's methods
/// resolve as members of the using class.
fn php_super_types(node: &N<'_>) -> Vec<String> {
    let is_type_name = |k: &str| k == "name" || k == "qualified_name";
    let mut out = Vec::new();
    for child in node.children() {
        match child.kind().as_ref() {
            "base_clause" | "class_interface_clause" => {
                for t in child.children() {
                    if is_type_name(t.kind().as_ref()) {
                        // Tree-sitter keeps the leading `\` of an absolute name,
                        // but graph FQNs are stored without it.
                        out.push(t.text().trim_start_matches('\\').to_string());
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(body) = node.field("body") {
        for u in body.children().filter(|c| c.kind() == "use_declaration") {
            for t in u.children() {
                if is_type_name(t.kind().as_ref()) {
                    out.push(t.text().to_string());
                }
            }
        }
    }
    out
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
            on_import: Some(php_extract_use),
            on_import_kinds: &["namespace_use_declaration"],
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(php_super_types);

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
            // `$id` in the source; index the bare member name so `$this->id`
            // member access resolves to this property.
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(
                    child_of_kind("property_element")
                        .field("name")
                        .child_of_kind("name"),
                )
                .metadata(metadata().type_annotation(field("type").descendant("name"))),
            // Constructor property promotion (PHP 8.0): a promoted param is
            // declared in the constructor's parameter list but is a member of
            // the class, so hoist its FQN to the enclosing type scope.
            scope("property_promotion_parameter", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .hoist_to_type_scope()
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
            // Foo::CONST, self::VERSION, EnumType::Case used as a value.
            // The scope and const name are positional `name` children (no
            // tree-sitter fields): first named child is the scope, last is
            // the constant/case name.
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
            // Bare type references in parameter/return/property types,
            // `instanceof`, and catch clauses.
            reference("named_type").name_from(text()),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        // Handled entirely by the `php_extract_use` hook.
        vec![]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            // `$this`/`$repo` are `variable_name`; `self`/`parent`/`static`
            // are `relative_scope`. Both reach SSA via their text, where
            // self_names/super_name bind them to the enclosing type.
            // `qualified_name` (`App\Models\User`) is a single class FQN, not
            // an `Outer::Inner` nesting, so it is one identifier, not a
            // qualified-type chain split.
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
            // PHP `new Foo()` / `new \App\Query\Builder()` — the class is the
            // first positional `name` / `qualified_name` child (no field).
            // Mirrors the `object_creation_expression` reference rule.
            positional_constructor: vec![PositionalConstructor {
                kind: "object_creation_expression",
                type_extract: Extract::one(Child, AnyKind(&["name", "qualified_name"])),
            }],
            // `(new Foo())->bar()` / `($x)->bar()` — walk through the parens.
            transparent_kinds: &["parenthesized_expression"],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        // `namespace App\Models;` becomes the FQN prefix for every
        // definition in the file. default_name() reads the `name` field
        // (the `namespace_name`), e.g. "App\Models".
        Some(("namespace_definition", default_name()))
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("assignment_expression", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        // $x = new \Vendor\Foo\Bar(...): qualified class name.
                        // resolve_type_name strips the leading `\` so the FQN
                        // matches the indexed class.
                        field("right")
                            .where_(Kind("object_creation_expression"))
                            .child_of_kind("qualified_name"),
                        // $x = new Foo(...): bare class name resolves via
                        // import_map (use Foo;) or module_prefix (same namespace).
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
            // `function foo(): self|static` exposes the declaring class so
            // chains like `$this->foo()->bar()` continue; `parent` rewrites
            // to the first declared super.
            rewrite_self_in_return_type: true,
            ..Default::default()
        }
    }
}

/// Extract `use` imports. Handles single (`use App\Models\User;`), aliased
/// (`use App\Support\Logger as Log;`), grouped (`use App\Sub\{Foo, Bar};`),
/// and `use function`/`use const` declarations. The leading `\` of a
/// fully-qualified name is irrelevant to the imported symbol name.
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

    // Split namespace prefix from the imported symbol so the engine
    // rejoins them into the full FQN target (path + sep + name).
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

/// Resolve a bare class name to its FQN. Used for chain bases that are
/// type names with no SSA value, e.g. an unimported `Logger::make()` or a
/// fully-qualified `\Vendor\Bare::call()` (the leading `\` is already
/// stripped by `resolve_type_name`).
fn php_resolve_ident_type(graph: &CodeGraph, name: &str) -> Option<String> {
    let lookup = name.trim_start_matches('\\');
    for &node in &graph.resolve_scope_nodes(lookup) {
        if let Some(did) = graph.graph[node].def_id() {
            let gdef = &graph.defs[did.0 as usize];
            if gdef.kind.is_type_container() {
                return Some(graph.str(gdef.fqn).to_string());
            }
        }
    }
    None
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
            // No ImplicitMember: PHP always qualifies member access ($this->,
            // self::, Foo::), so a bare unqualified name is a function/import,
            // never an enclosing-class member. ImplicitMember + chain_fallback
            // mis-routed bare builtins (substr, tap) and fluent-chain tails to
            // same-named class members, which was both wrong and O(n^2).
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
                // Last resort for global-namespace classes referenced without a
                // `use` (e.g. `new HTMLPurifier_Length()`); capped at 5 results.
                ImportStrategy::GlobalName,
            ],
            ReceiverMode::Keyword,
            "\\",
            &["$this", "self", "static"],
            Some("parent"),
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            resolve_ident_type: Some(php_resolve_ident_type),
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
