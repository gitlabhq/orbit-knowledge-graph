use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::Node;
use treesitter_visit::extract::{Emit, Extract, child_of_kind, default_name, field, text};
use treesitter_visit::predicate::has_child_text;
use treesitter_visit::syntax_tree as rw;
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

fn php_supertype_rules() -> Vec<rw::Rule> {
    use treesitter_visit::Match::AnyKind;
    let tk = AnyKind(PHP_TYPE_NAME_KINDS);
    let mut rules = Vec::new();
    for kind in [
        "class_declaration",
        "interface_declaration",
        "trait_declaration",
        "enum_declaration",
    ] {
        rules.push(rw::insert(
            kind,
            child_of_kind("base_clause")
                .collect(tk)
                .trim_start_char('\\'),
            "__supertype",
        ));
        rules.push(rw::insert(
            kind,
            child_of_kind("class_interface_clause")
                .collect(tk)
                .trim_start_char('\\'),
            "__supertype",
        ));
    }
    for kind in [
        "class_declaration",
        "interface_declaration",
        "trait_declaration",
        "enum_declaration",
    ] {
        rules.push(rw::insert(
            kind,
            field("body")
                .collect_nested(Kind("use_declaration"), AnyKind(PHP_TYPE_NAME_KINDS))
                .trim_start_char('\\'),
            "__supertype",
        ));
    }
    rules
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
            ..LanguageHooks::default()
        }
    }

    fn rewrite(tree: &mut SyntaxTree) {
        let mut rules = php_supertype_rules();
        // Strip leading `\` from fully-qualified names.
        rules.push(rw::set_text("qualified_name", text().strip_prefix("\\")));
        rules.push(rw::custom(rewrite_php_self_static_parent));
        rules.push(rw::custom(rewrite_php_imports));
        tree.apply_rewrites(&rules);
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
        use treesitter_visit::extract::child_of_kind;
        vec![
            import("__php_aliased_use")
                .label("AliasedImport")
                .path_from(child_of_kind("__import_path"))
                .symbol_from(child_of_kind("__import_name"))
                .alias_from(child_of_kind("__import_alias")),
            import("__php_use")
                .label("Import")
                .path_from(child_of_kind("__import_path"))
                .symbol_from(child_of_kind("__import_name")),
        ]
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

/// Resolve `self`/`static`/`parent` keywords to the enclosing class name.
/// Needs an ancestor walk with a `parent` → base-clause special case, so it
/// stays imperative.
fn rewrite_php_self_static_parent(tree: &mut SyntaxTree) {
    let mut rewrites: Vec<(u32, String)> = Vec::new();
    let ref_contexts = [
        "object_creation_expression",
        "class_constant_access_expression",
    ];
    for ctx_kind in ref_contexts {
        for node in tree.nodes_of_kind(ctx_kind).collect::<Vec<_>>() {
            let Some(fc) = tree.children(node).first().copied() else {
                continue;
            };
            let text = tree.text(fc);
            if !matches!(text, "self" | "static" | "parent") {
                continue;
            }
            if let Some(name) = find_enclosing_class_name(tree, node, text) {
                rewrites.push((fc, name));
            }
        }
    }
    for (id, text) in rewrites {
        tree.set_text(id, &text);
    }
}

fn find_enclosing_class_name(tree: &SyntaxTree, start: u32, keyword: &str) -> Option<String> {
    let mut cur = tree.parent(start);
    while let Some(p) = cur {
        match tree.kind(p) {
            "class_declaration" => {
                if keyword == "parent" {
                    let base = tree.children_of_kind(p, "base_clause").next()?;
                    let sup = tree
                        .children(base)
                        .iter()
                        .copied()
                        .find(|&c| matches!(tree.kind(c), "name" | "qualified_name"))?;
                    return Some(tree.text(sup).trim_start_matches('\\').to_string());
                }
                return tree.field_text(p, "name").map(|s| s.to_string());
            }
            "enum_declaration" => {
                return (keyword != "parent")
                    .then(|| tree.field_text(p, "name").map(|s| s.to_string()))
                    .flatten();
            }
            _ => {}
        }
        cur = tree.parent(p);
    }
    None
}

fn rewrite_php_imports(tree: &mut SyntaxTree) {
    struct PhpImport {
        decl: u32,
        path: String,
        name: String,
        alias: Option<String>,
    }

    let mut imports: Vec<PhpImport> = Vec::new();

    for decl in tree
        .nodes_of_kind("namespace_use_declaration")
        .collect::<Vec<_>>()
    {
        let group = tree.children_of_kind(decl, "namespace_use_group").next();
        let prefix = tree
            .children_of_kind(decl, "namespace_name")
            .next()
            .map(|n| tree.text(n).to_string());

        let clauses: Vec<_> = if let Some(g) = group {
            tree.children_of_kind(g, "namespace_use_clause").collect()
        } else {
            tree.children_of_kind(decl, "namespace_use_clause")
                .collect()
        };

        for clause in clauses {
            let target = tree
                .children(clause)
                .iter()
                .copied()
                .find(|&c| matches!(tree.kind(c), "qualified_name" | "name"));
            let Some(t) = target else { continue };

            let mut full = tree.text(t).to_string();
            if let Some(ref pfx) = prefix {
                full = format!("{pfx}\\{full}");
            }
            let full = full.trim_start_matches('\\').to_string();
            let alias = tree
                .field(clause, "alias")
                .map(|a| tree.text(a).to_string());

            let (path, name) = match full.rsplit_once('\\') {
                Some((p, n)) => (p.to_string(), n.to_string()),
                None => (String::new(), full),
            };

            imports.push(PhpImport {
                decl,
                path,
                name,
                alias,
            });
        }
    }

    for (i, imp) in imports.iter().enumerate() {
        let kind = if imp.alias.is_some() {
            "__php_aliased_use"
        } else {
            "__php_use"
        };
        // For the first import from this decl, rename the node. For subsequent ones
        // from grouped imports, insert siblings.
        if i == 0 || imports.get(i.wrapping_sub(1)).map(|p| p.decl) != Some(imp.decl) {
            tree.set_kind(imp.decl, kind);
        }
        tree.insert_child(imp.decl, "__import_path", &imp.path);
        tree.insert_child(imp.decl, "__import_name", &imp.name);
        if let Some(alias) = &imp.alias {
            tree.insert_child(imp.decl, "__import_alias", alias);
        }
    }
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
