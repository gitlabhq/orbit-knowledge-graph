use std::collections::HashMap;

static EMPTY_DEFS: std::sync::LazyLock<HashMap<String, (u16, usize)>> =
    std::sync::LazyLock::new(HashMap::new);
use std::sync::LazyLock;

use smallvec::SmallVec;

use super::expr::{Expr, Scope};
use super::pattern::Slots;
use super::{Links, Loc, Run, Tree, Val};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::sentinel::Sentinel;

const MODULES: &[&str] = &["text"];

static LINKS: LazyLock<Result<Links, LoadError>> = LazyLock::new(|| Links::load(&[]));

/// An expression over one node, bound to `$ROOT`, that `mod.text` evaluates
/// outside any phase walk.
pub struct NodeExpr(Expr);

impl NodeExpr {
    pub fn parse(src: &str, lang: &Lang) -> Result<Self, LoadError> {
        let links = LINKS.as_ref().map_err(Clone::clone)?;
        let modules: Vec<String> = MODULES.iter().map(|m| m.to_string()).collect();
        let scope = Scope {
            slots: &Slots::new(),
            modules: &modules,
            links,
            lang,
            defs: &EMPTY_DEFS,
            data: &HashMap::new(),
            effects: false,
        };
        Expr::parse(src, &scope).map(Self)
    }
}

/// Evaluates node expressions on one tree; calls share one memo.
pub struct Evaluator<'t> {
    run: Run<'t>,
    lang: &'t Lang,
}

impl<'t> Evaluator<'t> {
    pub fn new(tree: &'t Tree, lang: &'t Lang, budget: &'t Sentinel) -> Self {
        let links = LINKS.as_ref().expect("NodeExpr::parse loaded the links");
        Self {
            run: Run::detached(std::slice::from_ref(tree), 0, lang, links, budget, budget),
            lang,
        }
    }

    /// The first value's text, or `None` when the expression yields no text.
    pub fn text(&mut self, expr: &NodeExpr, node: u32) -> Option<u32> {
        let root: SmallVec<[Val; 2]> = SmallVec::from_elem(Val::Node(Loc { fi: 0, node }), 1);
        let vals = expr.0.eval(&mut self.run, std::slice::from_ref(&root));
        let sym = match *vals.first()? {
            Val::Sym(sym) => sym,
            Val::Node(loc) | Val::Type(loc) => self.run.sym_at(loc),
            Val::Kind(kind) => self.lang.syms.intern(self.lang.kind_name(kind)),
            Val::Bool(b) => self.lang.syms.intern(if b { "true" } else { "false" }),
            Val::Opaque => 0,
        };
        Some(sym).filter(|&s| s != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{Mutable, Node};

    fn node(lang: &Lang, kind: &str, text: &str) -> Node {
        Node {
            kind: lang.intern_kind(kind),
            sym: if text.is_empty() {
                0
            } else {
                lang.syms.intern(text)
            },
            named: true,
            ..Default::default()
        }
    }

    #[test]
    fn evaluates_text_expressions_on_a_node() {
        let lang = Lang::new();
        let mut tree = crate::tree::Tree::<Mutable>::new(node(&lang, "__root", ""));
        let root = tree.root;
        let dir = tree.append(root, node(&lang, "__dir", "src"));
        let file = tree.append(dir, node(&lang, "__file", "a.rs"));
        let import = lang.syms.intern("import_type");
        tree.set_tag(
            crate::tree::Tree::to_raw(dir),
            import,
            lang.syms.intern("Wildcard"),
        );
        let (root, dir, file) = [root, dir, file].map(crate::tree::Tree::to_raw).into();
        let tree: Tree = tree.into();
        let budget = Sentinel::disabled();
        let mut eval = Evaluator::new(&tree, &lang, &budget);
        let text = |eval: &mut Evaluator, src: &str, at: u32| {
            let expr = NodeExpr::parse(src, &lang).unwrap_or_else(|e| panic!("{e}"));
            eval.text(&expr, at)
                .map(|s| lang.syms.resolve(s).to_string())
        };
        let path = r#"$ROOT | tree_path("/") | default(".")"#;
        assert_eq!(text(&mut eval, path, file).as_deref(), Some("src/a.rs"));
        assert_eq!(text(&mut eval, path, root).as_deref(), Some("."));
        assert_eq!(text(&mut eval, "$ROOT", dir).as_deref(), Some("src"));
        let kind = r#"$ROOT | tag("import_type") | default($ROOT | ancestor_tag("import_type")) | default("NamedImport")"#;
        assert_eq!(text(&mut eval, kind, file).as_deref(), Some("Wildcard"));
        assert_eq!(text(&mut eval, kind, dir).as_deref(), Some("Wildcard"));
        assert_eq!(text(&mut eval, kind, root).as_deref(), Some("NamedImport"));
        assert_eq!(text(&mut eval, r#"$ROOT | tag("missing")"#, file), None);
        let err = NodeExpr::parse("$ROOT | nope", &lang).err().unwrap();
        assert!(err.0.contains("nope"), "{err}");
    }
}
