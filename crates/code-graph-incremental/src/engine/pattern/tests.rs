use indextree::NodeId;
use smallvec::SmallVec;

use super::*;
use crate::error::LoadError;
use crate::frontend::treesitter::parse;
use crate::intern::Lang;
use crate::language::SupportLang;
use crate::tree::{Node, Storage, Tree};

fn no_data(_: &str) -> Option<Vec<String>> {
    None
}

fn pat(lang: &Lang, src: &str) -> (Pat, Slots) {
    let mut slots = Slots::new();
    let data = |name: &str| match name {
        "loops" => Some(vec!["for_statement".into(), "while_statement".into()]),
        "names" => Some(vec!["a".into(), "b".into()]),
        "idents" => Some(vec!["identifier".into()]),
        _ => None,
    };
    let p = parse_pattern(src, &mut slots, lang, &data).unwrap_or_else(|e| panic!("{e}"));
    (p, slots)
}

fn tpl(lang: &Lang, slots: &Slots, src: &str) -> Tpl<String> {
    parse_template(src, slots, lang, &mut |s| Ok(s.to_string())).unwrap_or_else(|e| panic!("{e}"))
}

fn caps<I>(slots: &Slots) -> Vec<Cap<I>> {
    (0..slots.len()).map(|_| SmallVec::new()).collect()
}

struct T {
    lang: Lang,
    tree: Tree,
}

impl T {
    fn new(kind: &str) -> Self {
        let lang = Lang::new();
        let tree = Tree::new(Node {
            kind: lang.intern_kind(kind),
            named: true,
            ..Default::default()
        });
        Self { lang, tree }
    }

    fn root(&self) -> NodeId {
        self.tree.root
    }

    fn add(&mut self, parent: NodeId, kind: &str, text: &str) -> NodeId {
        self.add_field(parent, "", kind, text)
    }

    fn add_field(&mut self, parent: NodeId, field: &str, kind: &str, text: &str) -> NodeId {
        let node = Node {
            kind: self.lang.intern_kind(kind.trim_matches('"')),
            field: if field.is_empty() {
                0
            } else {
                self.lang.intern_field(field)
            },
            sym: if text.is_empty() {
                0
            } else {
                self.lang.syms.intern(text)
            },
            named: !kind.starts_with('"'),
            ..Default::default()
        };
        self.tree.append(parent, node)
    }

    fn matches(&self, src: &str, at: NodeId) -> Option<Vec<Cap<NodeId>>> {
        let (p, slots) = pat(&self.lang, src);
        let mut c = caps(&slots);
        matches(&self.tree, &self.lang, at, None, &p, &mut c, &[]).then_some(c)
    }

    fn show(&self, id: NodeId) -> String {
        let n = self.tree.node(id);
        let mut out = format!("({}", self.lang.kind_name(n.kind));
        if n.sym != 0 {
            out += &format!(" {:?}", self.lang.syms.resolve(n.sym));
        }
        for kid in id.children(&self.tree.storage.0) {
            out += " ";
            out += &self.show(kid);
        }
        out + ")"
    }

    fn build(
        &mut self,
        tpl: &Tpl<String>,
        caps: &[Cap<NodeId>],
        mv: Option<NodeId>,
    ) -> Vec<String> {
        let holder = self.tree.storage.0.new_node(Node::default());
        let lang = &self.lang;
        let built = build(
            &mut self.tree,
            lang,
            holder,
            tpl,
            caps,
            mv,
            &mut |index, expr| {
                assert!(std::ptr::eq(tpl.texts()[index], expr));
                lang.syms.intern(&format!("<{expr}>"))
            },
        );
        built.iter().map(|&id| self.show(id)).collect()
    }
}

#[test]
fn node_kind_text_and_fields() {
    let mut t = T::new("call");
    let r = t.root();
    let m = t.add_field(r, "method", "identifier", "send");
    t.add_field(r, "arguments", "argument_list", "");
    assert!(
        t.matches(r#"(call method: (identifier "send"))"#, r)
            .is_some()
    );
    assert!(
        t.matches(r#"(call method: (identifier "^se"))"#, r)
            .is_some()
    );
    assert!(
        t.matches(r#"(call method: (identifier "/^(send|public_send)$/"))"#, r)
            .is_some()
    );
    assert!(
        t.matches(r#"(call method: (identifier "other"))"#, r)
            .is_none()
    );
    assert!(t.matches(r#"(call arguments: (identifier))"#, r).is_none());
    assert!(t.matches("(call receiver: $R)", r).is_none());
    let c = t.matches("(call receiver:? $R method: $M)", r).unwrap();
    assert!(c[1].is_empty());
    assert_eq!(c[2].as_slice(), [m]);
    assert_eq!(c[0].as_slice(), [r]);
    assert!(t.matches("(_ (_))", r).is_some());
    assert!(t.matches(r#"("call")"#, r).is_some());
}

#[test]
fn data_lists_as_kinds_text_and_filters() {
    let mut t = T::new("while_statement");
    let r = t.root();
    let a = t.add(r, "identifier", "a");
    t.add(r, "identifier", "c");
    assert!(t.matches("(%loops (identifier %names))", r).is_some());
    let (p, slots) = pat(&t.lang, "(%loops $$$X:!%loops)");
    let mut c = caps(&slots);
    assert!(matches(&t.tree, &t.lang, r, None, &p, &mut c, &[]));
    assert_eq!(c[1].len(), 2);
    assert!(slots.filter(1).negate);
    assert!(anchor(&p).is_none());
    let (_, kinds) = anchor_kinds(&p).unwrap();
    assert!(kinds.admits(t.lang.intern_kind("for_statement")));
    let c = t.matches("(%loops $X:%idents)", r).unwrap();
    assert_eq!(c[1].as_slice(), [a]);
    let mut slots = Slots::new();
    let err = parse_pattern("(%nope)", &mut slots, &t.lang, &no_data)
        .err()
        .unwrap();
    assert!(err.0.contains("unknown data %nope"), "{err}");
}

#[test]
fn variadics_stop_at_the_next_positional_element_and_filter() {
    let mut t = T::new("block");
    let r = t.root();
    let x = t.add(r, "expr", "x");
    let semi = t.add(r, "\";\"", "");
    let y = t.add(r, "expr", "y");
    let ret = t.add(r, "return", "");
    let c = t.matches("(block $$$B (return))", r).unwrap();
    assert_eq!(c[1].as_slice(), [x, semi, y]);
    let c = t
        .matches("(block $$$B:expr !(missing) $R:return)", r)
        .unwrap();
    assert_eq!(c[1].as_slice(), [x, y]);
    assert_eq!(c[2].as_slice(), [ret]);
    let c = t.matches("(block $$$B:!expr)", r).unwrap();
    assert_eq!(c[1].as_slice(), [semi, ret]);
    let c = t.matches("(block $$$B:_*_named)", r).unwrap();
    assert_eq!(c[1].as_slice(), [x, y, ret]);
    let c = t.matches(r#"(block $$$A (expr "y") $$$B)"#, r).unwrap();
    assert_eq!(c[1].as_slice(), [x, semi]);
    assert_eq!(c[2].as_slice(), [ret]);
    let (_, slots) = pat(&t.lang, "(block $$$B:expr|return)");
    assert_eq!(slots.filter(1).kinds.len(), 2);
}

#[test]
fn negation_descendant_guard_and_optional() {
    let mut t = T::new("binding");
    let r = t.root();
    let rhs = t.add_field(r, "value", "rhs", "");
    let call = t.add(rhs, "call", "");
    t.add(call, "identifier", "f");
    assert!(t.matches("(binding !(declaration))", r).is_some());
    assert!(t.matches("(binding !(rhs))", r).is_none());
    assert!(t.matches("(binding receiver: !$R)", r).is_some());
    assert!(t.matches("(binding value: !$R)", r).is_none());
    assert!(t.matches(r#"(binding **/(identifier "f"))"#, r).is_some());
    assert!(t.matches(r#"(binding **/(identifier "g"))"#, r).is_none());
    let c = t.matches("(binding $V:(rhs $C:call))", r).unwrap();
    assert_eq!(c[1].as_slice(), [rhs]);
    assert_eq!(c[2].as_slice(), [call]);
    let c = t.matches("(binding $D?:declaration $V:rhs)", r).unwrap();
    assert!(c[1].is_empty());
    assert_eq!(c[2].as_slice(), [rhs]);
    let c = t
        .matches("(binding value:? (rhs $C:declaration) $V)", r)
        .unwrap();
    assert!(c[1].is_empty(), "a missed optional clears its captures");
    assert_eq!(c[2].as_slice(), [rhs]);
}

#[test]
fn anchored_patterns_pin_root() {
    let mut t = T::new("__def");
    let r = t.root();
    t.add(r, "__defname", "f");
    let d1 = t.add(r, "__decorator", "a");
    let d2 = t.add(r, "__decorator", "b");
    let (p, slots) = pat(&t.lang, "$D:(__def $ROOT:__decorator)");
    let kind = t.lang.intern_kind("__decorator");
    assert_eq!(anchor(&p), Some((1, kind)));
    let mut c = caps(&slots);
    assert!(matches(&t.tree, &t.lang, r, Some(d2), &p, &mut c, &[]));
    assert_eq!(c[0].as_slice(), [d2]);
    assert_eq!(c[1].as_slice(), [r]);
    assert!(matches(&t.tree, &t.lang, r, Some(d1), &p, &mut c, &[]));
    assert_eq!(c[0].as_slice(), [d1]);
    assert!(!matches(&t.tree, &t.lang, d1, Some(d1), &p, &mut c, &[]));
    let (p, _) = pat(&t.lang, "(__destructure $ROOT:(__binding))");
    assert_eq!(anchor(&p), Some((1, t.lang.intern_kind("__binding"))));
    let (p, _) = pat(&t.lang, "(__call $C)");
    assert_eq!(anchor(&p), Some((0, t.lang.intern_kind("__call"))));
}

#[test]
fn matches_a_parsed_python_tree() {
    let lang = Lang::new();
    let src = "class A(B, C):\n    x = 1\n";
    let tree = parse(src, SupportLang::Python, &lang, "a.py").unwrap();
    let class = tree.root().children().next().unwrap();
    let (p, slots) = pat(
        &lang,
        "(class_definition name: $N superclasses: (argument_list $$$S:identifier) body: $B)",
    );
    let mut c = caps(&slots);
    let id = tree.storage.id(class.index());
    assert!(matches(&tree, &lang, id, None, &p, &mut c, &[]));
    assert_eq!(tree.text(c[1][0], &lang), "A");
    let names: Vec<_> = c[2].iter().map(|&s| tree.text(s, &lang)).collect();
    assert_eq!(names, ["B", "C"]);
    let (p, slots) = pat(
        &lang,
        r#"(class_definition name: (identifier "A") **/(integer "1"))"#,
    );
    assert!(matches(&tree, &lang, id, None, &p, &mut caps(&slots), &[]));
}

#[test]
fn templates_build_text_fields_and_labels() {
    let mut t = T::new("call");
    let r = t.root();
    let m = t.add_field(r, "method", "identifier", "send");
    let a = t.add(r, "arg", "1");
    let (p, slots) = pat(&t.lang, "(call method: $M $A $E?:missing)");
    let mut c = caps(&slots);
    assert!(matches(&t.tree, &t.lang, r, None, &p, &mut c, &[]));
    let tp = tpl(
        &t.lang,
        &slots,
        r#"(__call (__callee @$M|strip=::|replace("::", "/") $M) "x" (__args $A))"#,
    );
    assert!(
        matches!(&tp, Tpl::Node { kids, .. } if matches!(&kids[0], Tpl::Node { labelled: true, .. }))
    );
    let built = t.build(&tp, &c, None);
    assert_eq!(
        built,
        [
            r#"(__call "x" (__callee "<@$M|strip=::|replace(\"::\", \"/\")>" (identifier "send")) (__args (arg "1")))"#
        ]
    );
    assert_eq!(m.parent(&t.tree.storage.0), Some(r), "no move without mv");
    assert_eq!(a.parent(&t.tree.storage.0), Some(r));
    let tp = tpl(
        &t.lang,
        &slots,
        r#"(x @$M | f(" ) ") (y? @$ROOT) (z? $E) (w? @$E $A))"#,
    );
    let built = t.build(&tp, &c, None);
    assert_eq!(built, [r#"(x "<@$M | f(\" ) \")>" (y "<@$ROOT>"))"#]);
}

#[test]
fn templates_move_once_used_captures_and_copy_the_rest() {
    let setup = |src: &str| {
        let mut t = T::new("file");
        let target = t.add(t.root(), "def", "");
        let name = t.add(target, "identifier", "f");
        let body = t.add(target, "block", "");
        t.add(body, "stmt", "s");
        let (p, slots) = pat(&t.lang, "(def $N $B:(block $S))");
        let mut c = caps(&slots);
        assert!(matches(&t.tree, &t.lang, target, None, &p, &mut c, &[]));
        let tp = tpl(&t.lang, &slots, src);
        let built = t.build(&tp, &c, Some(target));
        let parent = |id: NodeId| id.parent(&t.tree.storage.0) == Some(target);
        (built, parent(name), parent(body))
    };
    let (built, name_stays, body_stays) = setup("(__def $N $N (__body $B))");
    assert_eq!(
        built,
        [r#"(__def (identifier "f") (identifier "f") (__body (block (stmt "s"))))"#]
    );
    assert!(name_stays, "used twice: copied");
    assert!(!body_stays, "used once: moved");
    let (built, _, body_stays) = setup("(__def $S $B)");
    assert_eq!(built, [r#"(__def (stmt "s") (block (stmt "s")))"#]);
    assert!(body_stays, "a capture around another capture is copied");
    let (built, name_stays, body_stays) = setup("(__def $B $ROOT)");
    assert_eq!(
        built,
        [r#"(__def (block (stmt "s")) (def (identifier "f") (block (stmt "s"))))"#]
    );
    assert!(
        name_stays && body_stays,
        "captures inside a used target are copied"
    );
}

#[test]
fn templates_rekind_leaf_and_spread() {
    let mut t = T::new("class");
    let r = t.root();
    t.add(r, "identifier", "A");
    let sup = t.add(r, "superclass", "B");
    t.add(sup, "punct", "(");
    t.add(r, "\",\"", "");
    t.add(r, "superclass", "C");
    t.add(r, "body", "");
    let (p, slots) = pat(&t.lang, "(class $N $$$S:superclass|\",\" $B:body)");
    let mut c = caps(&slots);
    assert!(matches(&t.tree, &t.lang, r, None, &p, &mut c, &[]));
    let tp = tpl(
        &t.lang,
        &slots,
        "(__def $$$S:superclass=>__supertype @$N->__defname)",
    );
    assert_eq!(
        t.build(&tp, &c, None),
        [r#"(__def (__supertype "B") (__supertype "C") (__defname "A"))"#]
    );
    let tp = tpl(&t.lang, &slots, "(__def $$$S:_*_named->__supertype)");
    assert_eq!(
        t.build(&tp, &c, None),
        [r#"(__def (__supertype "B" (punct "(")) (__supertype "C"))"#]
    );
    let tp = tpl(&t.lang, &slots, "$B { (__scope) $N }");
    assert_eq!(
        t.build(&tp, &c, None),
        [r#"(body (__scope) (identifier "A"))"#]
    );
    let tp = tpl(&t.lang, &slots, "$$$S");
    assert_eq!(t.build(&tp, &c, None).len(), 3);
}

#[test]
fn malformed_sources_are_errors() {
    let lang = Lang::new();
    let bad_pat = |src: &str| {
        parse_pattern(src, &mut Slots::new(), &lang, &no_data)
            .err()
            .unwrap_or_else(|| panic!("{src:?} parsed"))
            .0
    };
    assert!(bad_pat("(a").contains("unclosed node"));
    assert!(bad_pat("(a) b").contains("trailing input"));
    assert!(bad_pat(r#"(a "x)"#).contains("unclosed quote"));
    assert!(bad_pat("(a b)").contains("expected `b:`"));
    assert!(bad_pat("(a f:? !$X)").contains("optional"));
    assert!(bad_pat(r#"(a "/(/")"#).contains("regex"));
    assert!(bad_pat("!(a)").contains("starts with a node"));
    let mut slots = Slots::new();
    slots.slot("A");
    let bad_tpl = |src: &str| {
        parse_template(src, &slots, &lang, &mut |_| Ok(()))
            .err()
            .unwrap_or_else(|| panic!("{src:?} parsed"))
            .0
    };
    assert!(bad_tpl("(a @$Z)").contains("unknown capture $Z"));
    assert!(bad_tpl(r#"(a "x" @$A)"#).contains("one text"));
    assert!(bad_tpl("(a @$A|f(\"x\")").contains("unclosed node"));
    assert!(bad_tpl("(a @$A|f(x").contains("unclosed `(`"));
    assert!(bad_tpl("(a @$A|)").contains("expected a function"));
    assert!(bad_tpl("$A=>k").contains("variadic"));
    assert!(bad_tpl("(a $$$A:%x)").contains("data is not available"));
    let err = parse_template("(a @$A|bad)", &slots, &lang, &mut |s| {
        Err::<(), _>(LoadError(format!("rejected {s}")))
    });
    assert_eq!(err.err().unwrap().0, "rejected @$A|bad");
}

#[test]
fn template_texts_are_indexed_in_source_order() {
    let lang = Lang::new();
    let mut slots = Slots::new();
    slots.slot("A");
    let tp = tpl(
        &lang,
        &slots,
        "(a (b @$A|f) $A { (c @$ROOT) } (d \"x\" (e @$A)))",
    );
    assert_eq!(tp.texts(), ["@$A|f", "@$ROOT", "@$A"]);
}
