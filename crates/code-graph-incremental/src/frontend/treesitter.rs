use std::ops::ControlFlow;

use crate::intern::Lang;
use crate::language::SupportLang;
use crate::sentinel::{Killed, Stop};
use crate::tree::{Node, Tree};

fn grammar(lang: SupportLang) -> tree_sitter::Language {
    match lang.grammar() {
        "tree-sitter-bash" => tree_sitter_bash::LANGUAGE.into(),
        "tree-sitter-c" => tree_sitter_c::LANGUAGE.into(),
        "tree-sitter-containerfile" => tree_sitter_containerfile::LANGUAGE.into(),
        "tree-sitter-cpp" => tree_sitter_cpp::LANGUAGE.into(),
        "tree-sitter-c-sharp" => tree_sitter_c_sharp::LANGUAGE.into(),
        "tree-sitter-elixir" => tree_sitter_elixir::LANGUAGE.into(),
        "tree-sitter-go" => tree_sitter_go::LANGUAGE.into(),
        "tree-sitter-hcl" => tree_sitter_hcl::LANGUAGE.into(),
        "tree-sitter-java" => tree_sitter_java::LANGUAGE.into(),
        "tree-sitter-kotlin-sg" => tree_sitter_kotlin::LANGUAGE.into(),
        "tree-sitter-lua" => tree_sitter_lua::LANGUAGE.into(),
        "tree-sitter-php" => tree_sitter_php::LANGUAGE_PHP.into(),
        "tree-sitter-python" => tree_sitter_python::LANGUAGE.into(),
        "tree-sitter-ruby" => tree_sitter_ruby::LANGUAGE.into(),
        "tree-sitter-rust" => tree_sitter_rust::LANGUAGE.into(),
        "tree-sitter-scala" => tree_sitter_scala::LANGUAGE.into(),
        "tree-sitter-swift" => tree_sitter_swift::LANGUAGE.into(),
        "tree-sitter-typescript" => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tree-sitter-typescript-tsx" => tree_sitter_typescript::LANGUAGE_TSX.into(),
        "tree-sitter-haskell" => tree_sitter_haskell::LANGUAGE.into(),
        "tree-sitter-ocaml" => tree_sitter_ocaml::LANGUAGE_OCAML.into(),
        "tree-sitter-zig" => tree_sitter_zig::LANGUAGE.into(),
        other => panic!("unknown grammar: {other}"),
    }
}

fn from_tree_sitter(
    source: &str,
    ts_tree: &tree_sitter::Tree,
    lang: &Lang,
    kind_map: &[u16],
    field_map: &[u16],
    label: &str,
    max_depth: usize,
) -> (Tree, bool) {
    let ts_root = ts_tree.root_node();
    let mut cursor = ts_root.walk();

    let kind = kind_map
        .get(cursor.node().kind_id() as usize)
        .copied()
        .unwrap_or(0);
    let sp = cursor.node().start_position();
    let ep = cursor.node().end_position();
    let root_sym = if !label.is_empty() {
        lang.syms.intern(label)
    } else if cursor.node().is_named() {
        let text = &source[cursor.node().start_byte()..cursor.node().end_byte()];
        lang.syms.intern(text)
    } else {
        0
    };

    let mut tree = Tree::with_capacity(
        ts_root.descendant_count(),
        Node {
            kind,
            field: 0,
            named: cursor.node().is_named(),
            synth: false,
            sym: root_sym,
            start: cursor.node().start_byte() as u32,
            end: cursor.node().end_byte() as u32,
            start_row: sp.row as u32,
            start_col: sp.column as u32,
            end_row: ep.row as u32,
            end_col: ep.column as u32,
            lifted: false,
            labelled: false,
        },
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    let root_nid = tree.root;
    let mut parent_stack = vec![(root_nid, ts_root.kind_id(), 1)];

    if !cursor.goto_first_child() {
        return (tree, false);
    }
    let mut truncated = false;

    loop {
        let ts = cursor.node();
        let kind = kind_map.get(ts.kind_id() as usize).copied().unwrap_or(0);
        let field = cursor
            .field_id()
            .map_or(0, |f| field_map.get(f.get() as usize).copied().unwrap_or(0));
        let sp = ts.start_position();
        let ep = ts.end_position();

        let (parent, parent_kind, parent_depth) =
            *parent_stack.last().expect("root is pushed before the walk");
        let nid = tree.append(
            parent,
            Node {
                kind,
                field,
                named: ts.is_named(),
                synth: false,
                sym: 0,
                start: ts.start_byte() as u32,
                end: ts.end_byte() as u32,
                start_row: sp.row as u32,
                start_col: sp.column as u32,
                end_row: ep.row as u32,
                end_col: ep.column as u32,
                lifted: false,
                labelled: false,
            },
        );

        let chained = ts.kind_id() == parent_kind && cursor.field_name() == Some("left");
        if cursor.goto_first_child() {
            let depth = parent_depth + usize::from(!chained);
            if max_depth == 0 || depth <= max_depth {
                parent_stack.push((nid, ts.kind_id(), depth));
                continue;
            }
            cursor.goto_parent();
            truncated = true;
        }

        if cursor.goto_next_sibling() {
            continue;
        }

        loop {
            if !cursor.goto_parent() {
                return (tree, truncated);
            }
            parent_stack.pop();
            if cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

struct TsCache {
    lang: Option<(SupportLang, u64)>,
    kinds: Vec<u16>,
    fields: Vec<u16>,
    parser: tree_sitter::Parser,
}

impl TsCache {
    fn ensure(&mut self, support_lang: SupportLang, lang: &Lang) {
        let key = (support_lang, lang.kinds.id());
        if self.lang == Some(key) {
            return;
        }
        let ts = grammar(support_lang);
        self.kinds = (0..ts.node_kind_count() as u16)
            .map(|id| ts.node_kind_for_id(id).map_or(0, |s| lang.intern_kind(s)))
            .collect();
        self.fields = std::iter::once(0)
            .chain(
                (1..=ts.field_count() as u16)
                    .map(|id| ts.field_name_for_id(id).map_or(0, |s| lang.intern_field(s))),
            )
            .collect();
        self.parser
            .set_language(&ts)
            .expect("compiled-in grammar matches the tree-sitter ABI");
        self.lang = Some(key);
    }
}

thread_local! {
    static CACHE: std::cell::RefCell<TsCache> = std::cell::RefCell::new(TsCache {
        lang: None, kinds: Vec::new(), fields: Vec::new(),
        parser: tree_sitter::Parser::new(),
    });
}

/// `None` from tree-sitter means its own timeout or cancellation tripped.
pub fn parse(
    source: &str,
    support_lang: SupportLang,
    lang: &Lang,
    label: &str,
) -> Result<Tree, Killed> {
    parse_with_sentinel(
        source,
        support_lang,
        lang,
        label,
        &crate::sentinel::Sentinel::disabled(),
        0,
    )
    .map(|(tree, _)| tree)
}

pub fn parse_with_sentinel(
    source: &str,
    support_lang: SupportLang,
    lang: &Lang,
    label: &str,
    sentinel: &crate::sentinel::Sentinel,
    max_depth: usize,
) -> Result<(Tree, Option<Killed>), Killed> {
    sentinel.check()?;
    CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.ensure(support_lang, lang);
        c.parser.reset();
        let mut stopped = None;
        let mut guard = parse_guard();
        let mut progress = |state: &tree_sitter::ParseState| {
            let stop = match sentinel.check() {
                Ok(()) => match guard(state.current_byte_offset()) {
                    ControlFlow::Continue(()) => return ControlFlow::Continue(()),
                    ControlFlow::Break((reason, stop)) => Killed::stopped(reason, label, stop),
                },
                Err(error) => error,
            };
            stopped = Some(stop);
            ControlFlow::Break(())
        };
        let mut input =
            |offset: usize, _: tree_sitter::Point| source.as_bytes().get(offset..).unwrap_or(&[]);
        let options = tree_sitter::ParseOptions::new().progress_callback(&mut progress);
        let ts_tree = c
            .parser
            .parse_with_options(&mut input, None, Some(options))
            .ok_or_else(|| {
                stopped.unwrap_or_else(|| Killed::stopped("parse", label, Stop::Failed))
            })?;
        sentinel.check()?;
        let (tree, truncated) = from_tree_sitter(
            source, &ts_tree, lang, &c.kinds, &c.fields, label, max_depth,
        );
        sentinel.check()?;
        let cut = truncated.then(|| Killed::limit("depth", label, depth_of(&ts_tree), max_depth));
        Ok((tree, cut))
    })
}

const MAX_STALL: usize = 100_000;
const PARSE_STACK_RESERVE: usize = 256 * 1024;

fn depth_of(tree: &tree_sitter::Tree) -> usize {
    let mut cursor = tree.walk();
    let mut stack = vec![(cursor.node().kind_id(), 1)];
    if !cursor.goto_first_child() {
        return 1;
    }
    let mut deepest = 1;
    loop {
        let kind = cursor.node().kind_id();
        let &(parent_kind, parent_depth) = stack.last().expect("the root is on the stack");
        let chained = kind == parent_kind && cursor.field_name() == Some("left");
        if cursor.goto_first_child() {
            let depth = parent_depth + usize::from(!chained);
            deepest = deepest.max(depth);
            stack.push((kind, depth));
            continue;
        }
        while !cursor.goto_next_sibling() {
            if !cursor.goto_parent() {
                return deepest;
            }
            stack.pop();
        }
    }
}

fn parse_guard() -> impl FnMut(usize) -> ControlFlow<(&'static str, Stop)> {
    let (mut last_offset, mut stalled) = (usize::MAX, 0);
    move |offset| {
        if let Some(left) = stacker::remaining_stack().filter(|&left| left < PARSE_STACK_RESERVE) {
            let reserve = PARSE_STACK_RESERVE;
            return ControlFlow::Break(("stack", Stop::Below { left, reserve }));
        }
        if offset != last_offset {
            (last_offset, stalled) = (offset, 0);
            return ControlFlow::Continue(());
        }
        stalled += 1;
        match stalled > MAX_STALL {
            true => ControlFlow::Break((
                "parse",
                Stop::Reached {
                    observed: stalled,
                    cap: MAX_STALL,
                },
            )),
            false => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[test]
    fn a_parse_stops_after_too_many_steps_without_progress() {
        let mut guard = parse_guard();
        assert!(guard(0).is_continue());
        assert!((0..MAX_STALL).all(|_| guard(0).is_continue()));
        assert!(guard(0).is_break());
        assert!(guard(1).is_continue());
    }

    #[test]
    fn a_parse_near_the_end_of_the_stack_stops_as_a_stack_skip() {
        let source = "def f(x):\n    return x\n".repeat(2_000);
        let killed = std::thread::Builder::new()
            .stack_size(224 * 1024)
            .spawn(move || parse(&source, SupportLang::Python, &Lang::default(), "main.py").err())
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(killed.label, "stack");
        assert!(matches!(killed.stop, Some(Stop::Below { left, reserve }) if left < reserve));
    }

    #[test]
    fn a_tree_far_past_the_depth_limit_is_cut_in_linear_time() {
        let levels = 50_000;
        let source = format!("{}{}", "{\n".repeat(levels), "}\n".repeat(levels));
        let started = cpu_time::ThreadTime::now();
        let killed = parse_with_sentinel(
            &source,
            SupportLang::JavaScript,
            &Lang::default(),
            "deep.js",
            &crate::sentinel::Sentinel::disabled(),
            10,
        )
        .unwrap()
        .1
        .unwrap();
        assert!(
            matches!(killed.stop, Some(Stop::Reached { observed, cap: 10 }) if observed > levels)
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_tree_cut_at_the_depth_limit_reports_its_real_depth() {
        let source = format!("x = {}1{}\n", "[".repeat(50), "]".repeat(50));
        let killed = parse_with_sentinel(
            &source,
            SupportLang::Python,
            &Lang::default(),
            "deep.py",
            &crate::sentinel::Sentinel::disabled(),
            10,
        )
        .unwrap()
        .1
        .unwrap();
        assert!(matches!(killed.stop, Some(Stop::Reached { observed, cap: 10 }) if observed > 50));
    }

    #[test]
    fn parsing_can_cancel_mid_file_and_reuse_the_parser() {
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = checks.clone();
        let sentinel = crate::sentinel::Sentinel::new("parse", "large.py", u64::MAX)
            .with_cancellation(Arc::new(move || {
                observed.fetch_add(1, Ordering::Relaxed) > 0
            }));
        let lang = Lang::default();
        let source = "def example():\n    return 42\n".repeat(20000);
        let error = parse_with_sentinel(
            &source,
            SupportLang::Python,
            &lang,
            "large.py",
            &sentinel,
            0,
        )
        .err()
        .unwrap();
        assert_eq!(error.label, "parse");
        assert!(error.cancelled);
        assert_eq!(error.to_string(), "tree-sitter cancelled");
        assert_eq!(error.path, "large.py");
        assert_eq!(error.budget, Duration::from_millis(u64::MAX));
        assert!(checks.load(Ordering::Relaxed) >= 2);
        assert!(
            parse(
                "def valid(): pass\n",
                SupportLang::Python,
                &lang,
                "valid.py"
            )
            .is_ok()
        );
    }
}
