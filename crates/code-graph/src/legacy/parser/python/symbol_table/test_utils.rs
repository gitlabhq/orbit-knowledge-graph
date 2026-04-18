use std::thread;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

pub(crate) fn parse_python(code: &str) -> Root<StrDoc<SupportLang>> {
    Root::new(code, SupportLang::Python)
}

pub(crate) fn find_first_node_by_kind<'a>(
    root: &'a Root<StrDoc<SupportLang>>,
    kind: &str,
) -> Option<Node<'a, StrDoc<SupportLang>>> {
    let mut stack = vec![root.root()];

    while let Some(node) = stack.pop() {
        if node.kind() == kind {
            return Some(node);
        }

        for child in node.children() {
            stack.push(child);
        }
    }

    None
}

pub(crate) fn run_on_small_stack<T>(f: impl FnOnce() -> T + Send + 'static) -> T
where
    T: Send + 'static,
{
    thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(f)
        .expect("small-stack thread should start")
        .join()
        .expect("small-stack thread should complete")
}
