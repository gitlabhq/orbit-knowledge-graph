use std::sync::Arc;

use code_graph_incremental::pipeline::Display;
use code_graph_incremental::templates;
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Limits};
use orbit_utils::vfs::{Decision, File, Pass, Vfs, sources::Directory};

struct KeepAll;

impl Pass for KeepAll {
    type Tag = ();
    fn metadata(&self, _: &File<'_, ()>) -> Decision<()> {
        Decision::Pending
    }
    fn content(&self, _: &File<'_, ()>) -> Decision<()> {
        Decision::Keep(())
    }
}

#[test]
fn zz_dump() {
    let Ok(dir) = std::env::var("DUMP_DIR") else {
        return;
    };
    let lang: SupportLang = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| SupportLang::from_path(e.unwrap().path().to_str().unwrap()))
        .next()
        .unwrap();
    let env = Env::with_limits(lang, Limits::UNLIMITED).unwrap();
    let repository = Arc::new(
        Vfs::load(
            Directory(std::path::Path::new(&dir)),
            KeepAll,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
    );
    let files: std::collections::HashMap<String, String> = repository
        .files()
        .map(|f| {
            (
                f.path.to_string(),
                String::from_utf8(repository.read(&f.path).unwrap().to_vec()).unwrap_or_default(),
            )
        })
        .collect();
    let graph = templates::index(Context::new(&env, repository)).unwrap();
    let (_, displayed) = graph.then(Display).unwrap().finish();
    let l = &env.lang;
    for (fi, tree) in displayed.state.trees.iter().enumerate() {
        let src = files.get(&tree.label).cloned().unwrap_or_default();
        println!("=== {fi} {}", tree.label);
        let mut stack = vec![(tree.root(), 0usize)];
        while let Some((c, depth)) = stack.pop() {
            let text = src
                .get(c.start() as usize..c.end() as usize)
                .unwrap_or("")
                .replace('\n', "\\n");
            let text: String = text.chars().take(60).collect();
            let tags: Vec<String> = tree
                .tags
                .get(&c.index())
                .into_iter()
                .flatten()
                .map(|t| format!("{}={:?}", l.syms.resolve(t.key), l.syms.resolve(t.val)))
                .collect();
            println!(
                "{:indent$}{} #{} {:?} [{}..{}] `{}` {}",
                "",
                l.kind_name(c.kind()),
                c.index(),
                l.syms.resolve(c.sym()),
                c.start(),
                c.end(),
                text,
                tags.join(" "),
                indent = depth * 2
            );
            let kids: Vec<_> = c.children().collect();
            for k in kids.into_iter().rev() {
                stack.push((k, depth + 1));
            }
        }
    }
    for e in &displayed.state.edges {
        println!("EDGE {:?}", e);
    }
}
