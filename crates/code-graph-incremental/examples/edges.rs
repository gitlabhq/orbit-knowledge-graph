use code_graph_incremental::{Engine, Limits, Sentinel, language::SupportLang};
use orbit_utils::{
    vfs::{Vfs, sources::Directory},
    vfs_policies::CodePolicy,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().ok_or("usage: edges DIR EXT")?;
    let ext = args.next().ok_or("usage: edges DIR EXT")?;
    let lang = SupportLang::from_path(&format!("x.{ext}")).ok_or("unknown extension")?;
    let engine = Engine::family(lang).limits(Limits::UNLIMITED).build()?;
    let repo = Vfs::load(
        Directory(std::path::Path::new(&root)),
        CodePolicy,
        Default::default(),
        Default::default(),
    )?;
    let (state, report) =
        code_graph_incremental::driver::index(&engine, &repo, &Sentinel::disabled())?;
    if std::env::var_os("CGI_PROFILE").is_some() {
        eprint!("{}", code_graph_incremental::engine::profile::report());
    }
    if std::env::var("TREE").is_ok() {
        for t in &state.trees {
            println!(
                "{}",
                code_graph_incremental::tree::pretty_print(t, &engine.lang, false)
            );
        }
    }
    if std::env::var("TAGS").is_ok() {
        for t in &state.trees {
            let mut nodes: Vec<_> = t.tags.iter().collect();
            nodes.sort_by_key(|(n, _)| **n);
            for (node, tags) in nodes {
                let c = t.cursor(*node);
                let kv: Vec<String> = tags
                    .iter()
                    .map(|tag| {
                        format!(
                            "{}={}",
                            engine.lang.syms.resolve(tag.key),
                            engine.lang.syms.resolve(tag.val)
                        )
                    })
                    .collect();
                println!(
                    "TAG {}:{} {} {}",
                    t.label,
                    node,
                    engine.lang.kind_name(c.kind()),
                    kv.join(" ")
                );
            }
        }
    }
    for k in report.skipped.iter().chain(&report.limited) {
        eprintln!("CUT {k:?}");
    }
    for e in &state.edges {
        let name = |fi: u32, n: u32| {
            let t = &state.trees[fi as usize];
            let c = t.cursor(n);
            let s = c
                .child_sym(engine.lang.lookup_kind("__defname"))
                .or(c.sym_opt())
                .unwrap_or(0);
            format!(
                "{}:{}@{}",
                t.label,
                engine.lang.syms.resolve(s),
                c.start_row()
            )
        };
        println!(
            "{} {} -> {}",
            engine.lang.edge_name(e.kind),
            name(e.from_tree, e.from_node),
            name(e.to_tree, e.to_node)
        );
    }
    Ok(())
}
