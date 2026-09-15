use std::io::{self, Read, Write};
use std::path::Path;

use crate::grammar::SupportLang;
use crate::lang::{Lang, LangSnapshot};
use crate::pipeline::{IndexResult, Pipeline, process_file};
use crate::tree::{Edge, LockedTree, TreeSnapshot};
use crate::{file_tree, resolver};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Snapshot {
    trees: Vec<TreeSnapshot>,
    cross_edges: Vec<Edge>,
    lang: LangSnapshot,
}

impl IndexResult {
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let snap = Snapshot {
            trees: self.trees.iter().map(TreeSnapshot::from).collect(),
            cross_edges: self.cross_edges.clone(),
            lang: LangSnapshot::from(&self.lang),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snap)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut f = std::fs::File::create(path)?;
        f.write_all(&bytes)
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<Self> {
        let mut f = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        let snap: Snapshot = rkyv::from_bytes::<Snapshot, rkyv::rancor::BoxedError>(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let (pipeline, _) = Pipeline::for_lang(lang_id);
        Ok(IndexResult {
            trees: snap.trees.into_iter().map(|s| {
                let t: crate::tree::Tree = s.into();
                LockedTree::from(t)
            }).collect(),
            cross_edges: snap.cross_edges,
            lang: Lang::from(snap.lang),
            pipeline,
            timings: Default::default(),
        })
    }

    pub fn update(
        &mut self,
        added: &[(String, String)],
        modified: &[(String, String)],
        removed: &[String],
    ) {
        use crate::tree::{LockedTree, Tree};

        let mut trees: Vec<Tree> = std::mem::take(&mut self.trees)
            .into_iter()
            .map(Tree::from)
            .collect();

        trees.retain(|t| {
            !removed.contains(&t.label)
                && !modified.iter().any(|(p, _)| p == &t.label)
        });

        for (path, source) in modified.iter().chain(added.iter()) {
            let tree = process_file(path, source, &mut self.lang, &self.pipeline);
            trees.push(tree);
        }

        let all_paths: Vec<String> = trees.iter().map(|t| t.label.clone()).collect();
        let all_files: Vec<(String, String)> =
            all_paths.iter().map(|p| (p.clone(), String::new())).collect();
        let walk = file_tree::walk(&all_paths, &all_files, &mut self.lang, &self.pipeline.resolve);
        self.cross_edges = resolver::resolve(
            &mut trees,
            &mut self.lang,
            self.pipeline.lang_id,
            &walk.lookup_prefixes,
            &self.pipeline.resolve.external,
        )
        .cross_edges;

        self.trees = trees.into_iter().map(LockedTree::from).collect();
    }
}
