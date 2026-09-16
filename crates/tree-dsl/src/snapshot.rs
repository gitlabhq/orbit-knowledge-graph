use std::io::{self, Read, Write};
use std::path::Path;

use crate::grammar::SupportLang;
use crate::lang::{Lang, LangSnapshot};
use crate::pipeline::{IndexResult, Pipeline, process_file};
use crate::tree::{Edge, TreeSnapshot};
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
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snap).map_err(io::Error::other)?;
        let mut f = std::fs::File::create(path)?;
        f.write_all(&bytes)
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<Self> {
        let mut f = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        let snap: Snapshot = rkyv::from_bytes::<Snapshot, rkyv::rancor::BoxedError>(&bytes)
            .map_err(io::Error::other)?;
        let (pipeline, _) = Pipeline::for_lang(lang_id);
        Ok(IndexResult {
            trees: snap.trees.into_iter().map(|t| t.into()).collect(),
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
        for path in removed {
            self.trees.retain(|t| t.label != *path);
        }
        for (path, _) in modified {
            self.trees.retain(|t| t.label != *path);
        }

        let new_files: Vec<&(String, String)> = modified.iter().chain(added.iter()).collect();
        for (path, source) in &new_files {
            let tree = process_file(path, source, &self.lang, &self.pipeline);
            self.trees.push(tree);
        }

        let all_paths: Vec<String> = self.trees.iter().map(|t| t.label.clone()).collect();
        let all_files: Vec<(String, String)> = all_paths
            .iter()
            .map(|p| (p.clone(), String::new()))
            .collect();
        let walk = file_tree::walk(&all_paths, &all_files, &self.lang, &self.pipeline.resolve);
        self.cross_edges = resolver::resolve(
            &mut self.trees,
            &self.lang,
            self.pipeline.lang_id,
            &walk.lookup_prefixes,
            &self.pipeline.resolve.external,
        )
        .cross_edges;
    }
}
