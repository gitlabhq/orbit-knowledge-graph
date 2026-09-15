use std::io::{self, Read, Write};
use std::path::Path;

use crate::grammar::SupportLang;
use crate::lang::{Lang, LangSnapshot};
use crate::pipeline::{IndexResult, Pipeline, process_file};
use crate::tree::{Edge, Tree};
use crate::{file_tree, resolver};

#[derive(rkyv::Archive, rkyv::Serialize)]
struct Snapshot<'a> {
    #[rkyv(with = rkyv::with::AsVec)]
    trees: &'a [Tree],
    #[rkyv(with = rkyv::with::AsVec)]
    intra_edges: &'a [Vec<Edge>],
    #[rkyv(with = rkyv::with::AsVec)]
    cross_edges: &'a [Edge],
    resolved_paths: Vec<((usize, u32), u32)>,
    lang: LangSnapshot,
}

#[derive(rkyv::Archive, rkyv::Deserialize)]
struct OwnedSnapshot {
    trees: Vec<Tree>,
    intra_edges: Vec<Vec<Edge>>,
    cross_edges: Vec<Edge>,
    resolved_paths: Vec<((usize, u32), u32)>,
    lang: LangSnapshot,
}

impl IndexResult {
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let snap = Snapshot {
            trees: &self.trees,
            intra_edges: &self.intra_edges,
            cross_edges: &self.cross_edges,
            resolved_paths: self
                .resolved_paths
                .iter()
                .map(|(&key, &value)| (key, value))
                .collect(),
            lang: LangSnapshot::from(&self.lang),
        };
        let file = std::io::BufWriter::new(std::fs::File::create(path)?);
        let writer = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::BoxedError>(
            &snap,
            rkyv::ser::writer::IoWriter::new(file),
        )
        .map_err(io::Error::other)?;
        writer.into_inner().flush()
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<Self> {
        let mut f = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        let snap: OwnedSnapshot =
            rkyv::from_bytes::<OwnedSnapshot, rkyv::rancor::BoxedError>(&bytes)
                .map_err(io::Error::other)?;
        let (pipeline, _) = Pipeline::for_lang(lang_id);
        Ok(IndexResult {
            trees: snap.trees,
            intra_edges: snap.intra_edges,
            cross_edges: snap.cross_edges,
            resolved_paths: snap.resolved_paths.into_iter().collect(),
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
        let replaced = |label: &str| {
            removed.iter().any(|path| path == label)
                || modified.iter().any(|(path, _)| path == label)
        };
        let trees = std::mem::take(&mut self.trees);
        let edges = std::mem::take(&mut self.intra_edges);
        for (tree, tree_edges) in trees.into_iter().zip(edges) {
            if !replaced(&tree.label) {
                self.trees.push(tree);
                self.intra_edges.push(tree_edges);
            }
        }

        let new_files: Vec<&(String, String)> = modified.iter().chain(added.iter()).collect();
        for (path, source) in &new_files {
            let (tree, edges) = process_file(path, source, &mut self.lang, &self.pipeline);
            self.intra_edges.push(edges);
            self.trees.push(tree);
        }

        let all_paths: Vec<String> = self.trees.iter().map(|t| t.label.clone()).collect();
        let all_files: Vec<(String, String)> = all_paths
            .iter()
            .map(|p| (p.clone(), String::new()))
            .collect();
        let walk = file_tree::walk(
            &all_paths,
            &all_files,
            &mut self.lang,
            &self.pipeline.resolve,
        );
        let resolved = resolver::resolve(
            &self.trees,
            &self.intra_edges,
            &mut self.lang,
            self.pipeline.lang_id,
            &walk.lookup_prefixes,
            &self.pipeline.resolve.external,
        );
        self.cross_edges = resolved.cross_edges;
        self.resolved_paths = resolved.resolved_paths;
    }
}
