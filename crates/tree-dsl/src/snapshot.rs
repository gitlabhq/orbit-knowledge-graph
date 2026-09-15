use std::io::{self, Read, Write};
use std::path::Path;

use crate::grammar::SupportLang;
use crate::lang::{Interner, InternerSnapshot, Lang, LangSnapshot};
use crate::pipeline::{IndexResult, Pipeline, process_file};
use crate::tree::{Edge, Tree, TreeSnapshot};
use crate::{file_tree, resolver};

impl IndexResult {
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let mut f = std::fs::File::create(path)?;
        let mut w = io::BufWriter::new(&mut f);

        let tree_count = self.trees.len() as u32;
        w.write_all(&tree_count.to_le_bytes())?;

        let mut ser_buf = rkyv::util::AlignedVec::<16>::new();
        for tree in &self.trees {
            let snap = TreeSnapshot::from(tree);
            ser_buf.clear();
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::BoxedError>(&snap, &mut ser_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let len = ser_buf.len() as u64;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(&ser_buf)?;
        }

        let edge_bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&self.cross_edges)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = edge_bytes.len() as u64;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&edge_bytes)?;

        let lang_snap = LangSnapshot::from(&self.lang);
        let lang_bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&lang_snap)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = lang_bytes.len() as u64;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&lang_bytes)?;

        w.flush()
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<Self> {
        let mut f = std::fs::File::open(path)?;
        let mut r = io::BufReader::new(&mut f);

        let mut buf4 = [0u8; 4];
        r.read_exact(&mut buf4)?;
        let tree_count = u32::from_le_bytes(buf4) as usize;

        let mut trees = Vec::with_capacity(tree_count);
        let mut buf8 = [0u8; 8];
        for _ in 0..tree_count {
            r.read_exact(&mut buf8)?;
            let len = u64::from_le_bytes(buf8) as usize;
            let mut bytes = vec![0u8; len];
            r.read_exact(&mut bytes)?;
            let snap: TreeSnapshot =
                rkyv::from_bytes::<TreeSnapshot, rkyv::rancor::BoxedError>(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            trees.push(Tree::from(snap));
        }

        r.read_exact(&mut buf8)?;
        let len = u64::from_le_bytes(buf8) as usize;
        let mut bytes = vec![0u8; len];
        r.read_exact(&mut bytes)?;
        let cross_edges: Vec<Edge> =
            rkyv::from_bytes::<Vec<Edge>, rkyv::rancor::BoxedError>(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        r.read_exact(&mut buf8)?;
        let len = u64::from_le_bytes(buf8) as usize;
        let mut bytes = vec![0u8; len];
        r.read_exact(&mut bytes)?;
        let lang_snap: LangSnapshot =
            rkyv::from_bytes::<LangSnapshot, rkyv::rancor::BoxedError>(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let (pipeline, _) = Pipeline::for_lang(lang_id);
        Ok(IndexResult {
            trees,
            cross_edges,
            lang: Lang::from(lang_snap),
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
            let tree = process_file(path, source, &mut self.lang, &self.pipeline);
            self.trees.push(tree);
        }

        let all_paths: Vec<String> = self.trees.iter().map(|t| t.label.clone()).collect();
        let all_files: Vec<(String, String)> =
            all_paths.iter().map(|p| (p.clone(), String::new())).collect();
        let walk = file_tree::walk(&all_paths, &all_files, &mut self.lang, &self.pipeline.resolve);
        self.cross_edges = resolver::resolve(
            &mut self.trees,
            &mut self.lang,
            self.pipeline.lang_id,
            &walk.lookup_prefixes,
            &self.pipeline.resolve.external,
        )
        .cross_edges;
    }
}
