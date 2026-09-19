use std::io::{self, Read, Write};
use std::path::Path;

use crate::intern::{Lang, LangSnapshot};
use crate::pipeline::{IndexResult, Pipeline};
use crate::tree::{Edge, TreeSnapshot};
use crate::treesitter::SupportLang;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Snapshot {
    trees: Vec<TreeSnapshot>,
    edges: Vec<Edge>,
    lang: LangSnapshot,
}

impl IndexResult {
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let snap = Snapshot {
            trees: self.trees.iter().map(TreeSnapshot::from).collect(),
            edges: self.edges.clone(),
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
            edges: snap.edges,
            lang: Lang::from(snap.lang),
            pipeline,
            timings: Default::default(),
        })
    }
}
