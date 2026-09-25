use rustc_hash::FxHashMap;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::intern::Lang;
use crate::tags::ReservedTags;

pub const CLASS_LIKE: &[C] = &[
    C::Class,
    C::Struct,
    C::ImplBlock,
    C::Interface,
    C::Trait,
    C::Enum,
];

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Loc {
    pub fi: usize,
    pub node: u32,
}

/// An import whose target file is known but whose symbol is not yet linked.
#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ImportReq {
    pub fi: usize,
    pub node: u32,
    pub target_fi: usize,
    pub target_path: String,
}

#[derive(Default)]
#[allow(dead_code)]
pub struct FileIndex {
    keys: FxHashMap<String, usize>,
    dirs: FxHashMap<String, Vec<usize>>,
}

/// Per file, the names visible in it and where each one is defined.
#[allow(dead_code)]
pub struct Resolver {
    visible: Vec<FxHashMap<u32, Loc>>,
    reqs: Vec<ImportReq>,
    file_index: FileIndex,
    wildcard_sym: u32,
    tags: ReservedTags,
}

impl Resolver {
    pub fn new(lang: &Lang) -> Self {
        Self {
            visible: Vec::new(),
            reqs: Vec::new(),
            file_index: FileIndex::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
            tags: ReservedTags::new(lang),
        }
    }
}
