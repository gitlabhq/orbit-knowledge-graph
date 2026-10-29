//! Compact storage for collected refs between the parse barrier and Phase 2.
//!
//! Every file's `Vec<CollectedRef>` stays live from the parse barrier until
//! Phase 2 consumes it, coexisting with the fully-built graph — on large
//! repos that plateau is hundreds of MB of small allocations (SmolStrs,
//! per-chain Vecs). `RefPack` flattens one file's refs into three dense
//! index buffers backed by a shared [`StringPool`] arena; Phase 2 decodes
//! one ref at a time into reusable scratch buffers.

use std::hash::{Hash, Hasher};

use crate::v2::dsl::engine::CollectedRef;
use crate::v2::linker::graph::EdgeTriple;
use crate::v2::types::ssa::ParseValue;
use crate::v2::types::{ExpressionStep, RefKind, TypeUsage};
use orbit_utils::strings::{StrId, StringPool};
use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;
use smol_str::SmolStr;

const NO_CHAIN: u32 = u32::MAX;
const NO_ENCLOSING: u32 = u32::MAX;
const KIND_SHIFT: u32 = 28;
const CHAIN_SHIFT: u32 = 16;
const CHAIN_MAX: u32 = (1 << (KIND_SHIFT - CHAIN_SHIFT)) - 1;
const VALUES_MAX: u32 = (1 << CHAIN_SHIFT) - 1;

fn encode_kind(kind: RefKind) -> u32 {
    match kind {
        RefKind::Call => 0,
        RefKind::Value => 1,
        RefKind::Write => 2,
        RefKind::Annotation => 3,
        RefKind::Type(usage) => {
            8 + match usage {
                TypeUsage::Unknown => 0,
                TypeUsage::Parameter => 1,
                TypeUsage::Return => 2,
                TypeUsage::Field => 3,
                TypeUsage::Generic => 4,
                TypeUsage::Cast => 5,
            }
        }
    }
}

fn decode_kind(code: u32) -> RefKind {
    match code {
        1 => RefKind::Value,
        2 => RefKind::Write,
        3 => RefKind::Annotation,
        8..=13 => RefKind::Type(match code - 8 {
            1 => TypeUsage::Parameter,
            2 => TypeUsage::Return,
            3 => TypeUsage::Field,
            4 => TypeUsage::Generic,
            5 => TypeUsage::Cast,
            _ => TypeUsage::Unknown,
        }),
        _ => RefKind::Call,
    }
}

#[derive(Debug)]
struct PackedRef {
    name: StrId,
    enclosing_def: u32,
    chain_start: u32,
    values_start: u32,
    /// Kind, chain length, and values length, high bits to low.
    lens: u32,
    line: u32,
}

#[derive(Debug, Clone, Copy)]
enum StepTag {
    Ident,
    Field,
    Call,
    New,
    This,
    Super,
}

#[derive(Debug)]
struct PackedStep {
    tag: StepTag,
    /// The empty string for `This`/`Super`, which carry no identifier.
    name: StrId,
}

#[derive(Debug, Clone, Copy)]
enum ValueTag {
    LocalDef,
    ImportRef,
    Type,
    Opaque,
}

#[derive(Debug)]
struct PackedValue {
    tag: ValueTag,
    /// The index into `type_names` for `Type`.
    num: u32,
}

#[derive(Default)]
pub struct RefPack {
    strings: StringPool,
    refs: Vec<PackedRef>,
    steps: Vec<PackedStep>,
    values: Vec<PackedValue>,
    type_names: Vec<StrId>,
    repeats: Vec<(u32, u32)>,
}

pub struct DecodedRef<'a> {
    pub name: &'a str,
    pub has_chain: bool,
    pub enclosing_def: Option<u32>,
    pub kind: RefKind,
    pub line: u32,
}

impl RefPack {
    pub fn from_refs(refs: &[CollectedRef]) -> Self {
        Self::pack(refs).0
    }

    /// Pack `refs`, folding refs that differ only by line into one slot.
    /// Returns the slot of every input ref.
    pub fn pack(refs: &[CollectedRef]) -> (Self, Vec<u32>) {
        let mut pack = RefPack::default();
        let mut slots = Vec::with_capacity(refs.len());
        let mut firsts: Vec<usize> = Vec::new();
        let mut by_site: FxHashMap<u64, SmallVec<[u32; 1]>> = FxHashMap::default();
        for (i, r) in refs.iter().enumerate() {
            let candidates = by_site.entry(site_hash(r)).or_default();
            if let Some(&slot) = candidates
                .iter()
                .find(|&&slot| same_site(&refs[firsts[slot as usize]], r))
            {
                pack.repeats.push((slot, r.line));
                slots.push(slot);
                continue;
            }
            let slot = pack.refs.len() as u32;
            candidates.push(slot);
            firsts.push(i);
            slots.push(slot);
            pack.push(r);
        }
        pack.finish();
        (pack, slots)
    }

    fn push(&mut self, r: &CollectedRef) {
        let name = self.strings.alloc(r.name.as_str());
        let (chain_start, chain_len) = match &r.chain {
            None => (NO_CHAIN, 0),
            Some(chain) => {
                let start = self.steps.len() as u32;
                for step in chain {
                    let (tag, ident) = match step {
                        ExpressionStep::Ident(s) => (StepTag::Ident, s.as_str()),
                        ExpressionStep::Field(s) => (StepTag::Field, s.as_str()),
                        ExpressionStep::Call(s) => (StepTag::Call, s.as_str()),
                        ExpressionStep::New(s) => (StepTag::New, s.as_str()),
                        ExpressionStep::This => (StepTag::This, ""),
                        ExpressionStep::Super => (StepTag::Super, ""),
                    };
                    let name = self.strings.alloc(ident);
                    self.steps.push(PackedStep { tag, name });
                }
                (start, (chain.len() as u32).min(CHAIN_MAX))
            }
        };
        let values_start = self.values.len() as u32;
        for v in &r.reaching {
            let packed = match v {
                ParseValue::LocalDef(i) => PackedValue {
                    tag: ValueTag::LocalDef,
                    num: *i,
                },
                ParseValue::ImportRef(i) => PackedValue {
                    tag: ValueTag::ImportRef,
                    num: *i,
                },
                ParseValue::Type(s) => self.type_value(s.as_str()),
                ParseValue::Opaque => PackedValue {
                    tag: ValueTag::Opaque,
                    num: 0,
                },
            };
            self.values.push(packed);
        }
        self.refs.push(PackedRef {
            name,
            enclosing_def: r.enclosing_def.unwrap_or(NO_ENCLOSING),
            chain_start,
            values_start,
            lens: (encode_kind(r.kind) << KIND_SHIFT)
                | (chain_len << CHAIN_SHIFT)
                | (r.reaching.len() as u32).min(VALUES_MAX),
            line: r.line,
        });
    }

    fn finish(&mut self) {
        self.strings.shrink_to_arena();
        self.refs.shrink_to_fit();
        self.steps.shrink_to_fit();
        self.values.shrink_to_fit();
        self.type_names.shrink_to_fit();
        self.repeats.sort_unstable();
        self.repeats.shrink_to_fit();
    }

    /// The lines of the refs folded into slot `idx`, after its own line.
    pub fn repeated_lines(&self, idx: usize) -> impl Iterator<Item = u32> + '_ {
        let idx = idx as u32;
        let start = self.repeats.partition_point(|&(slot, _)| slot < idx);
        self.repeats[start..]
            .iter()
            .take_while(move |&&(slot, _)| slot == idx)
            .map(|&(_, line)| line)
    }

    /// Copy the edges from `edges[from..]` that carry lines once per folded line of slot `idx`.
    pub fn repeat_edges(&self, idx: usize, edges: &mut Vec<EdgeTriple>, from: usize) {
        let end = edges.len();
        for line in self.repeated_lines(idx) {
            for k in from..end {
                if let Some(edge) = edges[k].2.at_line(line) {
                    edges.push((edges[k].0, edges[k].1, edge));
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.refs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    /// Decode ref `idx` into `chain_buf` and `values_buf`, both cleared first.
    pub fn decode(
        &self,
        idx: usize,
        chain_buf: &mut Vec<ExpressionStep>,
        values_buf: &mut Vec<ParseValue>,
    ) -> DecodedRef<'_> {
        let r = &self.refs[idx];

        chain_buf.clear();
        let has_chain = r.chain_start != NO_CHAIN;
        if has_chain {
            let start = r.chain_start as usize;
            for step in &self.steps[start..start + ((r.lens >> CHAIN_SHIFT) & CHAIN_MAX) as usize] {
                chain_buf.push(match step.tag {
                    StepTag::Ident => ExpressionStep::Ident(self.sym(step.name)),
                    StepTag::Field => ExpressionStep::Field(self.sym(step.name)),
                    StepTag::Call => ExpressionStep::Call(self.sym(step.name)),
                    StepTag::New => ExpressionStep::New(self.sym(step.name)),
                    StepTag::This => ExpressionStep::This,
                    StepTag::Super => ExpressionStep::Super,
                });
            }
        }

        values_buf.clear();
        let start = r.values_start as usize;
        for v in &self.values[start..start + (r.lens & VALUES_MAX) as usize] {
            values_buf.push(match v.tag {
                ValueTag::LocalDef => ParseValue::LocalDef(v.num),
                ValueTag::ImportRef => ParseValue::ImportRef(v.num),
                ValueTag::Type => ParseValue::Type(self.sym(self.type_names[v.num as usize])),
                ValueTag::Opaque => ParseValue::Opaque,
            });
        }

        DecodedRef {
            name: self.strings.get(r.name),
            has_chain,
            enclosing_def: (r.enclosing_def != NO_ENCLOSING).then_some(r.enclosing_def),
            kind: decode_kind(r.lens >> KIND_SHIFT),
            line: r.line,
        }
    }

    pub fn collected(
        &self,
        idx: usize,
        chain_buf: &mut Vec<ExpressionStep>,
        values_buf: &mut Vec<ParseValue>,
    ) -> CollectedRef {
        let d = self.decode(idx, chain_buf, values_buf);
        CollectedRef {
            name: SmolStr::from(d.name),
            chain: d.has_chain.then(|| chain_buf.clone()),
            reaching: values_buf.clone(),
            enclosing_def: d.enclosing_def,
            kind: d.kind,
            line: d.line,
        }
    }

    pub fn subset(&self, indices: impl IntoIterator<Item = usize>) -> Self {
        let mut chain_buf = Vec::new();
        let mut values_buf = Vec::new();
        let mut pack = RefPack::default();
        for i in indices {
            let slot = pack.refs.len() as u32;
            pack.push(&self.collected(i, &mut chain_buf, &mut values_buf));
            pack.repeats
                .extend(self.repeated_lines(i).map(|line| (slot, line)));
        }
        pack.finish();
        pack
    }

    /// Set ref `idx`'s reaching values to a single `Type(fqn)`; out-of-range `idx` does nothing.
    pub fn set_reaching_type(&mut self, idx: usize, fqn: &str) {
        if idx >= self.refs.len() {
            return;
        }
        let values_start = self.values.len() as u32;
        let value = self.type_value(fqn);
        self.values.push(value);
        let r = &mut self.refs[idx];
        r.values_start = values_start;
        r.lens = (r.lens & !VALUES_MAX) | 1;
    }

    fn type_value(&mut self, name: &str) -> PackedValue {
        let num = self.type_names.len() as u32;
        self.type_names.push(self.strings.alloc(name));
        PackedValue {
            tag: ValueTag::Type,
            num,
        }
    }

    fn sym(&self, name: StrId) -> SmolStr {
        SmolStr::from(self.strings.get(name))
    }
}

fn site_hash(r: &CollectedRef) -> u64 {
    let mut hasher = FxHasher::default();
    (&r.name, &r.chain, &r.reaching, r.enclosing_def, r.kind).hash(&mut hasher);
    hasher.finish()
}

fn same_site(a: &CollectedRef, b: &CollectedRef) -> bool {
    a.name == b.name
        && a.chain == b.chain
        && a.reaching == b.reaching
        && a.enclosing_def == b.enclosing_def
        && a.kind == b.kind
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(pack: &RefPack) -> Vec<CollectedRef> {
        let mut chain_buf = Vec::new();
        let mut values_buf = Vec::new();
        (0..pack.len())
            .map(|i| pack.collected(i, &mut chain_buf, &mut values_buf))
            .collect()
    }

    fn eq_refs(a: &CollectedRef, b: &CollectedRef) -> bool {
        a.name == b.name
            && a.chain == b.chain
            && a.reaching == b.reaching
            && a.enclosing_def == b.enclosing_def
            && a.kind == b.kind
            && a.line == b.line
    }

    #[test]
    fn roundtrip_all_variants() {
        let long = "a_very_long_identifier_exceeding_smolstr_inline_capacity";
        let refs = vec![
            CollectedRef {
                name: SmolStr::from("plain"),
                chain: None,
                reaching: vec![],
                enclosing_def: None,
                kind: RefKind::Call,
                line: 3,
            },
            CollectedRef {
                name: SmolStr::from(long),
                chain: Some(vec![
                    ExpressionStep::Ident(SmolStr::from("base")),
                    ExpressionStep::Field(SmolStr::from("field")),
                    ExpressionStep::Call(SmolStr::from(long)),
                    ExpressionStep::New(SmolStr::from("Ctor")),
                    ExpressionStep::This,
                    ExpressionStep::Super,
                ]),
                reaching: vec![
                    ParseValue::LocalDef(7),
                    ParseValue::ImportRef(3),
                    ParseValue::Type(SmolStr::from("com.example.Foo")),
                    ParseValue::Opaque,
                ],
                enclosing_def: Some(42),
                kind: RefKind::Type(TypeUsage::Return),
                line: 9,
            },
            CollectedRef {
                name: SmolStr::from("empty_chain"),
                chain: Some(vec![]),
                reaching: vec![ParseValue::Opaque],
                enclosing_def: Some(0),
                kind: RefKind::Annotation,
                line: 1,
            },
        ];

        let pack = RefPack::from_refs(&refs);
        let decoded = decode_all(&pack);

        assert_eq!(decoded.len(), refs.len());
        for (a, b) in refs.iter().zip(&decoded) {
            assert!(eq_refs(a, b), "mismatch: {:?} vs {:?}", a.name, b.name);
        }
    }

    #[test]
    fn dedup_shares_arena_bytes() {
        let refs: Vec<CollectedRef> = (0..100)
            .map(|_| CollectedRef {
                name: SmolStr::from("repeated_name"),
                chain: Some(vec![ExpressionStep::Field(SmolStr::from("repeated_name"))]),
                reaching: vec![],
                enclosing_def: None,
                kind: RefKind::Value,
                line: 0,
            })
            .collect();
        let pack = RefPack::from_refs(&refs);
        assert_eq!(pack.strings.len(), 1);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn refs_that_differ_by_line_share_a_slot() {
        let site = |line| CollectedRef {
            name: SmolStr::from("helper"),
            chain: None,
            reaching: vec![],
            enclosing_def: Some(2),
            kind: RefKind::Call,
            line,
        };
        let (pack, slots) = RefPack::pack(&[site(3), site(7), site(5)]);
        assert_eq!(pack.len(), 1);
        assert_eq!(slots, vec![0, 0, 0]);
        assert_eq!(pack.repeated_lines(0).collect::<Vec<_>>(), vec![5, 7]);
    }

    #[test]
    fn set_reaching_type_replaces_values() {
        let refs = vec![CollectedRef {
            name: SmolStr::from("x"),
            chain: None,
            reaching: vec![ParseValue::LocalDef(1), ParseValue::Opaque],
            enclosing_def: None,
            kind: RefKind::Call,
            line: 0,
        }];
        let mut pack = RefPack::from_refs(&refs);
        pack.set_reaching_type(0, "pkg.Alias");
        pack.set_reaching_type(99, "ignored");

        let decoded = decode_all(&pack);
        assert_eq!(
            decoded[0].reaching,
            vec![ParseValue::Type(SmolStr::from("pkg.Alias"))]
        );
    }

    #[test]
    fn empty_pack() {
        let pack = RefPack::from_refs(&[]);
        assert!(pack.is_empty());
        assert_eq!(pack.len(), 0);
    }
}
