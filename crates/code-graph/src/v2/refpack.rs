//! Compact storage for collected refs between the parse barrier and Phase 2.
//!
//! Every file's `Vec<CollectedRef>` stays live from the parse barrier until
//! Phase 2 consumes it, coexisting with the fully-built graph — on large
//! repos that plateau is hundreds of MB of small allocations (SmolStrs,
//! per-chain Vecs). `RefPack` flattens one file's refs into three dense
//! index buffers backed by a shared [`StringPool`] arena; Phase 2 decodes
//! one ref at a time into reusable scratch buffers.

use crate::v2::dsl::engine::CollectedRef;
use crate::v2::types::ExpressionStep;
use crate::v2::types::ssa::ParseValue;
use orbit_utils::strings::{StrId, StringPool};
use smol_str::SmolStr;

const NO_CHAIN: u32 = u32::MAX;
const NO_ENCLOSING: u32 = u32::MAX;

#[derive(Debug)]
struct PackedRef {
    name: StrId,
    enclosing_def: u32,
    chain_start: u32,
    chain_len: u32,
    values_start: u32,
    values_len: u32,
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
    /// `None` for `This`/`Super`, which carry no identifier.
    name: Option<StrId>,
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
    num: u32,
    /// `Some` only for `Type`.
    name: Option<StrId>,
}

#[derive(Default)]
pub struct RefPack {
    strings: StringPool,
    refs: Vec<PackedRef>,
    steps: Vec<PackedStep>,
    values: Vec<PackedValue>,
}

pub struct DecodedRef<'a> {
    pub name: &'a str,
    pub has_chain: bool,
    pub enclosing_def: Option<u32>,
}

impl RefPack {
    pub fn from_refs(refs: &[CollectedRef]) -> Self {
        let mut pack = RefPack {
            strings: StringPool::new(),
            refs: Vec::with_capacity(refs.len()),
            steps: Vec::new(),
            values: Vec::new(),
        };

        for r in refs {
            let name = pack.strings.alloc(r.name.as_str());

            let (chain_start, chain_len) = match &r.chain {
                None => (NO_CHAIN, 0),
                Some(chain) => {
                    let start = pack.steps.len() as u32;
                    for step in chain {
                        let (tag, ident) = match step {
                            ExpressionStep::Ident(s) => (StepTag::Ident, Some(s)),
                            ExpressionStep::Field(s) => (StepTag::Field, Some(s)),
                            ExpressionStep::Call(s) => (StepTag::Call, Some(s)),
                            ExpressionStep::New(s) => (StepTag::New, Some(s)),
                            ExpressionStep::This => (StepTag::This, None),
                            ExpressionStep::Super => (StepTag::Super, None),
                        };
                        let name = ident.map(|s| pack.strings.alloc(s.as_str()));
                        pack.steps.push(PackedStep { tag, name });
                    }
                    (start, chain.len() as u32)
                }
            };

            let values_start = pack.values.len() as u32;
            for v in &r.reaching {
                let packed = match v {
                    ParseValue::LocalDef(i) => PackedValue {
                        tag: ValueTag::LocalDef,
                        num: *i,
                        name: None,
                    },
                    ParseValue::ImportRef(i) => PackedValue {
                        tag: ValueTag::ImportRef,
                        num: *i,
                        name: None,
                    },
                    ParseValue::Type(s) => PackedValue {
                        tag: ValueTag::Type,
                        num: 0,
                        name: Some(pack.strings.alloc(s.as_str())),
                    },
                    ParseValue::Opaque => PackedValue {
                        tag: ValueTag::Opaque,
                        num: 0,
                        name: None,
                    },
                };
                pack.values.push(packed);
            }

            pack.refs.push(PackedRef {
                name,
                enclosing_def: r.enclosing_def.unwrap_or(NO_ENCLOSING),
                chain_start,
                chain_len,
                values_start,
                values_len: r.reaching.len() as u32,
            });
        }

        pack.strings.shrink_to_arena();
        pack.steps.shrink_to_fit();
        pack.values.shrink_to_fit();
        pack
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
            for step in &self.steps[start..start + r.chain_len as usize] {
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
        for v in &self.values[start..start + r.values_len as usize] {
            values_buf.push(match v.tag {
                ValueTag::LocalDef => ParseValue::LocalDef(v.num),
                ValueTag::ImportRef => ParseValue::ImportRef(v.num),
                ValueTag::Type => ParseValue::Type(self.sym(v.name)),
                ValueTag::Opaque => ParseValue::Opaque,
            });
        }

        DecodedRef {
            name: self.strings.get(r.name),
            has_chain,
            enclosing_def: (r.enclosing_def != NO_ENCLOSING).then_some(r.enclosing_def),
        }
    }

    /// Set ref `idx`'s reaching values to a single `Type(fqn)`; out-of-range `idx` does nothing.
    pub fn set_reaching_type(&mut self, idx: usize, fqn: &str) {
        if idx >= self.refs.len() {
            return;
        }
        let name = Some(self.strings.alloc(fqn));
        let values_start = self.values.len() as u32;
        self.values.push(PackedValue {
            tag: ValueTag::Type,
            num: 0,
            name,
        });
        self.refs[idx].values_start = values_start;
        self.refs[idx].values_len = 1;
    }

    /// Return the interned name, or an empty string when `name` is `None`.
    fn sym(&self, name: Option<StrId>) -> SmolStr {
        name.map_or_else(SmolStr::default, |id| SmolStr::from(self.strings.get(id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(pack: &RefPack) -> Vec<CollectedRef> {
        let mut chain_buf = Vec::new();
        let mut values_buf = Vec::new();
        (0..pack.len())
            .map(|i| {
                let d = pack.decode(i, &mut chain_buf, &mut values_buf);
                CollectedRef {
                    name: SmolStr::from(d.name),
                    chain: d.has_chain.then(|| chain_buf.clone()),
                    reaching: values_buf.clone(),
                    enclosing_def: d.enclosing_def,
                }
            })
            .collect()
    }

    fn eq_refs(a: &CollectedRef, b: &CollectedRef) -> bool {
        a.name == b.name
            && a.chain == b.chain
            && a.reaching == b.reaching
            && a.enclosing_def == b.enclosing_def
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
            },
            CollectedRef {
                name: SmolStr::from("empty_chain"),
                chain: Some(vec![]),
                reaching: vec![ParseValue::Opaque],
                enclosing_def: Some(0),
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
            })
            .collect();
        let pack = RefPack::from_refs(&refs);
        assert_eq!(pack.strings.len(), 1);
    }

    #[test]
    fn set_reaching_type_replaces_values() {
        let refs = vec![CollectedRef {
            name: SmolStr::from("x"),
            chain: None,
            reaching: vec![ParseValue::LocalDef(1), ParseValue::Opaque],
            enclosing_def: None,
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
