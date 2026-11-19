use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::engine::Loc;
use crate::tree::{Compact, Role, reachable};

type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
pub type Parts<'t> = SmallVec<[Cursor<'t>; 4]>;

/// What a pass knows about types: the defs that make up one type (impl
/// blocks, partial classes) and the types a part names as its supertypes.
pub trait Types<'t> {
    fn parts(&mut self, owner: Cursor<'t>) -> Parts<'t>;
    fn supers(&mut self, part: Cursor<'t>) -> Parts<'t>;
    fn declared(&mut self, owner: Cursor<'t>, name: u32, role: Role) -> Option<Cursor<'t>>;
}

/// The members named `name` at the nearest inheritance level that has any; a
/// type's own declaration hides the same name in its other parts.
pub fn members<'t>(
    t: &mut impl Types<'t>,
    owner: Cursor<'t>,
    name: u32,
    role: Role,
) -> Vec<Cursor<'t>> {
    let mut seen: FxHashSet<Loc> = FxHashSet::from_iter([owner.into()]);
    let mut level: Parts<'t> = SmallVec::from_elem(owner, 1);
    while !level.is_empty() {
        let mut found: Vec<Cursor<'t>> = Vec::new();
        let mut parts: Parts<'t> = SmallVec::new();
        for &owner in &level {
            let own = t.parts(owner);
            let members: Parts<'t> = match t.declared(own[0], name, role) {
                Some(member) => SmallVec::from_elem(member, 1),
                None => own[1..]
                    .iter()
                    .filter_map(|&p| t.declared(p, name, role))
                    .collect(),
            };
            for member in members {
                if !found.iter().any(|f| Loc::from(*f) == Loc::from(member)) {
                    found.push(member);
                }
            }
            parts.extend(own);
        }
        if !found.is_empty() {
            return found;
        }
        level = parts
            .iter()
            .flat_map(|&p| t.supers(p))
            .filter(|s| seen.insert((*s).into()))
            .collect();
    }
    Vec::new()
}

pub fn ancestors<'t>(t: &mut impl Types<'t>, class: Cursor<'t>) -> FxHashSet<Loc> {
    let t = std::cell::RefCell::new(t);
    let jump = |loc: Loc| class.jump(loc.fi, loc.node);
    reachable(Loc::from(class), |loc| {
        let mut t = t.borrow_mut();
        let parts = t.parts(jump(loc));
        parts
            .iter()
            .flat_map(|&p| t.supers(p))
            .map(Loc::from)
            .collect::<SmallVec<[Loc; 4]>>()
    })
    .collect()
}

/// `a.b.c`: a name at the root, then each member on what its object holds.
pub trait Chain<'t> {
    type Out: Default;
    fn early(&mut self, _c: Cursor<'t>) -> Option<Self::Out> {
        None
    }
    fn name(&mut self, c: Cursor<'t>) -> Self::Out;
    fn member(&mut self, c: Cursor<'t>, member: Cursor<'t>, object: Cursor<'t>) -> Self::Out;
}

pub fn chain<'t, P: Chain<'t>>(p: &mut P, c: Cursor<'t>) -> P::Out {
    if let Some(out) = p.early(c) {
        return out;
    }
    let c = c.reference();
    match c
        .has(C::Object)
        .then_some(c)
        .or_else(|| c.value_child(C::Member))
    {
        None => p.name(c),
        Some(member) => match member.child(C::Object) {
            Some(object) => p.member(c, member, object),
            None => P::Out::default(),
        },
    }
}
