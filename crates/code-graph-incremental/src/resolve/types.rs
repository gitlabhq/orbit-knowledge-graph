use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::Ctx;
use super::imports::call_edge;
use crate::canonical::Canonical as C;
use crate::constants::{RED_ZONE, STACK_SEGMENT};
use crate::engine::{Cx, Loc, Val, Vals, of};
use crate::modules::core::ops::{Chain, Parts, ancestors, chain, members};
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Compact, Edge, EdgeKind, Role, infer_return_type, is_name_text, reachable};

type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

pub(super) enum Owner<'a> {
    Class(Cursor<'a>),
    External((u32, u32)),
}

#[derive(Default)]
pub(crate) struct FileWalk {
    wild: Vec<u32>,
    named: FxHashSet<u32>,
    bound_in: FxHashMap<u32, FxHashSet<u32>>,
}

type Root<'r, 'a> = &'r dyn Fn(&mut Cx<'a>, Cursor<'a>) -> Option<Cursor<'a>>;

impl<'a> Cx<'a> {
    fn fam(&self) -> &'a Ctx<'a> {
        self.family.expect("a family phase")
    }

    fn cursor(&self, vals: &Vals) -> Option<Cursor<'a>> {
        vals.first().and_then(|v| v.loc()).map(|loc| self.jump(loc))
    }

    pub(super) fn resolve_file(ctx: &'a Ctx<'a>, fi: usize) -> Result<Vec<Edge>, Killed> {
        let root = ctx.corpus.jump(fi as u32, 0);
        let local = |n: Cursor| {
            n.child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(n.sym())
        };
        let names = root
            .descendants()
            .filter(|c| c.is(C::Import))
            .flat_map(|i| i.names());
        let (wild, named): (Vec<Cursor>, Vec<Cursor>) =
            names.partition(|n| local(*n) == ctx.wildcard_sym);
        let file =
            Sentinel::new("resolve", &ctx.trees[fi].label, ctx.file_resolve_ms).on_thread_cpu();
        let mut cx = Cx::for_family(ctx, fi as u32, Some(file));
        cx.walk = FileWalk {
            wild: wild.iter().map(|n| n.index()).collect(),
            named: named.iter().map(|n| local(*n)).collect(),
            bound_in: FxHashMap::default(),
        };
        ctx.env.pipeline.phase("resolve").flow(&mut cx, root);
        match cx.killed {
            Some(killed) => Err(killed),
            None => Ok(cx.edges),
        }
    }

    pub(super) fn resolve_chain(&mut self, c: Cursor<'a>) -> Option<Cursor<'a>> {
        let found = self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()));
        self.cursor(&found)
    }

    pub(super) fn chain_from(&mut self, c: Cursor<'a>, root: Root<'_, 'a>) -> Option<Cursor<'a>> {
        chain(&mut Rooted { r: self, root }, c)
    }

    pub(crate) fn names(&mut self, c: Cursor<'a>) -> Option<Cursor<'a>> {
        if self.fam().run.check().is_err() {
            self.cut();
            return None;
        }
        self.chain_from(c, &|r, c| {
            r.resolve_reference(c)
                .or_else(|| r.enclosing_alias(c))
                .or_else(|| r.visible_type(c.fi(), c.sym()))
        })
    }

    pub(super) fn resolve_reference(&self, reference: Cursor<'a>) -> Option<Cursor<'a>> {
        let references = &self.fam().references;
        reachable(Loc::from(reference), |id| references.get(&id).copied())
            .skip(1)
            .map(|loc| self.jump(loc))
            .find(|node| node.is(C::Def))
    }

    fn enclosing_alias(&mut self, r: Cursor<'a>) -> Option<Cursor<'a>> {
        let sym = r.sym();
        r.ancestors().filter(|a| a.is(C::Def)).find_map(|d| {
            if d.children_of(C::Alias).any(|a| a.sym() == sym) {
                return Some(d);
            }
            let bound = d
                .children()
                .filter(|c| c.is(C::Binding) && c.sym() == sym)
                .filter(|c| {
                    c.has(C::Qualifier) || (!r.has(C::Qualifier) && c.children().count() == 1)
                })
                .find_map(|c| c.child(C::SsaTyped))
                .filter(|bound| bound.sym() != sym)?;
            self.resolve_chain(bound)
        })
    }

    pub(super) fn visible_type(&self, fi: u32, sym: u32) -> Option<Cursor<'a>> {
        let loc = self.fam().visible[fi as usize]
            .get(&sym)
            .filter(|_| !self.fam().ambiguous.contains(&(fi, sym)))?;
        Some(self.jump(*loc))
    }

    fn value_type(&mut self, d: Cursor<'a>) -> Option<Cursor<'a>> {
        if d.has(C::EnumVariant) {
            return d.enclosing_def(&[C::Enum]);
        }
        let declared = if d.has(C::TypeAlias) {
            d.child(C::SsaTyped)
        } else if d.has(C::FieldDef) || d.has(C::Property) {
            let typed = d.child(C::Binding).and_then(Cursor::typed);
            typed.or_else(|| d.child(C::SsaReturnType))
        } else {
            None
        };
        match declared {
            Some(ty) => self.resolve_chain(ty),
            None => Some(d),
        }
    }

    pub(super) fn class_of(&mut self, callee: Cursor<'a>) -> Option<Cursor<'a>> {
        let target = self.value_type(callee)?;
        if target.is_class() {
            Some(target)
        } else if let Some(ty) = target.child(C::SsaReturnType) {
            self.resolve_chain(ty)
        } else if let Some(branch) = target
            .descendants_pruned(|n| n.is(C::Def) || n.is(C::Lambda))
            .find(|n| n.is(C::SsaReturn))
            .and_then(|r| r.child(C::SsaBranch))
        {
            self.branch_type(branch)
        } else {
            infer_return_type(target).and_then(|sym| self.visible_type(target.fi(), sym))
        }
    }

    fn branch_type(&mut self, branch: Cursor<'a>) -> Option<Cursor<'a>> {
        let arms: Vec<_> = branch
            .children()
            .filter(|a| a.is(C::SsaArm))
            .filter_map(|a| {
                let tail = a.tail_expr();
                if tail.is(C::SsaReturn)
                    || tail.is(C::SsaBranch) && !tail.children().any(|c| c.is(C::SsaArm))
                {
                    return None;
                }
                Some(if tail.is(C::SsaBranch) {
                    stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || self.branch_type(tail))
                } else {
                    tail.child(C::Callee)
                        .and_then(|callee| self.resolve_chain(callee))
                })
            })
            .collect();
        self.lub(arms)
    }

    fn lub(&mut self, classes: Vec<Option<Cursor<'a>>>) -> Option<Cursor<'a>> {
        let mut sets = classes
            .into_iter()
            .map(|c| c.filter(|t| t.is_class()).map(|t| ancestors(self, t)));
        let mut common = sets.next()??;
        for set in sets {
            common.retain(|t| set.as_ref().is_some_and(|a| a.contains(t)));
        }
        let above: FxHashMap<Loc, FxHashSet<Loc>> = common
            .iter()
            .map(|&u| (u, ancestors(self, self.jump(u))))
            .collect();
        let mut least = common
            .iter()
            .filter(|t| !common.iter().any(|u| u != *t && above[u].contains(*t)));
        let &least_one = least.next()?;
        least.next().is_none().then(|| self.jump(least_one))
    }

    pub(super) fn producer_class(&mut self, producer: Cursor<'a>) -> Option<Cursor<'a>> {
        if producer.typed().is_none()
            && let Some(rhs) = producer.bare_rhs()
        {
            return self.resolve_chain(rhs);
        }
        let callee = if producer.is(C::Binding) {
            self.resolve_chain(producer.typed()?)?
        } else {
            self.callee_of(producer)?
        };
        self.class_of(callee)
    }

    fn callee_of(&mut self, call: Cursor<'a>) -> Option<Cursor<'a>> {
        let ctx = self.fam();
        let local = ctx
            .call_at_site
            .get(&(call.fi(), call.index()))
            .map(|e| ctx.corpus.follow(e))
            .or_else(|| Some(self.jump(*ctx.wave_callees.get(&Loc::from(call))?)));
        local.or_else(|| self.resolve_chain(call.child(C::Callee)?))
    }

    pub(crate) fn site_class(
        &mut self,
        site: Loc,
        producer: Option<Loc>,
        class: Option<Loc>,
    ) -> Option<Cursor<'a>> {
        let ctx = self.fam();
        let reaching = ctx.producers.get(&(site.fi, site.node))?;
        let classes = reaching
            .iter()
            .map(|e| {
                let reached = ctx.corpus.follow(e);
                match producer == Some(reached.into()) {
                    true => class.map(|c| self.jump(c)),
                    false => self.producer_class(reached),
                }
            })
            .collect();
        self.lub(classes)
    }

    pub(super) fn resolve_type_edges(&mut self, ce: &Edge) -> Vec<Edge> {
        let Some((site, uses)) = ce
            .site
            .and_then(|site| Some((site, self.fam().type_uses.get(&(ce.from_tree, site))?)))
        else {
            return vec![];
        };
        let producer = self.fam().corpus.jump(ce.from_tree, site);
        let class = self.class_of(self.fam().corpus.follow(ce));
        self.dispatch(producer, class, uses)
    }

    pub(super) fn dispatch(
        &mut self,
        producer: Cursor<'a>,
        class: Option<Cursor<'a>>,
        uses: &[&Edge],
    ) -> Vec<Edge> {
        let ctx = self.fam();
        let mut own_class = None;
        let mut out = Vec::new();
        for usage in uses {
            let Some(site) = usage.site else { continue };
            let call = ctx.corpus.jump(usage.from_tree, site);
            let class = match ctx.producers.get(&(usage.from_tree, site)) {
                Some(reaching) if reaching.len() > 1 => {
                    let own = match own_class {
                        Some(own) => own,
                        None => {
                            let own = class.map(Loc::from)
                                == self.producer_class(producer).map(Loc::from);
                            *own_class.insert(own)
                        }
                    };
                    let site: Vals = SmallVec::from_elem(Val::Node(Loc::from(call)), 1);
                    let key = match own {
                        true => [site, Vals::new(), Vals::new()],
                        false => [site, of([producer]), of(class)],
                    };
                    let found = self.call(crate::fn_id!("core", "site_class"), &key);
                    self.cursor(&found).or(class)
                }
                _ => class,
            };
            let from = ctx.corpus.jump(usage.from_tree, usage.from_node);
            let Some(class) = class else {
                if call
                    .child(C::Callee)
                    .is_some_and(|callee| callee.sym_opt().is_some())
                    && let Some(imports) = ctx.saved_imports.get(&(producer.fi(), producer.index()))
                {
                    out.extend(imports.iter().map(|&node| Edge {
                        site: usage.site,
                        ..from.edge_to(from.jump(producer.fi(), node), EdgeKind::Imports)
                    }));
                    continue;
                }
                let (Some(external), Some(member)) = (self.external_of(producer), call.member())
                else {
                    continue;
                };
                let targets = self.extension_members(
                    Owner::External(external),
                    member.sym(),
                    usage.from_fi(),
                );
                out.extend(targets.into_iter().map(|t| call_edge(from, t, usage.site)));
                continue;
            };
            let class = match call.member().and_then(|m| m.child(C::Object)) {
                Some(object) if object.value_child(C::Member).is_some() => {
                    match self.chain_from(object, &|_, _| Some(class)) {
                        Some(class) => class,
                        None => continue,
                    }
                }
                _ => class,
            };
            if call.is(C::Binding) && class.initializer().and_then(Cursor::rhs_callee).is_none() {
                continue;
            }
            let targets = match call.member() {
                Some(member) => self.member_targets(member, class, call.role()),
                None => match class.child_sym(C::Callable) {
                    None => vec![class],
                    Some(name) => self.method_up(class, name, usage.from_fi(), Role::Callable),
                },
            };
            out.extend(targets.into_iter().map(|t| call_edge(from, t, usage.site)));
        }
        out
    }

    pub(super) fn external_of(&self, producer: Cursor<'a>) -> Option<(u32, u32)> {
        let named = match producer.is(C::Binding) {
            true => producer.typed(),
            false => producer.child(C::Callee),
        };
        let named = named.filter(|t| !t.has(C::Member))?;
        if self.visible_type(named.fi(), named.sym()).is_some() {
            return None;
        }
        self.fam().imports[named.fi() as usize]
            .get(&named.sym())
            .copied()
    }

    pub(super) fn qualified(
        &mut self,
        sym: u32,
        root: &dyn Fn(&mut Self, u32) -> Option<Cursor<'a>>,
    ) -> Option<Cursor<'a>> {
        if let Some(found) = root(self, sym) {
            return Some(found);
        }
        let syms = &self.fam().lang.syms;
        let mut segments = syms
            .resolve(sym)
            .split(self.fam().support_lang.fqn_separator());
        let mut current = root(self, syms.lookup(segments.next()?))?;
        for segment in segments {
            let name = syms.lookup(segment);
            current = self
                .method_up(current, name, current.fi() as usize, Role::Value)
                .into_iter()
                .exactly_one()
                .ok()?;
        }
        Some(current)
    }

    pub(crate) fn method_up(
        &mut self,
        class: Cursor<'a>,
        name: u32,
        fi: usize,
        role: Role,
    ) -> Vec<Cursor<'a>> {
        let inherited = members(self, class, name, role);
        if !inherited.is_empty() {
            return inherited;
        }
        self.extension_members(Owner::Class(class), name, fi)
    }

    pub(crate) fn member_targets(
        &mut self,
        member: Cursor<'a>,
        receiver: Cursor<'a>,
        role: Role,
    ) -> Vec<Cursor<'a>> {
        let Some(constraint) = member.child(C::Dispatch) else {
            return self.method_up(receiver, member.sym(), member.fi() as usize, role);
        };
        let Some(contract) = self
            .resolve_reference(constraint)
            .or_else(|| {
                self.qualified(constraint.sym(), &|r, name| {
                    r.visible_type(constraint.fi(), name)
                })
            })
            .filter(|owner| owner.is_dispatch_contract())
        else {
            return Vec::new();
        };
        let owners: Parts<'a> = self.impl_blocks(receiver).into_iter().collect();
        let mut implementations = std::iter::once(receiver).chain(owners).filter(|owner| {
            owner
                .children_of(C::SuperType)
                .filter_map(|constraint| self.resolve_chain(constraint))
                .any(|target| Loc::from(target) == Loc::from(contract))
        });
        let Some(implementation) = implementations.next() else {
            return Vec::new();
        };
        if implementations.next().is_some() {
            return Vec::new();
        }
        let (index, tags) = (self.members_index(), &self.tags);
        index
            .declared(implementation, member.sym(), role, tags)
            .or_else(|| {
                index
                    .declared(contract, member.sym(), role, tags)
                    .filter(|method| !method.has(C::Declaration))
            })
            .into_iter()
            .collect()
    }

    fn extension_members(&self, owner: Owner<'a>, name: u32, fi: usize) -> Vec<Cursor<'a>> {
        let ctx = self.fam();
        let extends_owner = |method: Cursor<'a>| {
            let receiver = method
                .enclosing_def(&[C::ImplBlock])?
                .child_sym(C::DefName)?;
            let same = match owner {
                Owner::Class(cls) => {
                    Loc::from(self.visible_type(method.fi(), receiver)?) == Loc::from(cls)
                }
                Owner::External(id) => {
                    self.visible_type(method.fi(), receiver).is_none()
                        && ctx.imports[method.fi() as usize].get(&receiver) == Some(&id)
                }
            };
            same.then_some(method)
        };
        let candidates = ctx.extensions.get(&name).into_iter().flatten();
        candidates
            .filter(|l| l.fi as usize == fi || ctx.exporters[fi].contains(&l.fi))
            .filter_map(|&l| extends_owner(self.jump(l)))
            .collect()
    }

    fn impl_blocks(&self, class: Cursor<'a>) -> Parts<'a> {
        let owner = class
            .child(C::DefName)
            .filter(|_| class.has(C::ImplBlock))
            .and_then(|name| self.resolve_reference(name))
            .unwrap_or(class);
        let implementations = self
            .fam()
            .implementations
            .get(&owner.into())
            .into_iter()
            .flatten();
        std::iter::once(owner)
            .chain(implementations.map(|&loc| self.jump(loc)))
            .filter(|node| Loc::from(*node) != Loc::from(class))
            .collect()
    }

    pub(crate) fn inherited(&mut self, class: Cursor<'a>, parent: Cursor<'a>) -> bool {
        let own: FxHashSet<u32> = class
            .descendants_pruned(|n| {
                n.index() != class.index() && n.is_class() && !n.has(C::ImplBlock)
            })
            .filter(|n| n.is(C::Def))
            .filter_map(|n| n.child_sym(C::DefName))
            .collect();
        let first = self.edges.len();
        for call in class.calls() {
            let owned = call
                .enclosing(|e| e.is_class())
                .is_some_and(|o| o.index() == class.index());
            let callee = call.child(C::Callee).and_then(|k| {
                k.child_sym(C::Ivar)
                    .or_else(|| call.has_tag(self.fam().tags.implicit_self).then(|| k.sym()))
            });
            let from = call.enclosing(|e| e.is(C::Def)).filter(|_| owned);
            let Some((from, name)) = from.zip(callee).filter(|(_, name)| !own.contains(name))
            else {
                continue;
            };
            let targets = self.method_up(parent, name, self.fi as usize, call.role());
            let site = Some(call.index());
            self.edges
                .extend(targets.into_iter().map(|t| call_edge(from, t, site)));
        }
        self.edges.len() > first
    }

    pub(crate) fn destructure_calls(&mut self, d: Cursor<'a>, from: Cursor<'a>) -> bool {
        let class = match (d.value_child(C::Call), d.child(C::Rhs)) {
            (Some(pattern), _) => pattern.child(C::Callee).and_then(|k| self.resolve_chain(k)),
            (None, Some(value)) => match value.value_child(C::Call) {
                Some(call) => self.producer_class(call),
                None => value
                    .sym_opt()
                    .and_then(|s| {
                        from.descendants()
                            .find(|b| b.is(C::Binding) && b.sym_opt() == Some(s))
                    })
                    .and_then(Cursor::typed)
                    .and_then(|typed| self.resolve_chain(typed)),
            },
            (None, None) => None,
        };
        let Some(class) = class else {
            return false;
        };
        let slots: Vec<Cursor> = d
            .children()
            .filter(|s| s.is(C::Binding) || s.is(C::Destructure))
            .collect();
        let components: Vec<Cursor> = class
            .child(C::Positional)
            .map(|p| p.children().filter(|c| c.is(C::Binding)).collect())
            .unwrap_or_default();
        if components.len() != slots.len() {
            return false;
        }
        let first = self.edges.len();
        for (slot, component) in slots.iter().zip(components) {
            let targets = self.method_up(class, component.sym(), self.fi as usize, Role::Callable);
            let site = Some(slot.index());
            self.edges
                .extend(targets.into_iter().map(|t| call_edge(from, t, site)));
        }
        self.edges.len() > first
    }

    pub(crate) fn unbound(&self, sym: u32) -> Vals {
        let walk = &self.walk;
        let ctx = self.fam();
        let builtins = &ctx
            .env
            .rules_for(&ctx.trees[self.fi as usize].label)
            .config
            .link
            .builtins;
        if !is_name_text(ctx.lang.syms.resolve(sym))
            || walk.named.contains(&sym)
            || builtins.contains(&sym)
        {
            return Vals::new();
        }
        let fi = self.fi;
        of(walk.wild.iter().map(|&node| Loc { fi, node }))
    }

    pub(crate) fn unbound_object(&mut self, from: Cursor<'a>, object: Cursor<'a>) -> Vals {
        let bound = self.walk.bound_in.entry(from.index()).or_insert_with(|| {
            from.descendants()
                .filter(|b| b.is(C::Binding))
                .filter_map(|b| b.sym_opt())
                .collect()
        });
        match bound.contains(&object.sym()) {
            true => Vals::new(),
            false => self.unbound(object.sym()),
        }
    }
}

impl<'a> Cx<'a> {
    pub(crate) fn family_parts(&mut self, owner: Cursor<'a>) -> Parts<'a> {
        let ctx = self.fam();
        let mut parts: Parts<'a> = SmallVec::from_elem(owner, 1);
        parts.extend(self.impl_blocks(owner));
        let key = super::partial_key(owner, ctx.packages).and_then(|k| ctx.partials.get(&k));
        for &loc in key.into_iter().flatten() {
            if !parts.iter().any(|p| Loc::from(*p) == loc) {
                parts.push(self.jump(loc));
            }
        }
        parts
    }

    pub(crate) fn family_supers(&mut self, part: Cursor<'a>) -> Parts<'a> {
        let mut supers: Parts<'a> = part
            .children()
            .filter(|s| s.is(C::SuperType))
            .filter_map(|s| self.resolve_chain(s))
            .collect();
        let linked = self
            .fam()
            .extends_of
            .get(&Loc::from(part))
            .into_iter()
            .flatten();
        supers.extend(linked.map(|&loc| self.jump(loc)));
        supers
    }
}

struct Rooted<'r, 'q, 'a> {
    r: &'r mut Cx<'a>,
    root: Root<'q, 'a>,
}

impl<'a> Chain<'a> for Rooted<'_, '_, 'a> {
    type Out = Option<Cursor<'a>>;

    fn name(&mut self, c: Cursor<'a>) -> Self::Out {
        let found = (self.root)(self.r, c)?;
        self.r.value_type(found)
    }

    fn member(&mut self, c: Cursor<'a>, member: Cursor<'a>, object: Cursor<'a>) -> Self::Out {
        if c.labelled()
            && let Some(found) = (self.root)(self.r, c).and_then(|r| self.r.value_type(r))
        {
            return Some(found);
        }
        let receiver = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || chain(self, object))?;
        let targets = self.r.member_targets(member, receiver, c.role());
        let member = targets.into_iter().exactly_one().ok()?;
        self.r.value_type(member)
    }
}
