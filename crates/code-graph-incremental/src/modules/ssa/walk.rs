//! Builds a tree's SSA in one walk over its canonical shapes, on the first
//! query, and records what each reference sees. The shapes carry the
//! meaning: imports bind names, named defs open scopes, bindings write their
//! right-hand side after visiting it, branches fork and join, loops get
//! headers, and a call whose callee is assigned later waits for the walk's end.

use smallvec::SmallVec;

use super::Ssa;
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Store, Val, Vals, one};

/// What runs when the walk leaves a node, or between a node's first child
/// group and the rest.
enum Act {
    Leave,
    Fork(Option<u32>),
    LoopHeader(u32),
    Bind {
        target: u32,
        values: Option<u32>,
        from: Option<u32>,
    },
    Arm(u32),
}

enum Step {
    Visit(u32),
    Do(Act),
}

/// The tag keys and data the walk reads.
struct Words {
    scoped: u32,
    hoisted: u32,
    lifted: u32,
    implicit_self: u32,
    type_only: u32,
    weak: u32,
}

const CLASS_LIKE: [C; 6] = [
    C::Class,
    C::Struct,
    C::ImplBlock,
    C::Interface,
    C::Trait,
    C::Enum,
];

impl<'t, S: Store> Cx<'_, 't, Ssa, S> {
    /// Walks the tree once; later reads answer from what it recorded.
    pub(super) fn walked(&mut self) {
        if self.state().walked || self.state().walking {
            return;
        }
        self.write().walking = true;
        let lang = self.lang();
        let words = Words {
            scoped: lang.syms.intern("scoped"),
            hoisted: lang.syms.intern("hoisted"),
            lifted: lang.syms.intern("lifted"),
            implicit_self: lang.syms.intern("implicit_self"),
            type_only: lang.syms.intern("type_only"),
            weak: lang.syms.intern("weak"),
        };
        let root = self.tree().root();
        let mut stack: Vec<Step> = root
            .children_rev()
            .map(|c| Step::Visit(c.index()))
            .collect();
        while let Some(step) = stack.pop() {
            match step {
                Step::Visit(node) => self.visit(&words, self.at(node), &mut stack),
                Step::Do(act) => self.act(act),
            }
        }
        let s = self.write();
        s.graph.seal_remaining();
        s.graph.remove_redundant_phi_sccs();
        let pending: Vec<u32> = s.pending.keys().copied().collect();
        for call in pending {
            let found = self.deferred(self.at(call));
            self.write().late.insert(call, found);
        }
        let s = self.write();
        s.walking = false;
        s.walked = true;
    }

    fn act(&mut self, act: Act) {
        match act {
            Act::Leave => self.leave(),
            Act::Fork(sink) => self.fork(sink.map(|n| self.at(n))),
            Act::LoopHeader(node) => self.loop_header(self.at(node)),
            Act::Bind {
                target,
                values,
                from,
            } => {
                let values = match values {
                    Some(node) => self.required("value_of", node),
                    None => Vals::new(),
                };
                let from =
                    from.and_then(|n| self.required("source", n).first().and_then(|v| v.loc()));
                self.bind(self.at(target), &values, from.map(|l| self.jump(l)));
            }
            Act::Arm(arm) => {
                if !self.required("valued", arm).is_empty() {
                    let values = self.required("value_of", arm);
                    let from = self.required("source", arm).first().and_then(|v| v.loc());
                    self.yields(&values, from.map(|l| self.jump(l)));
                }
                self.join();
            }
        }
    }

    fn required(&mut self, name: &str, node: u32) -> Vals {
        let id = match name {
            "value_of" => crate::fn_id!("ssa", "value_of"),
            "valued" => crate::fn_id!("ssa", "valued"),
            _ => crate::fn_id!("ssa", "source"),
        };
        let at = Val::Node(self.loc(node));
        self.call(id, &[one(at)])
    }

    fn record(&mut self, c: Cursor<'t, S>) {
        if c.sym_opt().is_some() {
            let found = self.read(c);
            self.write().seen.insert(c.index(), found);
        }
        if c.is(C::Call) {
            let wildcards = std::rc::Rc::clone(&self.state().wildcards);
            self.write().wild_at.insert(c.index(), wildcards);
        }
    }

    fn visit(&mut self, w: &Words, c: Cursor<'t, S>, stack: &mut Vec<Step>) {
        self.record(c);
        let mut exits: SmallVec<[Act; 2]> = SmallVec::new();
        let mut children: Option<Vec<Vec<u32>>> = None;
        let mut between: Option<Act> = None;
        let nodes =
            |it: &mut dyn Iterator<Item = Cursor<'t, S>>| it.map(|n| n.index()).collect::<Vec<_>>();
        let named = c.is(C::Def) && c.child_sym(C::DefName).is_some();

        if c.is(C::Import) || c.is(C::ImportType) {
            self.bind_import(c, !c.has_tag(w.weak), w.type_only);
            return;
        }
        if named {
            for s in c.children().filter(|s| s.is(C::SuperType)) {
                for i in s
                    .children()
                    .filter(|i| i.is(C::Import) || i.is(C::ImportType))
                {
                    self.bind_import(i, !i.has_tag(w.weak), w.type_only);
                }
            }
        }
        if c.is(C::Call) {
            for i in c
                .children()
                .filter(|i| i.is(C::Import) || i.is(C::ImportType))
            {
                self.bind_import(i, !i.has_tag(w.weak), w.type_only);
            }
        }
        if named {
            let scoped = c.has_tag(w.scoped);
            self.enter_def(c, scoped.then(|| c.tag(w.scoped)));
            if scoped {
                exits.push(Act::Leave);
            } else {
                let decorators = nodes(&mut c.children().filter(|d| d.is(C::Decorator)));
                children = Some(vec![decorators]);
            }
        } else if c.is(C::Def) {
            children = Some(Vec::new());
        }
        if c.is(C::Decorator)
            && !c
                .parent()
                .is_some_and(|p| p.is(C::Def) && p.has_tag(w.scoped))
        {
            children = Some(Vec::new());
        }
        if c.is(C::Call) && !c.has_tag(w.implicit_self) && self.deferrable(c) {
            self.defer(c);
        }
        if c.is(C::Binding) {
            if let Some(steer) = self.binding_order(w, c, &mut exits) {
                children = Some(steer);
            }
        }
        if c.is(C::Scope) {
            self.scope(c, c.tag(w.scoped), c.has_tag(w.hoisted), None);
            exits.push(Act::Leave);
        }
        if c.is(C::Lambda) {
            self.fork(None);
            exits.push(Act::Leave);
        }
        if c.is(C::SsaBranch) {
            let arms: Vec<u32> = nodes(&mut c.children().filter(|a| a.is(C::SsaArm)));
            if !arms.is_empty() {
                let parent = c.parent();
                let sink = match parent {
                    Some(p) if p.is(C::Rhs) => {
                        p.parent().filter(|b| b.is(C::Binding)).map(|b| b.index())
                    }
                    Some(p) if p.is(C::SsaArm) && p.children().all(|k| k.is(C::SsaBranch)) => {
                        Some(c.index())
                    }
                    _ => None,
                };
                let conds = nodes(&mut c.children().filter(|k| !k.is(C::SsaArm)));
                children = Some(vec![conds, vec![arms[0]], arms[1..].to_vec()]);
                between = Some(Act::Fork(sink));
                exits.push(Act::Leave);
            }
        }
        if c.is(C::SsaArm) && c.parent().is_some_and(|p| p.is(C::SsaBranch)) {
            exits.push(Act::Arm(c.index()));
        }
        if c.is(C::SsaLoop) {
            let lifted = c.children().find(|b| {
                b.is(C::Binding) && b.has(C::Declaration) && !b.has(C::Rhs) && !b.has_tag(w.lifted)
            });
            match lifted {
                Some(b) => {
                    let head = nodes(&mut c.children().take_while(|k| k.index() != b.index()));
                    let rest =
                        nodes(&mut c.children().skip_while(|k| k.index() != b.index()).skip(1));
                    children = Some(vec![head, vec![b.index()], rest]);
                    between = Some(Act::LoopHeader(c.index()));
                }
                None => self.loop_header(c),
            }
            exits.push(Act::Leave);
        }

        if let Some(groups) = &children {
            for skipped in c
                .children()
                .filter(|k| !groups.iter().flatten().any(|&n| n == k.index()))
            {
                for n in std::iter::once(skipped).chain(skipped.descendants()) {
                    self.record(n);
                }
            }
        }
        stack.extend(exits.into_iter().rev().map(Step::Do));
        match children {
            None => stack.extend(c.children_rev().map(|k| Step::Visit(k.index()))),
            Some(groups) => {
                for (at, group) in groups.iter().enumerate().rev() {
                    stack.extend(group.iter().rev().map(|&n| Step::Visit(n)));
                    if at == 1
                        && let Some(act) = between.take()
                    {
                        stack.push(Step::Do(act));
                    }
                }
            }
        }
    }

    /// A call that is not a dispatch through a receiver; `defer` decides
    /// whether its callee or its receiver waits for later assignments.
    fn deferrable(&mut self, call: Cursor<'t, S>) -> bool {
        call.child(C::Callee)
            .is_some_and(|callee| !callee.child(C::Member).is_some_and(|m| m.has(C::Dispatch)))
    }

    /// What a binding writes and in which order its children are visited.
    fn binding_order(
        &mut self,
        w: &Words,
        c: Cursor<'t, S>,
        exits: &mut SmallVec<[Act; 2]>,
    ) -> Option<Vec<Vec<u32>>> {
        let skip = Some(Vec::new());
        let parent = c.parent();
        if c.has(C::Qualifier) {
            let target = Val::Node(self.loc(c.index()));
            self.bind(c, &[target], None);
            return skip;
        }
        if parent.is_some_and(|p| p.is(C::Def) && CLASS_LIKE.iter().any(|&k| p.has(k)))
            && c.sym_opt().is_none()
        {
            return skip;
        }
        if parent.is_some_and(|p| p.is(C::Destructure)) {
            if c.sym_opt().is_some() {
                let target = Val::Node(self.loc(c.index()));
                let from = c.child(C::Rhs).and_then(|r| {
                    self.required("source", r.index())
                        .first()
                        .and_then(|v| v.loc())
                });
                self.bind(c, &[target], from.map(|l| self.jump(l)));
            }
            return skip;
        }
        let in_loop = parent.is_some_and(|p| p.is(C::SsaLoop));
        if in_loop && !c.has(C::Rhs) {
            let calls: Vec<Val> = parent
                .and_then(|p| p.child(C::Arr))
                .map(|arr| {
                    arr.children()
                        .filter(|k| k.is(C::Call))
                        .map(|k| Val::Node(k.into()))
                        .collect()
                })
                .unwrap_or_default();
            if !calls.is_empty() {
                self.bind(c, &calls, None);
                return skip;
            }
            if c.child_sym(C::Declaration).is_some() && self.target(c).is_some() {
                self.bind(c, &[Val::Opaque], None);
                return skip;
            }
        }
        if self.target(c).is_none() {
            return None;
        }
        if !c.has(C::Rhs) && c.child_sym(C::Declaration).is_some() {
            return skip;
        }
        let leading: Vec<u32> = c
            .children()
            .filter(|k| k.is(C::Call))
            .map(|k| k.index())
            .collect();
        let rhs = c.child(C::Rhs);
        let _ = w;
        match rhs {
            Some(r) if r.children().next().is_some_and(|b| b.is(C::SsaBranch)) => {
                let branch = r.children().next().map(|b| b.index()).into_iter().collect();
                let effects = r
                    .children()
                    .filter(|k| k.is(C::SideEffect))
                    .map(|k| k.index())
                    .collect();
                exits.push(Act::Bind {
                    target: c.index(),
                    values: None,
                    from: None,
                });
                Some(vec![branch, effects, leading])
            }
            _ => {
                let values: Vec<u32> = rhs
                    .map(|r| r.children().map(|k| k.index()).collect())
                    .unwrap_or_default();
                exits.push(Act::Bind {
                    target: c.index(),
                    values: Some(c.index()),
                    from: rhs.map(|r| r.index()),
                });
                Some(vec![values, leading])
            }
        }
    }
}
