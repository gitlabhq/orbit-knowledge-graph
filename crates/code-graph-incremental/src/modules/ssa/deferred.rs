use std::rc::Rc;

use smallvec::SmallVec;

use super::{Deferred, Ssa, Value, to_vals};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Val, Vals, one};
use crate::tree::{Edge, EdgeKind, is_name_text};

impl<'t> Cx<'_, 't, Ssa> {
    pub(super) fn pending_value(&mut self, callee: Cursor<'t>) -> Option<Value> {
        let implicit = callee
            .parent()
            .is_some_and(|c| c.has_tag(self.tags().implicit_self));
        match self.field_slot(callee) {
            Some(slot) => Some(self.read_value(slot)),
            None => callee
                .sym_opt()
                .filter(|_| !implicit)
                .and_then(|sym| self.read_phi(sym)),
        }
    }

    pub(super) fn defer(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        let Some(value) = self.pending_value(callee) else {
            return;
        };
        let fallback = (callee.sym_opt().is_some()
            && !callee.has(C::Member)
            && !callee.has(C::Ivar)
            && is_name_text(self.env().lang.syms.resolve(callee.sym())))
        .then(|| Rc::clone(&self.state().wildcards));
        let from = self.enclosing();
        self.write()
            .pending
            .push((value, from, c.index(), Deferred::Callee(fallback)));
    }

    pub(super) fn defer_receiver(&mut self, c: Cursor<'t>) {
        let Some(obj) = c.member().and_then(|m| m.child(C::Object)) else {
            return;
        };
        let leaf = !self.state().loops.is_empty() && obj.children().next().is_none();
        if let Some(phi) = leaf.then(|| self.read_phi(obj.sym())).flatten() {
            let seen = self.lookup(obj.sym());
            let from = self.enclosing();
            let receiver = Deferred::Receiver(obj.index(), seen);
            self.write().pending.push((phi, from, c.index(), receiver));
        }
    }

    /// The edges a reference to `target` makes, by what `target` is.
    fn refs(&mut self, target: Val) -> SmallVec<[(EdgeKind, Loc); 2]> {
        let mut out = SmallVec::new();
        for (kind, f) in [
            (EdgeKind::Calls, crate::fn_id!("core", "callable")),
            (EdgeKind::Imports, crate::fn_id!("core", "imported")),
            (EdgeKind::TypeFlow, crate::fn_id!("core", "produced")),
        ] {
            let found = self.call(f, &[one(target)]);
            out.extend(found.iter().filter_map(|v| v.loc()).map(|loc| (kind, loc)));
        }
        out
    }

    pub(super) fn resolve_pending(&mut self) {
        let pending = std::mem::take(&mut self.write().pending);
        for (value, from, site, deferred) in pending {
            if let Err(killed) = self.check() {
                self.kill(killed);
                return;
            }
            let mut targets = self.write().graph.resolve_value(&value);
            let mut edges: Vec<(EdgeKind, Loc)> = Vec::new();
            match deferred {
                Deferred::Callee(fallback) => {
                    if targets.is_empty() {
                        let wildcards = fallback.iter().flat_map(|w| w.iter());
                        targets.extend(wildcards.map(|&w| Value::ImportRef(w)));
                    }
                    for target in to_vals(&targets) {
                        edges.extend(self.refs(target));
                    }
                }
                Deferred::Receiver(object, seen) => {
                    targets.retain(|value| !seen.contains(value));
                    let Some(member) = self.at(object).parent() else {
                        continue;
                    };
                    let member = one(Val::Node(member.into()));
                    for target in to_vals(&targets) {
                        let args = [one(target), member.clone()];
                        let calls = self.call(crate::fn_id!("core", "member"), &args);
                        edges.extend(
                            calls
                                .iter()
                                .filter_map(|v| v.loc())
                                .map(|l| (EdgeKind::Calls, l)),
                        );
                        for unmatched in self.call(crate::fn_id!("core", "unmatched"), &args) {
                            edges.extend(self.refs(unmatched));
                        }
                    }
                }
            }
            for (kind, to) in edges {
                self.emit(Edge {
                    site: Some(site),
                    ..Edge::local(from, to.node, kind)
                });
            }
        }
    }

    pub(super) fn decorators(&mut self) {
        let declared = std::mem::take(&mut self.write().declared);
        for dn in declared {
            if let Err(killed) = self.check() {
                self.kill(killed);
                return;
            }
            for c in self.at(dn).children().filter(|c| c.is(C::Decorator)) {
                let found: Vals = self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()));
                let nodes = found.iter().filter_map(|v| match v {
                    Val::Node(loc) => Some(*loc),
                    _ => None,
                });
                for target in nodes {
                    let kind = self.at(target.node).kind();
                    if kind == C::Def {
                        self.emit(Edge::local(dn, target.node, EdgeKind::Calls));
                    } else if kind == C::Name {
                        self.emit(Edge {
                            site: Some(c.index()),
                            ..Edge::local(dn, target.node, EdgeKind::Imports)
                        });
                    }
                }
            }
        }
    }
}
