use std::collections::{HashMap, HashSet};

use super::{BodyId, Construction, Language, NodeId, Outputs, Plan, Ref, Rejected, Rows};

pub type Op<Q> = <<Q as Rebuilder>::Language as Language>::Operation;
pub type Value<Q> = <<Q as Rebuilder>::Language as Language>::Value;

/// A language builder that a rebuild drives: it replays each kept operation through its checks.
pub trait Rebuilder: Sized {
    type Language: Language;
    type Old;
    type Rows;
    type Error: From<Rejected>;

    fn plan(old: &Self::Old) -> &Plan<Self::Language>;
    fn construction(&mut self) -> &mut Construction<Self::Language>;
    fn rows(rows: &Self::Rows) -> &Rows;
    fn replay(
        &mut self,
        operation: Op<Self>,
        inputs: Vec<Self::Rows>,
        outputs: Outputs<Value<Self>>,
    ) -> Result<Self::Rows, Self::Error>;

    fn bodies(_operation: &mut Op<Self>, _visit: &mut dyn FnMut(&mut BodyId)) {}

    /// Rebuilds the bodies a scope defines, before any operation of the scope reads them.
    fn scope<H: Hook<Self>>(
        _rebuild: &mut Rebuild<'_, Self>,
        _hook: &mut H,
        _q: &mut Self,
        _body: Option<BodyId>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait Hook<Q: Rebuilder>: Sized {
    fn node(
        &mut self,
        rebuild: &mut Rebuild<'_, Q>,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Q::Rows, Q::Error>;

    fn body(
        &mut self,
        rebuild: &mut Rebuild<'_, Q>,
        _q: &mut Q,
        body: BodyId,
    ) -> Result<BodyId, Q::Error> {
        Ok(rebuild.defined(body)?)
    }

    fn scope(
        &mut self,
        rebuild: &mut Rebuild<'_, Q>,
        q: &mut Q,
        body: Option<BodyId>,
    ) -> Result<(), Q::Error> {
        Q::scope(rebuild, self, q, body)
    }
}

pub struct Rebuild<'p, Q: Rebuilder> {
    old: &'p Q::Old,
    pub(super) refs: HashMap<Ref, Ref>,
    pub(super) bodies: HashMap<BodyId, BodyId>,
    pub(super) exposed: HashSet<Ref>,
    pub(super) released: HashSet<Ref>,
}

/// Rebuilds `old` into the construction of `q`: inputs before their consumers, each defined body
/// once, and only the operations of `old` reach `hook`.
pub fn rebuild<Q: Rebuilder, H: Hook<Q>>(
    q: &mut Q,
    old: &Q::Old,
    hook: &mut H,
) -> Result<Q::Rows, Q::Error> {
    let plan = Q::plan(old);
    if plan.snapshot() != q.construction().snapshot() {
        return Err(Rejected::ForeignSnapshot.into());
    }
    let mut rebuild = Rebuild {
        old,
        refs: HashMap::new(),
        bodies: HashMap::new(),
        exposed: HashSet::new(),
        released: HashSet::new(),
    };
    hook.scope(&mut rebuild, q, None)?;
    let root = rebuild.visit(hook, q, plan.root())?;
    let order: Vec<Ref> = plan
        .declaration_order()
        .map(|output| rebuild.output(output))
        .collect::<Result<_, _>>()?;
    if !order.is_empty() {
        q.construction().declare_order(&order)?;
    }
    Ok(root)
}

impl<'p, Q: Rebuilder> Rebuild<'p, Q> {
    pub fn old(&self) -> &'p Q::Old {
        self.old
    }

    pub(super) fn plan(&self) -> &'p Plan<Q::Language> {
        Q::plan(self.old)
    }

    pub fn visit<H: Hook<Q>>(
        &mut self,
        hook: &mut H,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Q::Rows, Q::Error> {
        hook.node(self, q, node)
    }

    pub fn inputs<H: Hook<Q>>(
        &mut self,
        hook: &mut H,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Vec<Q::Rows>, Q::Error> {
        let plan = self.plan();
        plan.inputs(node)
            .iter()
            .map(|input| self.visit(hook, q, *input))
            .collect()
    }

    /// The operation of `node` with its outputs and bodies pointing into the rebuilt plan.
    pub fn operation<H: Hook<Q>>(
        &mut self,
        hook: &mut H,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Op<Q>, Q::Error> {
        let mut operation = self.plan().operation(node).clone();
        let mut read = Vec::new();
        Q::bodies(&mut operation, &mut |body| read.push(*body));
        let mut bodies = HashMap::new();
        for body in read {
            let rebuilt = hook.body(self, q, body)?;
            bodies.insert(body, rebuilt);
        }
        Q::bodies(&mut operation, &mut |body| *body = bodies[body]);
        let mut missing = None;
        Q::Language::references(&mut operation, &mut |output| match self.refs.get(output) {
            Some(mapped) => *output = *mapped,
            None => missing = Some(*output),
        });
        match missing {
            Some(output) => Err(Rejected::NotVisible(self.describe(output)).into()),
            None => Ok(operation),
        }
    }

    pub(super) fn describe(&self, output: Ref) -> String {
        self.plan().arena.describe(output.output)
    }
}
