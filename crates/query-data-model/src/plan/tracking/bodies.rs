use super::plan::BodyInfo;
use super::{
    Body, BodyId, Connected, Construction, Declare, Language, Output, Outputs, Rejected, Rows,
    ScopeId,
};

#[derive(Debug)]
pub struct Opening {
    scope: ScopeId,
    name: String,
}

impl<L: Language> Construction<L> {
    pub fn open(&mut self, name: impl Into<String>) -> Result<Opening, Rejected> {
        let name = name.into();
        self.attempt(|this| {
            this.arena.parents.push(Some(this.scope));
            this.scope = ScopeId(this.arena.parents.len() - 1);
            Ok(Opening {
                scope: this.scope,
                name,
            })
        })
    }

    pub fn close(&mut self, opening: Opening, root: Rows) -> Result<Body, Rejected> {
        self.attempt(|this| {
            if opening.scope != this.scope {
                return Err(Rejected::OutOfScope);
            }
            let root = this.node(&root)?;
            this.arena.nodes[root.0].consumed = true;
            this.arena.bodies.push(BodyInfo {
                name: opening.name,
                scope: opening.scope,
                root,
            });
            this.scope = this.arena.parents[opening.scope.0].unwrap_or(ScopeId(0));
            Ok(Body {
                plan: this.arena.plan,
                body: BodyId(this.arena.bodies.len() - 1),
            })
        })
    }

    pub fn share(&mut self, name: impl Into<String>, root: Rows) -> Result<Body, Rejected> {
        let name = name.into();
        self.attempt(|this| {
            let root = this.node(&root)?;
            this.arena.parents.push(Some(this.scope));
            let scope = ScopeId(this.arena.parents.len() - 1);
            let mut stack = vec![root];
            while let Some(node) = stack.pop() {
                let node = &mut this.arena.nodes[node.0];
                if node.scope == this.scope {
                    node.scope = scope;
                    stack.extend(node.inputs.iter().copied());
                }
            }
            this.arena.nodes[root.0].consumed = true;
            this.arena.bodies.push(BodyInfo { name, scope, root });
            Ok(Body {
                plan: this.arena.plan,
                body: BodyId(this.arena.bodies.len() - 1),
            })
        })
    }

    pub fn read(&mut self, body: Body, operation: L::Operation) -> Result<Connected, Rejected> {
        self.attempt(|this| {
            if body.plan != this.arena.plan {
                return Err(Rejected::ForeignPlan);
            }
            let info = &this.arena.bodies[body.body.0];
            let defined = this.arena.parents[info.scope.0].unwrap_or(ScopeId(0));
            if !this.arena.sees(this.scope, defined) {
                return Err(Rejected::BodyNotVisible(info.name.clone()));
            }
            let outputs = this.arena.nodes[info.root.0]
                .contract
                .iter()
                .map(|output| {
                    let output = &this.arena.outputs[output.0];
                    Output::Declare(Declare {
                        name: output.name.clone(),
                        value: output.value.clone(),
                    })
                })
                .collect::<Vec<_>>();
            this.connect(operation, Vec::new(), Outputs::replace(outputs))
        })
    }
}
