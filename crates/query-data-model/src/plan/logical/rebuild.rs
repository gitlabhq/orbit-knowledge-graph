use super::{Logical, LogicalBuilder, LogicalOperation, LogicalPlan, LogicalType, PlanError, Rows};
use crate::plan::tracking::{self, BodyId, Construction, Hook, Outputs, Rebuild, Rebuilder};

impl<'m> Rebuilder for LogicalBuilder<'m> {
    type Language = Logical;
    type Old = LogicalPlan;
    type Rows = Rows<'m>;
    type Error = PlanError;

    fn plan(old: &LogicalPlan) -> &LogicalPlan {
        old
    }

    fn construction(&mut self) -> &mut Construction<Logical> {
        LogicalBuilder::construction(self)
    }

    fn rows<'r>(rows: &'r Rows<'m>) -> &'r tracking::Rows {
        rows.inner()
    }

    fn apply(
        &mut self,
        operation: LogicalOperation,
        inputs: Vec<Rows<'m>>,
        outputs: Outputs<LogicalType>,
    ) -> Result<Rows<'m>, PlanError> {
        LogicalBuilder::apply(self, operation, inputs, outputs)
    }

    fn bodies(operation: &mut LogicalOperation, visit: &mut dyn FnMut(&mut BodyId)) {
        if let LogicalOperation::Read(body) = operation {
            visit(body);
        }
    }

    fn scope<H: Hook<Self>>(
        rebuild: &mut Rebuild<'_, Self>,
        hook: &mut H,
        q: &mut Self,
        body: Option<BodyId>,
    ) -> Result<(), PlanError> {
        let old = rebuild.old();
        for inner in old.defines(body) {
            let (name, root) = old.body(inner);
            let built = q.share(name, |q| {
                hook.scope(rebuild, q, Some(inner))?;
                rebuild.visit(hook, q, root)
            })?;
            rebuild.define(inner, built.id());
        }
        Ok(())
    }
}
