use super::{LogicalBuilder, LogicalOperation, PlanError, Rows};
use crate::RelationshipId;
use crate::plan::tracking::{Declare, Outputs, Ref};

#[derive(Debug)]
pub struct Detached<'m> {
    pub operation: LogicalOperation,
    pub inputs: Vec<Rows<'m>>,
    pub outputs: Vec<Ref>,
}

impl<'m> LogicalBuilder<'m> {
    pub fn outputs(&self, rows: &Rows<'m>) -> Result<Vec<Ref>, PlanError> {
        Ok(self.construction_ref().outputs(rows.inner())?)
    }

    pub fn detach(&mut self, rows: Rows<'m>) -> Result<Detached<'m>, PlanError> {
        let outputs = self.outputs(&rows)?;
        let (operation, inputs) = self.construction().detach(rows.into_inner())?;
        let inputs = inputs
            .into_iter()
            .map(|rows| self.view(rows))
            .collect::<Result<_, _>>()?;
        Ok(Detached {
            operation,
            inputs,
            outputs,
        })
    }

    pub fn reattach(&mut self, outputs: &[Ref], rows: Rows<'m>) -> Result<Rows<'m>, PlanError> {
        let fresh = self.outputs(&rows)?;
        for (old, new) in outputs.iter().zip(fresh) {
            if *old != new {
                self.reconnect(*old, new)?;
            }
        }
        Ok(rows)
    }

    pub fn retype(
        &mut self,
        rows: Rows<'m>,
        relationships: Vec<RelationshipId>,
    ) -> Result<Rows<'m>, PlanError> {
        let Detached {
            operation,
            mut inputs,
            outputs,
        } = self.detach(rows)?;
        let operation = match operation {
            LogicalOperation::Follow {
                from,
                direction,
                range,
                hops,
                ..
            } => LogicalOperation::Follow {
                hops: hops
                    .into_iter()
                    .filter(|hop| relationships.contains(&hop.relationship))
                    .collect(),
                from,
                relationships,
                direction,
                range,
            },
            LogicalOperation::Neighbors {
                from, direction, ..
            } => LogicalOperation::Neighbors {
                from,
                relationships,
                direction,
            },
            _ => return Err(PlanError::shape("only steps carry relationships")),
        };
        let added = &outputs[outputs.len().saturating_sub(2)..];
        let declares = added
            .iter()
            .map(|output| {
                let (name, value) = self.construction_ref().value(*output)?;
                Ok(Declare {
                    name: name.map(str::to_string),
                    value: *value,
                })
            })
            .collect::<Result<Vec<_>, PlanError>>()?;
        let input = inputs.remove(0);
        let rows = self.connect(operation, vec![input], |_, _, _| {
            Ok(Outputs::extend(declares))
        })?;
        self.reattach(&outputs, rows)
    }
}
