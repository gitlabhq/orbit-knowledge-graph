use super::render::{definition_name, output_name, relation_name};
use super::*;

impl<M: QueryDataModel + ?Sized, E: std::fmt::Debug, O: std::fmt::Debug> QueryGraph<'_, M, E, O> {
    pub fn explain(&self, root: BlockId) -> Result<String> {
        self.validate(root, |_, _, _| Ok(()))?;
        let mut text = String::new();
        self.explain_block(root, 0, &mut text)?;
        Ok(text)
    }

    fn explain_block(&self, id: BlockId, depth: usize, text: &mut String) -> Result<()> {
        use std::fmt::Write;
        let block = self.block(id)?;
        let indent = "  ".repeat(depth);
        writeln!(text, "{indent}(Block b{}", id.slot).unwrap();
        for (slot, definition) in block.definitions.iter().enumerate() {
            writeln!(
                text,
                "{indent}  (CTE {} {:?} recursive={}",
                definition_name(DefinitionId { block: id, slot }),
                definition.hint,
                definition.recursive
            )
            .unwrap();
            self.explain_block(definition.body, depth + 2, text)?;
            writeln!(text, "{indent}  )").unwrap();
        }
        match &block.body {
            Body::Select {
                relations,
                outputs,
                operation,
            } => {
                writeln!(text, "{indent}  (Operation {operation:#?})").unwrap();
                for (slot, relation) in relations.iter().enumerate() {
                    writeln!(
                        text,
                        "{indent}  (Relation {} {:?}",
                        relation_name(RelationId { block: id, slot }),
                        relation.hint
                    )
                    .unwrap();
                    match relation.source {
                        Source::Stored(table) => {
                            writeln!(text, "{indent}    (Scan {})", table.name()).unwrap()
                        }
                        Source::Derived(body) => self.explain_block(body, depth + 2, text)?,
                        Source::Definition(definition) => writeln!(
                            text,
                            "{indent}    (Reference {})",
                            definition_name(definition)
                        )
                        .unwrap(),
                    }
                    writeln!(text, "{indent}  )").unwrap();
                }
                for (slot, output) in outputs.iter().enumerate() {
                    writeln!(
                        text,
                        "{indent}  (Output {} {:?} {:?})",
                        output_name(OutputId { block: id, slot }),
                        output.label,
                        output.value
                    )
                    .unwrap();
                }
            }
            Body::UnionAll { arms, labels } => {
                writeln!(text, "{indent}  (UnionAll {labels:?}").unwrap();
                for arm in arms {
                    self.explain_block(*arm, depth + 2, text)?;
                }
                writeln!(text, "{indent}  )").unwrap();
            }
        }
        writeln!(text, "{indent})").unwrap();
        Ok(())
    }
}
