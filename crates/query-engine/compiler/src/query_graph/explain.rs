use super::*;
use std::fmt::Write;

impl<M: QueryDataModel + ?Sized, L: std::fmt::Debug> QueryGraph<'_, M, L> {
    pub fn explain(&self, root: BlockId) -> Result<String> {
        let mut text = String::new();
        for id in self.reachable_blocks(root)? {
            let block = self.block(id)?;
            writeln!(text, "(Block {id:?}").unwrap();
            for relation in &block.relations {
                writeln!(text, "  (Relation {} {:?})", relation.hint, relation.source).unwrap();
            }
            let operation = self.query_operation(id)?;
            match &operation.kind {
                QueryKind::Project(input) => writeln!(text, "  (Operation {input:#?})").unwrap(),
                QueryKind::UnionAll(arms) => writeln!(text, "  (UnionAll {arms:?})").unwrap(),
            }
            for output in &operation.outputs {
                writeln!(
                    text,
                    "  (Output {} {:?} {:?})",
                    output.label, output.data_type, output.value
                )
                .unwrap();
            }
            writeln!(text, ")").unwrap();
        }
        Ok(text)
    }
}
