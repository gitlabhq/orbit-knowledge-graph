use super::{RelationalBuilder, Subquery, supported};
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{
    JoinKind, Operation, ReadMode, Relation, RelationalError, Sharing, Table,
};
use crate::implementations::relational::{RelationalBackend, RelationalFacts};
use crate::plan::tracking::{Declare, Declared, Outputs};

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn scan(
        &mut self,
        table: Declared<Table>,
        read: ReadMode,
        alias: &str,
    ) -> Result<Relation, RelationalError> {
        let table = self
            .construction
            .attempt(|construction| construction.declared(table))?;
        let (_, facts) = self
            .catalog
            .stores()
            .nth(table.store)
            .expect("declared tables name a store of their catalog");
        let versioned = facts.relational().versioning.is_some();
        self.guard(supported(
            versioned || read == ReadMode::Current,
            "raw reads of an unversioned store",
        ))?;
        let schema = &facts.relational().tables[table.index];
        let columns = schema
            .columns
            .iter()
            .map(|column| Declare::named(&column.name, column.data_type));
        let operation = Operation::Scan {
            table,
            read,
            alias: alias.to_string(),
        };
        let outputs = Outputs::extend(columns);
        let (mut relation, declared) = self.connect(operation, Vec::new(), Stage::FROM, outputs)?;
        relation.visible = self.named(&declared, Some(alias))?;
        Ok(relation)
    }

    pub fn read(
        &mut self,
        subquery: &Subquery,
        alias: Option<&str>,
    ) -> Result<Relation, RelationalError> {
        let inline = self.body(subquery)?.sharing == Sharing::Inline;
        self.guard(match (inline, alias) {
            (true, None) => Err(RelationalError::Incomplete("an unnamed subquery read")),
            _ => Ok(()),
        })?;
        let body = subquery.body;
        let operation = Operation::Read {
            body: body.id(),
            alias: alias.map(str::to_string),
        };
        let connected = self
            .construction
            .attempt(|construction| construction.read(body, operation))?;
        let range = alias.unwrap_or(&subquery.name);
        let visible = self.named(&connected.declared, Some(range))?;
        Ok(Relation {
            rows: connected.rows,
            stage: Stage::FROM,
            visible,
            computed: Vec::new(),
        })
    }

    pub fn join(
        &mut self,
        left: Relation,
        right: Relation,
        kind: JoinKind,
    ) -> Result<Relation, RelationalError> {
        let joinable = left.stage.joinable() && right.stage.joinable();
        self.guard(match joinable {
            true => Ok(()),
            false => Err(RelationalError::Misplaced("join")),
        })?;
        if let JoinKind::Inner(on) | JoinKind::Left(on) = &kind {
            self.check(on)?;
        }
        let operation = Operation::Join(kind);
        let (relation, _) = self.connect(
            operation,
            vec![left, right],
            Stage::FROM,
            Outputs::preserve(),
        )?;
        Ok(relation)
    }
}
