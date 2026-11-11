use super::{RelationalBuilder, Subquery, supported};
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{
    JoinKind, Operation, ReadMode, Relation, RelationalError, Sharing, Table,
};
use crate::implementations::relational::{RelationalBackend, RelationalFacts};
use crate::plan::tracking::{BodyId, Declare, Declared, Outputs, Rejected};

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
        let alias = alias.to_string();
        self.apply(
            Operation::Scan { table, read, alias },
            Vec::new(),
            Outputs::preserve(),
        )
    }

    pub fn read(
        &mut self,
        subquery: &Subquery,
        alias: Option<&str>,
    ) -> Result<Relation, RelationalError> {
        let own = self.construction.body(subquery.id()) == Ok(subquery.body);
        self.guard(match own {
            true => Ok(()),
            false => Err(RelationalError::Rejected(Rejected::ForeignPlan)),
        })?;
        let body = subquery.id();
        let alias = alias.map(str::to_string);
        self.apply(
            Operation::Read { body, alias },
            Vec::new(),
            Outputs::preserve(),
        )
    }

    pub fn join(
        &mut self,
        left: Relation,
        right: Relation,
        kind: JoinKind,
    ) -> Result<Relation, RelationalError> {
        self.apply(
            Operation::Join(kind),
            vec![left, right],
            Outputs::preserve(),
        )
    }

    pub(super) fn scanned(
        &mut self,
        table: Table,
        read: ReadMode,
        alias: String,
    ) -> Result<Relation, RelationalError> {
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
        let outputs = Outputs::extend(columns);
        let range = alias.clone();
        let operation = Operation::Scan { table, read, alias };
        let (mut relation, declared) = self.connect(operation, Vec::new(), Stage::FROM, outputs)?;
        relation.visible = self.named(&declared, Some(&range))?;
        Ok(relation)
    }

    pub(super) fn reading(
        &mut self,
        body: BodyId,
        alias: Option<String>,
    ) -> Result<Relation, RelationalError> {
        let subquery = self.guard(self.subquery_of(body))?;
        let inline = self.body(&subquery)?.sharing == Sharing::Inline;
        self.guard(match (inline, &alias) {
            (true, None) => Err(RelationalError::Incomplete("an unnamed subquery read")),
            _ => Ok(()),
        })?;
        let range = alias.clone().unwrap_or(subquery.name);
        let operation = Operation::Read { body, alias };
        let connected = self
            .construction
            .attempt(|construction| construction.read(subquery.body, operation))?;
        let visible = self.named(&connected.declared, Some(&range))?;
        Ok(Relation {
            rows: connected.rows,
            stage: Stage::FROM,
            visible,
            computed: Vec::new(),
        })
    }

    pub(super) fn joined(
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
