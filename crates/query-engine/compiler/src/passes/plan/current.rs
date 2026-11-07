//! Current-row reads: one FINAL read when a predicate bounds it, else a latest-row pick narrowed by
//! the keys of matching rows. A repeated read returns the same share.

use ontology::constants::VERSION_COLUMN;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{Order, Relation, RelationalBuilder};

use super::predicates::{Column, Predicate, Projection};
use super::rows::{ReadScope, Reads, Rel, point_lookup};
use crate::error::Result;
use crate::passes::lower::build::{scan, stored};
use crate::tp_bounds::TpBound;

#[derive(PartialEq)]
pub(super) struct ReadKey {
    table: String,
    alias: String,
    bound: Option<TpBound>,
    key: Vec<String>,
    predicates: Vec<Predicate>,
    scope: ReadScope,
}

impl<B: RelationalBackend> Reads<'_, '_, B> {
    /// A table read with the owner's path bound first, then the predicates.
    pub fn table(
        &mut self,
        table: &str,
        alias: &str,
        final_: bool,
        bound: Option<&TpBound>,
        predicates: &[Predicate],
    ) -> Result<Rel> {
        let rows = match final_ {
            true => self.final_rows(table, alias, &[])?,
            false => scan(self.q, table, alias, false)?,
        };
        let rows = self.bound(rows, bound, alias)?;
        let rows = self.filter(rows, predicates)?;
        self.share(rows, Some(alias), point_lookup(predicates).unwrap_or(false))
    }

    /// A FINAL read in its own block with only sort-key predicates, so no other predicate moves before the merge.
    fn final_rows(&mut self, table: &str, alias: &str, keyed: &[Predicate]) -> Result<Relation> {
        let rows = self.subquery(alias, |reads| {
            let rows = scan(reads.q, table, alias, true)?;
            let rows = reads.filter(rows, keyed)?;
            Ok(reads.q.select_all(rows)?)
        })?;
        Ok(self.q.read(&rows, Some(alias))?)
    }

    pub fn current_rows(
        &mut self,
        table: &str,
        alias: &str,
        bound: Option<&TpBound>,
        key: &[String],
        predicates: Vec<Predicate>,
        scope: ReadScope,
    ) -> Result<Rel> {
        let read = ReadKey {
            table: table.to_string(),
            alias: alias.to_string(),
            bound: bound.cloned(),
            key: key.to_vec(),
            predicates: predicates.clone(),
            scope,
        };
        if let Some((_, rel)) = self.memo.iter().find(|(planned, _)| *planned == read) {
            return Ok(rel.clone());
        }
        let rel = self.plan_current_rows(table, alias, bound, key, predicates, scope)?;
        self.memo.push((read, rel.clone()));
        Ok(rel)
    }

    fn plan_current_rows(
        &mut self,
        table: &str,
        alias: &str,
        bound: Option<&TpBound>,
        key: &[String],
        predicates: Vec<Predicate>,
        scope: ReadScope,
    ) -> Result<Rel> {
        let bounds_scan = |predicate: &Predicate| {
            predicate.is_selective()
                && (!predicate.is_text_match() || predicate.filters_sort_key(key, true))
        };
        let bounded = self.selective_latest && predicates.iter().any(bounds_scan);
        let read_once = bounded
            && scope.reads_final(&predicates)
            && predicates.iter().any(|predicate| {
                predicate.is_selective() && !predicate.filters_sort_key(key, true)
            });
        if !bounded || read_once {
            let (keyed, rest): (Vec<_>, Vec<_>) = predicates
                .into_iter()
                .partition(|predicate| predicate.filters_sort_key(key, false));
            let rows = self.final_rows(table, alias, &keyed)?;
            let rows = self.bound(rows, bound, alias)?;
            let rows = self.filter(rows, &rest)?;
            let lookup = point_lookup(&rest).or_else(|| point_lookup(&keyed));
            return self.share(rows, Some(alias), lookup.unwrap_or(false));
        }
        let (mut identity, rest): (Vec<_>, Vec<_>) = predicates
            .iter()
            .cloned()
            .partition(|predicate| predicate.filters_sort_key(key, true));
        if rest.iter().any(Predicate::is_selective) {
            let candidate = self.table(table, alias, false, bound, &predicates)?;
            let outputs: Vec<_> = key
                .iter()
                .map(|column| Projection::col(alias, column))
                .collect();
            let candidate = self.project(&candidate, Some(alias), &outputs)?;
            identity.push(Predicate::Membership {
                columns: key
                    .iter()
                    .map(|column| Column::new(alias, column))
                    .collect(),
                relation: candidate,
                keys: key.to_vec(),
            });
        }
        let latest = self.subquery(alias, |reads| {
            let rows = scan(reads.q, table, alias, false)?;
            let rows = reads.bound(rows, bound, alias)?;
            let rows = reads.filter(rows, &identity)?;
            latest_rows(reads.q, rows, alias, key)
        })?;
        let rows = self.q.read(&latest, Some(alias))?;
        let rows = self.filter(rows, &rest)?;
        self.share(rows, Some(alias), point_lookup(&rest).unwrap_or(false))
    }
}

pub(super) fn latest_rows<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    rows: Relation,
    alias: &str,
    key: &[String],
) -> Result<Relation> {
    let keys = key
        .iter()
        .map(|column| rows.qualified(alias, column))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let rows = match stored::<B>() {
        true => {
            let orders = keys.into_iter().map(|key| Order {
                value: key.expr(),
                descending: false,
            });
            q.order(rows, orders.collect())?
        }
        false => {
            let version = rows.qualified(alias, VERSION_COLUMN)?;
            q.latest(rows, keys, version)?
        }
    };
    Ok(q.select_all(rows)?)
}
