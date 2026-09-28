use super::*;
pub(super) fn build<M: QueryDataModel, B: Flavor>(
    _bound: &BoundCatalog<M>,
    plan: Plan<B>,
    cost: Cost,
) -> Candidate<B> {
    Candidate { plan, cost }
}

pub(super) fn relation_columns<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    relation: RelationId,
) -> BTreeSet<ColumnId> {
    bound.columns_for(relation).collect()
}
