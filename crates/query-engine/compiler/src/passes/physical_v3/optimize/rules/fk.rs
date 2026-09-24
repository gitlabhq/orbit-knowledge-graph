use super::super::super::*;
pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    rewrite_joins(plan, &mut |join| {
        let relations: Vec<_> = join.relations().collect();
        let Some(keys) = catalog.foreign_keys(relations) else {
            return;
        };
        for key in keys {
            join.substitute(key.substitutions(catalog));
            join.remove(key.edge);
            join.add_condition(key.condition());
        }
    })
}
