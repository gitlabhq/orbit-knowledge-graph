use query_data_model::catalog::Rule;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::{CodePlan, Field, Matches, Trigrams};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, Literal, LogicalOperation, LogicalPlan, MATCH_LINE,
    Operand, Predicate, Projection, Structural,
};
use query_data_model::{Catalog, Endpoint, EntityId, PropertyId, StorageBackend};

use super::unsupported;
use crate::error::{QueryError, Result};

const BACKEND: &str = OrbitCode::KIND;

enum Returned {
    Field(String),
    Capture(String),
}

/// Literal atoms shorter than a trigram cannot narrow the candidate files.
const TRIGRAM: usize = 3;

pub fn plan(logical: &LogicalPlan, code: &Catalog<OrbitCode>) -> Result<CodePlan> {
    let graph = code.graph();
    let mut entity = None;
    let mut projects = Vec::new();
    let mut heads = Vec::new();
    let mut structural = None;
    let mut returned = Vec::new();
    let mut limit = None;
    for node in logical.nodes() {
        match logical.operation(node) {
            LogicalOperation::Entities(id) => entity = Some(*id),
            LogicalOperation::Follow { relationships, .. } => {
                let to = logical
                    .outputs(node)
                    .last()
                    .and_then(|to| match logical.value(to) {
                        query_data_model::plan::logical::LogicalType::Entity(to) => Some(*to),
                        _ => None,
                    });
                entity = Some(follow(code, entity, relationships, to)?);
            }
            LogicalOperation::Filter(predicate) => match branch_heads(predicate)? {
                Some(found) => heads.extend(found),
                None => {
                    let anchor = entity.and_then(|entity| anchor(code, entity));
                    projects.extend(repositories(predicate, anchor)?);
                }
            },
            LogicalOperation::Structural(matched) => structural = Some(matched),
            LogicalOperation::Select(items) => {
                for item in items {
                    returned.extend(fields(logical, item, |property| {
                        graph.property(property).name.clone()
                    })?);
                }
            }
            LogicalOperation::Limit { rows, skip: None } => limit = Some(*rows),
            _ => return Err(unsupported(BACKEND, "run this query shape")),
        }
    }
    let entity = entity.ok_or_else(|| unsupported(BACKEND, "search without an entity"))?;
    for check in &code.model().access().entity(entity).checks {
        if let query_data_model::catalog::Check::Rows { id, .. } = check {
            let field = graph.property(*id).name.clone();
            if !returned
                .iter()
                .any(|item| matches!(item, Returned::Field(name) if *name == field))
            {
                returned.push(Returned::Field(field));
            }
        }
    }
    let Structural {
        pattern, language, ..
    } = structural.ok_or_else(|| unsupported(BACKEND, "search without a structural pattern"))?;
    let store = code
        .home(entity)
        .map(|(store, _)| store.name.clone())
        .ok_or_else(|| unsupported(BACKEND, "find the entity's index"))?;
    let atoms: Vec<&str> = pattern
        .literals()
        .into_iter()
        .filter(|atom| atom.len() >= TRIGRAM)
        .collect();
    Ok(code.build(|q| {
        let index = code.index(&store, &graph.entity(entity).name)?;
        let files = q.repositories(
            index,
            projects
                .iter()
                .copied()
                .chain(heads.iter().map(|(project, _)| *project)),
        )?;
        let files = match heads.is_empty() {
            true => files,
            false => q.branches(files, heads.iter().cloned())?,
        };
        let files = q.languages(files, [*language])?;
        let files = match atoms.is_empty() {
            true => files,
            false => q.prefilter(files, Trigrams::of(atoms))?,
        };
        let hits = q.structural(files, pattern.clone())?;
        let hits = match limit {
            Some(rows) => q.top(hits, rows)?,
            None => hits,
        };
        let fields = returned
            .iter()
            .map(|field| select(&hits, field))
            .collect::<std::result::Result<Vec<Field>, _>>()?;
        q.select(hits, fields)
    })?)
}

/// The repositories a code plan names: the conditions on its entity's repository key.
pub fn named_repositories(logical: &LogicalPlan, code: &Catalog<OrbitCode>) -> Result<Vec<i64>> {
    let mut entity = None;
    let mut projects = Vec::new();
    for node in logical.nodes() {
        match logical.operation(node) {
            LogicalOperation::Entities(id) => entity = Some(*id),
            LogicalOperation::Filter(predicate) => match branch_heads(predicate)? {
                Some(heads) => projects.extend(heads.into_iter().map(|(project, _)| project)),
                None => {
                    let anchor = entity.and_then(|entity| anchor(code, entity));
                    projects.extend(repositories(predicate, anchor)?);
                }
            },
            _ => {}
        }
    }
    Ok(projects)
}

/// The foreign key on the entity that names its repository: the first step of its `via` chain.
fn anchor(code: &Catalog<OrbitCode>, entity: EntityId) -> Option<PropertyId> {
    let Rule::Via(hops) = &code.guard(entity)?.rule else {
        return None;
    };
    let key = code.model().foreign_key(hops.first()?.variant)?;
    (key.holder == Endpoint::Source).then_some(key.property)
}

fn repositories(predicate: &Predicate, anchor: Option<PropertyId>) -> Result<Vec<i64>> {
    let mut projects = Vec::new();
    for conjunct in predicate.conjuncts() {
        let Conjunct::Leaf {
            predicate,
            negated: false,
        } = conjunct
        else {
            return Err(unsupported(BACKEND, "negate a condition"));
        };
        let on_anchor = |left: &Expr| matches!(left, Expr::Property { property, .. } if Some(*property) == anchor);
        match predicate {
            Predicate::Compare {
                left,
                comparison: Comparison::Eq,
                right: Operand::Literal(Literal::Int(project)),
            } if on_anchor(left) => projects.push(*project),
            Predicate::In { left, values } if on_anchor(left) => {
                for value in values {
                    match value {
                        Literal::Int(project) => projects.push(*project),
                        _ => return Err(unsupported(BACKEND, "name a repository by text")),
                    }
                }
            }
            _ => {
                return Err(unsupported(
                    BACKEND,
                    "filter on anything but the repository's project",
                ));
            }
        }
    }
    Ok(projects)
}

fn fields(
    logical: &LogicalPlan,
    item: &Projection,
    name: impl Fn(PropertyId) -> String,
) -> Result<Vec<Returned>> {
    Ok(match item {
        Projection::Graph {
            columns: Columns::Listed(properties),
            ..
        } => properties
            .iter()
            .map(|property| Returned::Field(name(*property)))
            .collect(),
        Projection::Graph { .. } => vec![Returned::Field("path".into())],
        Projection::Value(Expr::Output(output)) => {
            let column = logical.name(*output).unwrap_or_default();
            match column {
                MATCH_LINE => vec![Returned::Field("line".into())],
                other => match other.strip_prefix("match.") {
                    Some(capture) => vec![Returned::Capture(capture.to_string())],
                    None => {
                        return Err(QueryError::ReferenceError(format!(
                            "unknown column {other}"
                        )));
                    }
                },
            }
        }
        Projection::Value(_) => return Err(unsupported(BACKEND, "return an expression")),
    })
}

fn select(
    hits: &Matches,
    returned: &Returned,
) -> std::result::Result<Field, query_data_model::plan::code::CodeError> {
    match returned {
        Returned::Field(name) => hits.field(name),
        Returned::Capture(name) => hits.capture(name),
    }
}
/// A step between files and the branch heads they are at, either way: the side that holds a
/// composite key on the other's identity is what the search returns.
fn follow(
    code: &Catalog<OrbitCode>,
    from: Option<EntityId>,
    relationships: &[query_data_model::RelationshipId],
    to: Option<EntityId>,
) -> Result<EntityId> {
    let (Some(from), Some(to), [relationship]) = (from, to, relationships) else {
        return Err(unsupported(BACKEND, "follow this relationship"));
    };
    code.graph()
        .variants()
        .filter(|variant| variant.relationship == *relationship)
        .filter(|variant| {
            (variant.source, variant.target) == (from, to)
                || (variant.source, variant.target) == (to, from)
        })
        .find_map(|variant| {
            let (holder, _) = code.model().composite_key(variant.id)?;
            Some(variant.endpoint(holder))
        })
        .ok_or_else(|| unsupported(BACKEND, "follow a relationship that keys no branch head"))
}

/// The (repository, branch) heads a tuple IN names; none for any other condition.
fn branch_heads(predicate: &Predicate) -> Result<Option<Vec<(i64, String)>>> {
    let Predicate::InTuple { values, .. } = predicate else {
        return Ok(None);
    };
    values
        .iter()
        .map(|tuple| match &tuple[..] {
            [Literal::Int(project), Literal::String(branch)] => Ok((*project, branch.clone())),
            _ => Err(unsupported(
                BACKEND,
                "search heads named other than by (repository, branch)",
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}
