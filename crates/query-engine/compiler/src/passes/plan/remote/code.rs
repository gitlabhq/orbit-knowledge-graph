use query_data_model::implementations::orbit_code::{OrbitCode, Repository};
use query_data_model::plan::code::{CodePlan, Field, Matches, Trigrams};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, Literal, LogicalOperation, LogicalPlan, MATCH_LINE,
    Operand, Predicate, Projection, Structural,
};
use query_data_model::{Catalog, EntityId, PropertyId, StorageBackend};

use super::unsupported;
use crate::error::{QueryError, Result};

const BACKEND: &str = OrbitCode::KIND;

enum Returned {
    Column(PropertyId),
    Line,
    Capture(String),
}

/// Literal atoms shorter than a trigram cannot narrow the candidate files.
const TRIGRAM: usize = 3;

pub fn plan(logical: &LogicalPlan, code: &Catalog<OrbitCode>) -> Result<CodePlan> {
    let mut entity = None;
    let mut projects = Vec::new();
    let mut heads = Vec::new();
    let mut structural = None;
    let mut returned = Vec::new();
    let mut limit = None;
    let mut keyed = Vec::new();
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
                let (searched, pairs) = follow(code, entity, relationships, to)?;
                entity = Some(searched);
                keyed.extend(pairs);
            }
            LogicalOperation::Filter(predicate) => {
                let repository = repository(code, entity)?;
                match branch_heads(predicate, repository)? {
                    Some(found) => heads.extend(found),
                    None => projects.extend(repositories(predicate, repository)?),
                }
            }
            LogicalOperation::Structural(matched) => structural = Some(matched),
            LogicalOperation::Select(items) => {
                for item in items {
                    returned.extend(fields(logical, code, entity, &keyed, item)?);
                }
            }
            LogicalOperation::Limit { rows, skip: None } => limit = Some(*rows),
            _ => return Err(unsupported(BACKEND, "run this query shape")),
        }
    }
    let entity = entity.ok_or_else(|| unsupported(BACKEND, "search without an entity"))?;
    let Structural {
        pattern, language, ..
    } = structural.ok_or_else(|| unsupported(BACKEND, "search without a structural pattern"))?;
    let atoms: Vec<&str> = pattern
        .literals()
        .into_iter()
        .filter(|atom| atom.len() >= TRIGRAM)
        .collect();
    Ok(code.build(|q| {
        let index = code.index(entity)?;
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

/// The repositories a code plan names: the conditions on its entity's repository columns.
pub fn named_repositories(logical: &LogicalPlan, code: &Catalog<OrbitCode>) -> Result<Vec<i64>> {
    let mut entity = None;
    let mut projects = Vec::new();
    for node in logical.nodes() {
        match logical.operation(node) {
            LogicalOperation::Entities(id) => entity = Some(*id),
            LogicalOperation::Filter(predicate) => {
                let repository = repository(code, entity)?;
                match branch_heads(predicate, repository)? {
                    Some(heads) => projects.extend(heads.into_iter().map(|(project, _)| project)),
                    None => projects.extend(repositories(predicate, repository)?),
                }
            }
            _ => {}
        }
    }
    Ok(projects)
}

fn repository(code: &Catalog<OrbitCode>, entity: Option<EntityId>) -> Result<Repository> {
    entity
        .and_then(|entity| code.repository(entity))
        .ok_or_else(|| unsupported(BACKEND, "filter before it names an indexed entity"))
}

fn repositories(predicate: &Predicate, repository: Repository) -> Result<Vec<i64>> {
    let mut projects = Vec::new();
    for conjunct in predicate.conjuncts() {
        let Conjunct::Leaf {
            predicate,
            negated: false,
        } = conjunct
        else {
            return Err(unsupported(BACKEND, "negate a condition"));
        };
        let on_project = |left: &Expr| matches!(left, Expr::Property { property, .. } if *property == repository.project);
        match predicate {
            Predicate::Compare {
                left,
                comparison: Comparison::Eq,
                right: Operand::Literal(Literal::Int(project)),
            } if on_project(left) => projects.push(*project),
            Predicate::In { left, values } if on_project(left) => {
                for value in values {
                    let Literal::Int(project) = value else {
                        return Err(QueryError::PipelineInvariant(
                            "a repository column holds integers".into(),
                        ));
                    };
                    projects.push(*project);
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

/// The fields a projection reads off each match; a property of a node the search keys by
/// reads the match column that holds it.
fn fields(
    logical: &LogicalPlan,
    code: &Catalog<OrbitCode>,
    entity: Option<EntityId>,
    keyed: &[(PropertyId, PropertyId)],
    item: &Projection,
) -> Result<Vec<Returned>> {
    let held = |property: PropertyId| {
        keyed
            .iter()
            .find(|(_, referenced)| *referenced == property)
            .map_or(property, |(held, _)| *held)
    };
    let returned = || -> Result<Vec<PropertyId>> {
        let entity =
            entity.ok_or_else(|| unsupported(BACKEND, "return a node it has not named"))?;
        let (store, _) = code
            .home(entity)
            .ok_or_else(|| unsupported(BACKEND, "find the entity's index"))?;
        Ok(store.returns(entity).collect())
    };
    Ok(match item {
        Projection::Graph {
            columns: Columns::Listed(properties),
            ..
        } => {
            let returned = returned()?;
            let graph = code.graph();
            let properties: Vec<PropertyId> =
                properties.iter().map(|property| held(*property)).collect();
            match properties
                .iter()
                .find(|property| !returned.contains(property))
            {
                Some(missing) => {
                    let available: Vec<&str> = returned
                        .iter()
                        .map(|property| graph.property(*property).name.as_str())
                        .collect();
                    return Err(QueryError::ReferenceError(format!(
                        "the {BACKEND} backend returns no {}.{}; it returns {}",
                        graph.entity(graph.property(*missing).entity).name,
                        graph.property(*missing).name,
                        available.join(", ")
                    )));
                }
                None => properties.iter().copied().map(Returned::Column).collect(),
            }
        }
        Projection::Graph {
            columns: Columns::Default,
            ..
        } => {
            let defaults = &code
                .graph()
                .entity(entity.unwrap_or_default())
                .default_columns;
            returned()?
                .into_iter()
                .filter(|property| defaults.contains(property))
                .map(Returned::Column)
                .collect()
        }
        Projection::Graph { .. } => returned()?.into_iter().map(Returned::Column).collect(),
        Projection::Value(Expr::Output(output)) => {
            let column = logical.name(*output).unwrap_or_default();
            match column {
                MATCH_LINE => vec![Returned::Line],
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
        Returned::Column(property) => hits.column(*property),
        Returned::Line => hits.line(),
        Returned::Capture(name) => hits.capture(name),
    }
}
/// A step between files and the branch heads they are at, either way: the side that holds a
/// composite key on the other's identity is what the search returns, with each held column and
/// the identity property it matches.
fn follow(
    code: &Catalog<OrbitCode>,
    from: Option<EntityId>,
    relationships: &[query_data_model::RelationshipId],
    to: Option<EntityId>,
) -> Result<(EntityId, Vec<(PropertyId, PropertyId)>)> {
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
            let (holder, pairs) = code.model().composite_key(variant.id)?;
            Some((variant.endpoint(holder), pairs.to_vec()))
        })
        .ok_or_else(|| unsupported(BACKEND, "follow a relationship that keys no branch head"))
}

/// The (repository, branch) heads a tuple IN on the repository columns names; none for any
/// other condition.
fn branch_heads(
    predicate: &Predicate,
    repository: Repository,
) -> Result<Option<Vec<(i64, String)>>> {
    let Predicate::InTuple { left, values } = predicate else {
        return Ok(None);
    };
    let position = |column: PropertyId| {
        left.iter()
            .position(|expr| matches!(expr, Expr::Property { property, .. } if *property == column))
    };
    let (Some(project), Some(branch), 2) = (
        position(repository.project),
        position(repository.branch),
        left.len(),
    ) else {
        return Err(unsupported(
            BACKEND,
            "search heads named other than by their repository columns",
        ));
    };
    values
        .iter()
        .map(|tuple| match (&tuple[project], &tuple[branch]) {
            (Literal::Int(project), Literal::String(branch)) => Ok((*project, branch.clone())),
            _ => Err(QueryError::PipelineInvariant(
                "repository columns hold an integer project and a text branch".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}
