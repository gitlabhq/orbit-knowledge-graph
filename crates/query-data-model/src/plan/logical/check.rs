use std::collections::BTreeSet;

use crate::DataType;

use super::{EdgeField, Expr, Logical, LogicalOperation, LogicalType, Measure, PlanError};
use crate::catalog::PropertyAccess;
use crate::generic::AttributeId;
use crate::plan::tracking::{Construction, Ref};
use crate::{Model, Property, RelationshipId, RelationshipVariantId};

pub(super) struct Typed<'m> {
    pub(super) data_type: DataType,
    pub(super) property: Option<&'m Property>,
    /// How the property's complete store keeps it, when it is an enum property.
    pub(super) enum_type: Option<crate::EnumType>,
    pub(super) access: PropertyAccess,
    pub(super) name: String,
}

pub(super) struct EdgeAttribute {
    pub(super) id: AttributeId,
    pub(super) access: PropertyAccess,
    pub(super) name: String,
}

pub(super) fn edge_relationships(
    model: &Model,
    construction: &Construction<Logical>,
    edge: Ref,
) -> Result<Vec<RelationshipId>, PlanError> {
    let relationships: BTreeSet<RelationshipId> = match construction.producer(edge)? {
        LogicalOperation::Follow { hops, .. } => hops.iter().map(|hop| hop.relationship).collect(),
        LogicalOperation::Neighbors {
            relationships,
            direction,
            from,
        } if relationships.is_empty() => match construction.value(*from)? {
            (_, LogicalType::Entity(entity)) => model
                .graph()
                .hops(*entity, *direction)
                .map(|hop| hop.relationship)
                .collect(),
            (name, found) => return Err(mismatch(name.unwrap_or("node"), "an entity", *found)),
        },
        LogicalOperation::Neighbors { relationships, .. } => {
            relationships.iter().copied().collect()
        }
        _ => {
            let (name, found) = construction.value(edge)?;
            return Err(mismatch(name.unwrap_or("edge"), "a relationship", *found));
        }
    };
    Ok(relationships.into_iter().collect())
}

pub(super) fn edge_variants(
    model: &Model,
    construction: &Construction<Logical>,
    edge: Ref,
) -> Result<Vec<RelationshipVariantId>, PlanError> {
    Ok(match construction.producer(edge)? {
        LogicalOperation::Follow { hops, .. } => hops.iter().map(|hop| hop.variant).collect(),
        LogicalOperation::Neighbors {
            relationships,
            direction,
            from,
        } => match construction.value(*from)? {
            (_, LogicalType::Entity(entity)) => model
                .graph()
                .hops(*entity, *direction)
                .filter(|hop| relationships.is_empty() || relationships.contains(&hop.relationship))
                .map(|hop| hop.variant)
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    })
}

pub(super) fn edge_attribute(
    model: &Model,
    relationships: &[RelationshipId],
    name: &str,
) -> Result<EdgeAttribute, PlanError> {
    let graph = model.graph();
    let declared = |name: &str| -> Option<Vec<AttributeId>> {
        let ids: Vec<AttributeId> = relationships
            .iter()
            .map(|relationship| graph.attribute_id(*relationship, name))
            .collect::<Option<_>>()?;
        let first = graph.attribute(*ids.first()?);
        ids.iter()
            .all(|id| graph.attribute(*id).data_type == first.data_type)
            .then_some(ids)
    };
    let Some(ids) = declared(name) else {
        let attributes: BTreeSet<&str> = relationships
            .iter()
            .flat_map(|relationship| &graph.relationship(*relationship).attributes)
            .map(|id| graph.attribute(*id).name.as_str())
            .filter(|name| declared(name).is_some())
            .collect();
        return Err(PlanError::UnknownColumn {
            name: name.to_string(),
            available: <EdgeField as strum::VariantNames>::VARIANTS
                .iter()
                .copied()
                .chain(attributes)
                .map(str::to_string)
                .collect(),
        });
    };
    let accesses = ids.iter().map(|id| model.access().attribute(*id));
    let access = accesses.fold(PropertyAccess::COMPUTED, |all, one| PropertyAccess {
        hidden: all.hidden || one.hidden,
        admin_only: all.admin_only || one.admin_only,
        filterable: all.filterable && one.filterable,
        like: all.like && one.like,
    });
    let owners: Vec<&str> = relationships
        .iter()
        .map(|relationship| graph.relationship(*relationship).name.as_str())
        .collect();
    Ok(EdgeAttribute {
        id: ids[0],
        access,
        name: format!("{}.{name}", owners.join("|")),
    })
}

/// The one identity type every source or target of the edge's relationships has.
fn endpoint_identity(
    model: &Model,
    construction: &Construction<Logical>,
    edge: Ref,
    source: bool,
) -> Result<DataType, PlanError> {
    let graph = model.graph();
    let mut types = Vec::new();
    for relationship in edge_relationships(model, construction, edge)? {
        for variant in &graph.relationship(relationship).variants {
            let variant = graph.variant(*variant);
            let endpoint = if source {
                variant.source
            } else {
                variant.target
            };
            let [identity] = graph.entity(endpoint).identity.as_slice() else {
                return Err(PlanError::shape(
                    "an edge endpoint with a composite identity has no one id",
                ));
            };
            let data_type = graph.property(*identity).data_type;
            if !types.contains(&data_type) {
                types.push(data_type);
            }
        }
    }
    match types.as_slice() {
        [only] => Ok(*only),
        _ => Err(PlanError::shape(
            "the edge's endpoints have identities of different types; name one relationship",
        )),
    }
}

pub(super) fn scalar<'m>(
    model: &'m Model,
    construction: &Construction<Logical>,
    expr: Expr,
) -> Result<Typed<'m>, PlanError> {
    match expr {
        Expr::Property { binding, property } => property_of(model, construction, binding, property),
        Expr::Truncate {
            binding,
            property,
            unit,
        } => {
            let typed = property_of(model, construction, binding, property)?;
            match typed.data_type {
                DataType::Date | DataType::DateTime => Ok(typed),
                found => Err(PlanError::Type {
                    expr: format!("{} truncated to {unit:?}", typed.name),
                    expected: "a Date or DateTime".to_string(),
                    found: format!("{found:?}"),
                }),
            }
        }
        Expr::Edge { binding, field } => match construction.value(binding)? {
            (_, LogicalType::Edge) => Ok(Typed {
                data_type: match field {
                    EdgeField::SourceId => endpoint_identity(model, construction, binding, true)?,
                    EdgeField::TargetId => endpoint_identity(model, construction, binding, false)?,
                    EdgeField::Kind | EdgeField::SourceKind | EdgeField::TargetKind => {
                        DataType::String
                    }
                },
                property: None,
                enum_type: None,
                access: PropertyAccess {
                    like: false,
                    ..PropertyAccess::COMPUTED
                },
                name: format!("{field:?}"),
            }),
            (name, found) => Err(mismatch(name.unwrap_or("edge"), "a relationship", *found)),
        },
        Expr::Attribute { binding, attribute } => {
            let declared = model.graph().attribute(attribute);
            let relationships = edge_relationships(model, construction, binding)?;
            let resolved = edge_attribute(model, &relationships, &declared.name)?;
            Ok(Typed {
                data_type: declared.data_type,
                property: None,
                enum_type: None,
                access: PropertyAccess {
                    like: false,
                    ..resolved.access
                },
                name: resolved.name,
            })
        }
        Expr::Node(binding) => {
            let (name, found) = construction.value(binding)?;
            Err(mismatch(name.unwrap_or("node"), "a scalar", *found))
        }
        Expr::Output(output) => match construction.value(output)? {
            (name, LogicalType::Scalar(data_type)) => Ok(Typed {
                data_type: *data_type,
                property: None,
                enum_type: None,
                access: PropertyAccess::COMPUTED,
                name: name.unwrap_or("output").to_string(),
            }),
            (name, found) => Err(mismatch(name.unwrap_or("output"), "a scalar", *found)),
        },
    }
}

fn property_of<'m>(
    model: &'m Model,
    construction: &Construction<Logical>,
    binding: Ref,
    property: crate::PropertyId,
) -> Result<Typed<'m>, PlanError> {
    let property = model.graph().property(property);
    let entity = &model.graph().entity(property.entity).name;
    let name = format!("{entity}.{}", property.name);
    match construction.value(binding)? {
        (_, LogicalType::Entity(bound)) if *bound == property.entity => Ok(Typed {
            data_type: property.data_type,
            property: Some(property),
            enum_type: Some(model.enum_type(property.id)),
            access: model.access().property(property.id),
            name,
        }),
        (_, found) => Err(mismatch(name, entity, *found)),
    }
}

pub(super) fn mismatch(expr: impl Into<String>, expected: &str, found: LogicalType) -> PlanError {
    PlanError::Type {
        expr: expr.into(),
        expected: expected.to_string(),
        found: format!("{found:?}"),
    }
}

const COMPUTED: &str = "structural matches cannot be aggregated";

pub(super) fn group(
    model: &Model,
    construction: &Construction<Logical>,
    expr: Expr,
) -> Result<LogicalType, PlanError> {
    match expr {
        Expr::Node(binding) => {
            return match construction.value(binding)? {
                (_, kind @ LogicalType::Entity(_)) => Ok(*kind),
                (_, LogicalType::Edge) => Err(PlanError::shape(
                    "grouping by a relationship is not supported yet",
                )),
                (name, found) => Err(mismatch(name.unwrap_or("node"), "an entity", *found)),
            };
        }
        Expr::Edge { .. } | Expr::Attribute { .. } => {
            return Err(PlanError::shape(
                "grouping by a relationship is not supported yet",
            ));
        }
        Expr::Output(_) => return Err(PlanError::shape(COMPUTED)),
        Expr::Property { .. } | Expr::Truncate { .. } => {}
    }
    let typed = super::filter::filterable(model, construction, expr)?;
    Ok(LogicalType::Scalar(typed.data_type))
}

pub(super) fn measure(
    model: &Model,
    construction: &Construction<Logical>,
    mut measure: Measure,
) -> Result<DataType, PlanError> {
    let counts = matches!(measure, Measure::Count(_) | Measure::CountDistinct(_));
    let Some(input) = measure.input().copied() else {
        return Ok(DataType::Int);
    };
    if let Expr::Output(_) = input {
        return Err(PlanError::shape(COMPUTED));
    }
    if counts {
        if let Expr::Node(binding) = input {
            group(model, construction, Expr::Node(binding))?;
        } else {
            scalar(model, construction, input)?;
        }
        return Ok(DataType::Int);
    }
    if let Expr::Node(_) = input {
        return Err(PlanError::shape(
            "sum, avg, min, and max take a node property, such as sum(mr.added_lines)",
        ));
    }
    let typed = scalar(model, construction, input)?;
    match (measure, typed.data_type) {
        (Measure::Sum(_), DataType::Int) => Ok(DataType::Int),
        (Measure::Sum(_) | Measure::Avg(_), DataType::Int | DataType::Float) => Ok(DataType::Float),
        (
            Measure::Min(_) | Measure::Max(_),
            DataType::Int | DataType::Float | DataType::Bool | DataType::Date | DataType::DateTime,
        ) => Ok(typed.data_type),
        (Measure::Min(_) | Measure::Max(_), DataType::Enum) => Err(PlanError::Invalid {
            expr: typed.name,
            reason: "min and max do not apply to an enum, because enum values have no order; group by it instead".to_string(),
        }),
        (Measure::Min(_) | Measure::Max(_), found) => Err(PlanError::Type {
            expr: typed.name,
            expected: "an ordered scalar".to_string(),
            found: format!("{found:?}"),
        }),
        (_, found) => Err(PlanError::Type {
            expr: typed.name,
            expected: "a number".to_string(),
            found: format!("{found:?}"),
        }),
    }
}
