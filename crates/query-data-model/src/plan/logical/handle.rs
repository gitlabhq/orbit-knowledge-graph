use super::{EdgeField, Expr, Logical, LogicalCatalog, LogicalType, PlanError, TimeUnit};
use crate::plan::tracking::{self, Construction, Ref};
use crate::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone, Copy)]
pub struct Binding<'m> {
    catalog: LogicalCatalog<'m>,
    kind: LogicalType,
    output: Ref,
}

impl<'m> Binding<'m> {
    pub fn kind(self) -> LogicalType {
        self.kind
    }

    pub fn entity(self) -> Option<EntityId> {
        match self.kind {
            LogicalType::Entity(entity) => Some(entity),
            _ => None,
        }
    }

    pub fn output(self) -> Ref {
        self.output
    }

    pub fn catalog(self) -> LogicalCatalog<'m> {
        self.catalog
    }

    pub fn node(self) -> Expr {
        Expr::Node(self.output)
    }

    pub fn property(self, name: &str) -> Result<Expr, PlanError> {
        Ok(Expr::Property {
            binding: self.output,
            property: self.resolve(name)?,
        })
    }

    pub fn truncate(self, name: &str, unit: TimeUnit) -> Result<Expr, PlanError> {
        Ok(Expr::Truncate {
            binding: self.output,
            property: self.resolve(name)?,
            unit,
        })
    }

    pub fn field(self, field: EdgeField) -> Result<Expr, PlanError> {
        match self.kind {
            LogicalType::Edge => Ok(Expr::Edge {
                binding: self.output,
                field,
            }),
            found => Err(PlanError::Type {
                expr: format!("{field:?}"),
                expected: "a relationship".to_string(),
                found: format!("{found:?}"),
            }),
        }
    }

    fn resolve(self, name: &str) -> Result<PropertyId, PlanError> {
        let entity = self.entity().ok_or(PlanError::NoEntity)?;
        let model = self.catalog.model();
        let property = model.property(entity, name)?;
        let access = model.access().property(property.id);
        let qualified = || format!("{}.{name}", model.graph().entity(entity).name);
        if access.hidden {
            return Err(model.unknown_property(entity, name).into());
        }
        if access.admin_only && !self.catalog.admin() {
            return Err(PlanError::AdminOnly(qualified()));
        }
        if !self.catalog.realized(entity, property.id) {
            return Err(PlanError::Unavailable {
                owner: model.graph().entity(entity).name.clone(),
                name: name.to_string(),
            });
        }
        Ok(property.id)
    }
}

#[derive(Debug)]
pub struct Rows<'m> {
    rows: tracking::Rows,
    bindings: Vec<(Option<String>, Binding<'m>)>,
    edges: Vec<(Ref, Vec<RelationshipId>, Vec<RelationshipVariantId>)>,
    columns: Vec<(String, Ref)>,
}

impl<'m> Rows<'m> {
    pub(super) fn view(
        catalog: LogicalCatalog<'m>,
        construction: &Construction<Logical>,
        rows: tracking::Rows,
    ) -> Result<Self, PlanError> {
        let mut bindings = Vec::new();
        let mut edges = Vec::new();
        let mut columns = Vec::new();
        for output in construction.outputs(&rows)? {
            match construction.value(output)? {
                (Some(name), LogicalType::Scalar(_)) => columns.push((name.to_string(), output)),
                (_, LogicalType::Scalar(_)) => {}
                (name, LogicalType::Edge) => {
                    let model = catalog.model();
                    edges.push((
                        output,
                        super::check::edge_relationships(model, construction, output)?,
                        super::check::edge_variants(model, construction, output)?,
                    ));
                    bindings.push((
                        name.map(str::to_string),
                        Binding {
                            catalog,
                            kind: LogicalType::Edge,
                            output,
                        },
                    ));
                }
                (name, kind) => bindings.push((
                    name.map(str::to_string),
                    Binding {
                        catalog,
                        kind: *kind,
                        output,
                    },
                )),
            }
        }
        Ok(Self {
            rows,
            bindings,
            edges,
            columns,
        })
    }

    pub(super) fn into_inner(self) -> tracking::Rows {
        self.rows
    }

    pub(super) fn inner(&self) -> &tracking::Rows {
        &self.rows
    }

    pub fn endpoint(&self) -> Result<Binding<'m>, PlanError> {
        self.bindings
            .iter()
            .rev()
            .map(|(_, binding)| *binding)
            .find(|binding| matches!(binding.kind, LogicalType::Entity(_) | LogicalType::Node))
            .ok_or(PlanError::NoEntity)
    }

    pub fn edge(&self) -> Option<Binding<'m>> {
        self.bindings
            .iter()
            .rev()
            .map(|(_, binding)| *binding)
            .find(|binding| binding.kind == LogicalType::Edge)
    }

    pub fn binding(&self, name: &str) -> Result<Binding<'m>, PlanError> {
        self.bindings
            .iter()
            .find(|(bound, _)| bound.as_deref() == Some(name))
            .map(|(_, binding)| *binding)
            .ok_or_else(|| PlanError::UnknownBinding {
                name: name.to_string(),
                available: self
                    .bindings
                    .iter()
                    .filter_map(|(name, _)| name.clone())
                    .collect(),
            })
    }

    pub fn property(&self, name: &str) -> Result<Expr, PlanError> {
        self.endpoint()?.property(name)
    }

    pub fn edge_column(&self, edge: Binding<'m>, name: &str) -> Result<Expr, PlanError> {
        if let Ok(field) = name.parse() {
            return edge.field(field);
        }
        let Some((_, relationships, variants)) = self
            .edges
            .iter()
            .find(|(output, ..)| *output == edge.output)
        else {
            return Err(super::check::mismatch(name, "a relationship", edge.kind));
        };
        let attribute = super::check::edge_attribute(edge.catalog.model(), relationships, name)?;
        if attribute.access.hidden {
            return Err(PlanError::Hidden(attribute.name));
        }
        if attribute.access.admin_only && !edge.catalog.admin() {
            return Err(PlanError::AdminOnly(attribute.name));
        }
        if !edge.catalog.attribute_realized(variants, attribute.id) {
            let (owner, name) = attribute.name.split_once('.').unwrap_or(("", name));
            return Err(PlanError::Unavailable {
                owner: owner.to_string(),
                name: name.to_string(),
            });
        }
        Ok(Expr::Attribute {
            binding: edge.output,
            attribute: attribute.id,
        })
    }

    pub(super) fn names(&self) -> impl Iterator<Item = &str> {
        self.bindings.iter().filter_map(|(name, _)| name.as_deref())
    }

    pub(super) fn fresh(&self, name: &str) -> Result<(), PlanError> {
        match self.binding(name) {
            Ok(_) => Err(PlanError::Duplicate(name.to_string())),
            Err(_) => Ok(()),
        }
    }

    pub fn column(&self, name: &str) -> Result<Expr, PlanError> {
        self.columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, output)| Expr::Output(*output))
            .ok_or_else(|| PlanError::UnknownColumn {
                name: name.to_string(),
                available: self.columns.iter().map(|(name, _)| name.clone()).collect(),
            })
    }
}
