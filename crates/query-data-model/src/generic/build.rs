use std::collections::HashMap;

use crate::constants::{DEFAULT_PRIMARY_KEY as KEY, EDGE_RESERVED_COLUMNS};
use crate::{DataType, EnumType};
use indexmap::IndexMap;

use super::catalog::{
    Attribute, Domain, Entity, GraphCatalog, Property, Relationship, RelationshipVariant,
};
use super::{AttributeId, EntityId, PropertyId, RelationshipId, RelationshipVariantId};
use crate::catalog::Diagnostics;
use crate::spec::{
    AttributeSpec, DefaultValue, EntitySpec, GraphFragment, PropertyDetail, PropertySpec,
    RelationshipSpec, parse_variant, parse_word,
};

impl GraphCatalog {
    pub(crate) fn from_spec(
        fragments: impl IntoIterator<Item = GraphFragment>,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut graph = Self::default();
        let mut entities = Vec::new();
        let mut relationships = Vec::new();
        for fragment in fragments {
            if let Some(domain) = &fragment.domain {
                graph.declare_domain(
                    &fragment.package,
                    domain,
                    fragment.domain_description.clone(),
                    diagnostics,
                );
            }
            for (name, spec) in fragment.entities {
                entities.push((
                    fragment.package.clone(),
                    fragment.domain.clone(),
                    name,
                    spec,
                ));
            }
            relationships.push((fragment.package, fragment.relationships));
        }
        entities.sort_by(|left, right| left.2.cmp(&right.2));
        for (package, domain, name, spec) in entities {
            graph.declare_entity(&package, domain, name, spec, diagnostics);
        }
        for (package, relationships) in relationships {
            graph.declare_relationships(&package, relationships, diagnostics);
        }
        graph.index_adjacency();
        graph
    }

    fn declare_domain(
        &mut self,
        package: &str,
        name: &str,
        description: Option<String>,
        diagnostics: &mut Diagnostics,
    ) {
        let domain = self
            .domains
            .entry(name.to_string())
            .or_insert_with(|| Domain {
                name: name.to_string(),
                description: None,
            });
        let Some(description) = description else {
            return;
        };
        if domain
            .description
            .as_ref()
            .is_some_and(|existing| *existing != description)
        {
            diagnostics.error(
                package,
                format!("domain {name} already has another description"),
            );
            return;
        }
        domain.description = Some(description);
    }

    fn declare_entity(
        &mut self,
        package: &str,
        domain: Option<String>,
        name: String,
        spec: EntitySpec,
        diagnostics: &mut Diagnostics,
    ) {
        let id = EntityId(self.entities.len());
        if let Some(existing) = self.entity_ids.get(&name) {
            let owner = &self.entity(*existing).package;
            diagnostics.error(
                package,
                format!("entity {name} is already declared by {owner}"),
            );
            return;
        }
        if !spec.properties.contains_key(KEY) {
            diagnostics.error(package, format!("entity {name} has no `{KEY}` property"));
        }
        self.entity_ids.insert(name.clone(), id);
        let mut names = HashMap::with_capacity(spec.properties.len());
        let mut properties = Vec::with_capacity(spec.properties.len());
        for (property, spec) in spec.properties {
            let property_id = PropertyId(self.properties.len());
            let Some(declared) =
                declare_property(id, property_id, property, spec, package, diagnostics)
            else {
                continue;
            };
            names.insert(declared.name.clone(), property_id);
            properties.push(property_id);
            self.properties.push(declared);
        }
        let defaults = spec
            .defaults
            .iter()
            .filter_map(|default| {
                diagnostics.require(names.get(default).copied(), package, || {
                    format!("{name} default {default} is not a property")
                })
            })
            .collect();
        if let Some(label) = &spec.label
            && !names.contains_key(label)
        {
            diagnostics.error(package, format!("{name} label {label} is not a property"));
        }
        self.property_ids.push(names);
        self.entities.push(Entity {
            id,
            name,
            package: package.to_string(),
            domain,
            description: spec.description,
            label: spec.label.unwrap_or_default(),
            style: spec.style.unwrap_or_default(),
            properties,
            defaults,
        });
    }

    fn declare_relationships(
        &mut self,
        package: &str,
        relationships: IndexMap<String, RelationshipSpec>,
        diagnostics: &mut Diagnostics,
    ) {
        for (name, spec) in relationships {
            let relationship = match self.relationship_ids.get(&name) {
                Some(id) => *id,
                None => {
                    let id = RelationshipId(self.relationships.len());
                    self.relationship_ids.insert(name.clone(), id);
                    self.relationships.push(Relationship {
                        id,
                        name: name.clone(),
                        description: spec.description,
                        variants: Vec::new(),
                        attributes: Vec::new(),
                    });
                    id
                }
            };
            for (attribute, spec) in spec.attributes {
                self.declare_attribute(relationship, attribute, spec, package, diagnostics);
            }
            for (variant, declared) in &spec.variants {
                let Some((source, target)) = parse_variant(variant) else {
                    diagnostics.error(
                        package,
                        format!("{name} variant `{variant}` is not `Source -> Target`"),
                    );
                    continue;
                };
                let (Some(source), Some(target)) = (self.entity_id(source), self.entity_id(target))
                else {
                    diagnostics.error(
                        package,
                        format!("{name} variant `{variant}` names an unknown entity"),
                    );
                    continue;
                };
                let id = RelationshipVariantId(self.variants.len());
                if self
                    .variant_ids
                    .insert((relationship, source, target), id)
                    .is_some()
                {
                    diagnostics.error(
                        package,
                        format!("{name} variant `{variant}` is declared twice"),
                    );
                    continue;
                }
                self.relationships[relationship.index()].variants.push(id);
                self.variants.push(RelationshipVariant {
                    id,
                    relationship,
                    source,
                    target,
                    cardinality: declared.cardinality,
                });
            }
        }
    }

    fn declare_attribute(
        &mut self,
        relationship: RelationshipId,
        name: String,
        spec: AttributeSpec,
        package: &str,
        diagnostics: &mut Diagnostics,
    ) {
        let qualified = format!("{}.{name}", self.relationship(relationship).name);
        if EDGE_RESERVED_COLUMNS.contains(&name.as_str()) {
            diagnostics.error(
                package,
                format!("attribute {qualified} is a reserved edge column"),
            );
            return;
        }
        let written = spec.data_type();
        let Some((data_type, nullable)) =
            parse_type(written).filter(|(data_type, _)| *data_type != DataType::Enum)
        else {
            diagnostics.error(
                package,
                format!("attribute {qualified} has unsupported type `{written}`"),
            );
            return;
        };
        let id = AttributeId(self.attributes.len());
        if self
            .attribute_ids
            .insert((relationship, name.clone()), id)
            .is_some()
        {
            diagnostics.error(package, format!("attribute {qualified} is declared twice"));
            return;
        }
        self.relationships[relationship.index()].attributes.push(id);
        self.attributes.push(Attribute {
            id,
            relationship,
            name,
            description: spec.description().map(str::to_string),
            data_type,
            nullable,
        });
    }

    fn index_adjacency(&mut self) {
        self.outgoing = vec![Vec::new(); self.entities.len()];
        self.incoming = vec![Vec::new(); self.entities.len()];
        for variant in &self.variants {
            self.outgoing[variant.source.index()].push(variant.id);
            self.incoming[variant.target.index()].push(variant.id);
        }
    }
}

fn declare_property(
    entity: EntityId,
    id: PropertyId,
    name: String,
    spec: PropertySpec,
    package: &str,
    diagnostics: &mut Diagnostics,
) -> Option<Property> {
    let written = spec.data_type();
    let Some((data_type, nullable)) = parse_type(written) else {
        diagnostics.error(
            package,
            format!("property {name} has unknown type `{written}`"),
        );
        return None;
    };
    let detail = match spec {
        PropertySpec::Type(_) => PropertyDetail::default(),
        PropertySpec::Detail(detail) => detail,
    };
    if data_type == DataType::Enum
        && detail
            .values
            .as_ref()
            .is_none_or(|values| values.is_empty())
    {
        diagnostics.error(package, format!("enum property {name} declares no values"));
    }
    let enum_type = if detail.stored_as_text {
        EnumType::String
    } else {
        EnumType::Int
    };
    let selectivity = detail
        .selectivity
        .unwrap_or_else(|| crate::FieldSelectivity::from_data_type(data_type));
    let property = Property {
        id,
        selectivity,
        entity,
        name,
        description: detail.description,
        data_type,
        nullable,
        unique: detail.unique,
        enum_values: detail.values,
        enum_type,
        terminal_values: detail.terminal,
        default: detail.default,
    };
    check_default(&property, package, diagnostics);
    Some(property)
}

fn check_default(property: &Property, package: &str, diagnostics: &mut Diagnostics) {
    let Some(default) = &property.default else {
        return;
    };
    let fits = matches!(
        (property.data_type, default),
        (DataType::Bool, DefaultValue::Bool(_))
            | (DataType::Int, DefaultValue::Int(_))
            | (
                DataType::Float,
                DefaultValue::Float(_) | DefaultValue::Int(_)
            )
            | (
                DataType::String | DataType::Enum | DataType::Uuid,
                DefaultValue::String(_)
            )
    );
    if !fits {
        diagnostics.error(
            package,
            format!(
                "property {} default {default:?} is not a {:?}",
                property.name, property.data_type
            ),
        );
    }
}

fn parse_type(written: &str) -> Option<(DataType, bool)> {
    let (word, nullable) = match written.strip_suffix('?') {
        Some(word) => (word, true),
        None => (written, false),
    };
    Some((parse_word(word)?, nullable))
}
