//! Arrow schema generation from plugin node definitions.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

use crate::types::NodeDefinition;

pub fn build_arrow_schema(node_definition: &NodeDefinition) -> Schema {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("traversal_path", DataType::Utf8, false),
    ];

    for property in &node_definition.properties {
        let arrow_type = property.property_type.to_arrow_data_type();
        fields.push(Field::new(&property.name, arrow_type, property.nullable));
    }

    fields.push(Field::new(
        "_version",
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        false,
    ));
    fields.push(Field::new("_deleted", DataType::Boolean, false));

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PropertyDefinition, PropertyType};

    #[test]
    fn generates_schema_with_standard_columns() {
        let node = NodeDefinition::new("test_Vulnerability")
            .with_property(PropertyDefinition::new("score", PropertyType::Float))
            .with_property(PropertyDefinition::new("cve_id", PropertyType::String).nullable());

        let schema = build_arrow_schema(&node);
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(field_names[0], "id");
        assert_eq!(field_names[1], "traversal_path");
        assert_eq!(field_names[2], "score");
        assert_eq!(field_names[3], "cve_id");
        assert_eq!(field_names[4], "_version");
        assert_eq!(field_names[5], "_deleted");
    }

    #[test]
    fn respects_nullable_flag() {
        let node = NodeDefinition::new("test_Node")
            .with_property(PropertyDefinition::new(
                "required_field",
                PropertyType::String,
            ))
            .with_property(
                PropertyDefinition::new("optional_field", PropertyType::String).nullable(),
            );

        let schema = build_arrow_schema(&node);

        let required = schema.field_with_name("required_field").unwrap();
        let optional = schema.field_with_name("optional_field").unwrap();

        assert!(!required.is_nullable());
        assert!(optional.is_nullable());
    }

    #[test]
    fn maps_property_types_correctly() {
        let node = NodeDefinition::new("test_Node")
            .with_property(PropertyDefinition::new("string_prop", PropertyType::String))
            .with_property(PropertyDefinition::new("int_prop", PropertyType::Int64))
            .with_property(PropertyDefinition::new("float_prop", PropertyType::Float))
            .with_property(PropertyDefinition::new("bool_prop", PropertyType::Boolean))
            .with_property(PropertyDefinition::new("date_prop", PropertyType::Date))
            .with_property(PropertyDefinition::new(
                "timestamp_prop",
                PropertyType::Timestamp,
            ))
            .with_property(
                PropertyDefinition::new("enum_prop", PropertyType::Enum)
                    .with_enum_values(vec!["a".into(), "b".into()]),
            );

        let schema = build_arrow_schema(&node);

        assert_eq!(
            schema.field_with_name("string_prop").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("int_prop").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("float_prop").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("bool_prop").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            schema.field_with_name("date_prop").unwrap().data_type(),
            &DataType::Date32
        );
        assert!(matches!(
            schema
                .field_with_name("timestamp_prop")
                .unwrap()
                .data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert_eq!(
            schema.field_with_name("enum_prop").unwrap().data_type(),
            &DataType::Utf8
        );
    }
}
