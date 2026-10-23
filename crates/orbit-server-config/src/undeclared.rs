//! What lenient protobuf decoding would drop without an error.

use std::sync::LazyLock;

use prost_reflect::{DescriptorPool, ExtensionDescriptor, Kind, MessageDescriptor};
use serde_json::Value;

use crate::proto::FILE_DESCRIPTOR_SET;

static POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("the generated descriptor set decodes")
});

/// One line per enum name the schema does not declare, and per key a message
/// marked `option (closed)` does not declare. Decoding drops both before
/// validation can see them.
pub fn undeclared(document: &Value) -> Vec<String> {
    let config = POOL
        .get_message_by_name("orbit.config.v1.Config")
        .expect("the schema declares Config");
    let closed = POOL
        .get_extension_by_name("orbit.config.v1.closed")
        .expect("the schema declares the closed option");
    let mut errors = Vec::new();
    message(document, &config, &closed, "", &mut errors);
    errors
}

fn message(
    value: &Value,
    descriptor: &MessageDescriptor,
    closed: &ExtensionDescriptor,
    path: &str,
    errors: &mut Vec<String>,
) {
    let Value::Object(object) = value else { return };
    let is_closed = descriptor.options().get_extension(closed).as_bool() == Some(true);
    for (key, value) in object {
        let at = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        let Some(field) = descriptor
            .get_field_by_json_name(key)
            .or_else(|| descriptor.get_field_by_name(key))
        else {
            if is_closed {
                errors.push(format!("{at}: unknown field"));
            }
            continue;
        };
        match value {
            Value::Object(entries) if field.is_map() => {
                let kind = field.kind();
                let entry = kind.as_message().expect("a map field is a message");
                for (name, value) in entries {
                    element(
                        value,
                        &entry.map_entry_value_field().kind(),
                        closed,
                        &format!("{at}.{name}"),
                        errors,
                    );
                }
            }
            Value::Array(items) if field.is_list() => {
                for (index, item) in items.iter().enumerate() {
                    element(
                        item,
                        &field.kind(),
                        closed,
                        &format!("{at}[{index}]"),
                        errors,
                    );
                }
            }
            value => element(value, &field.kind(), closed, &at, errors),
        }
    }
}

fn element(
    value: &Value,
    kind: &Kind,
    closed: &ExtensionDescriptor,
    path: &str,
    errors: &mut Vec<String>,
) {
    match (kind, value) {
        (Kind::Message(descriptor), _) => message(value, descriptor, closed, path, errors),
        (Kind::Enum(descriptor), Value::String(name))
            if descriptor.get_value_by_name(name).is_none() =>
        {
            let declared: Vec<_> = descriptor
                .values()
                .map(|value| value.name().to_owned())
                .collect();
            errors.push(format!(
                "{path}: unknown value {name:?}, expected one of {}",
                declared.join(", ")
            ));
        }
        _ => {}
    }
}
