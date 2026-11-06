use query_data_model::OrbitQueryModel;

use crate::Input;
use crate::input::ColumnSelection;

pub(super) fn fill(input: &mut Input, model: &impl OrbitQueryModel) {
    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref().and_then(|name| model.entity(name)) else {
            continue;
        };
        if matches!(node.columns, Some(ColumnSelection::List(_))) {
            continue;
        }
        let defaults = model.default_properties(entity.id);
        let properties = match node.columns.is_none() && !defaults.is_empty() {
            true => defaults,
            false => &entity.properties,
        };
        let columns = properties
            .iter()
            .filter(|property| model.property_realization(**property).is_some())
            .filter(|property| !model.property_is_hidden(**property))
            .filter(|property| !model.property_is_admin_only(**property))
            .map(|property| model.graph().property(*property).name.clone())
            .collect();
        node.columns = Some(ColumnSelection::List(columns));
    }
}
