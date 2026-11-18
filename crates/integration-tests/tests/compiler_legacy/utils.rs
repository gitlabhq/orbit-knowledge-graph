use std::collections::HashMap;

use compiler::passes::codegen::ParamValue;

pub fn has_param_value(params: &HashMap<String, ParamValue>, val: &serde_json::Value) -> bool {
    params.values().any(|p| &p.value == val)
}
