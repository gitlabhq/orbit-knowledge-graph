use super::client::OrbitClient;
use super::error::RemoteError;
use super::{pretty_value, write_stdout};

pub(crate) async fn run_tools() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let body = client.get_tools().await?;
    write_stdout(&tools_output(&body))
}

fn tools_output(body: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) => match value.get("tools") {
            Some(tools) => pretty_value(tools),
            None => pretty_value(&value),
        },
        Err(_) => body.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tools_output_unwraps_inner_array() {
        let out = tools_output(br#"{"tools":[{"name":"query_graph"}]}"#);
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(value.is_array());
        assert_eq!(value[0]["name"], "query_graph");
    }

    #[test]
    fn tools_output_falls_back_to_whole_body_without_tools_key() {
        let out = tools_output(br#"{"unexpected":1}"#);
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["unexpected"], 1);
    }
}
