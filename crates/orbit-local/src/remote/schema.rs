use super::client::OrbitClient;
use super::error::RemoteError;
use super::{pretty_json, write_stdout};

const SCHEMA_PATH: &str = "/api/v4/orbit/schema";

pub(crate) async fn run_schema(nodes: Vec<String>) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let params: Vec<(&str, String)> = expand_param(&nodes).into_iter().collect();
    let body = client.get_body(SCHEMA_PATH, &params).await?;
    write_stdout(&pretty_json(&body))
}

fn expand_param(nodes: &[String]) -> Option<(&'static str, String)> {
    if nodes.is_empty() {
        None
    } else {
        Some(("expand", nodes.join(",")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_param_is_absent_without_nodes() {
        assert!(expand_param(&[]).is_none());
    }

    #[test]
    fn expand_param_comma_joins_nodes() {
        let param = expand_param(&["User".to_string(), "Project".to_string()]).unwrap();
        assert_eq!(param, ("expand", "User,Project".to_string()));
    }
}
