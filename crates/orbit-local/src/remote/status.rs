use super::client::OrbitClient;
use super::error::{EXIT_UNAVAILABLE, RemoteError};
use super::{pretty_json, pretty_value, write_stdout};

pub(crate) const STATUS_PATH: &str = "/api/v4/orbit/status";

pub(crate) async fn run_status() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let body = client.get_body(STATUS_PATH, &[]).await?;
    write_stdout(&select_status_output(&body)?)
}

fn select_status_output(body: &[u8]) -> Result<Vec<u8>, RemoteError> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Ok(body.to_vec());
    };

    let Some(user) = value.get("user") else {
        return Ok(pretty_json(body));
    };

    let available = user
        .get("available")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !available {
        return Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit is not available for your user. The `knowledge_graph` feature flag is likely \
             disabled, or the instance lacks the `:orbit` license.",
        ));
    }

    match value.get("system") {
        Some(system) => Ok(pretty_value(system)),
        None => Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit status: user has access but system health is absent. This is unexpected and \
             may indicate an API contract change.",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_flat_shape_is_printed_verbatim() {
        let out = select_status_output(br#"{"status":"healthy","version":"1.0"}"#).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["status"], "healthy");
    }

    #[test]
    fn status_nested_available_prints_system_only() {
        let out =
            select_status_output(br#"{"user":{"available":true},"system":{"status":"healthy"}}"#)
                .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["status"], "healthy");
        assert!(value.get("user").is_none());
    }

    #[test]
    fn status_unavailable_user_is_exit_unavailable() {
        let err = select_status_output(br#"{"user":{"available":false}}"#).unwrap_err();
        assert_eq!(err.exit_code, EXIT_UNAVAILABLE);
    }

    #[test]
    fn status_available_without_system_is_exit_unavailable() {
        let err = select_status_output(br#"{"user":{"available":true}}"#).unwrap_err();
        assert_eq!(err.exit_code, EXIT_UNAVAILABLE);
    }
}
