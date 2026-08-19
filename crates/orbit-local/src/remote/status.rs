use super::client::OrbitClient;
use super::error::{EXIT_UNAVAILABLE, RemoteError};
use super::{pretty_json, pretty_value, write_stdout};

pub(crate) async fn run_status() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let body = client.get_status().await?;
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
            "Orbit is not available for your user\n\n\
             The Orbit status API reports that your user does not have access.\n\
             The most common causes are:\n\
             \x20 - The `knowledge_graph` feature flag is disabled for your user.\n\
             \x20 - The instance does not include the `:orbit` license add-on.\n\
             Contact an instance administrator to enable it.",
        ));
    }

    match value.get("system") {
        Some(system) => Ok(pretty_value(system)),
        None => Err(RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit status: user has access but system health is absent\n\n\
             The Orbit status API reports user.available=true but did not include\n\
             the system health object. This is unexpected and may indicate an API\n\
             contract change. Update the CLI or contact your instance administrator.",
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
