use super::ResponseFormat;
use super::client::OrbitClient;
use super::error::RemoteError;
use super::{pretty_json, write_stdout};

const GRAPH_STATUS_PATH: &str = "/api/v4/orbit/graph_status";

pub(crate) async fn run_graph_status(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let params = graph_status_params(full_path, namespace_id, project_id, format);
    let body = client.get_body(GRAPH_STATUS_PATH, &params).await?;
    write_stdout(&pretty_json(&body))
}

fn graph_status_params(
    full_path: Option<String>,
    namespace_id: Option<i64>,
    project_id: Option<i64>,
    format: Option<ResponseFormat>,
) -> Vec<(&'static str, String)> {
    let mut params = Vec::new();
    if let Some(id) = namespace_id {
        params.push(("namespace_id", id.to_string()));
    }
    if let Some(id) = project_id {
        params.push(("project_id", id.to_string()));
    }
    if let Some(path) = full_path {
        params.push(("full_path", path));
    }
    if let Some(format) = format {
        params.push(("response_format", format.as_str().to_string()));
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_status_sends_full_path_only_when_present() {
        let params = graph_status_params(Some("gitlab-org/gitlab".to_string()), None, None, None);
        assert_eq!(params, vec![("full_path", "gitlab-org/gitlab".to_string())]);
    }

    #[test]
    fn graph_status_sends_ids_and_format() {
        let params = graph_status_params(None, Some(9970), None, Some(ResponseFormat::Llm));
        assert_eq!(
            params,
            vec![
                ("namespace_id", "9970".to_string()),
                ("response_format", "llm".to_string()),
            ]
        );
    }

    #[test]
    fn graph_status_omits_format_when_unset() {
        let params = graph_status_params(None, None, Some(278964), None);
        assert_eq!(params, vec![("project_id", "278964".to_string())]);
    }
}
