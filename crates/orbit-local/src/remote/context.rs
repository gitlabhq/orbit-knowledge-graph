use std::sync::OnceLock;

use regex::Regex;

use super::client::OrbitClient;
use super::error::RemoteError;
use super::{ResponseFormat, write_stdout_raw};

pub(crate) async fn run_context(
    refs: Vec<String>,
    format: ResponseFormat,
) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let params = context_params(refs, format);
    let body = client.get_context(&params).await?;
    write_stdout_raw(&body)
}

pub(crate) fn parse_entity_ref(value: &str) -> Result<String, String> {
    static ENTITY_REF: OnceLock<Regex> = OnceLock::new();
    let pattern = ENTITY_REF.get_or_init(|| {
        Regex::new(r"^[A-Za-z]+\[\d+\]$").expect("entity reference regex is valid")
    });

    let value = value.trim();
    if pattern.is_match(value) {
        Ok(value.to_string())
    } else {
        Err(format!(
            "invalid entity reference '{value}'; expected Type[id], for example MergeRequest[123]"
        ))
    }
}

fn context_params(refs: Vec<String>, format: ResponseFormat) -> Vec<(&'static str, String)> {
    refs.into_iter()
        .map(|entity_ref| ("refs[]", entity_ref))
        .chain(std::iter::once((
            "response_format",
            format.as_str().to_string(),
        )))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_params_preserve_ref_order_and_format() {
        let params = context_params(
            vec!["MergeRequest[123]".to_string(), "Issue[456]".to_string()],
            ResponseFormat::Raw,
        );

        assert_eq!(
            params,
            vec![
                ("refs[]", "MergeRequest[123]".to_string()),
                ("refs[]", "Issue[456]".to_string()),
                ("response_format", "raw".to_string()),
            ]
        );
    }
}
