use super::{client::OrbitClient, error::RemoteError, write_stdout_raw};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Default)]
pub(crate) enum ResponseFormat {
    Json,
    #[default]
    Llm,
}

pub(crate) async fn run(
    refs: Vec<String>,
    response_format: Option<ResponseFormat>,
) -> Result<(), RemoteError> {
    let mut params: Vec<_> = refs
        .into_iter()
        .map(|reference| ("refs[]", reference))
        .collect();
    params.push((
        "response_format",
        match response_format.unwrap_or_default() {
            ResponseFormat::Json => "json",
            ResponseFormat::Llm => "llm",
        }
        .into(),
    ));
    let bytes = OrbitClient::from_env()?.get_context(&params).await?;
    write_stdout_raw(&bytes)
}
