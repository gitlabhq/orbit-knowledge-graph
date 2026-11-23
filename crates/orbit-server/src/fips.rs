use anyhow::Context;

pub const DECLARED_MODULE_GENERATION: u32 = 4;

const BUILD_DEFECT_HINT: &str = "This is a defect in how this binary was built, not a configuration \
problem: no setting turns FIPS mode on or off. Run an official GitLab Orbit image, or rebuild \
gkg-server from an unmodified checkout so the FIPS build features apply.";

pub fn install_crypto_provider() -> anyhow::Result<&'static str> {
    aws_lc_rs::try_fips_mode()
        .map_err(anyhow::Error::msg)
        .with_context(|| {
            format!(
                "GitLab Orbit was built with an incorrectly linked AWS-LC FIPS module: the \
                 linked AWS-LC {} does not run in FIPS mode. {BUILD_DEFECT_HINT}",
                aws_lc_rs::awslc_version()
            )
        })?;
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    anyhow::ensure!(
        provider.fips(),
        "GitLab Orbit was built with a TLS provider that includes non-FIPS algorithms. \
         {BUILD_DEFECT_HINT}"
    );
    provider.install_default().map_err(|_| {
        anyhow::anyhow!(
            "GitLab Orbit started a TLS component before its FIPS provider was installed. \
             This is a bug in gkg-server startup ordering; report it with the exact version."
        )
    })?;
    Ok(aws_lc_rs::awslc_version())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_module_is_the_declared_generation() {
        assert_eq!(aws_lc_rs::fips_version(), Some(DECLARED_MODULE_GENERATION));
    }

    #[test]
    fn default_provider_is_fips() {
        assert!(rustls::crypto::aws_lc_rs::default_provider().fips());
    }
}
