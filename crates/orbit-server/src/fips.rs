//! FIPS 140-3 posture of the server binary. Every TLS and JWT primitive runs
//! inside the AWS-LC FIPS module that the `rustls` `fips` feature links; this
//! module makes a mislinked binary refuse to start instead of serving traffic.

use anyhow::Context;

/// AWS-LC FIPS module generation the build is declared against. The CMVP
/// status of that generation is documented in
/// `docs/design-documents/security.md`; moving to another generation is a
/// compliance event, not a routine dependency bump.
pub const DECLARED_MODULE_GENERATION: u32 = 4;

/// Installs the FIPS-restricted rustls provider as the process default and
/// returns the linked AWS-LC version. Must run before any TLS client or JWT
/// operation so that every rustls consumer picks up this provider.
pub fn install_crypto_provider() -> anyhow::Result<&'static str> {
    aws_lc_rs::try_fips_mode()
        .map_err(anyhow::Error::msg)
        .context("gkg-server must link the AWS-LC FIPS module")?;
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    anyhow::ensure!(
        provider.fips(),
        "rustls default provider carries a non-FIPS component"
    );
    provider
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider was already installed"))?;
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
