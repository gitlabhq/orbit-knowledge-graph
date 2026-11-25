use std::net::{IpAddr, Ipv4Addr};

pub fn generate_test_certs() -> (String, String) {
    let signing_key = rcgen::KeyPair::generate().expect("failed to generate key pair");
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("failed to create cert params");
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let certificate = params
        .self_signed(&signing_key)
        .expect("failed to self-sign certificate");

    (certificate.pem(), signing_key.serialize_pem())
}

pub fn init_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
