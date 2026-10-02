fn main() {
    println!("cargo:rerun-if-env-changed=GKG_BILLING_ENFORCED");
    println!("cargo:rustc-check-cfg=cfg(gkg_billing_enforced)");
    if std::env::var("GKG_BILLING_ENFORCED").as_deref() == Ok("true") {
        println!("cargo:rustc-cfg=gkg_billing_enforced");
    }
}
