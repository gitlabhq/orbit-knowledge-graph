fn main() {
    println!("cargo:rerun-if-env-changed=GKG_BILLING_ENFORCED");
    println!("cargo:rustc-check-cfg=cfg(gkg_billing_enforced)");
    match std::env::var("GKG_BILLING_ENFORCED").as_deref() {
        Ok("true") => println!("cargo:rustc-cfg=gkg_billing_enforced"),
        Ok("false" | "") | Err(_) => {}
        Ok(other) => panic!("GKG_BILLING_ENFORCED must be `true` or `false`, got `{other}`"),
    }
}
