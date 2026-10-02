fn main() {
    println!("cargo:rerun-if-env-changed=ORBIT_BILLING_ENFORCED");
    println!("cargo:rustc-check-cfg=cfg(orbit_billing_enforced)");
    match std::env::var("ORBIT_BILLING_ENFORCED").as_deref() {
        Ok("true") => println!("cargo:rustc-cfg=orbit_billing_enforced"),
        Ok("false" | "") | Err(_) => {}
        Ok(other) => panic!("ORBIT_BILLING_ENFORCED must be `true` or `false`, got `{other}`"),
    }
}
