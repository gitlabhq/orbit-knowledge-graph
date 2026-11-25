fn main() {
    let error = String::from("customer text");
    orbit_log_fields::warn!(safe(ERROR_TYPE) = error.as_str(), "rejected");
}
