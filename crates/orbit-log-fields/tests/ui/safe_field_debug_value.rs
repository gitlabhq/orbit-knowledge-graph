fn main() {
    orbit_log_fields::info!(safe(KIND) = ?Some("x"), "rejected");
}
