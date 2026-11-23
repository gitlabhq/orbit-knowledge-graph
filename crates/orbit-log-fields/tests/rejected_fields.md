```compile_fail
orbit_log_fields::info!(safe(KIND) = ?Some("x"), "rejected");
```

```compile_fail
orbit_log_fields::info!(safe(KIND) = %"x", "rejected");
```

```compile_fail
let error = String::from("customer text");
orbit_log_fields::warn!(safe(ERROR_TYPE) = error.as_str(), "rejected");
```

```compile_fail
let error = String::from("customer text");
orbit_log_fields::warn!(safe(ERROR_TYPE) = <&str>::from(&*error), "rejected");
```
