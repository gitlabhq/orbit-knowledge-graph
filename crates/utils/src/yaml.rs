pub use serde_saphyr::{Error, SerializeError};

fn options() -> serde_saphyr::Options {
    serde_saphyr::options! { strict_booleans: true }
}

pub fn from_str<'de, T: serde::Deserialize<'de>>(input: &'de str) -> Result<T, Error> {
    serde_saphyr::from_str_with_options(input, options())
}

pub fn from_slice<'de, T: serde::Deserialize<'de>>(input: &'de [u8]) -> Result<T, Error> {
    serde_saphyr::from_slice_with_options(input, options())
}

pub fn to_string<T: serde::Serialize>(value: &T) -> Result<String, SerializeError> {
    serde_saphyr::to_string(value)
}

#[cfg(test)]
mod tests {
    #[test]
    fn strict_booleans_keep_yaml_11_scalars_as_strings() {
        let value: serde_json::Value = super::from_str("a: [y, n, yes, on, true]").unwrap();
        assert_eq!(
            value,
            serde_json::json!({"a": ["y", "n", "yes", "on", true]})
        );
    }

    #[test]
    fn duplicate_keys_error() {
        let result: Result<serde_json::Value, _> = super::from_str("a: 1\na: 2\n");
        assert!(result.is_err());
    }

    #[test]
    fn round_trips_nested_maps() {
        let value: serde_json::Value =
            super::from_str("outer:\n  list:\n    - 1\n    - two\n  flag: false\n").unwrap();
        let rendered = super::to_string(&value).unwrap();
        let reparsed: serde_json::Value = super::from_str(&rendered).unwrap();
        assert_eq!(value, reparsed);
    }
}
