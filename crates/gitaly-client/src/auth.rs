use crate::GitalyError;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use regex::Regex;
use sha2::Sha256;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::metadata::MetadataValue;

static TOKEN_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:(?P<bearer>Bearer .+)|(?P<v2>v2\..+))$").expect("invalid token regex")
});

pub fn authorization_metadata(
    token: &str,
) -> Result<MetadataValue<tonic::metadata::Ascii>, GitalyError> {
    let header_value = if let Some(caps) = TOKEN_PATTERN.captures(token) {
        if caps.name("bearer").is_some() {
            token.to_string()
        } else {
            format!("Bearer {token}")
        }
    } else {
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| GitalyError::InvalidToken(format!("system time error: {e}")))?
            .as_secs()
            .to_string();
        let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes())
            .map_err(|e| GitalyError::InvalidToken(format!("invalid token length: {e}")))?;
        mac.update(issued_at.as_bytes());
        let hmac = hex_encode(mac.finalize().into_bytes());
        format!("Bearer v2.{hmac}.{issued_at}")
    };

    MetadataValue::try_from(header_value).map_err(|e| {
        GitalyError::InvalidToken(format!("failed to build authorization metadata: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_raw_secret_as_v2_token() {
        let value = authorization_metadata("super-secret-token").unwrap();
        let header = value.to_str().unwrap();
        assert!(header.starts_with("Bearer v2."), "header was {header}");
        let parts: Vec<&str> = header.trim_start_matches("Bearer ").split('.').collect();
        assert_eq!(parts.len(), 3, "expected v2.HMAC.issued_at, got {header}");
        assert_eq!(parts[0], "v2");
        assert_eq!(parts[1].len(), 64, "HMAC should be 64 hex chars");
        assert!(
            parts[2].parse::<u64>().is_ok(),
            "issued_at should be unix timestamp"
        );
    }

    #[test]
    fn keeps_v2_token_with_bearer_prefix() {
        let input = "v2.deadbeef.123";
        let value = authorization_metadata(input).unwrap();
        assert_eq!(value.to_str().unwrap(), format!("Bearer {input}"));
    }

    #[test]
    fn keeps_bearer_token_unchanged() {
        let input = "Bearer some-precomputed-token";
        let value = authorization_metadata(input).unwrap();
        assert_eq!(value.to_str().unwrap(), input);
    }
}
