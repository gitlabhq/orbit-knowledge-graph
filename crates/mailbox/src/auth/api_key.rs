//! Argon2 API key hashing and verification.

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};

use crate::error::MailboxError;

pub fn hash_api_key(api_key: &str) -> Result<String, MailboxError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    let hash = argon2
        .hash_password(api_key.as_bytes(), &salt)
        .map_err(|e| MailboxError::internal(format!("failed to hash API key: {}", e)))?;

    Ok(hash.to_string())
}

pub fn verify_api_key(api_key: &str, hash: &str) -> Result<bool, MailboxError> {
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|e| MailboxError::internal(format!("invalid password hash format: {}", e)))?;

    Ok(Argon2::default()
        .verify_password(api_key.as_bytes(), &parsed_hash)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_succeeds_with_correct_key() {
        let api_key = "my-secret-api-key-12345";
        let hash = hash_api_key(api_key).unwrap();

        assert!(verify_api_key(api_key, &hash).unwrap());
    }

    #[test]
    fn verify_fails_with_incorrect_key() {
        let api_key = "correct-key";
        let hash = hash_api_key(api_key).unwrap();

        assert!(!verify_api_key("wrong-key", &hash).unwrap());
    }

    #[test]
    fn different_calls_produce_different_hashes() {
        let api_key = "same-key";
        let hash1 = hash_api_key(api_key).unwrap();
        let hash2 = hash_api_key(api_key).unwrap();

        assert_ne!(hash1, hash2);
        assert!(verify_api_key(api_key, &hash1).unwrap());
        assert!(verify_api_key(api_key, &hash2).unwrap());
    }

    #[test]
    fn verify_with_invalid_hash_returns_error() {
        let result = verify_api_key("key", "not-a-valid-hash");
        assert!(result.is_err());
    }
}
