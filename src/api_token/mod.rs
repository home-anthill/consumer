use aes_gcm::aead::{Aead, Generate};
use aes_gcm::{Aes256Gcm, KeyInit as AesKeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::digest::KeyInit as HmacKeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;

const API_TOKEN_NONCE_SIZE: usize = 12;

pub fn hash_api_token(api_token: &str, secret: &str) -> Result<String, String> {
    let mut mac =
        <Hmac<Sha256> as HmacKeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(api_token.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub fn encrypt_api_token(api_token: &str, encryption_key: &str) -> Result<String, String> {
    let cipher = <Aes256Gcm as AesKeyInit>::new_from_slice(&api_token_encryption_key(encryption_key)?)
        .map_err(|err| err.to_string())?;
    let nonce = Nonce::generate();
    let ciphertext = cipher.encrypt(&nonce, api_token.as_bytes()).map_err(|err| err.to_string())?;
    let mut encoded = nonce.to_vec();
    encoded.extend_from_slice(&ciphertext);
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

pub fn decrypt_api_token(encrypted: &str, encryption_key: &str) -> Result<String, String> {
    let raw = URL_SAFE_NO_PAD.decode(encrypted).map_err(|err| err.to_string())?;
    if raw.len() <= API_TOKEN_NONCE_SIZE {
        return Err("encrypted api token is too short".to_string());
    }
    let cipher = <Aes256Gcm as AesKeyInit>::new_from_slice(&api_token_encryption_key(encryption_key)?)
        .map_err(|err| err.to_string())?;
    let nonce = Nonce::try_from(&raw[..API_TOKEN_NONCE_SIZE]).map_err(|err| err.to_string())?;
    let plaintext = cipher.decrypt(&nonce, &raw[API_TOKEN_NONCE_SIZE..]).map_err(|err| err.to_string())?;
    String::from_utf8(plaintext).map_err(|err| err.to_string())
}

fn api_token_encryption_key(key: &str) -> Result<[u8; 32], String> {
    if let Ok(decoded) = URL_SAFE_NO_PAD.decode(key)
        && decoded.len() == 32
    {
        return Ok(decoded.try_into().expect("length checked"));
    }
    if let Ok(decoded) = STANDARD.decode(key)
        && decoded.len() == 32
    {
        return Ok(decoded.try_into().expect("length checked"));
    }
    if key.len() == 32 {
        return Ok(key.as_bytes().try_into().expect("length checked"));
    }
    Err("API_TOKEN_ENCRYPTION_KEY must be 32 raw bytes or base64-encoded 32 bytes".to_string())
}

#[cfg(test)]
mod tests {
    use super::{decrypt_api_token, encrypt_api_token, hash_api_token};

    const ENCRYPTION_KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn hash_api_token_is_deterministic() {
        let first = hash_api_token("api-token", "hash-secret").expect("hash should succeed");
        let second = hash_api_token("api-token", "hash-secret").expect("hash should succeed");

        assert_eq!(first, second);
    }

    #[test]
    fn encrypt_then_decrypt_api_token() {
        let encrypted = encrypt_api_token("api-token", ENCRYPTION_KEY).expect("encryption should succeed");
        let decrypted = decrypt_api_token(&encrypted, ENCRYPTION_KEY).expect("decryption should succeed");

        assert_eq!(decrypted, "api-token");
    }

    #[test]
    fn encrypt_api_token_rejects_invalid_key() {
        let err = encrypt_api_token("api-token", "too-short").expect_err("invalid key must fail");

        assert!(err.contains("API_TOKEN_ENCRYPTION_KEY"));
    }

    #[test]
    fn decrypt_api_token_rejects_invalid_base64() {
        let err = decrypt_api_token("a", ENCRYPTION_KEY).expect_err("invalid base64 must fail");

        assert!(err.to_ascii_lowercase().contains("invalid"));
    }

    #[test]
    fn decrypt_api_token_rejects_short_ciphertext() {
        let err = decrypt_api_token("YWJj", ENCRYPTION_KEY).expect_err("short ciphertext must fail");

        assert_eq!(err, "encrypted api token is too short");
    }

    #[test]
    fn decrypt_api_token_rejects_wrong_key() {
        let encrypted = encrypt_api_token("api-token", ENCRYPTION_KEY).expect("encryption should succeed");

        let err = decrypt_api_token(&encrypted, "abcdef0123456789abcdef0123456789")
            .expect_err("wrong key must fail decryption");

        assert!(!err.is_empty());
    }
}
