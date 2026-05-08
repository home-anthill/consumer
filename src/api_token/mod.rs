use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit as AesKeyInit, Nonce};
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
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
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
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&raw[..API_TOKEN_NONCE_SIZE]), &raw[API_TOKEN_NONCE_SIZE..])
        .map_err(|err| err.to_string())?;
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
