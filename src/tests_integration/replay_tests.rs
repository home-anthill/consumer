use dotenvy::dotenv;
use hmac::{Hmac, KeyInit, Mac};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde_json::json;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use consumer::db::sensor::SensorAuth;
use consumer::errors::message_error::MessageError;
use consumer::models::generic_message::GenericMessage;
use consumer::models::topic::Topic;

use crate::{
    build_signed_mqtt_payload, claim_signed_nonce, ensure_signed_nonce_claimed,
    ensure_topic_matches_registered_feature, signed_replay_key, verify_hmac, verify_mqtt_signature,
};

fn generic_message_with_nonce(device_uuid: &str, feature_uuid: &str, nonce: &str) -> GenericMessage {
    GenericMessage {
        device_uuid: device_uuid.to_string(),
        feature_uuid: feature_uuid.to_string(),
        timestamp: 1_777_630_000,
        nonce: nonce.to_string(),
        signature: "00".repeat(32),
        topic: Topic::new(&format!("sensors/{device_uuid}/temperature")).expect("valid topic"),
        payload: json!({ "value": 21.0 }),
    }
}

fn sign_message(api_token: &str, generic_msg: &GenericMessage) -> String {
    let signed_payload = build_signed_mqtt_payload(generic_msg).expect("signed payload should build");
    let mut mac = Hmac::<Sha256>::new_from_slice(api_token.as_bytes()).expect("HMAC key should be valid");
    mac.update(signed_payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[test]
fn signed_replay_key_scopes_nonce_by_device_and_feature() {
    let key = signed_replay_key("device-a", "feature-b", "nonce-c");

    assert_eq!(key, "signed-replay:v1:device-a:feature-b:nonce-c");
}

#[test]
fn signed_nonce_claim_result_rejects_existing_key() {
    assert!(ensure_signed_nonce_claimed(Some("OK".to_string())).is_ok());

    let err = ensure_signed_nonce_claimed(None).expect_err("duplicate nonce must be rejected");
    assert!(matches!(err, MessageError::ReplayDetected));
}

#[test]
fn signed_payload_binds_feature_name() {
    let msg = generic_message_with_nonce(
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb",
        "41cb3f47-894c-45e9-90d9-a4d4de903896",
        "00112233445566778899aabbccddeeff",
    );

    let signed_payload = build_signed_mqtt_payload(&msg).expect("signed payload");

    assert_eq!(
        signed_payload,
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb\n41cb3f47-894c-45e9-90d9-a4d4de903896\ntemperature\n1777630000\n00112233445566778899aabbccddeeff\n{\"value\":21.0}"
    );
}

#[test]
fn topic_feature_must_match_registered_feature() {
    let msg = generic_message_with_nonce(
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb",
        "41cb3f47-894c-45e9-90d9-a4d4de903896",
        "00112233445566778899aabbccddeeff",
    );
    let sensor_auth = SensorAuth { api_token: "token".to_string(), feature_name: "humidity".to_string() };

    let err =
        ensure_topic_matches_registered_feature(&msg, &sensor_auth).expect_err("mismatched topic feature must fail");

    assert!(matches!(err, MessageError::ValidationError(_)));
}

#[test]
fn hmac_verification_accepts_valid_signature() {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").expect("HMAC key should be valid");
    mac.update(b"message");
    let signature = hex::encode(mac.finalize().into_bytes());

    assert!(verify_hmac("secret", b"message", &signature));
}

#[test]
fn hmac_verification_rejects_invalid_hex_signature() {
    assert!(!verify_hmac("secret", b"message", "not-hex"));
}

#[test]
fn mqtt_signature_accepts_valid_signed_message() {
    let api_token = "api-token";
    let mut msg = generic_message_with_nonce(
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb",
        "41cb3f47-894c-45e9-90d9-a4d4de903896",
        "00112233445566778899aabbccddeeff",
    );
    msg.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    msg.signature = sign_message(api_token, &msg);

    assert!(verify_mqtt_signature(api_token, &msg).is_ok());
}

#[test]
fn mqtt_signature_rejects_stale_timestamp() {
    let msg = generic_message_with_nonce(
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb",
        "41cb3f47-894c-45e9-90d9-a4d4de903896",
        "00112233445566778899aabbccddeeff",
    );

    let err = verify_mqtt_signature("api-token", &msg).expect_err("stale message must fail");

    assert!(matches!(err, MessageError::StaleTimestamp));
}

#[test]
fn mqtt_signature_rejects_wrong_signature() {
    let mut msg = generic_message_with_nonce(
        "246e3256-f0dd-4fcb-82c5-ee20c2267eeb",
        "41cb3f47-894c-45e9-90d9-a4d4de903896",
        "00112233445566778899aabbccddeeff",
    );
    msg.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    msg.signature = "00".repeat(32);

    let err = verify_mqtt_signature("api-token", &msg).expect_err("wrong signature must fail");

    assert!(matches!(err, MessageError::InvalidHmac));
}

#[tokio::test]
// Replaying a valid signed payload would repeat the original side effect even though the
// HMAC is still valid, so this verifies Redis rejects the same signed nonce after first use.
async fn claim_signed_nonce_rejects_duplicate_with_real_redis() {
    dotenv().ok();
    let redis_url = std::env::var("REDIS_URI").unwrap();
    let redis_client = redis::Client::open(redis_url).expect("valid Redis URL");
    let con: ConnectionManager = redis_client.get_connection_manager().await.expect("Redis connection");

    let device_uuid = Uuid::new_v4().to_string();
    let feature_uuid = Uuid::new_v4().to_string();
    let nonce = Uuid::new_v4().simple().to_string();
    let generic_msg = generic_message_with_nonce(&device_uuid, &feature_uuid, &nonce);
    let key = signed_replay_key(&device_uuid, &feature_uuid, &nonce);

    let mut cleanup_con = con.clone();
    let _: usize = cleanup_con.del(&key).await.expect("pre-test cleanup");

    claim_signed_nonce(&con, &generic_msg).await.expect("first nonce claim should pass");
    let err = claim_signed_nonce(&con, &generic_msg).await.expect_err("second nonce claim should be replay");
    assert!(matches!(err, MessageError::ReplayDetected));

    let _: usize = cleanup_con.del(&key).await.expect("post-test cleanup");
}
