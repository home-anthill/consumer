use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde_json::json;
use uuid::Uuid;

use consumer::errors::message_error::MessageError;
use consumer::models::generic_message::GenericMessage;
use consumer::models::topic::Topic;

use crate::{claim_signed_nonce, ensure_signed_nonce_claimed, signed_replay_key};

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

#[tokio::test]
#[ignore = "requires a live Redis instance; set REDIS_TEST_URL when localhost has no unauthenticated Redis"]
// Replaying a valid signed payload would repeat the original side effect even though the
// HMAC is still valid, so this verifies Redis rejects the same signed nonce after first use.
async fn claim_signed_nonce_rejects_duplicate_with_real_redis() {
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url).expect("valid Redis URL");
    let con: ConnectionManager = redis_client.get_connection_manager().await.expect("Redis connection");

    let device_uuid = Uuid::new_v4().to_string();
    let feature_uuid = Uuid::new_v4().to_string();
    let nonce = Uuid::new_v4().to_string();
    let generic_msg = generic_message_with_nonce(&device_uuid, &feature_uuid, &nonce);
    let key = signed_replay_key(&device_uuid, &feature_uuid, &nonce);

    let mut cleanup_con = con.clone();
    let _: usize = cleanup_con.del(&key).await.expect("pre-test cleanup");

    claim_signed_nonce(&con, &generic_msg).await.expect("first nonce claim should pass");
    let err = claim_signed_nonce(&con, &generic_msg).await.expect_err("second nonce claim should be replay");
    assert!(matches!(err, MessageError::ReplayDetected));

    let _: usize = cleanup_con.del(&key).await.expect("post-test cleanup");
}
