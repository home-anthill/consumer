#![allow(clippy::uninlined_format_args)]
use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures_lite::StreamExt;
use hmac::{Hmac, KeyInit, Mac};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::AMQPValue;
use mongodb::Database;
use sha2::Sha256;
use tracing::{debug, error, info};

type HmacSha256 = Hmac<Sha256>;

/// TTL for replay-detection cache entries.
const REPLAY_CACHE_TTL: Duration = Duration::from_secs(300);
/// Maximum number of entries kept in the replay cache at any time.
const REPLAY_CACHE_MAX_CAPACITY: usize = 10_000;

/// In-memory store of recently seen message IDs used to detect AMQP replays.
struct ReplayCache {
    seen: HashMap<String, Instant>,
}

impl ReplayCache {
    fn new() -> Self {
        Self { seen: HashMap::new() }
    }

    /// Returns `true` if `message_id` has been seen within the TTL window (replay detected).
    /// Otherwise records it and returns `false`.
    fn check_and_insert(&mut self, message_id: &str) -> bool {
        let now = Instant::now();
        self.seen.retain(|_, ts| now.duration_since(*ts) < REPLAY_CACHE_TTL);
        if self.seen.contains_key(message_id) {
            return true;
        }
        if self.seen.len() >= REPLAY_CACHE_MAX_CAPACITY
            && let Some(oldest) = self.seen.iter().min_by_key(|(_, ts)| *ts).map(|(k, _)| k.clone())
        {
            self.seen.remove(&oldest);
        }
        self.seen.insert(message_id.to_string(), now);
        false
    }
}

// M1: constant-time HMAC verification — on hex-decode failure we finalize the HMAC
// and discard the result so both paths take the same time.
fn verify_hmac(secret: &str, message: &[u8], expected_hex: &str) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(message);
    match hex::decode(expected_hex) {
        Ok(expected_bytes) => mac.verify_slice(&expected_bytes).is_ok(),
        Err(_) => {
            // Keep execution time constant-time relative to a valid-but-wrong signature:
            // finalize the HMAC and discard so the hot path can't be timed.
            let _ = mac.finalize();
            false
        }
    }
}

use consumer::amqp::{AmqpClient, read_message};
use consumer::config::init;
use consumer::db::connect;
use consumer::db::sensor::update_sensor;
use consumer::errors::message_error::MessageError;
use consumer::models::generic_message::GenericMessage;
use consumer::models::sensor::Sensor;

#[tokio::main]
async fn main() {
    // 1. Init logger and env
    let (env, _app_env) = init();

    // 2. Init MongoDB
    info!(target: "app", "Initializing MongoDB...");
    let database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        std::process::exit(1)
    });

    // 3. Init RabbitMQ
    info!(target: "app", "Initializing RabbitMQ...");
    let mut amqp_client =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.unwrap_or_else(|error| {
        error!(target: "app", "RabbitMQ - cannot connect {:?}", error);
        std::process::exit(1)
    });

    let mut replay_cache = ReplayCache::new();

    // Pin the shutdown future once so signal handlers are registered before the loop starts
    // and are not re-created on each iteration.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let delivery_res = tokio::select! {
            biased;
            sig = &mut shutdown => {
                info!(target: "app", "Received {}, shutting down gracefully", sig);
                break;
            }
            result = async {
                match amqp_client.consumer.as_mut() {
                    Some(c) => c.next().await,
                    None => None,
                }
            } => {
                if let Some(res) = result {
                    res
                } else {
                    if amqp_client.consumer.is_none() {
                        error!(target: "app", "AMQP consumer not initialized");
                    }
                    break;
                }
            }
        };
        match delivery_res {
            Ok(delivery) => {
                // L3: inline ack/nack so nack failure triggers connection recovery.
                match process_delivery(&delivery, &database, &env.amqp_hmac_secret, &mut replay_cache).await {
                    Ok(_) => {
                        if let Err(nack_err) = delivery.ack(BasicAckOptions::default()).await {
                            error!(target: "app", "Failed to ack delivery: {:?}", nack_err);
                            let _ = amqp_client.wait_for_recovery(nack_err).await;
                        }
                    }
                    Err(processing_err) => {
                        error!(target: "app", "Nacking message due to processing error: {}", processing_err);
                        if let Err(nack_err) =
                            delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await
                        {
                            error!(target: "app", "Failed to nack delivery: {:?}", nack_err);
                            let _ = amqp_client.wait_for_recovery(nack_err).await;
                        }
                    }
                }
            }
            Err(err) => {
                error!(target: "app", "AMQP consumer - delivery_res error = {:?}", err);
                info!(target: "app", "AMQP consumer - waiting for recovery...");
                let _ = amqp_client.wait_for_recovery(err).await;
            }
        }
    }

    info!(target: "app", "Closing AMQP connection...");
    if let Err(err) = amqp_client.close_connection().await {
        error!(target: "app", "Failed to close AMQP connection: {:?}", err);
    }
    info!(target: "app", "Shutdown complete");
}

async fn process_delivery(
    delivery: &Delivery,
    database: &Database,
    hmac_secret: &str,
    replay_cache: &mut ReplayCache,
) -> Result<Option<Sensor>, MessageError> {
    let headers = delivery.properties.headers().as_ref().ok_or(MessageError::MissingHmac)?;
    let hmac_val = headers.inner().get("x-hmac-sha256").ok_or(MessageError::MissingHmac)?;
    let expected_hex_bytes = match hmac_val {
        AMQPValue::LongString(s) => s.as_bytes(),
        _ => return Err(MessageError::InvalidHmac),
    };
    // M2: propagate UTF-8 conversion failure as InvalidHmac instead of silently substituting "".
    let expected_hex = std::str::from_utf8(expected_hex_bytes).map_err(|_| MessageError::InvalidHmac)?;

    if !verify_hmac(hmac_secret, &delivery.data, expected_hex) {
        error!(target: "app", "process_delivery - invalid HMAC signature");
        return Err(MessageError::InvalidHmac);
    }

    // H3: Reject replayed messages via message_id.
    let message_id = delivery.properties.message_id().as_ref().ok_or(MessageError::MissingMessageId)?.to_string();
    if replay_cache.check_and_insert(&message_id) {
        error!(target: "app", "process_delivery - replayed message_id detected: {}", message_id);
        return Err(MessageError::ReplayDetected);
    }

    let payload_str = read_message(delivery).inspect_err(|err| {
        error!(target: "app", "process_delivery - cannot read payload: {}", err);
    })?;
    debug!(target: "app", "process_delivery - payload_str = {}", payload_str);

    let generic_msg = serde_json::from_str::<GenericMessage>(payload_str).map_err(|err| {
        error!(target: "app", "process_delivery - cannot parse payload as JSON: {:?}", err);
        MessageError::MessageParsingError
    })?;
    generic_msg.validate().inspect_err(|err| {
        error!(target: "app", "process_delivery - message validation failed: {}", err);
    })?;
    debug!(target: "app", "process_delivery - message received of type = {}", generic_msg.topic.feature_name);
    debug!(target: "app", "process_delivery - message payload deserialized from JSON = {}", generic_msg);

    let bson_value = generic_msg.get_bson_value().ok_or_else(|| {
        error!(target: "app", "process_delivery - cannot extract BSON value from payload");
        MessageError::NoneValuePayloadError
    })?;

    debug!(target: "app", "process_delivery - bson_value = {:?}", &bson_value);

    let sensor = update_sensor(database, &generic_msg, &bson_value).await.map_err(|err| {
        error!(target: "app", "process_delivery - cannot update sensor db: {:?}", err);
        MessageError::UpdateDbError(err)
    })?;

    debug!(target: "app", "process_delivery - sensor db updated with result = {:?}", sensor);
    Ok(sensor)
}

/// Resolves when SIGTERM or SIGINT is received, returning the signal name.
#[cfg(unix)]
async fn shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> &'static str {
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(target: "app", "Failed to register ctrl_c handler: {e}");
    }
    "ctrl_c"
}

// testing
#[cfg(test)]
mod tests_integration;
