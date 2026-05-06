#![allow(clippy::uninlined_format_args)]
use std::time::{SystemTime, UNIX_EPOCH};

use futures_lite::StreamExt;
use hmac::{Hmac, KeyInit, Mac};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::AMQPValue;
use mongodb::Database;
use redis::aio::ConnectionManager;
use sha2::Sha256;
use tracing::{debug, error, info, warn};

use consumer::amqp::{AmqpClient, read_message};
use consumer::config::init;
use consumer::db::connect;
use consumer::db::sensor::{find_sensor_api_token, update_sensor};
use consumer::errors::message_error::MessageError;
use consumer::models::generic_message::GenericMessage;
use consumer::models::sensor::Sensor;

type HmacSha256 = Hmac<Sha256>;

const SIGNED_MESSAGE_MAX_SKEW_SECS: i64 = 300;
const SIGNED_REPLAY_CACHE_TTL_SECS: usize = 720;

// Constant-time HMAC verification — on hex-decode failure we finalize the HMAC
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

fn build_signed_mqtt_payload(generic_msg: &GenericMessage) -> Result<String, MessageError> {
    let payload_json = serde_json::to_string(&generic_msg.payload).map_err(|_| MessageError::MessageParsingError)?;
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}",
        generic_msg.device_uuid, generic_msg.feature_uuid, generic_msg.timestamp, generic_msg.nonce, payload_json
    ))
}

fn verify_mqtt_signature(api_token: &str, generic_msg: &GenericMessage) -> Result<(), MessageError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| MessageError::StaleTimestamp)?.as_secs() as i64;
    if (now - generic_msg.timestamp).abs() > SIGNED_MESSAGE_MAX_SKEW_SECS {
        return Err(MessageError::StaleTimestamp);
    }
    let signed_payload = build_signed_mqtt_payload(generic_msg)?;
    if !verify_hmac(api_token, signed_payload.as_bytes(), &generic_msg.signature) {
        return Err(MessageError::InvalidHmac);
    }
    Ok(())
}

fn signed_replay_key(device_uuid: &str, feature_uuid: &str, nonce: &str) -> String {
    format!("signed-replay:v1:{device_uuid}:{feature_uuid}:{nonce}")
}

async fn claim_signed_nonce(con: &ConnectionManager, generic_msg: &GenericMessage) -> Result<(), MessageError> {
    let mut con = con.clone();
    let key = signed_replay_key(&generic_msg.device_uuid, &generic_msg.feature_uuid, &generic_msg.nonce);
    let result: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(SIGNED_REPLAY_CACHE_TTL_SECS)
        .query_async(&mut con)
        .await?;
    ensure_signed_nonce_claimed(result)
}

fn ensure_signed_nonce_claimed(result: Option<String>) -> Result<(), MessageError> {
    if result.is_none() {
        return Err(MessageError::ReplayDetected);
    }
    Ok(())
}

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

    // 3. Init Redis for signed MQTT nonce replay protection.
    // If credentials are configured, inject them into the URI:
    //   redis://host:port -> redis://username:password@host:port
    if !env.redis_username.is_empty() && env.redis_password.is_empty() {
        warn!(target: "app", "REDIS_USERNAME is set but REDIS_PASSWORD is empty — no authentication will be attempted");
    }
    let redis_url = if env.redis_password.is_empty() {
        env.redis_uri.clone()
    } else {
        match env.redis_uri.find("://") {
            Some(scheme_end) => format!(
                "{scheme}{username}:{password}@{rest}",
                scheme = &env.redis_uri[..scheme_end + 3],
                username = urlencoding::encode(&env.redis_username),
                password = urlencoding::encode(&env.redis_password),
                rest = &env.redis_uri[scheme_end + 3..],
            ),
            None => {
                warn!(target: "app", "REDIS_URI has no recognizable scheme (missing '://'), skipping credential injection");
                env.redis_uri.clone()
            }
        }
    };
    let redis_client = redis::Client::open(redis_url).expect("invalid Redis URI");
    let redis_con: ConnectionManager = redis_client.get_connection_manager().await.expect("failed to connect to Redis");

    // 4. Init RabbitMQ
    info!(target: "app", "Initializing RabbitMQ...");
    let mut amqp_client =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.unwrap_or_else(|error| {
        error!(target: "app", "RabbitMQ - cannot connect {:?}", error);
        std::process::exit(1)
    });

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
                match process_delivery(&delivery, &database, &redis_con, &env.amqp_hmac_secret).await {
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
    redis_con: &ConnectionManager,
    hmac_secret: &str,
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
    if generic_msg.topic.device_id != generic_msg.device_uuid {
        error!(target: "app", "process_delivery - topic device does not match payload device");
        return Err(MessageError::ValidationError("topic device does not match payload device".into()));
    }
    debug!(target: "app", "process_delivery - message received of type = {}", generic_msg.topic.feature_name);
    debug!(target: "app", "process_delivery - message payload deserialized from JSON = {}", generic_msg);

    let api_token = find_sensor_api_token(database, &generic_msg.device_uuid, &generic_msg.feature_uuid)
        .await
        .map_err(|err| {
            error!(target: "app", "process_delivery - cannot load sensor api token: {:?}", err);
            MessageError::UpdateDbError(err)
        })?
        .ok_or_else(|| {
            error!(target: "app", "process_delivery - sensor not found for signed message");
            MessageError::ValidationError("sensor not found".into())
        })?;
    verify_mqtt_signature(&api_token, &generic_msg).inspect_err(|err| {
        error!(target: "app", "process_delivery - signed MQTT payload verification failed: {}", err);
    })?;
    claim_signed_nonce(redis_con, &generic_msg).await.inspect_err(|err| {
        error!(target: "app", "process_delivery - signed MQTT nonce replay check failed: {}", err);
    })?;

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
