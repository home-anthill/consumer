use futures_lite::StreamExt;

use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use mongodb::Database;
use pretty_assertions::assert_eq;
use redis::aio::ConnectionManager;
use serde_json::json;
use sha2::Sha256;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use consumer::amqp::AmqpClient;
use consumer::api_token::hash_api_token;
use consumer::config::{Env, init};
use consumer::db::connect;
use consumer::errors::message_error::MessageError;

use crate::process_delivery;
use crate::tests_integration::db_utils::{RegisterInput, drop_all_collections, insert_sensor};
use crate::tests_integration::test_utils::{create_register_input, get_random_mac};

/// Returns (username, password) for the RabbitMQ Management HTTP API.
/// Checks AMQP_MANAGEMENT_USER / AMQP_MANAGEMENT_PASS env vars first; if either is absent,
/// falls back to parsing the AMQP URI, then to ("guest", "guest").
fn extract_management_credentials(amqp_uri: &str) -> (String, String) {
    if let (Ok(user), Ok(pass)) = (std::env::var("AMQP_MANAGEMENT_USER"), std::env::var("AMQP_MANAGEMENT_PASS")) {
        return (user, pass);
    }
    let rest = amqp_uri.strip_prefix("amqps://").or_else(|| amqp_uri.strip_prefix("amqp://")).unwrap_or(amqp_uri);
    if let Some(at_pos) = rest.find('@') {
        let creds = &rest[..at_pos];
        if let Some(colon_pos) = creds.find(':') {
            return (creds[..colon_pos].to_string(), creds[colon_pos + 1..].to_string());
        }
    }
    ("guest".to_string(), "guest".to_string())
}

fn run_rabbitmqadmin_cli(payload: &str, hmac_secret: &str, message_id: &str, username: &str, password: &str) {
    let mut mac = Hmac::<Sha256>::new_from_slice(hmac_secret.as_bytes()).unwrap();
    mac.update(payload.as_bytes());
    let sig_hex = hex::encode(mac.finalize().into_bytes());

    Command::new("rabbitmqadmin")
        .arg("-P")
        .arg("15672")
        .arg("-u")
        .arg(username)
        .arg("-p")
        .arg(password)
        .arg("publish")
        .arg("message")
        .arg("-k")
        .arg("ks89")
        .arg("-e")
        .arg("amq.default")
        .arg("-m")
        .arg(payload)
        .arg("--properties")
        .arg(format!("{{\"headers\": {{\"x-hmac-sha256\": \"{sig_hex}\"}}, \"message_id\": \"{message_id}\"}}"))
        .spawn()
        .unwrap()
        .wait()
        .expect("publish command failed to start");
}

async fn connect_redis(env: &Env) -> ConnectionManager {
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
    redis_client.get_connection_manager().await.expect("failed to connect to Redis")
}

fn build_signed_mqtt_message(
    api_token: &str,
    device_uuid: &str,
    feature_uuid: &str,
    sensor_type: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let nonce = Uuid::new_v4().to_string();
    let payload_json = serde_json::to_string(&payload).unwrap();
    let signed_payload = format!("{device_uuid}\n{feature_uuid}\n{timestamp}\n{nonce}\n{payload_json}");

    let mut mac = Hmac::<Sha256>::new_from_slice(api_token.as_bytes()).unwrap();
    mac.update(signed_payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    json!({
        "deviceUuid": device_uuid,
        "featureUuid": feature_uuid,
        "timestamp": timestamp,
        "nonce": nonce,
        "signature": signature,
        "topic": {
            "family": "sensors",
            "deviceId": device_uuid,
            "featureName": sensor_type
        },
        "payload": payload
    })
}

fn purge_queue_rabbitmqadmin_cli(username: &str, password: &str) {
    Command::new("rabbitmqadmin")
        .arg("-P")
        .arg("15672")
        .arg("-u")
        .arg(username)
        .arg("-p")
        .arg(password)
        .arg("purge")
        .arg("queue")
        .arg("--name")
        .arg("ks89")
        .spawn()
        .unwrap()
        .wait()
        .expect("purge command failed to start");
}

#[tokio::test]
#[test_log::test]
async fn ok_receive_float_amqp_message() {
    // init logger and env variables
    let (env, app_env) = init();
    let api_token_hash_secret = env.api_token_hash_secret(app_env).expect("api token hash secret must be configured");
    let (mgmt_user, mgmt_pass) = extract_management_credentials(&env.amqp_uri);

    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {error:?}")
    });
    let redis_con = connect_redis(&env).await;
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.expect("cannot connect to RabbitMQ");

    // create AMQP message payload
    let device_uuid: String = Uuid::new_v4().to_string();
    let feature_uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "temperature";
    let value = 12.23;
    let json_val = build_signed_mqtt_message(
        &api_token,
        &device_uuid,
        &feature_uuid,
        sensor_type,
        json!({
            "value": value
        }),
    );
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    // register a sensor, otherwise it won't be possible to update it's value
    let mac: String = get_random_mac();
    let profile_owner_id = "63963ce7c7fd6d463c6c77a3";
    let manufacturer = "ks89";
    let model = "test-model";
    let register_body: RegisterInput =
        create_register_input(profile_owner_id, &api_token, &device_uuid, &mac, model, manufacturer, &feature_uuid);
    let _ = insert_sensor(&db, register_body, sensor_type, api_token_hash_secret, &env.api_token_encryption_key).await;

    let hmac_secret_clone = env.amqp_hmac_secret.clone();
    let message_id = Uuid::new_v4().to_string();
    let message_id_clone = message_id.clone();
    let mgmt_user_clone = mgmt_user.clone();
    let mgmt_pass_clone = mgmt_pass.clone();
    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(
            json_str.as_str(),
            &hmac_secret_clone,
            &message_id_clone,
            &mgmt_user_clone,
            &mgmt_pass_clone,
        );
    });
    // read and process AMQP message
    let delivery = amqp_client
        .consumer
        .as_mut()
        .expect("consumer initialized")
        .next()
        .await
        .expect("consumer stream not ended")
        .expect("delivery not an error");
    let result =
        process_delivery(&delivery, &db, &redis_con, &env.amqp_hmac_secret, &env.api_token_encryption_key).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    // profile info
    assert_eq!(sensor.profile_owner_id, profile_owner_id);
    assert_eq!(
        sensor.api_token_hash,
        hash_api_token(&api_token, api_token_hash_secret).expect("api token hashing must succeed")
    );
    // device info
    assert_eq!(sensor.device_uuid, device_uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.manufacturer, manufacturer);
    // feature info
    assert_eq!(sensor.feature_uuid, feature_uuid);
    assert_eq!(sensor.feature_name, sensor_type);
    assert!((sensor.value - value).abs() < f64::EPSILON);

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}

#[tokio::test]
#[test_log::test]
async fn ok_receive_int_amqp_message() {
    // init logger and env variables
    let (env, app_env) = init();
    let api_token_hash_secret = env.api_token_hash_secret(app_env).expect("api token hash secret must be configured");
    let (mgmt_user, mgmt_pass) = extract_management_credentials(&env.amqp_uri);

    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {error:?}")
    });
    let redis_con = connect_redis(&env).await;
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.expect("cannot connect to RabbitMQ");

    // create AMQP message payload
    let device_uuid: String = Uuid::new_v4().to_string();
    let feature_uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "motion";
    let value: i64 = 1;
    let json_val = build_signed_mqtt_message(
        &api_token,
        &device_uuid,
        &feature_uuid,
        sensor_type,
        json!({
            "value": value
        }),
    );
    let json_str = serde_json::to_string(&json_val).unwrap();
    info!(target: "app", "json_str = {}", json_str);

    // register a sensor, otherwise it won't be possible to update it's value
    let mac: String = get_random_mac();
    let profile_owner_id = "63963ce7c7fd6d463c6c77a3";
    let manufacturer = "ks89";
    let model = "test-model";
    let register_body: RegisterInput =
        create_register_input(profile_owner_id, &api_token, &device_uuid, &mac, model, manufacturer, &feature_uuid);
    info!(target: "app", "inserting sensor");
    let _ = insert_sensor(&db, register_body, sensor_type, api_token_hash_secret, &env.api_token_encryption_key).await;

    let hmac_secret_clone = env.amqp_hmac_secret.clone();
    let message_id = Uuid::new_v4().to_string();
    let message_id_clone = message_id.clone();
    let mgmt_user_clone = mgmt_user.clone();
    let mgmt_pass_clone = mgmt_pass.clone();
    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(
            json_str.as_str(),
            &hmac_secret_clone,
            &message_id_clone,
            &mgmt_user_clone,
            &mgmt_pass_clone,
        );
    });

    // read and process AMQP message
    let delivery = amqp_client
        .consumer
        .as_mut()
        .expect("consumer initialized")
        .next()
        .await
        .expect("consumer stream not ended")
        .expect("delivery not an error");
    let result =
        process_delivery(&delivery, &db, &redis_con, &env.amqp_hmac_secret, &env.api_token_encryption_key).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    // profile info
    assert_eq!(sensor.profile_owner_id, profile_owner_id);
    assert_eq!(
        sensor.api_token_hash,
        hash_api_token(&api_token, api_token_hash_secret).expect("api token hashing must succeed")
    );
    // device info
    assert_eq!(sensor.device_uuid, device_uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.manufacturer, manufacturer);
    // feature info
    assert_eq!(sensor.feature_uuid, feature_uuid);
    assert_eq!(sensor.feature_name, sensor_type);
    assert_eq!(sensor.value, value as f64);

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}

#[tokio::test]
#[test_log::test]
async fn missing_sensor_receive_amqp_message() {
    // init logger and env variables
    let (env, _app_env) = init();
    let (mgmt_user, mgmt_pass) = extract_management_credentials(&env.amqp_uri);

    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {error:?}")
    });
    let redis_con = connect_redis(&env).await;
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.expect("cannot connect to RabbitMQ");

    // create AMQP message payload
    let device_uuid: String = Uuid::new_v4().to_string();
    let feature_uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "unknowntype";
    let value: f64 = 1.0;
    let json_val = build_signed_mqtt_message(
        &api_token,
        &device_uuid,
        &feature_uuid,
        sensor_type,
        json!({
            "value": value
        }),
    );
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    let hmac_secret_clone = env.amqp_hmac_secret.clone();
    let message_id = Uuid::new_v4().to_string();
    let message_id_clone = message_id.clone();
    let mgmt_user_clone = mgmt_user.clone();
    let mgmt_pass_clone = mgmt_pass.clone();
    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(
            json_str.as_str(),
            &hmac_secret_clone,
            &message_id_clone,
            &mgmt_user_clone,
            &mgmt_pass_clone,
        );
    });

    // read and process AMQP message
    let delivery = amqp_client
        .consumer
        .as_mut()
        .expect("consumer initialized")
        .next()
        .await
        .expect("consumer stream not ended")
        .expect("delivery not an error");
    let result =
        process_delivery(&delivery, &db, &redis_con, &env.amqp_hmac_secret, &env.api_token_encryption_key).await;

    // check results: it must be an error, because `sensor_type="unknowntype"` is rejected by validate()
    assert_eq!(
        result.err().unwrap().to_string(),
        MessageError::ValidationError("unknown feature_name: unknowntype".to_string()).to_string()
    );

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}

#[tokio::test]
#[test_log::test]
async fn bad_payload_receive_amqp_message() {
    // init logger and env variables
    let (env, _app_env) = init();
    let (mgmt_user, mgmt_pass) = extract_management_credentials(&env.amqp_uri);

    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {error:?}")
    });
    let redis_con = connect_redis(&env).await;
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient =
        AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer(env.amqp_consumer_tag.clone());
    amqp_client.connect(true).await.expect("cannot connect to RabbitMQ");

    // create AMQP message payload
    let json_val = json!({
        "bad_json_payload": "bla bla"
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    let hmac_secret_clone = env.amqp_hmac_secret.clone();
    let message_id = Uuid::new_v4().to_string();
    let message_id_clone = message_id.clone();
    let mgmt_user_clone = mgmt_user.clone();
    let mgmt_pass_clone = mgmt_pass.clone();
    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(
            json_str.as_str(),
            &hmac_secret_clone,
            &message_id_clone,
            &mgmt_user_clone,
            &mgmt_pass_clone,
        );
    });

    // read and process AMQP message
    let delivery = amqp_client
        .consumer
        .as_mut()
        .expect("consumer initialized")
        .next()
        .await
        .expect("consumer stream not ended")
        .expect("delivery not an error");
    let result =
        process_delivery(&delivery, &db, &redis_con, &env.amqp_hmac_secret, &env.api_token_encryption_key).await;

    // check results: it must be an error, because json message is not valid (not deserializable as GenericMessage)
    assert_eq!(result.err().unwrap().to_string(), MessageError::MessageParsingError.to_string());

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli(&mgmt_user, &mgmt_pass);
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}
