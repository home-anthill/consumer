use futures_lite::StreamExt;
use mongodb::Database;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};
use uuid::Uuid;

use consumer::amqp::AmqpClient;
use consumer::config::{Env, init};
use consumer::db::connect;
use consumer::errors::message_error::MessageError;

use crate::process_amqp_message;
use crate::tests_integration::db_utils::{RegisterInput, drop_all_collections, insert_sensor};
use crate::tests_integration::test_utils::{create_register_input, get_random_mac};

fn run_rabbitmqadmin_cli(payload: &str) {
    Command::new("rabbitmqadmin")
        .arg("-P")
        .arg("15672")
        .arg("-u")
        .arg("guest")
        .arg("-p")
        .arg("guest")
        .arg("publish")
        .arg("message")
        .arg("-k")
        .arg("ks89")
        .arg("-e")
        .arg("amq.default")
        .arg("-m")
        .arg(payload)
        .spawn()
        .unwrap()
        .wait()
        .expect("publish command failed to start");
}

fn purge_queue_rabbitmqadmin_cli() {
    Command::new("rabbitmqadmin")
        .arg("-P")
        .arg("15672")
        .arg("-u")
        .arg("guest")
        .arg("-p")
        .arg("guest")
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
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;

    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {:?}", error)
    });
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let device_uuid: String = Uuid::new_v4().to_string();
    let feature_uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "temperature";
    let value = 12.23;
    let json_val = json!({
        "deviceUuid": device_uuid,
        "apiToken": api_token,
        "featureUuid": feature_uuid,
        "topic": {
            "family": "sensors",
            "deviceId": device_uuid,
            "featureName": sensor_type
        },
        "payload": {
            "value": value
        }
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    // register a sensor, otherwise it won't be possible to update it's value
    let mac: String = get_random_mac();
    let profile_owner_id = "63963ce7c7fd6d463c6c77a3";
    let manufacturer = "ks89";
    let model = "test-model";
    let register_body: RegisterInput = create_register_input(
        profile_owner_id,
        &api_token,
        &device_uuid,
        &mac,
        model,
        manufacturer,
        &feature_uuid,
    );
    let _ = insert_sensor(&db, register_body, sensor_type).await;

    // send an AMQP message to the server via `rabbitmqadmin` cli
    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(json_str.as_str());
    });
    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    // profile info
    assert_eq!(sensor.profileOwnerId, profile_owner_id);
    assert_eq!(sensor.apiToken, api_token);
    // device info
    assert_eq!(sensor.deviceUuid, device_uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.manufacturer, manufacturer);
    // feature info
    assert_eq!(sensor.featureUuid, feature_uuid);
    assert_eq!(sensor.featureName, sensor_type);
    assert_eq!(sensor.value, value);

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}

#[tokio::test]
#[test_log::test]
async fn ok_receive_int_amqp_message() {
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;

    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {:?}", error)
    });
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let device_uuid: String = Uuid::new_v4().to_string();
    let feature_uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "motion";
    let value: i64 = 1;
    let json_val = json!({
        "deviceUuid": device_uuid,
        "apiToken": api_token,
        "featureUuid": feature_uuid,
        "topic": {
            "family": "sensors",
            "deviceId": device_uuid,
            "featureName": sensor_type
        },
        "payload": {
            "value": value
        }
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    info!(target: "app", "json_str = {}", json_str);

    // register a sensor, otherwise it won't be possible to update it's value
    let mac: String = get_random_mac();
    let profile_owner_id = "63963ce7c7fd6d463c6c77a3";
    let manufacturer = "ks89";
    let model = "test-model";
    let register_body: RegisterInput = create_register_input(
        profile_owner_id,
        &api_token,
        &device_uuid,
        &mac,
        model,
        manufacturer,
        &feature_uuid,
    );
    info!(target: "app", "inserting sensor");
    let _ = insert_sensor(&db, register_body, sensor_type).await;

    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(json_str.as_str());
    });

    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    // profile info
    assert_eq!(sensor.profileOwnerId, profile_owner_id);
    assert_eq!(sensor.apiToken, api_token);
    // device info
    assert_eq!(sensor.deviceUuid, device_uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.manufacturer, manufacturer);
    // feature info
    assert_eq!(sensor.featureUuid, feature_uuid);
    assert_eq!(sensor.featureName, sensor_type);
    assert_eq!(sensor.value as i64, value);

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}

// #[tokio::test]
// #[test_log::test]
// async fn missing_sensor_receive_amqp_message() {
//     purge_queue_rabbitmqadmin_cli();
//     sleep(Duration::from_millis(1000)).await;
//
//     // init logger and env variables
//     let env: Env = init();
//
//     // init DB client
//     let db: Database = connect(&env).await.unwrap_or_else(|error| {
//         error!(target: "app", "MongoDB - cannot connect {:?}", error);
//         panic!("cannot connect to MongoDB:: {:?}", error)
//     });
//     drop_all_collections(&db).await;
//
//     // init AMQP client
//     let mut amqp_client: AmqpClient = AmqpClient::new(
//         env.amqp_uri.clone(),
//         env.amqp_queue_name.clone(),
//         env.amqp_consumer_tag.clone(),
//     );
//     amqp_client.connect_with_retry_loop().await;
//
//     // create AMQP message payload
//     let device_uuid: String = Uuid::new_v4().to_string();
//     let feature_uuid: String = Uuid::new_v4().to_string();
//     let api_token: String = Uuid::new_v4().to_string();
//     let sensor_type = "unknowntype";
//     let value: f64 = 1.0;
//     let json_val = json!({
//         "deviceUuid": device_uuid,
//         "apiToken": api_token,
//         "featureUuid": feature_uuid,
//         "topic": {
//             "family": "sensors",
//             "deviceId": device_uuid,
//             "featureName": sensor_type
//         },
//         "payload": {
//             "value": value
//         }
//     });
//     let json_str = serde_json::to_string(&json_val).unwrap();
//     debug!(target: "app", "json_str = {}", json_str);
//
//     tokio::spawn(async move {
//         info!(target: "app", "waiting 2s before running cli command...");
//         sleep(Duration::from_millis(2000)).await;
//         // send an AMQP message to the server via `rabbitmqadmin` cli
//         run_rabbitmqadmin_cli(json_str.as_str());
//     });
//
//     // read and process AMQP message
//     let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
//     let result = process_amqp_message(&delivery, &db).await;
//
//     // check results: it must be an error, because `sensor_type="unknowntype"` is not valid
//     assert_eq!(
//         result.err().unwrap().to_string(),
//         anyhow::Error::from(MessageError::NoneValuePayloadError).to_string()
//     );
//
//     // cleanup
//     drop_all_collections(&db).await;
//     purge_queue_rabbitmqadmin_cli();
//     sleep(Duration::from_millis(1000)).await;
//     amqp_client.close_connection().await.expect("cannot close connection");
// }

#[tokio::test]
#[test_log::test]
async fn bad_payload_receive_amqp_message() {
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;

    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {:?}", error)
    });
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let json_val = json!({
        "bad_json_payload": "bla bla"
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    tokio::spawn(async move {
        info!(target: "app", "waiting 2s before running cli command...");
        sleep(Duration::from_millis(2000)).await;
        // send an AMQP message to the server via `rabbitmqadmin` cli
        run_rabbitmqadmin_cli(json_str.as_str());
    });

    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: it must be an error, because json message is not valid (not deserializable as GenericMessage)
    assert_eq!(
        result.err().unwrap().to_string(),
        MessageError::MessageParsingError.to_string()
    );

    // cleanup
    drop_all_collections(&db).await;
    purge_queue_rabbitmqadmin_cli();
    sleep(Duration::from_millis(1000)).await;
    amqp_client.close_connection().await.expect("cannot close connection");
}
