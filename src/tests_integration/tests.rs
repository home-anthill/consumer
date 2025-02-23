use futures_lite::StreamExt;
use log::{debug, error};
use mongodb::Database;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::process::Command;
use uuid::Uuid;

use consumer::amqp::AmqpClient;
use consumer::config::{Env, init};
use consumer::db::connect;
use consumer::errors::message_error::MessageError;

use crate::process_amqp_message;
use crate::tests_integration::db_utils::{RegisterInput, drop_all_collections, insert_sensor};
use crate::tests_integration::test_utils::{create_register_input, get_random_mac};

fn run_rabbitmqadmin_cli(payload: &str) {
    Command::new("/usr/local/sbin/rabbitmqadmin")
        .arg("-P")
        .arg("15672")
        .arg("-u")
        .arg("guest")
        .arg("-p")
        .arg("guest")
        .arg("publish")
        .arg("exchange=amq.default")
        .arg("routing_key=ks89")
        .arg("payload=".to_owned() + payload)
        // .arg(r#"properties={"delivery_mode":1}"#)
        .spawn()
        .expect("command failed to start");
}

#[tokio::test]
async fn ok_receive_float_amqp_message() {
    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = match connect(&env).await {
        Ok(database) => database,
        Err(error) => {
            error!(target: "app", "MongoDB - cannot connect {:?}", error);
            panic!("cannot connect to MongoDB:: {:?}", error)
        }
    };
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "temperature";
    let value = 12.23;
    let json_val = json!({
        "uuid": uuid,
        "apiToken": api_token,
        "topic": {
            "family": "sensors",
            "deviceId": uuid,
            "feature": sensor_type
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
    let register_body: RegisterInput =
        create_register_input(&uuid, &mac, manufacturer, model, &api_token, profile_owner_id);
    let _ = insert_sensor(&db, register_body, sensor_type).await;

    // send an AMQP message to the server via `rabbitmqadmin` cli
    run_rabbitmqadmin_cli(json_str.as_str());

    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    assert_eq!(sensor.uuid, uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.manufacturer, manufacturer);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.profileOwnerId, profile_owner_id);
    assert_eq!(sensor.apiToken, api_token);
    assert_eq!(sensor.value, value);

    // cleanup
    drop_all_collections(&db).await;
}

#[tokio::test]
async fn ok_receive_int_amqp_message() {
    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = match connect(&env).await {
        Ok(database) => database,
        Err(error) => {
            error!(target: "app", "MongoDB - cannot connect {:?}", error);
            panic!("cannot connect to MongoDB:: {:?}", error)
        }
    };
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "motion";
    let value: i64 = 1;
    let json_val = json!({
        "uuid": uuid,
        "apiToken": api_token,
        "topic": {
            "family": "sensors",
            "deviceId": uuid,
            "feature": sensor_type
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
    let register_body: RegisterInput =
        create_register_input(&uuid, &mac, manufacturer, model, &api_token, profile_owner_id);
    let _ = insert_sensor(&db, register_body, sensor_type).await;

    // send an AMQP message to the server via `rabbitmqadmin` cli
    run_rabbitmqadmin_cli(json_str.as_str());

    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: resulting sensor should have the updated 'value'
    let sensor = result.unwrap().unwrap();
    assert_eq!(sensor.uuid, uuid);
    assert_eq!(sensor.mac, mac);
    assert_eq!(sensor.manufacturer, manufacturer);
    assert_eq!(sensor.model, model);
    assert_eq!(sensor.profileOwnerId, profile_owner_id);
    assert_eq!(sensor.apiToken, api_token);
    assert_eq!(sensor.value as i64, value);

    // cleanup
    drop_all_collections(&db).await;
}

#[tokio::test]
async fn missing_sensor_receive_amqp_message() {
    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = match connect(&env).await {
        Ok(database) => database,
        Err(error) => {
            error!(target: "app", "MongoDB - cannot connect {:?}", error);
            panic!("cannot connect to MongoDB:: {:?}", error)
        }
    };
    drop_all_collections(&db).await;

    // init AMQP client
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;

    // create AMQP message payload
    let uuid: String = Uuid::new_v4().to_string();
    let api_token: String = Uuid::new_v4().to_string();
    let sensor_type = "unknowntype";
    let value: f64 = 1.0;
    let json_val = json!({
        "uuid": uuid,
        "apiToken": api_token,
        "topic": {
            "family": "sensors",
            "deviceId": uuid,
            "feature": sensor_type
        },
        "payload": {
            "value": value
        }
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    debug!(target: "app", "json_str = {}", json_str);

    // send an AMQP message to the server via `rabbitmqadmin` cli
    run_rabbitmqadmin_cli(json_str.as_str());

    // read and process AMQP message
    let delivery = amqp_client.consumer.as_mut().unwrap().next().await.unwrap().unwrap();
    let result = process_amqp_message(&delivery, &db).await;

    // check results: it must be an error, because `sensor_type="unknowntype"` is not valid
    assert_eq!(
        result.err().unwrap().to_string(),
        anyhow::Error::from(MessageError::NoneValuePayloadError).to_string()
    );

    // cleanup
    drop_all_collections(&db).await;
}

#[tokio::test]
async fn bad_payload_receive_amqp_message() {
    // init logger and env variables
    let env: Env = init();

    // init DB client
    let db: Database = match connect(&env).await {
        Ok(database) => database,
        Err(error) => {
            error!(target: "app", "MongoDB - cannot connect {:?}", error);
            panic!("cannot connect to MongoDB:: {:?}", error)
        }
    };
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

    // send an AMQP message to the server via `rabbitmqadmin` cli
    run_rabbitmqadmin_cli(json_str.as_str());

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
}
