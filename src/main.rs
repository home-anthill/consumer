use futures_lite::StreamExt;
use lapin::message::Delivery;
use mongodb::Database;
use mongodb::bson::Bson;
use tracing::{debug, error, info};

use consumer::amqp::{AmqpClient, read_message};
use consumer::config::{Env, init};
use consumer::db::connect;
use consumer::db::sensor::update_sensor;
use consumer::errors::message_error::MessageError;
use consumer::models::generic_message::GenericMessage;
use consumer::models::sensor::Sensor;

#[tokio::main]
async fn main() {
    // 1. Init logger and env
    let env: Env = init();

    // 2. Init MongoDB
    info!(target: "app", "Initializing MongoDB...");
    let database: Database = connect(&env).await.unwrap_or_else(|error| {
        error!(target: "app", "MongoDB - cannot connect {:?}", error);
        panic!("cannot connect to MongoDB:: {:?}", error)
    });

    // 3. Init RabbitMQ
    info!(target: "app", "Initializing RabbitMQ...");
    let mut amqp_client: AmqpClient = AmqpClient::new(
        env.amqp_uri.clone(),
        env.amqp_queue_name.clone(),
        env.amqp_consumer_tag.clone(),
    );
    amqp_client.connect_with_retry_loop().await;
    while let Some(delivery_res) = amqp_client.consumer.as_mut().unwrap().next().await {
        if let Ok(delivery) = delivery_res {
            let _ = process_amqp_message(&delivery, &database).await;
        } else {
            error!(target: "app", "AMQP consumer - delivery_res error = {:?}", delivery_res.err());
            info!(target: "app", "AMQP reconnecting...");
            amqp_client.connect_with_retry_loop().await;
        }
    }
}

async fn process_amqp_message(delivery: &Delivery, database: &Database) -> Result<Option<Sensor>, MessageError> {
    let payload_str: &str = read_message(delivery).await;
    debug!(target: "app", "process_amqp_message - payload_str = {}", payload_str);
    // deserialize to a GenericMessage (with turbofish operator "::<GenericMessage>")
    match serde_json::from_str::<GenericMessage>(payload_str) {
        Ok(generic_msg) => {
            debug!(target: "app", "AMQP message received of type = {}", generic_msg.topic.feature);
            debug!(target: "app", "AMQP message payload deserialized from JSON = {:?}", generic_msg);

            let bson_value_opt: Option<Bson> = match generic_msg.topic.feature.as_str() {
                // f64 sensors
                "temperature" | "humidity" | "light" | "airpressure" => generic_msg.get_value_as_bson_f64(),
                // i64 sensors
                "motion" | "airquality" | "poweroutage" => generic_msg.get_value_as_bson_i64(),
                _ => {
                    error!(target: "app", "cannot recognize Message payload type = {}", generic_msg.topic.feature);
                    None
                }
            };
            debug!(target: "app", "bson_value_opt = {:?}", &bson_value_opt);
            if let Some(bson_value) = bson_value_opt {
                match update_sensor(database, &generic_msg, &bson_value).await {
                    Ok(sensor) => {
                        debug!(target: "app", "sensor db updated with result = {:?}", sensor);
                        Ok(sensor)
                    }
                    Err(err) => {
                        error!(target: "app", "cannot update sensor db, err = {:?}", err);
                        Err(MessageError::UpdateDbError(err))
                    }
                }
            } else {
                error!(target: "app", "cannot update sensor, because bson_value_opt is None");
                Err(MessageError::NoneValuePayloadError)
            }
        }
        Err(err) => {
            error!(target: "app", "Cannot convert payload as json Message. Error = {:?}", err);
            Err(MessageError::MessageParsingError)
        }
    }
}

// testing
#[cfg(test)]
mod tests_integration;
