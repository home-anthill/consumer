use log::{debug, error, info};

use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer, Queue, Result,
};

pub async fn amqp_connect(
    amqp_uri: &str,
    amqp_queue_name: &str,
    consumer_tag: &str,
) -> Result<Consumer> {
    // Create connection
    info!(target: "app", "amqp_connect - trying to connect via AMQP...");
    let connection_result = create_connection(amqp_uri).await;
    let connection = connection_result.unwrap();
    connection.on_error(|err| {
        error!(target: "app", "amqp_connect - AMQP connection error = {:?}", err);
    });
    // Create channel
    let channel_result = create_channel(&connection).await;
    let channel: Channel = match channel_result {
        Ok(channel) => channel,
        Err(err) => {
            error!(target: "app", "amqp_connect - cannot create AMQP channel");
            return Err(err);
        }
    };
    // Create queue
    let queue_result = create_queue(&channel, amqp_queue_name).await;
    let queue: Queue = match queue_result {
        Ok(queue) => queue,
        Err(err) => {
            error!(target: "app", "amqp_connect - cannot create AMQP queue");
            return Err(err);
        }
    };
    debug!(target: "app", "amqp_connect - declared queue = {:?}", queue);
    // Create consumer
    let consumer_result = create_consumer(&channel, amqp_queue_name, consumer_tag).await;
    let consumer: Consumer = match consumer_result {
        Ok(consumer) => consumer,
        Err(err) => {
            error!(target: "app", "amqp_connect - cannot create AMQP consumer");
            return Err(err);
        }
    };
    info!(target: "app", "amqp_connect - consumer created, returning it");
    Ok(consumer)
}

pub async fn read_message(delivery: &Delivery) -> &str {
    delivery
        .ack(BasicAckOptions::default())
        .await
        .expect("basic_ack");

    let payload_str = match std::str::from_utf8(&delivery.data) {
        Ok(res) => {
            debug!(target: "app", "read_message - payload_str: {}", res);
            res
        }
        Err(err) => {
            error!(target: "app", "read_message - cannot read payload as utf8. Error = {}", err);
            ""
        }
    };
    payload_str
}

async fn create_connection(amqp_uri: &str) -> Result<Connection> {
    // Use tokio executor and reactor.
    // At the moment the reactor is only available for unix.
    let options = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);
    Connection::connect(amqp_uri, options).await
}

async fn create_channel(connection: &Connection) -> Result<Channel> {
    connection.create_channel().await
}

async fn create_queue(channel: &Channel, amqp_queue_name: &str) -> Result<Queue> {
    channel
        .queue_declare(
            amqp_queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
}

async fn create_consumer(
    channel: &Channel,
    amqp_queue_name: &str,
    consumer_tag: &str,
) -> Result<Consumer> {
    channel
        .basic_consume(
            amqp_queue_name,
            consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
}
