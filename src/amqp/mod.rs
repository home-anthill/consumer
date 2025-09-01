use std::string::String;
use std::time::Duration;

use lapin::{
    Channel, Connection, ConnectionProperties, Consumer, Error, Queue, RecoveryConfig,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
};
use tracing::{debug, error, info};

use crate::errors::amqp_error::AmqpError;

pub struct AmqpClient {
    connecting: bool,
    connection: Option<Connection>,
    channel: Option<Channel>,
    queue: Option<Queue>,
    pub consumer: Option<Consumer>,
    amqp_uri: String,
    amqp_queue_name: String,
    consumer_tag: String,
}

impl AmqpClient {
    pub fn new(amqp_uri: String, amqp_queue_name: String, consumer_tag: String) -> Self {
        Self {
            connecting: false,
            connection: None,
            channel: None,
            queue: None,
            consumer: None,
            amqp_uri,
            amqp_queue_name,
            consumer_tag,
        }
    }

    // init or re-init the amqp client trying to connect in a loop until success
    pub async fn connect_with_retry_loop(&mut self) {
        info!(target: "app", "connect_with_retry_loop - trying to connect to amqp_uri={} with queue={}", &self.amqp_uri, &self.amqp_queue_name);
        self.connecting = true;
        self.create_connection().await;
        self.create_channel().await.unwrap();
        self.declare_queue().await.unwrap();
        self.create_consumer().await.unwrap();
        self.connecting = false;
        info!(target: "app", "connect_with_retry_loop - AMQP connection done!");
    }

    pub fn is_connected(&self) -> bool {
        // check if you are calling this method on an initialized amqp_client
        // instance (with both connection, channel, queue and consumer)
        let init_result: Result<(), AmqpError> = self.is_initialized(true, true, true, true);
        if init_result.is_err() {
            return false;
        }
        self.channel.as_ref().unwrap().status().connected()
            && self.channel.as_ref().unwrap().status().connected()
            && self.consumer.as_ref().unwrap().state().is_active()
    }

    async fn create_connection(&mut self) {
        info!(target: "app", "create_connection - creating AMQP connection...");
        self.connection = loop {
            let options = ConnectionProperties::default()
                .with_experimental_recovery_config(RecoveryConfig::full())
                .with_executor(tokio_executor_trait::Tokio::current());
            match Connection::connect(&self.amqp_uri, options).await {
                Ok(connection) => {
                    info!(target: "app", "create_connection - AMQP connection established");
                    break Some(connection);
                }
                Err(err) => {
                    error!(target: "app", "create_connection - cannot create AMQP connection, retrying in 10 seconds. Err = {:?}", err);
                    tokio::time::sleep(Duration::from_millis(10000)).await;
                }
            };
        };
        self.connection.as_ref().unwrap().on_error(|err| {
            error!(target: "app", "amqp_connect - AMQP connection error = {:?}", err);
        });
    }

    pub async fn close_connection(&mut self) -> Result<(), Error> {
        self.connection.as_ref().unwrap().close(0, "".as_ref()).await
    }

    // private method that must be called after create_connection()
    async fn create_channel(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_channel - creating AMQP channel...");
        // check if you are calling this method on an initialized amqp_client instance (with ONLY connection)
        let init_result: Result<(), AmqpError> = self.is_initialized(true, false, false, false);
        // if initialization fails, return the error
        // I'm using the '?' operator as https://rust-lang.github.io/rust-clippy/master/index.html#/question_mark
        // instead of the verbose syntax
        // if let Err(err) = init_result { return Err(err); }
        init_result?;
        self.channel = loop {
            match self.connection.as_ref().unwrap().create_channel().await {
                Ok(channel) => {
                    info!(target: "app", "create_channel - AMQP channel created");
                    break Some(channel);
                }
                Err(err) => {
                    error!(target: "app", "create_channel - cannot create AMQP channel, retrying in 10 seconds. Err = {:?}", err);
                    tokio::time::sleep(Duration::from_millis(10000)).await;
                }
            };
        };
        Ok(())
    }

    // private method that must be called after both create_connection() and create_channel()
    async fn declare_queue(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_queue - creating AMQP queue...");
        // check if you are calling this method on an initialized amqp_client instance
        // (with both connection and channel, but not queue and consumer)
        let init_result: Result<(), AmqpError> = self.is_initialized(true, true, false, false);
        // if initialization fails, return the error
        // I'm using the '?' operator as https://rust-lang.github.io/rust-clippy/master/index.html#/question_mark
        // instead of the verbose syntax
        // if let Err(err) = init_result { return Err(err); }
        init_result?;
        self.queue = loop {
            match self
                .channel
                .as_ref()
                .unwrap()
                .queue_declare(
                    &self.amqp_queue_name,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                Ok(channel) => {
                    info!(target: "app", "create_queue - AMQP queue created");
                    break Some(channel);
                }
                Err(err) => {
                    error!(target: "app", "create_queue - cannot create AMQP queue, retrying in 10 seconds. Err = {:?}", err);
                    tokio::time::sleep(Duration::from_millis(10000)).await;
                }
            };
        };
        Ok(())
    }

    // private method that must be called after both create_connection(), create_channel() and create_queue()
    async fn create_consumer(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_consumer - creating AMQP consumer...");
        // check if you are calling this method on an initialized amqp_client instance
        // (with both connection, channel and queue, but not consumer)
        let init_result: Result<(), AmqpError> = self.is_initialized(true, true, true, false);
        // if initialization fails, return the error
        // I'm using the '?' operator as https://rust-lang.github.io/rust-clippy/master/index.html#/question_mark
        // instead of the verbose syntax
        // if let Err(err) = init_result { return Err(err); }
        init_result?;
        self.consumer = loop {
            match self
                .channel
                .as_ref()
                .unwrap()
                .basic_consume(
                    &self.amqp_queue_name,
                    &self.consumer_tag,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                Ok(consumer) => {
                    info!(target: "app", "create_consumer - AMQP consumer created");
                    break Some(consumer);
                }
                Err(err) => {
                    error!(target: "app", "create_consumer - cannot create AMQP consumer, retrying in 10 seconds. Err = {:?}", err);
                    tokio::time::sleep(Duration::from_millis(10000)).await;
                }
            };
        };
        Ok(())
    }

    fn is_initialized(
        &self,
        check_connection: bool,
        check_channel: bool,
        check_queue: bool,
        check_consumer: bool,
    ) -> Result<(), AmqpError> {
        if check_connection && self.connection.is_none() {
            error!(target: "app", "is_initialized - amqp_client connection not initialized. You must call AmqpClient::new()");
            return Err(AmqpError::Uninitialized(String::from(
                "amqp_client connection not initialized. You must call AmqpClient::new()",
            )));
        }
        if check_channel && self.channel.is_none() {
            error!(target: "app", "is_initialized - amqp_client channel not initialized. You must call AmqpClient::new()");
            return Err(AmqpError::Uninitialized(String::from(
                "amqp_client channel not initialized. You must call AmqpClient::new()",
            )));
        }
        if check_queue && self.queue.is_none() {
            error!(target: "app", "is_initialized - amqp_client queue not initialized. You must call AmqpClient::new()");
            return Err(AmqpError::Uninitialized(String::from(
                "amqp_client queue not initialized. You must call AmqpClient::new()",
            )));
        }
        if check_consumer && self.consumer.is_none() {
            error!(target: "app", "is_initialized - amqp_client consumer not initialized. You must call AmqpClient::new()");
            return Err(AmqpError::Uninitialized(String::from(
                "amqp_client consumer not initialized. You must call AmqpClient::new()",
            )));
        }
        Ok(())
    }
}

pub async fn read_message(delivery: &Delivery) -> &str {
    delivery.ack(BasicAckOptions::default()).await.expect("basic_ack");

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

#[cfg(test)]
mod tests {
    use crate::amqp::AmqpClient;
    use crate::config::{Env, init};
    use crate::errors::amqp_error::AmqpError;
    use pretty_assertions::assert_eq;

    #[test]
    #[test_log::test]
    fn wrong_is_initialized() {
        // init logger and env variables
        let env: Env = init();
        // create amqp_client without connecting it to the AMQP server
        let amqp_client = AmqpClient::new(
            env.amqp_uri.clone(),
            env.amqp_queue_name.clone(),
            env.amqp_consumer_tag.clone(),
        );

        // cover all possible errors returned by `is_initialized` method
        let mut res = amqp_client.is_initialized(true, true, true, true);
        assert_eq!(
            res.err().unwrap().to_string(),
            anyhow::Error::from(AmqpError::Uninitialized(String::from(
                "amqp_client connection not initialized. You must call AmqpClient::new()",
            )))
            .to_string()
        );
        res = amqp_client.is_initialized(false, true, true, true);
        assert_eq!(
            res.err().unwrap().to_string(),
            anyhow::Error::from(AmqpError::Uninitialized(String::from(
                "amqp_client channel not initialized. You must call AmqpClient::new()",
            )))
            .to_string()
        );
        res = amqp_client.is_initialized(false, false, true, true);
        assert_eq!(
            res.err().unwrap().to_string(),
            anyhow::Error::from(AmqpError::Uninitialized(String::from(
                "amqp_client queue not initialized. You must call AmqpClient::new()",
            )))
            .to_string()
        );
        res = amqp_client.is_initialized(false, false, false, true);
        assert_eq!(
            res.err().unwrap().to_string(),
            anyhow::Error::from(AmqpError::Uninitialized(String::from(
                "amqp_client consumer not initialized. You must call AmqpClient::new()",
            )))
            .to_string()
        );
    }
}
