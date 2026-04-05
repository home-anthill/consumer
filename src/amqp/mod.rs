use lapin::message::Delivery;
use lapin::options::BasicConsumeOptions;
use lapin::types::ShortString;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, Queue,
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use tracing::{debug, error, info};
use zeroize::Zeroizing;

use crate::config::redact_uri;
use crate::errors::amqp_error::AmqpError;

// Fix 8: replace four boolean parameters with a hierarchical level enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum InitLevel {
    Connection,
    Channel,
    Queue,
    Consumer,
}

pub struct AmqpClient {
    amqp_uri: Zeroizing<String>,
    amqp_queue_name: ShortString,
    consumer_tag: ShortString,
    properties: ConnectionProperties,
    connection: Option<Connection>,
    channel: Option<Channel>,
    queue: Option<Queue>,
    pub consumer: Option<Consumer>,
    connecting: bool,
}

impl AmqpClient {
    pub fn new(amqp_uri: String, amqp_queue_name: String) -> Self {
        Self {
            amqp_uri: Zeroizing::new(amqp_uri),
            amqp_queue_name: amqp_queue_name.into(),
            properties: ConnectionProperties::default()
                .with_connection_name("amqp-client".into())
                .enable_auto_recover(),
            connection: None,
            channel: None,
            queue: None,
            connecting: false,
            consumer: None,
            consumer_tag: "".into(),
        }
    }

    // Fix 5: return Self instead of the concrete type name
    #[must_use]
    pub fn consumer(mut self, consumer_tag: String) -> Self {
        self.consumer_tag = consumer_tag.into();
        self
    }

    pub fn is_connected(&self, with_consumer: bool) -> bool {
        let level = if with_consumer { InitLevel::Consumer } else { InitLevel::Queue };
        if self.is_initialized(level).is_err() {
            return false;
        }
        self.connection.as_ref().is_some_and(|c| c.status().connected())
            && self.channel.as_ref().is_some_and(|c| c.status().connected())
    }

    pub async fn connect(&mut self, is_consumer: bool) -> Result<(), AmqpError> {
        info!(target: "app", "connect - trying to connect to amqp_uri={} with queue={}", redact_uri(&self.amqp_uri), &self.amqp_queue_name);
        self.connecting = true;
        let result = self.do_connect(is_consumer).await;
        self.connecting = false;
        if result.is_ok() {
            info!(target: "app", "connect - AMQP connection done!");
        }
        result
    }

    async fn do_connect(&mut self, is_consumer: bool) -> Result<(), AmqpError> {
        self.create_connection().await?;
        info!(target: "app", "connect - creating channel...");
        self.create_channel().await?;
        info!(target: "app", "connect - declaring queue...");
        self.declare_queue().await?;
        if is_consumer {
            info!(target: "app", "connect - creating consumer...");
            self.create_consumer().await?;
        }
        Ok(())
    }

    // Fix 3: use ? + map_err instead of match-then-assign
    async fn create_connection(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_connection - creating AMQP connection...");
        let connection = Connection::connect(&self.amqp_uri, self.properties.clone()).await.map_err(|err| {
            error!(target: "app", "create_connection - cannot create AMQP connection. Err = {:?}", err);
            AmqpError::ConnectionError("amqp_client connection error".into())
        })?;
        info!(target: "app", "create_connection - AMQP connection established");
        self.connection = Some(connection);
        Ok(())
    }

    // private method that must be called after create_connection()
    async fn create_channel(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_channel - creating AMQP channel...");
        self.is_initialized(InitLevel::Connection)?;
        let connection = self.connection.as_ref().expect("connection is Some: checked by is_initialized");
        let channel = connection.create_channel().await.map_err(|err| {
            error!(target: "app", "create_channel - cannot create AMQP channel. Err = {:?}", err);
            AmqpError::ConnectionError("amqp_client channel creation error".into())
        })?;
        info!(target: "app", "create_channel - AMQP channel created");
        self.channel = Some(channel);
        Ok(())
    }

    // private method that must be called after both create_connection() and create_channel()
    async fn declare_queue(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "declare_queue - creating AMQP queue...");
        self.is_initialized(InitLevel::Channel)?;
        let channel = self.channel.as_ref().expect("channel is Some: checked by is_initialized");
        let queue = channel
            .queue_declare(self.amqp_queue_name.clone(), QueueDeclareOptions::default(), FieldTable::default())
            .await
            .map_err(|err| {
                error!(target: "app", "declare_queue - cannot create AMQP queue. Err = {:?}", err);
                AmqpError::ConnectionError("amqp_client queue declaration error".into())
            })?;
        info!(target: "app", "declare_queue - AMQP queue created");
        self.queue = Some(queue);
        Ok(())
    }

    // private method that must be called after create_connection(), create_channel() and declare_queue()
    async fn create_consumer(&mut self) -> Result<(), AmqpError> {
        info!(target: "app", "create_consumer - creating AMQP consumer...");
        self.is_initialized(InitLevel::Queue)?;
        let channel = self.channel.as_ref().expect("channel is Some: checked by is_initialized");
        let consumer = channel
            .basic_consume(
                self.amqp_queue_name.clone(),
                self.consumer_tag.clone(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|err| {
                error!(target: "app", "create_consumer - cannot create AMQP consumer. Err = {:?}", err);
                AmqpError::ConnectionError("amqp_client consumer creation error".into())
            })?;
        info!(target: "app", "create_consumer - AMQP consumer created");
        self.consumer = Some(consumer);
        Ok(())
    }

    // before calling this method you must be sure that a channel has been created
    pub async fn publish_message(&mut self, amqp_queue_name: &str, msg_byte: &[u8]) -> Result<(), AmqpError> {
        debug!(target: "app", "publish_message - publishing byte message to queue {}...", amqp_queue_name);
        if self.connecting {
            error!(target: "app", "publish_message - cannot publish while amqp_client is not initialized");
            return Err(AmqpError::Uninitialized("cannot publish while amqp_client is not initialized".into()));
        }
        self.is_initialized(InitLevel::Queue)?;
        let channel = self.channel.as_ref().expect("channel is Some: checked by is_initialized");
        let publish_result = channel
            .basic_publish(
                "".into(),
                amqp_queue_name.into(),
                BasicPublishOptions::default(),
                msg_byte,
                BasicProperties::default(),
            )
            .await;
        match publish_result {
            Ok(_) => Ok(()),
            Err(err) => {
                error!(target: "app", "publish_message - cannot publish, waiting for recovery...");
                self.wait_for_recovery(err).await
            }
        }
    }

    // Fix 8: is_initialized now takes an InitLevel enum instead of four booleans.
    // Connection is always checked; Channel/Queue/Consumer only when level >= that tier.
    fn is_initialized(&self, level: InitLevel) -> Result<(), AmqpError> {
        if self.connection.is_none() {
            error!(target: "app", "is_initialized - amqp_client connection not initialized");
            return Err(AmqpError::Uninitialized("amqp_client connection not initialized".into()));
        }
        if level >= InitLevel::Channel && self.channel.is_none() {
            error!(target: "app", "is_initialized - amqp_client channel not initialized");
            return Err(AmqpError::Uninitialized("amqp_client channel not initialized".into()));
        }
        if level >= InitLevel::Queue && self.queue.is_none() {
            error!(target: "app", "is_initialized - amqp_client queue not initialized");
            return Err(AmqpError::Uninitialized("amqp_client queue not initialized".into()));
        }
        if level >= InitLevel::Consumer && self.consumer.is_none() {
            error!(target: "app", "is_initialized - amqp_client consumer not initialized");
            return Err(AmqpError::Uninitialized("amqp_client consumer not initialized".into()));
        }
        Ok(())
    }

    pub async fn close_connection(&mut self) -> Result<(), AmqpError> {
        self.is_initialized(InitLevel::Connection)?;
        let connection = self.connection.as_ref().expect("connection is Some: checked by is_initialized");
        connection
            .close(0, "".into())
            .await
            .map_err(|e| AmqpError::ConnectionError(format!("cannot close connection: {}", e)))
    }

    pub async fn wait_for_recovery(&mut self, err: lapin::Error) -> Result<(), AmqpError> {
        info!(target: "app", "wait_for_recovery");
        self.is_initialized(InitLevel::Queue)?;
        self.connecting = true;
        let channel = self.channel.as_ref().expect("channel is Some: checked by is_initialized");
        let recovery_result = channel.wait_for_recovery(err).await;
        self.connecting = false;
        if recovery_result.is_ok() {
            Err(AmqpError::ErrorButRecovered("amqp_client error, but connection recovered".into()))
        } else {
            Err(AmqpError::ErrorCannotRecover("amqp_client error, cannot auto recover".into()))
        }
    }
}

const MAX_MESSAGE_BYTES: usize = 65_536; // 64 KiB

pub fn read_message(delivery: &Delivery) -> Result<&str, crate::errors::message_error::MessageError> {
    if delivery.data.len() > MAX_MESSAGE_BYTES {
        return Err(crate::errors::message_error::MessageError::MessageTooLarge(delivery.data.len()));
    }
    Ok(std::str::from_utf8(&delivery.data)?)
}

#[cfg(test)]
mod tests {
    use crate::amqp::{AmqpClient, InitLevel};
    use crate::config::Env;
    use pretty_assertions::assert_eq;

    #[test]
    #[test_log::test]
    fn wrong_is_initialized() {
        // Load env vars without calling init() to avoid conflicting with the
        // global tracing subscriber already installed by #[test_log::test].
        dotenvy::dotenv().ok();
        let env = envy::from_env::<Env>().expect("failed to parse environment variables");
        // create amqp_client without connecting it to the AMQP server
        let amqp_client =
            AmqpClient::new(env.amqp_uri.clone(), env.amqp_queue_name.clone()).consumer("consumer-tag".to_string());

        // When nothing is initialized, all levels fail at the connection check
        for level in [InitLevel::Connection, InitLevel::Channel, InitLevel::Queue, InitLevel::Consumer] {
            let res = amqp_client.is_initialized(level);
            assert_eq!(
                res.err().unwrap().to_string(),
                "amqp_client not initialized: amqp_client connection not initialized"
            );
        }
    }
}
