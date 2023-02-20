use thiserror::Error;

// custom error
#[derive(Error, Debug)]
pub enum AmqpError {
    #[error("amqp_client not initialized error")]
    Uninitialized(String),
}
