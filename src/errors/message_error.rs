use thiserror::Error;

// custom error, based on 'thiserror' library
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("Payload exceeds maximum allowed size of {0} bytes")]
    MessageTooLarge(usize),
    #[error("Payload is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("Payload value is None error")]
    NoneValuePayloadError,
    #[error("Cannot parse message as JSON error")]
    MessageParsingError,
    #[error("Message validation error: {0}")]
    ValidationError(String),
    #[error("Cannot update db with message error")]
    UpdateDbError(mongodb::error::Error),
    #[error("Missing HMAC signature in message properties")]
    MissingHmac,
    #[error("Invalid HMAC signature")]
    InvalidHmac,
    #[error("Missing message_id in message properties")]
    MissingMessageId,
    #[error("Replayed message_id detected")]
    ReplayDetected,
}
