use thiserror::Error;

// custom error, based on 'thiserror' library
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("Payload value is None error")]
    NoneValuePayloadError,
    #[error("Cannot parse message as JSON error")]
    MessageParsingError,
    #[error("Cannot update db with message error")]
    UpdateDbError(mongodb::error::Error),
}
