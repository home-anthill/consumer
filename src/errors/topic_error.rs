use thiserror::Error;

#[derive(Error, Debug)]
pub enum TopicError {
    #[error("expected 3 segments in topic '{topic}', got {got}")]
    InvalidSegmentCount { topic: String, got: usize },
}
