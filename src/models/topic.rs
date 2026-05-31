use std::fmt;

use serde::{Deserialize, Serialize};

use crate::errors::topic_error::TopicError;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub family: String,
    pub device_id: String,
    pub feature_name: String,
}

impl Topic {
    pub fn new(topic: &str) -> Result<Self, TopicError> {
        let mut parts = topic.splitn(4, '/');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(family), Some(device_id), Some(feature_name), None) => Ok(Self {
                family: family.to_string(),
                device_id: device_id.to_string(),
                feature_name: feature_name.to_string(),
            }),
            _ => Err(TopicError::InvalidSegmentCount { topic: topic.to_string(), got: topic.split('/').count() }),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}/{}", self.family, self.device_id, self.feature_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::topic_error::TopicError;
    use crate::models::topic::Topic;
    use pretty_assertions::assert_eq;

    #[test]
    #[test_log::test]
    fn check_topic_display() {
        let uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let sensor_type = "temperature";

        let topic: Topic = Topic::new(format!("sensors/{}/{}", uuid, sensor_type).as_str()).unwrap();
        let expected = topic.to_string();
        assert_eq!(format!("sensors/{}/{}", uuid, sensor_type), expected);
    }

    #[test]
    fn topic_new_rejects_missing_segment() {
        let err = Topic::new("sensors/device-only").expect_err("missing feature segment must fail");

        assert!(matches!(err, TopicError::InvalidSegmentCount { got: 2, .. }));
        assert_eq!(err.to_string(), "expected 3 segments in topic 'sensors/device-only', got 2");
    }

    #[test]
    fn topic_new_rejects_extra_segment() {
        let err = Topic::new("sensors/device/temperature/extra").expect_err("extra topic segment must fail");

        assert!(matches!(err, TopicError::InvalidSegmentCount { got: 4, .. }));
    }
}
