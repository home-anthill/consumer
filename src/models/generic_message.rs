use std::fmt;

use mongodb::bson::Bson;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::errors::message_error::MessageError;
use crate::models::topic::Topic;

const KNOWN_FEATURES: &[&str] = &["temperature", "humidity", "light", "airpressure", "motion", "airquality", "online"];
const SIGNED_NONCE_HEX_LEN: usize = 32;
const SIGNED_SIGNATURE_HEX_LEN: usize = 64;

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

// input message from RabbitMQ
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericMessage {
    pub device_uuid: String,
    pub feature_uuid: String,
    pub timestamp: i64,
    pub nonce: String,
    pub signature: String,
    pub topic: Topic,
    // payload is variable, because it can be PayloadTrait (Temperature, Humidity...)
    // so I need to parse something that cannot be expressed with a fixed struct
    pub payload: Value,
}

impl fmt::Debug for GenericMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericMessage")
            .field("device_uuid", &self.device_uuid)
            .field("feature_uuid", &self.feature_uuid)
            .field("timestamp", &self.timestamp)
            .field("nonce", &self.nonce)
            .field("signature", &"[REDACTED]")
            .field("topic", &self.topic)
            .field("payload", &self.payload)
            .finish()
    }
}

impl fmt::Display for GenericMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GenericMessage {{ device_uuid: {}, feature_uuid: {}, timestamp: {}, nonce: {}, signature: [REDACTED], topic: {}, payload: {} }}",
            self.device_uuid, self.feature_uuid, self.timestamp, self.nonce, self.topic, self.payload
        )
    }
}

impl GenericMessage {
    pub fn validate(&self) -> Result<(), MessageError> {
        if Uuid::parse_str(&self.device_uuid).is_err() {
            return Err(MessageError::ValidationError("device_uuid is not a valid UUID".into()));
        }
        if Uuid::parse_str(&self.feature_uuid).is_err() {
            return Err(MessageError::ValidationError("feature_uuid is not a valid UUID".into()));
        }
        if self.topic.family.is_empty() || self.topic.device_id.is_empty() || self.topic.feature_name.is_empty() {
            return Err(MessageError::ValidationError("topic contains empty segments".into()));
        }
        // M3: reject unknown feature names before any DB work is attempted
        if !KNOWN_FEATURES.contains(&self.topic.feature_name.as_str()) {
            return Err(MessageError::ValidationError(format!("unknown feature_name: {}", self.topic.feature_name)));
        }
        if self.timestamp <= 0 {
            return Err(MessageError::ValidationError("timestamp must be positive".into()));
        }
        if !is_lower_hex(&self.nonce, SIGNED_NONCE_HEX_LEN) {
            return Err(MessageError::ValidationError("nonce must be 32 lowercase hex characters".into()));
        }
        if !is_lower_hex(&self.signature, SIGNED_SIGNATURE_HEX_LEN) {
            return Err(MessageError::ValidationError("signature must be 64 lowercase hex characters".into()));
        }
        Ok(())
    }

    pub fn get_bson_value(&self) -> Option<Bson> {
        match self.topic.feature_name.as_str() {
            "temperature" | "humidity" | "light" | "airpressure" => self.get_value_as_bson_f64(),
            "motion" | "airquality" | "online" => self.get_value_as_bson_i64(),
            _ => None,
        }
    }

    pub fn get_value_as_bson_f64(&self) -> Option<Bson> {
        self.payload.get("value").and_then(serde_json::Value::as_f64).filter(|v| v.is_finite()).map(Bson::Double)
    }
    pub fn get_value_as_bson_i64(&self) -> Option<Bson> {
        let value: i64 = self.payload.get("value").and_then(serde_json::Value::as_i64)?;
        Some(Bson::Int64(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::message_error::MessageError;
    use crate::models::generic_message::GenericMessage;
    use crate::models::topic::Topic;
    use mongodb::bson::to_bson;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn valid_generic_message() -> GenericMessage {
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let sensor_type = "temperature";
        GenericMessage {
            device_uuid: device_uuid.to_string(),
            feature_uuid: "41cb3f47-894c-45e9-90d9-a4d4de903896".to_string(),
            timestamp: 1_777_630_000,
            nonce: "00112233445566778899aabbccddeeff".to_string(),
            signature: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
            topic: Topic::new(format!("sensors/{}/{}", device_uuid, sensor_type).as_str()).unwrap(),
            payload: json!({ "value": 21.0 }),
        }
    }

    #[test]
    #[test_log::test]
    fn ok_get_value_as_bson_f64() {
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let feature_uuid = "41cb3f47-894c-45e9-90d9-a4d4de903896";
        let sensor_type = "temperature";
        let value: f64 = 21.0;

        let topic: Topic = Topic::new(format!("sensors/{}/{}", device_uuid, sensor_type).as_str()).unwrap();
        let generic_msg: GenericMessage = GenericMessage {
            device_uuid: device_uuid.to_string(),
            feature_uuid: feature_uuid.to_string(),
            timestamp: 1_777_630_000,
            nonce: "00112233445566778899aabbccddeeff".to_string(),
            signature: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
            topic,
            payload: json!({ "value": value }),
        };
        let result = generic_msg.get_value_as_bson_f64().unwrap();
        let expected = to_bson::<f64>(&value).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[test_log::test]
    fn ok_get_value_as_bson_i64() {
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let feature_uuid = "41cb3f47-894c-45e9-90d9-a4d4de903896";
        let sensor_type = "motion";
        let value: i64 = 1;

        let topic: Topic = Topic::new(format!("sensors/{}/{}", device_uuid, sensor_type).as_str()).unwrap();
        let generic_msg: GenericMessage = GenericMessage {
            device_uuid: device_uuid.to_string(),
            feature_uuid: feature_uuid.to_string(),
            timestamp: 1_777_630_000,
            nonce: "00112233445566778899aabbccddeeff".to_string(),
            signature: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
            topic,
            payload: json!({ "value": value }),
        };
        let result = generic_msg.get_value_as_bson_i64().unwrap();
        let expected = to_bson::<i64>(&value).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn validate_rejects_malformed_nonce() {
        let mut generic_msg = valid_generic_message();
        generic_msg.nonce = "00112233-4455-6677-8899-aabbccddeeff".to_string();

        let err = generic_msg.validate().expect_err("malformed nonce must fail validation");

        assert!(matches!(err, MessageError::ValidationError(_)));
    }

    #[test]
    fn validate_rejects_malformed_signature() {
        let mut generic_msg = valid_generic_message();
        generic_msg.signature = "not-hex".to_string();

        let err = generic_msg.validate().expect_err("malformed signature must fail validation");

        assert!(matches!(err, MessageError::ValidationError(_)));
    }

    #[test]
    fn validate_rejects_invalid_device_uuid() {
        let mut generic_msg = valid_generic_message();
        generic_msg.device_uuid = "not-a-uuid".to_string();

        let err = generic_msg.validate().expect_err("invalid device UUID must fail validation");

        assert_eq!(err.to_string(), "Message validation error: device_uuid is not a valid UUID");
    }

    #[test]
    fn validate_rejects_invalid_feature_uuid() {
        let mut generic_msg = valid_generic_message();
        generic_msg.feature_uuid = "not-a-uuid".to_string();

        let err = generic_msg.validate().expect_err("invalid feature UUID must fail validation");

        assert_eq!(err.to_string(), "Message validation error: feature_uuid is not a valid UUID");
    }

    #[test]
    fn validate_rejects_empty_topic_segment() {
        let mut generic_msg = valid_generic_message();
        generic_msg.topic.feature_name = String::new();

        let err = generic_msg.validate().expect_err("empty topic segment must fail validation");

        assert_eq!(err.to_string(), "Message validation error: topic contains empty segments");
    }

    #[test]
    fn validate_rejects_unknown_feature_name() {
        let mut generic_msg = valid_generic_message();
        generic_msg.topic.feature_name = "unknown".to_string();

        let err = generic_msg.validate().expect_err("unknown feature must fail validation");

        assert_eq!(err.to_string(), "Message validation error: unknown feature_name: unknown");
    }

    #[test]
    fn validate_rejects_non_positive_timestamp() {
        let mut generic_msg = valid_generic_message();
        generic_msg.timestamp = 0;

        let err = generic_msg.validate().expect_err("non-positive timestamp must fail validation");

        assert_eq!(err.to_string(), "Message validation error: timestamp must be positive");
    }

    #[test]
    fn get_bson_value_returns_none_for_unknown_feature() {
        let mut generic_msg = valid_generic_message();
        generic_msg.topic.feature_name = "unknown".to_string();

        assert_eq!(generic_msg.get_bson_value(), None);
    }

    #[test]
    fn get_value_as_bson_f64_returns_none_for_missing_value() {
        let mut generic_msg = valid_generic_message();
        generic_msg.payload = json!({});

        assert_eq!(generic_msg.get_value_as_bson_f64(), None);
    }

    #[test]
    fn get_value_as_bson_i64_returns_none_for_float_value() {
        let mut generic_msg = valid_generic_message();
        generic_msg.payload = json!({ "value": 1.5 });

        assert_eq!(generic_msg.get_value_as_bson_i64(), None);
    }
}
