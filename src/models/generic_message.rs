use mongodb::bson::{Bson, to_bson};
use serde::Deserialize;
use serde_json::Value;

use crate::models::topic::Topic;

// input message from RabbitMQ
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericMessage {
    pub api_token: String,
    pub device_uuid: String,
    pub feature_uuid: String,
    pub topic: Topic,
    // payload is variable, because it can be PayloadTrait (Temperature, Humidity...)
    // so I need to parse something that cannot be expressed with a fixed struct
    pub payload: Value,
}

impl GenericMessage {
    pub fn get_value_as_bson_f64(&self) -> Option<Bson> {
        let value: f64 = self.payload.get("value").and_then(|value| value.as_f64())?;
        to_bson::<f64>(&value).ok()
    }
    pub fn get_value_as_bson_i64(&self) -> Option<Bson> {
        let value: i64 = self.payload.get("value").and_then(|value| value.as_i64())?;
        to_bson::<i64>(&value).ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::models::generic_message::GenericMessage;
    use crate::models::topic::Topic;
    use mongodb::bson::to_bson;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    #[test_log::test]
    fn ok_get_value_as_bson_f64() {
        let api_token = "473a4861-632b-4915-b01e-cf1d418966c6";
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let feature_uuid = "41cb3f47-894c-45e9-90d9-a4d4de903896";
        let sensor_type = "temperature";
        let value: f64 = 21.0;

        let topic: Topic = Topic::new(format!("sensors/{}/{}", device_uuid, sensor_type).as_str());
        let generic_msg: GenericMessage = GenericMessage {
            api_token: api_token.to_string(),
            device_uuid: device_uuid.to_string(),
            feature_uuid: feature_uuid.to_string(),
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
        let api_token = "473a4861-632b-4915-b01e-cf1d418966c6";
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let feature_uuid = "41cb3f47-894c-45e9-90d9-a4d4de903896";
        let sensor_type = "motion";
        let value: i64 = 1;

        let topic: Topic = Topic::new(format!("sensors/{}/{}", device_uuid, sensor_type).as_str());
        let generic_msg: GenericMessage = GenericMessage {
            api_token: api_token.to_string(),
            device_uuid: device_uuid.to_string(),
            feature_uuid: feature_uuid.to_string(),
            topic,
            payload: json!({ "value": value }),
        };
        let result = generic_msg.get_value_as_bson_i64().unwrap();
        let expected = to_bson::<i64>(&value).unwrap();
        assert_eq!(result, expected);
    }
}
