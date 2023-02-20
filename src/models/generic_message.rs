use mongodb::bson::{to_bson, Bson};
use serde::Deserialize;
use serde_json::Value;

use crate::models::topic::Topic;

// input message from RabbitMQ
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericMessage {
    pub uuid: String,
    pub api_token: String,
    pub topic: Topic,
    // payload is variable, because it can be PayloadTrait (Temperature, Humidity...)
    // so I need to parse something that cannot be expressed with a fixed struct
    pub payload: Value,
}

impl GenericMessage {
    pub fn get_value_as_bson_f32(&self) -> Option<Bson> {
        let res_opt: Option<f64> = self.payload.get("value").and_then(|value| value.as_f64());
        if res_opt.is_none() {
            return None;
        }
        let value = res_opt.unwrap() as f32;
        match to_bson::<f32>(&value) {
            Ok(val) => Some(val),
            Err(err) => None,
        }
    }
    pub fn get_value_as_bson_i32(&self) -> Option<Bson> {
        let res_opt: Option<i64> = self.payload.get("value").and_then(|value| value.as_i64());
        if res_opt.is_none() {
            return None;
        }
        let value = res_opt.unwrap() as i32;
        match to_bson::<i32>(&value) {
            Ok(val) => Some(val),
            Err(err) => None,
        }
    }
}
