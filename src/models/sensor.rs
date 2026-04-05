use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, DateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SensorDocument {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    // profile info
    pub profile_owner_id: ObjectId,
    pub api_token: String,
    // device info
    pub device_uuid: String,
    pub mac: String,
    pub model: String,
    pub manufacturer: String,
    // feature info
    pub feature_uuid: String,
    pub feature_name: String,
    pub value: Bson,
    // dates
    pub created_at: DateTime,
    pub modified_at: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Sensor {
    #[serde(rename = "_id")]
    pub id: String,
    // profile info
    pub profile_owner_id: String,
    pub api_token: String,
    // device info
    pub device_uuid: String,
    pub mac: String,
    pub model: String,
    pub manufacturer: String,
    // feature info
    pub feature_uuid: String,
    pub feature_name: String,
    pub value: f64,
    // dates
    pub created_at: String,
    pub modified_at: String,
}
