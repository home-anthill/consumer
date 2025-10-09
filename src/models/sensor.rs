use mongodb::bson::DateTime;
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SensorDocument {
    pub _id: ObjectId,
    // profile info
    pub profileOwnerId: ObjectId,
    pub apiToken: String,
    // device info
    pub deviceUuid: String,
    pub mac: String,
    pub model: String,
    pub manufacturer: String,
    // feature info
    pub featureUuid: String,
    pub featureName: String,
    pub value: f64,
    // dates
    pub createdAt: DateTime,
    pub modifiedAt: DateTime,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Sensor {
    pub _id: String,
    // profile info
    pub profileOwnerId: String,
    pub apiToken: String,
    // device info
    pub deviceUuid: String,
    pub mac: String,
    pub model: String,
    pub manufacturer: String,
    // feature info
    pub featureUuid: String,
    pub featureName: String,
    pub value: f64,
    // dates
    pub createdAt: String,
    pub modifiedAt: String,
}
