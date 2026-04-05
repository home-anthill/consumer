use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, DateTime, to_bson};
use serde::{Deserialize, Serialize};

use crate::tests_integration::db_utils::RegisterInput;

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntSensor {
    #[serde(rename = "_id")]
    pub id: ObjectId,
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
    pub value: i64,
    // dates
    pub createdAt: DateTime,
    pub modifiedAt: DateTime,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FloatSensor {
    #[serde(rename = "_id")]
    pub id: ObjectId,
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
    pub createdAt: DateTime,
    pub modifiedAt: DateTime,
}

pub struct SensorConfig {
    pub profile_owner_id: String,
    pub api_token: String,
    pub device_uuid: String,
    pub mac: String,
    pub model: String,
    pub manufacturer: String,
    pub feature_uuid: String,
    pub feature_name: String,
}

pub trait Sensor {
    fn new(config: SensorConfig) -> Self;
}

impl Sensor for IntSensor {
    fn new(config: SensorConfig) -> Self {
        IntSensor::new(config)
    }
}

impl Sensor for FloatSensor {
    fn new(config: SensorConfig) -> Self {
        FloatSensor::new(config)
    }
}

impl IntSensor {
    pub fn new(config: SensorConfig) -> Self {
        let date_now: DateTime = DateTime::now();
        Self {
            id: ObjectId::new(),
            profileOwnerId: config.profile_owner_id,
            apiToken: config.api_token,
            deviceUuid: config.device_uuid,
            mac: config.mac,
            model: config.model,
            manufacturer: config.manufacturer,
            featureUuid: config.feature_uuid,
            featureName: config.feature_name,
            value: 0,
            createdAt: date_now,
            modifiedAt: date_now,
        }
    }
}

impl FloatSensor {
    pub fn new(config: SensorConfig) -> Self {
        let date_now: DateTime = DateTime::now();
        Self {
            id: ObjectId::new(),
            profileOwnerId: config.profile_owner_id,
            apiToken: config.api_token,
            deviceUuid: config.device_uuid,
            mac: config.mac,
            model: config.model,
            manufacturer: config.manufacturer,
            featureUuid: config.feature_uuid,
            featureName: config.feature_name,
            value: 0.0,
            createdAt: date_now,
            modifiedAt: date_now,
        }
    }
}

pub fn new_from_register_input<T: Sensor + Serialize>(input: RegisterInput, sensor_type: &str) -> Bson {
    let config = SensorConfig {
        profile_owner_id: input.profileOwnerId,
        api_token: input.apiToken,
        device_uuid: input.deviceUuid,
        mac: input.mac,
        model: input.model,
        manufacturer: input.manufacturer,
        feature_uuid: input.featureUuid,
        feature_name: sensor_type.to_string(),
    };
    let result = T::new(config);
    to_bson(&result).expect("sensor serialization to BSON cannot fail")
}
