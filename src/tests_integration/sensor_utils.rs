use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, DateTime, to_bson};
use serde::{Deserialize, Serialize};

use crate::tests_integration::db_utils::RegisterInput;
use consumer::api_token::{encrypt_api_token, hash_api_token};

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntSensor {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    // profile info
    pub profileOwnerId: String,
    pub apiTokenHash: String,
    pub apiTokenEncrypted: String,
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
    pub apiTokenHash: String,
    pub apiTokenEncrypted: String,
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
    pub api_token_hash_secret: String,
    pub api_token_encryption_key: String,
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
            apiTokenHash: hash_api_token(&config.api_token, &config.api_token_hash_secret)
                .expect("api token hashing must succeed"),
            apiTokenEncrypted: encrypt_api_token(&config.api_token, &config.api_token_encryption_key)
                .expect("api token encryption must succeed"),
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
            apiTokenHash: hash_api_token(&config.api_token, &config.api_token_hash_secret)
                .expect("api token hashing must succeed"),
            apiTokenEncrypted: encrypt_api_token(&config.api_token, &config.api_token_encryption_key)
                .expect("api token encryption must succeed"),
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

pub fn new_from_register_input<T: Sensor + Serialize>(
    input: RegisterInput,
    sensor_type: &str,
    api_token_hash_secret: &str,
    api_token_encryption_key: &str,
) -> Bson {
    let config = SensorConfig {
        profile_owner_id: input.profileOwnerId,
        api_token: input.apiToken,
        device_uuid: input.deviceUuid,
        mac: input.mac,
        model: input.model,
        manufacturer: input.manufacturer,
        feature_uuid: input.featureUuid,
        feature_name: sensor_type.to_string(),
        api_token_hash_secret: api_token_hash_secret.to_string(),
        api_token_encryption_key: api_token_encryption_key.to_string(),
    };
    let result = T::new(config);
    to_bson(&result).expect("sensor serialization to BSON cannot fail")
}
