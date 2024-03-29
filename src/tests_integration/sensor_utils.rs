use mongodb::bson::oid::ObjectId;
use mongodb::bson::{oid, to_bson, Bson, DateTime};
use oid::Error;
use serde::{Deserialize, Serialize};

use crate::tests_integration::db_utils::RegisterInput;

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntSensor {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub uuid: String,
    pub mac: String,
    pub manufacturer: String,
    pub model: String,
    pub profileOwnerId: String,
    pub apiToken: String,
    pub createdAt: DateTime,
    pub modifiedAt: DateTime,
    pub value: i64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FloatSensor {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub uuid: String,
    pub mac: String,
    pub manufacturer: String,
    pub model: String,
    pub profileOwnerId: String,
    pub apiToken: String,
    pub createdAt: DateTime,
    pub modifiedAt: DateTime,
    pub value: f64,
}

pub trait Sensor {
    fn new(
        uuid: String,
        mac: String,
        manufacturer: String,
        model: String,
        profile_owner_id: String,
        api_token: String,
    ) -> Self;
}

impl Sensor for IntSensor {
    fn new(
        uuid: String,
        mac: String,
        manufacturer: String,
        model: String,
        profile_owner_id: String,
        api_token: String,
    ) -> Self {
        Self::new(uuid, mac, manufacturer, model, profile_owner_id, api_token)
    }
}

impl Sensor for FloatSensor {
    fn new(
        uuid: String,
        mac: String,
        manufacturer: String,
        model: String,
        profile_owner_id: String,
        api_token: String,
    ) -> Self {
        Self::new(uuid, mac, manufacturer, model, profile_owner_id, api_token)
    }
}

impl IntSensor {
    pub fn new(
        uuid: String,
        mac: String,
        manufacturer: String,
        model: String,
        profile_owner_id: String,
        api_token: String,
    ) -> Self {
        let date_now: DateTime = DateTime::now();
        Self {
            id: ObjectId::new(),
            uuid,
            mac,
            manufacturer,
            model,
            profileOwnerId: profile_owner_id,
            apiToken: api_token,
            createdAt: date_now,
            modifiedAt: date_now,
            value: 0_i64,
        }
    }
}

impl FloatSensor {
    pub fn new(
        uuid: String,
        mac: String,
        manufacturer: String,
        model: String,
        profile_owner_id: String,
        api_token: String,
    ) -> Self {
        let date_now: DateTime = DateTime::now();
        Self {
            id: ObjectId::new(),
            uuid,
            mac,
            manufacturer,
            model,
            profileOwnerId: profile_owner_id,
            apiToken: api_token,
            createdAt: date_now,
            modifiedAt: date_now,
            value: 0.0_f64,
        }
    }
}

pub fn new_from_register_input<T: Sensor + Serialize>(input: RegisterInput) -> Result<Bson, Error> {
    let result = T::new(
        input.uuid.clone(),
        input.mac.clone(),
        input.manufacturer.clone(),
        input.model.clone(),
        input.profileOwnerId.clone(),
        input.apiToken,
    );
    Ok(to_bson(&result).unwrap())
}
