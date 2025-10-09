use mongodb::Database;
use mongodb::bson::{Bson, Document, doc};
use serde::{Deserialize, Serialize};

use crate::tests_integration::sensor_utils::{FloatSensor, IntSensor, new_from_register_input};

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegisterInput {
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
}

pub async fn drop_all_collections(db: &Database) {
    db.collection::<Document>("sensors")
        .drop()
        .await
        .expect("drop 'sensors' collection");
}

pub async fn insert_sensor(db: &Database, input: RegisterInput, sensor_type: &str) -> Result<String, anyhow::Error> {
    let collection = db.collection::<Document>("sensors");

    let serialized_data: Bson = match sensor_type {
        "temperature" | "humidity" | "light" => new_from_register_input::<FloatSensor>(input, sensor_type)?,
        "motion" | "airquality" | "airpressure" => new_from_register_input::<IntSensor>(input, sensor_type)?,
        _ => {
            panic!("Unknown type")
        }
    };
    let document = serialized_data.as_document().unwrap();
    let insert_one_result = collection.insert_one(document.to_owned()).await?;
    Ok(insert_one_result.inserted_id.as_object_id().unwrap().to_hex())
}
