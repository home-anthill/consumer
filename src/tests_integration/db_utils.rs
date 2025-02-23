use mongodb::Database;
use mongodb::bson::{Bson, Document, doc};
use serde::{Deserialize, Serialize};

use crate::tests_integration::sensor_utils::{FloatSensor, IntSensor, new_from_register_input};

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegisterInput {
    pub uuid: String,
    pub mac: String,
    pub manufacturer: String,
    pub model: String,
    pub profileOwnerId: String,
    pub apiToken: String,
}

pub async fn drop_all_collections(db: &Database) {
    db.collection::<Document>("temperature")
        .drop()
        .await
        .expect("drop 'temperature' collection");
    db.collection::<Document>("humidity")
        .drop()
        .await
        .expect("drop 'humidity' collection");
    db.collection::<Document>("light")
        .drop()
        .await
        .expect("drop 'light' collection");
    db.collection::<Document>("motion")
        .drop()
        .await
        .expect("drop 'motion' collection");
    db.collection::<Document>("airpressure")
        .drop()
        .await
        .expect("drop 'airpressure' collection");
    db.collection::<Document>("airquality")
        .drop()
        .await
        .expect("drop 'airquality' collection");
}

pub async fn insert_sensor(db: &Database, input: RegisterInput, sensor_type: &str) -> Result<String, anyhow::Error> {
    let collection = db.collection::<Document>(sensor_type);

    let serialized_data: Bson = match sensor_type {
        "temperature" | "humidity" | "light" => new_from_register_input::<FloatSensor>(input)?,
        "motion" | "airquality" | "airpressure" => new_from_register_input::<IntSensor>(input)?,
        _ => {
            panic!("Unknown type")
        }
    };
    let document = serialized_data.as_document().unwrap();
    let insert_one_result = collection.insert_one(document.to_owned()).await?;
    Ok(insert_one_result.inserted_id.as_object_id().unwrap().to_hex())
}
