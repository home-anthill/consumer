use mongodb::bson::{doc, Bson, Document};
use mongodb::Database;
use serde::{Deserialize, Serialize};

use crate::tests_integration::sensor_utils::{new_from_register_input, FloatSensor, IntSensor};

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
        .drop(None)
        .await
        .expect("drop 'temperature' collection");
    db.collection::<Document>("humidity")
        .drop(None)
        .await
        .expect("drop 'humidity' collection");
    db.collection::<Document>("light")
        .drop(None)
        .await
        .expect("drop 'light' collection");
    db.collection::<Document>("motion")
        .drop(None)
        .await
        .expect("drop 'motion' collection");
    db.collection::<Document>("airpressure")
        .drop(None)
        .await
        .expect("drop 'airpressure' collection");
    db.collection::<Document>("airquality")
        .drop(None)
        .await
        .expect("drop 'airquality' collection");
}

pub async fn find_sensor_by_uuid(
    db: &Database,
    uuid: &String,
    sensor_type: &str,
) -> mongodb::error::Result<Option<Document>> {
    let collection = db.collection::<Document>(sensor_type);
    let filter = doc! { "uuid": uuid };
    collection.find_one(filter, None).await
}

pub async fn insert_sensor(db: &Database, input: RegisterInput, sensor_type: &str) -> Result<String, anyhow::Error> {
    let collection = db.collection::<Document>(sensor_type);

    let serialized_data: Bson = match sensor_type {
        "temperature" | "humidity" | "light" => new_from_register_input::<FloatSensor>(input).unwrap(),
        "motion" | "airquality" | "airpressure" => new_from_register_input::<IntSensor>(input).unwrap(),
        _ => {
            panic!("Unknown type")
        }
    };
    let document = serialized_data.as_document().unwrap();
    let insert_one_result = collection.insert_one(document.to_owned(), None).await.unwrap();
    Ok(insert_one_result.inserted_id.as_object_id().unwrap().to_hex())
}
