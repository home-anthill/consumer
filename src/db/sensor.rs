use log::{error, info};

use crate::models::generic_message::GenericMessage;
use mongodb::bson::{doc, Bson, DateTime};
use mongodb::options::ReturnDocument;
use mongodb::Database;

use crate::models::sensor::Sensor;
use crate::models::sensor::SensorDocument;

pub async fn update_sensor(
    db: &Database,
    generic_msg: &GenericMessage,
    value: &Bson,
) -> mongodb::error::Result<Option<Sensor>> {
    info!(target: "app", "update_sensor - Called with generic_msg = {:?}", generic_msg);

    let collection = db.collection::<SensorDocument>(&generic_msg.topic.feature);

    let uuid: String = generic_msg.uuid.clone();
    let api_token: String = generic_msg.api_token.clone();
    let sensor_doc = collection
        .find_one_and_update(
            doc! { "uuid": uuid, "apiToken": api_token },
            doc! { "$set": {
                    "value": value,
                    "modifiedAt": DateTime::now()
                }
            },
            // find_one_and_update_options,
        )
        .return_document(ReturnDocument::After)
        .await
        .unwrap(); // TODO ATTENTION I should check and return a custom DbError here Err(....) and not unwrap and ignore the error.

    // return result
    match sensor_doc {
        Some(sensor_doc) => Ok(Some(document_to_json(&sensor_doc))),
        None => {
            error!(target: "app", "update_sensor - Cannot find and update sensor with uuid = {}", generic_msg.uuid);
            // TODO ATTENTION I should return a custom DbError here Err(....) and not Ok.
            Ok(None)
        }
    }
}

fn document_to_json(sensor_doc: &SensorDocument) -> Sensor {
    Sensor {
        _id: sensor_doc._id.to_string(),
        uuid: sensor_doc.uuid.to_string(),
        mac: sensor_doc.mac.to_string(),
        manufacturer: sensor_doc.manufacturer.to_string(),
        model: sensor_doc.model.to_string(),
        profileOwnerId: sensor_doc.profileOwnerId.to_string(),
        apiToken: sensor_doc.apiToken.to_string(),
        createdAt: sensor_doc.createdAt.to_string(),
        modifiedAt: sensor_doc.modifiedAt.to_string(),
        value: sensor_doc.value,
    }
}

#[cfg(test)]
mod tests {
    use crate::db::sensor::document_to_json;
    use crate::models::sensor::{Sensor, SensorDocument};
    use mongodb::bson::oid::ObjectId;
    use mongodb::bson::DateTime;
    use pretty_assertions::assert_eq;
    use std::str::FromStr;

    #[test]
    fn call_document_to_json() {
        let oid = ObjectId::from_str("63963ce7c7fd6d463c6c77a3").unwrap();
        let uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let mac = "60:55:F9:DF:F8:92";
        let manufacturer = "ks89";
        let model = "dht-light";
        let profile_owner_id = ObjectId::from_str("620d710e4e8fe8f3394084bc").unwrap();
        let api_token = "473a4861-632b-4915-b01e-cf1d418966c6";
        let date = DateTime::now();
        let value: f64 = 10.2;
        let sensor_doc = SensorDocument {
            _id: oid,
            uuid: uuid.to_string(),
            mac: mac.to_string(),
            manufacturer: manufacturer.to_string(),
            model: model.to_string(),
            profileOwnerId: profile_owner_id,
            apiToken: api_token.to_string(),
            createdAt: date,
            modifiedAt: date,
            value,
        };
        let sensor: Sensor = document_to_json(&sensor_doc);
        assert_eq!(sensor._id, oid.to_string());
        assert_eq!(sensor.uuid, uuid.to_string());
        assert_eq!(sensor.mac, mac.to_string());
        assert_eq!(sensor.manufacturer, manufacturer.to_string());
        assert_eq!(sensor.model, model.to_string());
        assert_eq!(sensor.profileOwnerId, profile_owner_id.to_string());
        assert_eq!(sensor.apiToken, api_token.to_string());
        assert_eq!(sensor.createdAt, date.to_string());
        assert_eq!(sensor.modifiedAt, date.to_string());
        assert_eq!(sensor.value, value);
    }
}
