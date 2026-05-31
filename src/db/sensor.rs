use std::time::Duration;

use tracing::{error, info};

use mongodb::Database;
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::options::ReturnDocument;

use crate::api_token::decrypt_api_token;
use crate::models::generic_message::GenericMessage;
use crate::models::sensor::Sensor;
use crate::models::sensor::SensorDocument;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensorAuth {
    pub api_token: String,
    pub feature_name: String,
}

impl From<&SensorDocument> for Sensor {
    fn from(sensor_doc: &SensorDocument) -> Self {
        Self {
            id: sensor_doc.id.to_string(),
            // profile info
            profile_owner_id: sensor_doc.profile_owner_id.to_string(),
            api_token_hash: sensor_doc.api_token_hash.clone(),
            // device info
            device_uuid: sensor_doc.device_uuid.clone(),
            mac: sensor_doc.mac.clone(),
            model: sensor_doc.model.clone(),
            manufacturer: sensor_doc.manufacturer.clone(),
            // feature info
            feature_uuid: sensor_doc.feature_uuid.clone(),
            feature_name: sensor_doc.feature_name.clone(),
            value: match &sensor_doc.value {
                Bson::Double(d) => *d,
                Bson::Int64(i) => *i as f64,
                Bson::Int32(i) => f64::from(*i),
                _ => 0.0,
            },
            // dates
            created_at: sensor_doc.created_at.to_string(),
            modified_at: sensor_doc.modified_at.to_string(),
        }
    }
}

pub async fn update_sensor(
    db: &Database,
    generic_msg: &GenericMessage,
    value: &Bson,
) -> mongodb::error::Result<Option<Sensor>> {
    info!(target: "app", "update_sensor - Called with generic_msg = {}", generic_msg);

    let collection = db.collection::<SensorDocument>("sensors");

    let sensor_doc = collection
        .find_one_and_update(
            doc! {
                "deviceUuid": &generic_msg.device_uuid,
                "featureUuid": &generic_msg.feature_uuid,
            },
            doc! { "$set": {
                    "value": value,
                    "modifiedAt": DateTime::now()
                }
            },
        )
        .return_document(ReturnDocument::After)
        .max_time(Duration::from_secs(30))
        .await?;

    if let Some(sensor_doc) = sensor_doc {
        Ok(Some(Sensor::from(&sensor_doc)))
    } else {
        error!(target: "app", "update_sensor - Cannot find and update sensor with device_uuid = {} and feature_uuid = {}",
            generic_msg.device_uuid, generic_msg.feature_uuid);
        Ok(None)
    }
}

pub async fn find_sensor_auth(
    db: &Database,
    device_uuid: &str,
    feature_uuid: &str,
    api_token_encryption_key: &str,
) -> mongodb::error::Result<Option<SensorAuth>> {
    let collection = db.collection::<Document>("sensors");
    let sensor_doc = collection
        .find_one(doc! {
            "deviceUuid": device_uuid,
            "featureUuid": feature_uuid,
        })
        .projection(doc! {"apiTokenEncrypted": 1, "featureName": 1})
        .max_time(Duration::from_secs(30))
        .await?;
    Ok(sensor_doc.and_then(|doc| {
        let api_token = doc
            .get_str("apiTokenEncrypted")
            .ok()
            .and_then(|encrypted| decrypt_api_token(encrypted, api_token_encryption_key).ok())?;
        let feature_name = doc.get_str("featureName").ok()?.to_string();
        Some(SensorAuth { api_token, feature_name })
    }))
}

#[cfg(test)]
mod tests {
    use crate::models::sensor::{Sensor, SensorDocument};
    use mongodb::bson::oid::ObjectId;
    use mongodb::bson::{Bson, DateTime};
    use pretty_assertions::assert_eq;
    use std::str::FromStr;

    fn sensor_document_with_value(value: Bson) -> SensorDocument {
        SensorDocument {
            id: ObjectId::from_str("63963ce7c7fd6d463c6c77a3").unwrap(),
            profile_owner_id: ObjectId::from_str("620d710e4e8fe8f3394084bc").unwrap(),
            api_token_hash: "token-hash".to_string(),
            api_token_encrypted: "token-encrypted".to_string(),
            device_uuid: "246e3256-f0dd-4fcb-82c5-ee20c2267eeb".to_string(),
            mac: "60:55:F9:DF:F8:92".to_string(),
            model: "dht-light".to_string(),
            manufacturer: "ks89".to_string(),
            feature_uuid: "41cb3f47-894c-45e9-90d9-a4d4de903896".to_string(),
            feature_name: "temperature".to_string(),
            value,
            created_at: DateTime::now(),
            modified_at: DateTime::now(),
        }
    }

    #[test]
    #[test_log::test]
    fn call_sensor_from_sensor_document() {
        let oid = ObjectId::from_str("63963ce7c7fd6d463c6c77a3").unwrap();
        let device_uuid = "246e3256-f0dd-4fcb-82c5-ee20c2267eeb";
        let mac = "60:55:F9:DF:F8:92";
        let manufacturer = "ks89";
        let model = "dht-light";
        let profile_owner_id = ObjectId::from_str("620d710e4e8fe8f3394084bc").unwrap();
        let api_token_hash = "token-hash";
        let api_token_encrypted = "token-encrypted";
        let date = DateTime::now();
        let value_f64: f64 = 10.2;
        let feature_uuid = "41cb3f47-894c-45e9-90d9-a4d4de903896";
        let feature_name = "temperature";
        let sensor_doc = SensorDocument {
            id: oid,
            // profile info
            profile_owner_id,
            api_token_hash: api_token_hash.to_string(),
            api_token_encrypted: api_token_encrypted.to_string(),
            // device info
            device_uuid: device_uuid.to_string(),
            mac: mac.to_string(),
            model: model.to_string(),
            manufacturer: manufacturer.to_string(),
            // feature info
            feature_uuid: feature_uuid.to_string(),
            feature_name: feature_name.to_string(),
            value: Bson::Double(value_f64),
            // dates
            created_at: date,
            modified_at: date,
        };
        let sensor: Sensor = Sensor::from(&sensor_doc);
        assert_eq!(sensor.id, oid.to_string());

        assert_eq!(sensor.profile_owner_id, profile_owner_id.to_string());
        assert_eq!(sensor.api_token_hash, api_token_hash.to_string());

        assert_eq!(sensor.device_uuid, device_uuid.to_string());
        assert_eq!(sensor.mac, mac.to_string());
        assert_eq!(sensor.model, model.to_string());
        assert_eq!(sensor.manufacturer, manufacturer.to_string());

        assert_eq!(sensor.feature_uuid, feature_uuid.to_string());
        assert_eq!(sensor.feature_name, feature_name.to_string());
        assert!((sensor.value - value_f64).abs() < f64::EPSILON);

        assert_eq!(sensor.created_at, date.to_string());
        assert_eq!(sensor.modified_at, date.to_string());
    }

    #[test]
    fn sensor_from_sensor_document_converts_int64_value() {
        let sensor_doc = sensor_document_with_value(Bson::Int64(42));

        let sensor = Sensor::from(&sensor_doc);

        assert_eq!(sensor.value, 42.0);
    }

    #[test]
    fn sensor_from_sensor_document_converts_int32_value() {
        let sensor_doc = sensor_document_with_value(Bson::Int32(7));

        let sensor = Sensor::from(&sensor_doc);

        assert_eq!(sensor.value, 7.0);
    }

    #[test]
    fn sensor_from_sensor_document_defaults_unknown_value_type_to_zero() {
        let sensor_doc = sensor_document_with_value(Bson::String("not numeric".to_string()));

        let sensor = Sensor::from(&sensor_doc);

        assert_eq!(sensor.value, 0.0);
    }
}
