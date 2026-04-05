use rand::prelude::*;

use crate::tests_integration::db_utils::RegisterInput;

pub fn create_register_input(
    profile_owner_id: &str,
    api_token: &str,
    device_uuid: &str,
    mac: &str,
    model: &str,
    manufacturer: &str,
    feature_uuid: &str,
) -> RegisterInput {
    RegisterInput {
        profileOwnerId: profile_owner_id.to_string(),
        apiToken: api_token.to_string(),
        deviceUuid: device_uuid.to_string(),
        mac: mac.to_string(),
        model: model.to_string(),
        manufacturer: manufacturer.to_string(),
        featureUuid: feature_uuid.to_string(),
    }
}

pub fn get_random_mac() -> String {
    const CHARSET: &[u8] = b"ABCDEF0123456789";
    let mut rng = rand::rng();
    let groups: Vec<String> = (0..6)
        .map(|_| {
            (0..2)
                .map(|_| {
                    let idx = rng.random_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect()
        })
        .collect();
    groups.join(":")
}
