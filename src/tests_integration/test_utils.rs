use crate::tests_integration::db_utils::RegisterInput;
use rand::Rng;

pub fn create_register_input(
    sensor_uuid: &str,
    mac: &str,
    manufacturer: &str,
    model: &str,
    api_token: &str,
    profile_owner_id: &str,
) -> RegisterInput {
    RegisterInput {
        uuid: sensor_uuid.to_string(),
        mac: mac.to_string(),
        manufacturer: manufacturer.to_string(),
        model: model.to_string(),
        profileOwnerId: profile_owner_id.to_string(),
        apiToken: api_token.to_string(),
    }
}

pub fn build_register_input(
    sensor_uuid: &str,
    mac: &str,
    manufacturer: &str,
    model: &str,
    api_token: &str,
    profile_owner_id: &str,
) -> String {
    serde_json::to_string(&create_register_input(
        sensor_uuid,
        mac,
        manufacturer,
        model,
        api_token,
        profile_owner_id,
    ))
    .unwrap()
}

pub fn get_random_mac() -> String {
    const CHARSET: &[u8] = b"ABCDEF0123456789";
    let mut rng = rand::thread_rng();
    let mut mac = String::from("");
    for i in 0..6 {
        let group: String = (0..2)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();
        if i == 0 {
            mac = format!("{}:", group);
        } else if i > 0 && i < 5 {
            mac = format!("{}{}:", mac, group);
        } else {
            mac = format!("{}{}", mac, group);
        }
    }
    mac
}
