use log::{info, warn};
use mongodb::options::ClientOptions;
use mongodb::{Client, Database};
use std::env;

use crate::config::Env;

pub mod sensor;

pub async fn connect(env_config: &Env) -> mongodb::error::Result<Database> {
    let mongo_uri = env_config.mongo_uri.clone();
    let mongo_db_name = if env::var("ENV") == Ok(String::from("testing")) {
        warn!("TESTING ENVIRONMENT - forcing mongo_db_name = 'sensors_test'");
        String::from("sensors_test")
    } else {
        env_config.mongo_db_name.clone()
    };

    let mut client_options = ClientOptions::parse(mongo_uri).await?;
    client_options.app_name = Some("register".to_string());
    let client = Client::with_options(client_options)?;
    let database = client.database(mongo_db_name.as_str());

    info!(target: "app", "MongoDB connected!");

    Ok(database)
}
