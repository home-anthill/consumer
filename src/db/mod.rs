use std::env;
use std::future::Future;
use std::time::Duration;

use mongodb::bson::doc;
use mongodb::error::ErrorKind;
use mongodb::options::{ClientOptions, IndexOptions, ServerApi, ServerApiVersion};
use mongodb::{Client, Database, IndexModel};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::Env;

pub mod sensor;

pub async fn connect(env_config: &Env) -> mongodb::error::Result<Database> {
    // Fix 4: use as_deref() to avoid allocating a String for the comparison
    let mongo_db_name = if env::var("ENV").as_deref() == Ok("testing") {
        error!(target: "app", "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        error!(target: "app", "!!! WARNING: ENV=testing — using database 'sensors_test' !!!");
        error!(target: "app", "!!! ALL PRODUCTION DATA WILL BE IGNORED                  !!!");
        error!(target: "app", "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        String::from("sensors_test")
    } else {
        env_config.mongo_db_name.clone()
    };

    let mut client_options = ClientOptions::parse(&env_config.mongo_uri).await?;
    // Set the server_api field of the client_options object to Stable API version 1
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);
    // Set app_name
    client_options.app_name = Some("consumer".to_string());
    // Prevent indefinite hangs on unresponsive server
    client_options.server_selection_timeout = Some(Duration::from_secs(30));
    client_options.connect_timeout = Some(Duration::from_secs(10));

    // Create a new client and connect to the server
    let client = Client::with_options(client_options)?;
    let database = client.database(mongo_db_name.as_str());

    info!(target: "app", "Pinging MongoDB server...");
    retry_connect_mongodb(|| async { database.run_command(doc! { "ping": 1 }).await }, 50).await?;

    ensure_indexes(&database).await?;

    Ok(database)
}

// Fix 6: extract a builder so ensure_indexes never needs to clone the IndexModel eagerly
fn build_sensor_index() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {
            "deviceUuid": 1,
            "featureUuid": 1,
            "featureName": 1,
        })
        .options(IndexOptions::builder().name("idx_device_feature_name".to_string()).unique(true).build())
        .build()
}

async fn ensure_indexes(database: &Database) -> mongodb::error::Result<()> {
    let collection = database.collection::<mongodb::bson::Document>("sensors");
    match collection.create_index(build_sensor_index()).await {
        Ok(_) => {
            info!(target: "app", "MongoDB indexes ensured");
        }
        Err(ref e) if matches!(e.kind.as_ref(), ErrorKind::Command(cmd) if cmd.code == 86) => {
            warn!(target: "app", "Index conflict detected for idx_device_feature_name, dropping and recreating");
            collection.drop_index("idx_device_feature_name").await.map_err(|e| {
                error!(target: "app", "MongoDB - cannot drop conflicting index: {}", e);
                e
            })?;
            collection.create_index(build_sensor_index()).await?;
            info!(target: "app", "MongoDB indexes recreated successfully");
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

// Fix 9: use u64 throughout to eliminate the `count as u64` cast
async fn retry_connect_mongodb<T, E, Fut, F>(mut f: F, retries: u64) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut count: u64 = 0;
    loop {
        let result = f().await;
        if result.is_ok() {
            info!(target: "app", "MongoDB connected!");
            return result;
        } else if count >= retries {
            error!(target: "app", "Cannot connect to MongoDB, max tries reached");
            return result;
        }
        count += 1;
        let delay = Duration::from_secs(count.min(30));
        warn!(target: "app", "MongoDB ping failed (count={}), retrying in {}s...", count, delay.as_secs());
        sleep(delay).await;
    }
}
