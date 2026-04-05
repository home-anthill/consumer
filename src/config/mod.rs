use std::env;
use std::fmt;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use dotenvy::dotenv;
use serde::Deserialize;
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::writer::MakeWriterExt;

#[derive(Deserialize)]
pub struct Env {
    pub mongo_uri: String,
    pub mongo_db_name: String,
    pub amqp_uri: String,
    pub amqp_hmac_secret: String,
    pub amqp_queue_name: String,
    pub amqp_consumer_tag: String,
    pub log_level: Option<String>,
}

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Env")
            .field("mongo_uri", &redact_uri(&self.mongo_uri))
            .field("mongo_db_name", &self.mongo_db_name)
            .field("amqp_uri", &redact_uri(&self.amqp_uri))
            .field("amqp_hmac_secret", &"[REDACTED]")
            .field("amqp_queue_name", &self.amqp_queue_name)
            .field("amqp_consumer_tag", &self.amqp_consumer_tag)
            .field("log_level", &self.log_level)
            .finish()
    }
}

pub fn init() -> Env {
    // Load the .env file
    dotenv().inspect_err(|e| eprintln!("Warning: .env file not loaded: {e}")).ok();
    let env = envy::from_env::<Env>().expect("failed to parse environment variables");
    assert!(!env.amqp_hmac_secret.is_empty(), "amqp_hmac_secret must not be empty");

    // Configure logging if not in test env
    if env::var("ENV").as_deref() != Ok("testing") {
        let stdout_max_level = match env.log_level.as_deref().unwrap_or("debug").to_lowercase().as_str() {
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::DEBUG,
        };
        let log_dir = "./logs";
        fs::create_dir_all(log_dir).expect("failed to create log directory");
        #[cfg(unix)]
        fs::set_permissions(log_dir, fs::Permissions::from_mode(0o700))
            .expect("failed to set log directory permissions");

        let stdout = std::io::stdout.with_filter(|meta| meta.target() == "app").with_max_level(stdout_max_level);
        let debug_file = RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("info")
            .filename_suffix("log")
            .max_log_files(5)
            .build("./logs")
            .expect("initializing rolling info_file appender failed")
            .with_max_level(tracing::Level::INFO);
        let error_file = RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("error")
            .filename_suffix("log")
            .max_log_files(5)
            .build("./logs")
            .expect("initializing rolling error_file appender failed")
            .with_filter(|meta| meta.target() == "app")
            .with_max_level(tracing::Level::ERROR);
        let writer = debug_file.and(error_file).and(stdout);
        tracing_subscriber::fmt()
            .compact()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .init();
    }

    info!(target: "app", "Starting application...");

    // Print .env vars
    print_env(&env);
    env
}

pub(crate) fn redact_uri(uri: &str) -> String {
    // Redact credentials from mongodb://user:pass@host/db URIs
    let mut redacted = if let Some(at_pos) = uri.find('@')
        && let Some(scheme_end) = uri.find("://")
    {
        format!("{}://***:***@{}", &uri[..scheme_end], &uri[at_pos + 1..])
    } else {
        uri.to_string()
    };

    // Redact sensitive query parameters (password, authSource credentials, TLS key passwords, etc.)
    for param in &["password", "passwd", "pass", "tlscertificatekeyfilepassword"] {
        let lower = redacted.to_lowercase();
        // Search for ?param= or &param= to avoid partial matches (e.g. "nopassword=")
        let value_start = [format!("?{}=", param), format!("&{}=", param)]
            .iter()
            .find_map(|prefix| lower.find(prefix.as_str()).map(|pos| pos + prefix.len()));
        if let Some(start) = value_start {
            let end = redacted[start..].find('&').map_or(redacted.len(), |i| start + i);
            redacted.replace_range(start..end, "***");
        }
    }

    redacted
}

fn print_env(env: &Env) {
    info!(target: "app", "env = {:?}", env);
    info!(target: "app", "mongo_uri = {}", redact_uri(&env.mongo_uri));
    info!(target: "app", "mongo_db_name = {}", env.mongo_db_name);
    info!(target: "app", "amqp_uri = {}", redact_uri(&env.amqp_uri));
    info!(target: "app", "amqp_hmac_secret = [REDACTED]");
    info!(target: "app", "amqp_queue_name = {}", env.amqp_queue_name);
    info!(target: "app", "amqp_consumer_tag = {}", env.amqp_consumer_tag);
}
