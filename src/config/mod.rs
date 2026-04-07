use std::env;
use std::fmt;

use dotenvy::dotenv;
use serde::Deserialize;
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::writer::MakeWriterExt;

/// Which runtime environment the application is running in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Testing,
    Production,
}

impl AppEnv {
    /// Reads the `ENV` environment variable. Returns `Testing` only when the
    /// value is exactly `"testing"`; any other value (including absent) is
    /// treated as `Production`.
    pub fn from_env() -> Self {
        match env::var("ENV").as_deref() {
            Ok("testing") => Self::Testing,
            _ => Self::Production,
        }
    }

    pub fn is_testing(&self) -> bool {
        matches!(self, Self::Testing)
    }
}

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

pub fn init() -> (Env, AppEnv) {
    // Load the .env file
    dotenv().ok();
    let env = envy::from_env::<Env>().expect("failed to parse environment variables");
    let app_env = AppEnv::from_env();

    // Configure logging if not in test env.
    // We use set_global_default (not .init()) intentionally: .init() would also install
    // a LogTracer bridge for the `log` crate, which prevents Rocket from installing its
    // own RocketLogger. Without RocketLogger, Rocket's startup output (routes, config,
    // launched URL) is silently dropped. By skipping LogTracer, Rocket gets to install
    // its own logger and prints its startup info directly to stdout.
    if !app_env.is_testing() {
        let stdout_max_level =
            env.log_level.as_deref().and_then(|s| s.parse::<tracing::Level>().ok()).unwrap_or(tracing::Level::DEBUG);
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
        let subscriber = tracing_subscriber::fmt()
            .compact()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("Unable to install global subscriber");
    }

    info!(target: "app", "Starting application...");

    // Print .env vars
    print_env(&env);
    (env, app_env)
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
