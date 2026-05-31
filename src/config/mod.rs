use std::env;
use std::fmt;

use dotenvy::dotenv;
use serde::Deserialize;
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::writer::MakeWriterExt;

// this is useful only in testing
pub const TEST_API_TOKEN_HASH_SECRET: &str = "test-api-token-hash-secret";
const MIN_API_TOKEN_HASH_SECRET_LEN: usize = 32;

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
    pub redis_uri: String,
    pub redis_username: String,
    pub redis_password: String,
    pub amqp_uri: String,
    pub amqp_hmac_secret: String,
    pub amqp_queue_name: String,
    pub amqp_consumer_tag: String,
    pub api_token_encryption_key: String,
    pub api_token_hash_secret: Option<String>,
    pub log_level: Option<String>,
}

impl Env {
    pub fn api_token_hash_secret(&self, app_env: AppEnv) -> Result<&str, String> {
        if let Some(secret) = self.api_token_hash_secret.as_deref() {
            if !app_env.is_testing() && secret.trim().len() < MIN_API_TOKEN_HASH_SECRET_LEN {
                return Err(format!(
                    "API_TOKEN_HASH_SECRET must be at least {MIN_API_TOKEN_HASH_SECRET_LEN} characters"
                ));
            }
            return Ok(secret);
        }
        if app_env.is_testing() {
            return Ok(TEST_API_TOKEN_HASH_SECRET);
        }
        Err("API_TOKEN_HASH_SECRET is required".to_string())
    }
}

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Env")
            .field("mongo_uri", &"[REDACTED]")
            .field("mongo_db_name", &self.mongo_db_name)
            .field("redis_uri", &"[REDACTED]")
            .field("redis_username", &self.redis_username)
            .field("redis_password", &"[REDACTED]")
            .field("amqp_uri", &"[REDACTED]")
            .field("amqp_hmac_secret", &"[REDACTED]")
            .field("amqp_queue_name", &self.amqp_queue_name)
            .field("amqp_consumer_tag", &self.amqp_consumer_tag)
            .field("api_token_encryption_key", &"[REDACTED]")
            .field("api_token_hash_secret", &"[REDACTED]")
            .field("log_level", &self.log_level)
            .finish()
    }
}

pub fn init() -> (Env, AppEnv) {
    // Load the .env file
    dotenv().ok();
    let env = envy::from_env::<Env>().expect("failed to parse environment variables");
    let app_env = AppEnv::from_env();
    validate_env(&env, app_env).expect("invalid environment variables");

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
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    info!(target: "app", "Starting application...");

    // Print .env vars
    print_env(&env);
    (env, app_env)
}

fn validate_env(env: &Env, app_env: AppEnv) -> Result<(), String> {
    if app_env.is_testing() {
        return Ok(());
    }
    env.api_token_hash_secret(app_env).map(|_| ())
}

fn print_env(env: &Env) {
    info!(target: "app", "env = {:?}", env);
    info!(target: "app", "mongo_uri = [REDACTED]");
    info!(target: "app", "mongo_db_name = {}", env.mongo_db_name);
    info!(target: "app", "redis_uri = [REDACTED]");
    info!(target: "app", "redis_username = {}", env.redis_username);
    info!(target: "app", "redis_password = {}", !env.redis_password.is_empty());
    info!(target: "app", "amqp_uri = [REDACTED]");
    info!(target: "app", "amqp_hmac_secret = [REDACTED]");
    info!(target: "app", "amqp_queue_name = {}", env.amqp_queue_name);
    info!(target: "app", "amqp_consumer_tag = {}", env.amqp_consumer_tag);
    info!(target: "app", "api_token_encryption_key = [REDACTED]");
    info!(target: "app", "api_token_hash_secret = [REDACTED]");
}

#[cfg(test)]
mod tests {
    use super::{AppEnv, Env, validate_env};

    fn valid_env(api_token_hash_secret: Option<String>) -> Env {
        Env {
            mongo_uri: "mongodb://localhost:27017".to_string(),
            mongo_db_name: "sensors".to_string(),
            redis_uri: "redis://localhost:6379".to_string(),
            redis_username: String::new(),
            redis_password: String::new(),
            amqp_uri: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            amqp_hmac_secret: "amqp-secret".to_string(),
            amqp_queue_name: "ks89".to_string(),
            amqp_consumer_tag: "consumer".to_string(),
            api_token_encryption_key: "0123456789abcdef0123456789abcdef".to_string(),
            api_token_hash_secret,
            log_level: Some("debug".to_string()),
        }
    }

    #[test]
    fn production_validate_env_rejects_missing_api_token_hash_secret() {
        let env = valid_env(None);

        let err = validate_env(&env, AppEnv::Production).expect_err("missing secret must fail in production");

        assert!(err.contains("API_TOKEN_HASH_SECRET"));
    }

    #[test]
    fn production_validate_env_rejects_short_api_token_hash_secret() {
        let env = valid_env(Some("short".to_string()));

        let err = validate_env(&env, AppEnv::Production).expect_err("short secret must fail in production");

        assert!(err.contains("API_TOKEN_HASH_SECRET"));
    }

    #[test]
    fn testing_validate_env_allows_missing_api_token_hash_secret() {
        let env = valid_env(None);

        assert!(validate_env(&env, AppEnv::Testing).is_ok());
    }

    #[test]
    fn production_api_token_hash_secret_accepts_long_secret() {
        let env = valid_env(Some("12345678901234567890123456789012".to_string()));

        let secret = env.api_token_hash_secret(AppEnv::Production).expect("long production secret should pass");

        assert_eq!(secret, "12345678901234567890123456789012");
    }

    #[test]
    fn testing_api_token_hash_secret_uses_default_when_missing() {
        let env = valid_env(None);

        let secret = env.api_token_hash_secret(AppEnv::Testing).expect("testing default should pass");

        assert_eq!(secret, super::TEST_API_TOKEN_HASH_SECRET);
    }
}
