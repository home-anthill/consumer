# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **ks89-consumer**, a Rust async AMQP consumer service that reads sensor data messages from RabbitMQ and persists them to MongoDB. Part of the [home-anthill](https://github.com/home-anthill/docs) IoT platform.

Current version: **3.1.0** (Cargo.toml)

## Build & Development Commands

```bash
make build          # Format + lint + build (debug)
make release        # Format + lint + build (release)
make run            # Watch mode with cargo-watch (requires `make deps`)
make fmt            # cargo fmt
make lint           # cargo clippy
make test           # Run all tests (unit + integration, sequential)
make test-coverage  # Run tests with grcov coverage report → coverage/html/
make deps           # Install all dev dependencies
make clean          # cargo clean
```

**Run a single test:**
```bash
ENV=testing RUST_BACKTRACE=full cargo test <test_name> -- --nocapture --test-threads 1
```

## Testing

There are two layers of tests; all run sequentially (`--test-threads 1`):

- **Unit tests** — inside `src/amqp/mod.rs` and `src/models/generic_message.rs`; no external infrastructure required
- **Integration tests** — inside `src/tests_integration/tests.rs` (in-tree, accessed via `crate::` imports); require real infrastructure

**Infrastructure requirements for integration tests:**
- MongoDB replica set running on `localhost:27017` (needed for transactions; sharded-mongodb-compose provides this)
- RabbitMQ running on `localhost:5672` with management UI on `15672` (default guest/guest credentials)
- `rabbitmqadmin` CLI installed (tool for publishing test messages to queues via management API)

**Test behavior:**
- `ENV=testing` switches MongoDB to `sensors_test` database and disables file logging (test-log crate captures to stdout)
- Tests purge the RabbitMQ queue before each test to ensure clean state
- Test messages are published via `rabbitmqadmin` CLI with HMAC signature and `message_id` headers set by the test
- RabbitMQ management credentials default to parsing the `AMQP_URI`; override with `AMQP_MANAGEMENT_USER` / `AMQP_MANAGEMENT_PASS` env vars
- The application queue is declared durable. RabbitMQ 4.x rejects transient non-exclusive queues by default via the deprecated `transient_nonexcl_queues` feature, so do not switch named shared queues back to `QueueDeclareOptions::default()`.
- `verify_hmac` and `process_delivery` live in `main.rs`; integration tests import `process_delivery` via `use crate::process_delivery`
- Assertions use `pretty_assertions` for readable diffs

## Architecture

**Message processing pipeline:**
1. `main()` → loads config (env vars via `dotenvy`/`envy`), connects to MongoDB (with retry), connects to Redis for signed nonce replay protection, connects to RabbitMQ via `AmqpClient`
2. Consumes messages in a loop via `tokio::select!` with biased SIGTERM/SIGINT shutdown (graceful drain, then `close_connection()`)
3. Each delivery goes through `process_delivery()`: AMQP HMAC-SHA256 verification (constant-time; reads `x-hmac-sha256` header), JSON deserialization into `GenericMessage`, validation (UUID format, feature_name whitelist, non-empty fields), signed MQTT HMAC verification, Redis nonce replay claim
4. `feature_name` routes the sensor value to either `f64` (temperature, humidity, light, airpressure) or `i64` (motion, airquality, online)
5. The `api_token` from the message is used directly as a query filter in MongoDB (plain UUIDv4)
6. Updates the sensor document in MongoDB via atomic `findOneAndUpdate` with 30-second timeout
7. Acks on success, Nacks (no requeue) on error; ack/nack failures trigger `wait_for_recovery()`

**Error recovery flow:**
- Ack/nack failures → trigger `wait_for_recovery()` → calls `channel.wait_for_recovery(err)` from lapin's auto-recovery → resets `connecting` flag
- `wait_for_recovery()` returns `ErrorButRecovered` (recoverable) or `ErrorCannotRecover` (unrecoverable; log and loop continues with next delivery)
- This allows temporary network blips to self-heal without crashing the service

**Key modules:**
- `amqp/` — `AmqpClient` with builder pattern, hierarchical `InitLevel` enum for state tracking. `connect()` delegates to private `do_connect()` so the `connecting` flag is always reset on error. The named shared queue is declared durable because RabbitMQ 4.x denies transient non-exclusive queues by default. `publish_message()` available but not used by consumer (only by tests). `read_message()` enforces 64 KiB message size limit before UTF-8 decoding.
- `config/` — Environment loading into `Env` struct via `envy`; validates `amqp_hmac_secret` is non-empty at startup. Logging setup: daily rolling file appender with 5-file cap, separate info/error logs in `./logs/`, stdout filtered to target "app", timestamps omitted in compact format (for K8s log aggregation). `redact_uri()` redacts credentials and sensitive query parameters before logging.
- `db/` — MongoDB connection with exponential backoff retry (50 attempts, ~5 seconds per attempt = ~250 seconds total). Ensures unique index on `(deviceUuid, featureUuid, featureName)` at startup; detects conflicts via `ErrorKind::Command` code 86. `update_sensor()` uses `find_one_and_update` with 30-second timeout. Test database: ENV=testing → switches to `sensors_test`.
- `models/` — `GenericMessage` (AMQP payload; `api_token` redacted in both `Display` and `Debug` for safety; `get_bson_value()` centralises feature-type routing), `Sensor`/`SensorDocument` (DB models — `SensorDocument.value` is `Bson` to handle both `Double` and `Int64`; `Sensor.value` is `f64`; `id` field uses `#[serde(rename = "_id")]`), `Topic` (parses "family/device_id/feature_name" using `splitn(4, '/')` iterator)
- `errors/` — `AmqpError`, `MessageError`, and `TopicError` using `thiserror`; errors are never discarded (always propagated with `?` or `inspect_err()`)

## Configuration

Environment variables (see `.env_template`):
- `MONGO_URI`, `MONGO_DB_NAME`, `REDIS_URI`, `REDIS_USERNAME`, `REDIS_PASSWORD`, `AMQP_URI`, `AMQP_HMAC_SECRET`, `AMQP_QUEUE_NAME`, `AMQP_CONSUMER_TAG`
- `LOG_LEVEL` — optional; controls stdout log level (`debug` default, or `info`/`warn`/`error`)

## Security

- `api_token` arriving in AMQP messages is a plain UUIDv4 and is used directly as a query filter in MongoDB.
- `amqp_hmac_secret` must be non-empty — enforced at startup via `assert!`.
- `api_token` is redacted in both `Display` and `Debug` impls of `GenericMessage`.
- Every delivery is HMAC-SHA256 verified using `verify_hmac()` (constant-time: on hex-decode failure the MAC is finalized and discarded so both paths take equal time). HMAC is read from the `x-hmac-sha256` AMQP header.
- Signed MQTT replay protection uses Redis `SET signed-replay:v1:{device_uuid}:{feature_uuid}:{nonce} 1 NX EX 720` after HMAC verification and before MongoDB updates.
- AMQP URI credentials are redacted via `redact_uri()` before any logging.
- `amqp_uri` is stored as `Zeroizing<String>` (from the `zeroize` crate) so the URI is zeroed in memory on drop.

## Docker & Deployment

Multi-stage Dockerfile optimizes layer caching and runtime size:
1. **Chef stage** (`rust:trixie`) — installs `cargo-chef` + build deps (g++, openssl, cmake)
2. **Planner stage** — generates `recipe.json` for dependency-only builds
3. **Builder stage** — builds dependencies (cached) then full binary in release mode
4. **System-deps stage** — collects CA certificates and pre-creates `/app/logs` owned by UID 65534
5. **Runtime stage** (`dhi.io/debian-base:trixie`) — hardened base (no package manager, no root user); contains only CA certs, logs dir, binary, and `.env` template

**Runtime environment:**
- Non-root user: UID 65534 (`nobody`)
- Reads `.env` file on startup (via `dotenvy`, then `envy` for validation)
- Logs to `./logs/` daily-rolled files (separate info/error logs, max 5 files each)
- Stdout: info-level logs with "app" target only
- Graceful shutdown: SIGTERM/SIGINT → drain in-flight messages → close connections

**CI/CD:**
- GitHub Actions: test → lint (`cargo clippy`) → build release binary → push to Docker Hub (`ks89/consumer`) on semver tags and `develop`/`master` branches
- Image tags: `latest` (master), `develop` (develop branch), semver tags (e.g., `3.1.0`)

## Code Style

- Rust edition 2024, resolver 3
- Formatting: 4 spaces, max line width 120 (see `rustfmt.toml`)
- Logging uses `tracing` with target "app" for filtering
- No `unwrap()` or bare `expect()` in production code; use `?` and `if let` instead
- No mocking of external services in integration tests; all hit real infrastructure
- Custom `Debug` impls redact sensitive fields rather than deriving (e.g., `api_token`, `amqp_hmac_secret`)

## Common Development Patterns

**Adding a new sensor feature type:**
1. Add the feature name to the whitelist in `GenericMessage::validate()` (models/generic_message.rs)
2. Add routing in `GenericMessage::get_bson_value()` — decide if it's `f64` or `i64`
3. Write an integration test in `src/tests_integration/tests.rs` using the pattern `ok_receive_<type>_amqp_message()`
4. Tests must be sequential and hit real RabbitMQ/MongoDB

**Modifying error handling:**
- Propagate errors up via `?` operator or `inspect_err()` for logging
- Never silently drop errors (no bare `_` discard)
- If an error should trigger reconnection, call `amqp_client.wait_for_recovery(err).await`

**Changing the message validation logic:**
- Update `GenericMessage::validate()` for field checks
- Update `Topic::new()` if parsing logic changes
- Consider backward-compat: existing messages in the queue will be replayed on redeployment

**Database schema changes:**
- Changes to `SensorDocument` fields require MongoDB migration (existing documents may not have new fields)
- Unique index ensures `(deviceUuid, featureUuid, featureName)` uniqueness; changing this affects `update_sensor()`
- The consumer uses `find_one_and_update` with `ReturnDocument::After`, so it gets the updated doc back

**Updating dependencies:**
- Run `make build` to verify compilation and linter passes
- Large dep updates may affect binary size; check final image size post-build
- Security vulnerabilities: `cargo audit` (part of `make check`)
