# Changelog

## 4.0.1

### Tests

- Added unit coverage for AMQP payload reading, including valid UTF-8, invalid UTF-8, and the 64 KiB message-size guard.
- Added `GenericMessage` validation coverage for invalid device/feature UUIDs, empty topic segments, unknown feature names, non-positive timestamps, missing payload values, and invalid BSON value routing.
- Added `Topic::new` coverage for missing and extra topic segments.
- Added API token tests for deterministic hashing, encrypt/decrypt round trips, invalid encryption keys, invalid base64, short ciphertext, and wrong-key decryption failures.
- Added config validation tests for accepted production hash secrets and the testing-only default hash secret.
- Added sensor conversion tests for `Int64`, `Int32`, and non-numeric BSON values.
- Added signed-message tests for AMQP HMAC verification, invalid HMAC hex, MQTT signature success, stale timestamps, wrong signatures, topic/feature matching, and Redis replay-key scoping.


## 4.0.0

### Features

- `validate()` on `GenericMessage` checks non-empty fields, UUID format for `api_token`/`device_uuid`/`feature_uuid`, and `feature_name` against a known-sensor whitelist.
- `get_bson_value()` centralises feature-name -> BSON-type routing.
- `TopicError` (thiserror) replaces the previous `Result<Self, String>` from `Topic::new`.
- New `MessageError` variants: `ValidationError`, `MessageTooLarge`, `ReplayDetected`, `ReplayCacheError`.

### Bug fixes

- `connect()` delegates to private `do_connect()` so the `connecting` flag is always reset on error, preventing the client from getting stuck.
- AMQP queue declaration now uses `durable: true` because RabbitMQ 4.x rejects transient non-exclusive named queues by default through the deprecated `transient_nonexcl_queues` feature.
- `IndexKeySpecsConflict` detected via `ErrorKind::Command` code 86 instead of fragile string matching.
- Ack/nack failures now call `wait_for_recovery()` instead of being silently ignored.
- Non-Unix `ctrl_c()` handler registration failure is now logged instead of silently swallowed.
- `SensorDocument.value` changed from `f64` to `Bson` to correctly handle both `Double` and `Int64` sensor types without silent truncation.

### Security issues

- Signed envelope validation tightened: `nonce` must be 32 lowercase-hex characters and `signature` must be 64 lowercase-hex characters before HMAC verification and Redis replay-cache keying.
- Production startup now rejects missing or weak `API_TOKEN_HASH_SECRET` values; the secret must be at least 32 characters. `.env_template` includes the required variable.
- HMAC-SHA256 message authentication added: `x-hmac-sha256` header is verified against the payload using `AMQP_HMAC_SECRET`; missing or invalid signatures are NACKed without requeue.
- `verify_hmac` uses constant-time comparison to prevent timing side-channels; hex-decode failures follow the same code path so both cases take equal time.
- Redis-backed signed nonce replay protection rejects duplicate `device_uuid` + `feature_uuid` + `nonce` combinations with `SET ... NX EX 720`.
- Signed sensor payloads now bind `feature_name` into the HMAC input and the consumer verifies the MQTT topic feature name matches the registered MongoDB sensor feature before nonce claim or DB update.
- Sensor API tokens are no longer read from plaintext MongoDB fields. The consumer decrypts `apiTokenEncrypted` with mandatory `API_TOKEN_ENCRYPTION_KEY` before signed MQTT verification.
- `amqp_hmac_secret` asserted non-empty at startup; missing `.env` file now surfaces an error instead of being silently ignored.
- `amqp_uri` wrapped in `Zeroizing<String>`; credentials are zeroed in memory when `AmqpClient` is dropped.
- `api_token` redacted in both `Display` and `Debug` impls of `GenericMessage`; AMQP URI redacted via `redact_uri()` before any logging.
- 64 KiB message size limit enforced before UTF-8 decoding to prevent OOM from oversized payloads.
- 30-second timeout added on `find_one_and_update` to prevent indefinite hangs.
- Startup panics replaced with `std::process::exit(1)` to avoid leaking credentials via panic messages.
- All `unwrap()` and bare `expect()` calls removed from production code; replaced with `?` and `if let`.
- Log directory permissions set to `0o700`.
- Added mandatory `AMQP_HMAC_SECRET` variable to `.env_template`.

### Idiomatic Rust issues

- `InitLevel` enum replaces a four-boolean `is_initialized` signature in `AmqpClient`; builder method returns `Self`.
- `document_to_json` replaced with `impl From<&SensorDocument> for Sensor`.
- `Topic::new` uses `splitn(4, '/')` iterator instead of allocating a `Vec<&str>` per message.
- `process_amqp_message` wrapper removed; HMAC extraction avoids an extra `String` allocation via `from_utf8` on header bytes.
- `_id` field renamed to `id` with `#[serde(rename = "_id")]` for idiomatic naming.
- Redundant `#[serde(default)]` removed; explicit `format!("{}", var)` style used throughout.
- Graceful shutdown on SIGTERM/SIGINT: drains in-flight messages then calls `close_connection()`.

### Chores

- `anyhow` and `uuid` (v4/fast-rng features) moved to `[dev-dependencies]`; `zeroize` added to runtime dependencies.
- Added script to `local-development.md` to generate a hardened RabbitMQ configuration (`rabbitmq.conf` + `definitions.json`) with strict ACLs for local testing.

### Tests

- `SensorConfig` struct introduced to resolve `clippy::too_many_arguments` in integration test helpers.
- `panic!("Unknown type")` in test utilities replaced with proper `Err` propagation; `.unwrap()` on BSON conversions replaced with `ok_or_else`.
- Strict float assertions replaced with epsilon-based checks; cast truncation replaced with correct `as f64` comparisons.
- `get_random_mac` refactored to `collect + join`, eliminating repeated string reallocations.
