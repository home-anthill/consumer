# Changelog (Claude)

## Security

- HMAC-SHA256 message authentication added: `x-hmac-sha256` header is verified against the payload using `AMQP_HMAC_SECRET`; missing or invalid signatures are NACKed without requeue.
- `verify_hmac` uses constant-time comparison to prevent timing side-channels; hex-decode failures follow the same code path so both cases take equal time.
- `ReplayCache` (5-minute TTL, 10 000-entry cap) added to reject duplicate messages by `message_id`; oldest entry is evicted on overflow.
- `amqp_hmac_secret` asserted non-empty at startup; missing `.env` file now surfaces an error instead of being silently ignored.
- `amqp_uri` wrapped in `Zeroizing<String>`; credentials are zeroed in memory when `AmqpClient` is dropped.
- `api_token` redacted in both `Display` and `Debug` impls of `GenericMessage`; AMQP URI redacted via `redact_uri()` before any logging.
- 64 KiB message size limit enforced before UTF-8 decoding to prevent OOM from oversized payloads.
- 30-second timeout added on `find_one_and_update` to prevent indefinite hangs.
- Startup panics replaced with `std::process::exit(1)` to avoid leaking credentials via panic messages.
- All `unwrap()` and bare `expect()` calls removed from production code; replaced with `?` and `if let`.
- Log directory permissions set to `0o700`.
- Added mandatory `AMQP_HMAC_SECRET` variable to `.env_template`.

## Bug Fixes

- `connect()` delegates to private `do_connect()` so the `connecting` flag is always reset on error, preventing the client from getting stuck.
- AMQP queue declaration now uses `durable: true` because RabbitMQ 4.x rejects transient non-exclusive named queues by default through the deprecated `transient_nonexcl_queues` feature.
- `IndexKeySpecsConflict` detected via `ErrorKind::Command` code 86 instead of fragile string matching.
- Ack/nack failures now call `wait_for_recovery()` instead of being silently ignored.
- Non-Unix `ctrl_c()` handler registration failure is now logged instead of silently swallowed.
- `SensorDocument.value` changed from `f64` to `Bson` to correctly handle both `Double` and `Int64` sensor types without silent truncation.

## Validation & Error Handling

- `validate()` on `GenericMessage` checks non-empty fields, UUID format for `api_token`/`device_uuid`/`feature_uuid`, and `feature_name` against a known-sensor whitelist.
- `get_bson_value()` centralises feature-name → BSON-type routing.
- `TopicError` (thiserror) replaces the previous `Result<Self, String>` from `Topic::new`.
- New `MessageError` variants: `ValidationError`, `MessageTooLarge`, `MissingMessageId`, `ReplayDetected`.

## Idiomatic Rust & Refactoring

- `InitLevel` enum replaces a four-boolean `is_initialized` signature in `AmqpClient`; builder method returns `Self`.
- `document_to_json` replaced with `impl From<&SensorDocument> for Sensor`.
- `Topic::new` uses `splitn(4, '/')` iterator instead of allocating a `Vec<&str>` per message.
- `process_amqp_message` wrapper removed; HMAC extraction avoids an extra `String` allocation via `from_utf8` on header bytes.
- `_id` field renamed to `id` with `#[serde(rename = "_id")]` for idiomatic naming.
- `anyhow` and `uuid` (v4/fast-rng features) moved to `[dev-dependencies]`; `zeroize` added to runtime dependencies.
- Redundant `#[serde(default)]` removed; explicit `format!("{}", var)` style used throughout.
- Graceful shutdown on SIGTERM/SIGINT: drains in-flight messages then calls `close_connection()`.

## Testing

- `SensorConfig` struct introduced to resolve `clippy::too_many_arguments` in integration test helpers.
- `panic!("Unknown type")` in test utilities replaced with proper `Err` propagation; `.unwrap()` on BSON conversions replaced with `ok_or_else`.
- Strict float assertions replaced with epsilon-based checks; cast truncation replaced with correct `as f64` comparisons.
- `get_random_mac` refactored to `collect + join`, eliminating repeated string reallocations.

## Documentation

- Added script to `local-development.md` to generate a hardened RabbitMQ configuration (`rabbitmq.conf` + `definitions.json`) with strict ACLs for local testing.
