# Task 2 report — Versioned message contract and codec

## Status

Complete. The versioned immutable `Message` record, Jackson-based `MessageCodec`, and `MessageValidationException` are implemented without changing producer or consumer behavior.

## Implementation

- Added `messaging.Message` as an immutable Java record with schema version, identifiers, content, timestamp, producer, and routing key.
- Added `messaging.MessageCodec` with:
  - Jackson JSON encoding and decoding;
  - explicit UTF-8 conversion through `StandardCharsets.UTF_8`;
  - `Instant` serialization and deserialization using ISO-8601 through `JavaTimeModule`;
  - schema version and required-field validation;
  - malformed payload rejection with preserved causes;
  - no generic `Map` casts or partially populated-message fallback.
- Added `messaging.MessageValidationException` with cause-preserving constructors.
- Added the Jackson JSR-310 module dependency at version 2.18.9.
- Added focused tests for valid round trips, Portuguese UTF-8 content, ISO-8601 timestamps, schema versions, required fields, malformed JSON, and exception causes.

## Verification

- Focused command: `mvn -q -Dtest=MessageCodecTest test` — 9 tests passed.
- Full command: `mvn -q clean test` — 23 tests passed, 0 failures, 0 errors, using JDK 25 (`Temurin 25.0.4`).

## Commit

The working directory did not contain a Git repository before this task, so no pre-existing commit history was available. The task changes were committed after initializing Git locally.

## Concerns

- Existing compiler output still reports unrelated unchecked-operation warnings in `consumer/Consumidor2.java`; producer and consumer behavior was intentionally left unchanged.
