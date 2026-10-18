# Task 4 report — confirmed publishing

## Scope

Implemented `RabbitPublisher`, `PublishResult`, and `PublishException`; migrated `Produtor` from ad-hoc `Map` JSON construction to `Message`/publisher APIs; and added broker-independent unit tests plus opt-in RabbitMQ integration tests.

## Implementation

- Publisher opens a managed connection/channel, declares topology through the existing connection manager, and closes owned resources.
- Publishes with persistent delivery mode (`2`), `application/json`, UTF-8, AMQP `messageId`, and `mandatory=true`.
- Uses `MessageCodec` for UTF-8 JSON serialization and validates messages before publishing.
- Waits for publisher confirmation and reports `CONFIRMED` only when confirmed and not returned.
- Captures mandatory returned messages as `RETURNED`, including when the broker also confirms the publish.
- Reports negative confirmations as `FAILED` and wraps transport/close errors in `PublishException`.
- Producer destinations `1`, `2`, and `3` remain mapped to the existing routing keys.

## TDD evidence

The focused publisher test was run before production implementation with JDK 25 and failed at compilation because the requested publisher/result/exception types did not exist. After implementation, the focused tests passed.

## Validation

Command: `mvn clean test` with `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot`

- 38 tests run
- 35 passed
- 0 failures/errors
- 3 skipped (existing and new broker integration tests, opt-in via `-Drabbitmq.integration=true`)
- Build successful

The integration test covers bound routing for destinations 1, 2, and 3 and deliberately unbound mandatory publishing. It was not executed because no opt-in broker run was requested/available.

## Concerns

- Integration tests require a running RabbitMQ broker and valid environment credentials.
- `mvn` initially used an older JDK; final validation explicitly used installed JDK 25.
