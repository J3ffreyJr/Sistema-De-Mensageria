# Task 6 Report — Structured logging and controlled shutdown

## Scope

Task 6 replaces the legacy file-per-event `Registo` implementation with structured SLF4J/Logback events and adds orderly JVM shutdown hooks. Message bodies are never accepted by the structured logging API, so sensitive payloads are not written to logs. Existing message IDs and broker correlation IDs are retained where available.

## Implementation

- Added `utility.MessageLog` with events for publish, publish confirmation/error, receive, retry, ack, nack, dead-letter, connection, informational, and shutdown states.
- Added newline-safe structured key/value formatting and explicit error-level markers.
- Replaced `Registo` file writes with a compatibility facade delegating to `MessageLog`; `iniciarNovoLog()` now appends a startup event and never truncates logs.
- Added Logback rolling output at `logs/mensageria.log`, with size/time rotation, 30-day history, 200 MB cap, console output, and configurable `LOG_LEVEL`.
- Migrated producer and consumers to structured events.
- Added producer shutdown hook to close the publisher.
- Added consumer shutdown hooks with the required ordering: stop consumption first, then close the connection manager/channels.
- Added structured retry, acknowledgement, nack, and dead-letter events in Rabbit consumer and publish/confirmation events in Rabbit publisher.
- Extended focused tests for IDs, error levels, payload omission, and preservation of existing log content.

## TDD and validation

1. Extended `RegistoTest` before implementation.
2. Initial focused run was attempted as required; the first environment invocation failed because Maven used JDK 17 against Java 25 class files. Re-running with JDK 25 produced the expected pre-implementation compilation failure (`MessageLog` did not yet exist).
3. Focused validation: `mvn -q -Dtest=RegistoTest test` — passed (2 tests).
4. Full validation: `mvn -q clean test` on Eclipse Temurin JDK 25.0.4 — passed.
5. Self-review completed; working tree is clean after commit.

## Manual start/stop check

A live RabbitMQ start/stop check was not executed because it requires an external broker and the task environment did not provide broker availability. Shutdown code is deterministic and uses the existing consumer lifecycle APIs; unit/full Maven tests remain green.

## Commit

`97494df Replace event logging and add controlled shutdown`

## Review finding fix — legacy receipt status

- Fixed `Registo.logRecebimento` so its legacy `status` argument is emitted as the structured `status` field through the dedicated `MessageLog.receiveStatus` compatibility event, rather than being mislabeled as `consumer`.
- Preserved the message ID and payload omission/redaction behavior.
- Added `RegistoTest.compatibilityReceiveLogsStatusInsteadOfConsumer`, asserting `SUCESSO` is logged as status, never as consumer, and the payload is absent.
- Focused validation: `mvn -q -Dtest=RegistoTest test` on Eclipse Temurin JDK 25.0.4 — passed (3 tests).
- Full validation: `mvn -q clean test` on Eclipse Temurin JDK 25.0.4 — passed.

### Concern

No external RabbitMQ broker was required or exercised; this fix is limited to compatibility logging semantics and is covered by the focused and full unit test suites.
