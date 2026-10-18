# Task 3 report — Centralize topology and connection lifecycle

## Implemented

- Added `RabbitTopology`, with idempotent durable direct exchanges, consumer queues, routing bindings for `consumidor1`, `consumidor2` and `ambos`, bounded-TTL retry queues, and dead-letter exchanges/queues.
- Added `RabbitConnectionManager`, built from `RabbitMQProperties`/`ConnectionFactory`, with optional TLS inherited from the configured factory, bounded connection attempts and backoff, topology declaration on connection, channel tracking, connection state reporting, and idempotent resource shutdown.
- Added `RabbitConnectionException` for configuration, connection, channel, topology and recovery failures.
- Added broker-independent unit tests using recording channel/connection proxies.
- Added an opt-in integration-test scaffold (`-Drabbitmq.integration=true`) so normal unit runs never require a broker.
- Producer and consumer mains were not modified.

## TDD and verification

- Focused tests were run before implementation and failed because the requested production classes did not exist.
- Focused tests pass after implementation: `RabbitTopologyTest`, `RabbitConnectionManagerTest` (3 tests).
- A complete suite was run with the available JDK 17 using temporary, restored compiler settings: 32 tests, 30 passed, 1 skipped, 2 pre-existing `utility.RegistoTest` errors caused by UTF-8 decoding of log output.
- Required `mvn clean test` with JDK 25 was attempted, but this environment only provides Temurin JDK 17; Maven therefore reports `invalid target release: 25`.

## Concerns

- JDK 25 must be installed and the full suite rerun before release.
- The existing `RegistoTest` encoding failures remain outside Task 3.
- The integration scaffold is intentionally opt-in; broker-restart execution still requires a running RabbitMQ test harness.
