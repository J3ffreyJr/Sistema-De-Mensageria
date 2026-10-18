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

## Fix report — Task 3 review findings (2026-10-18)

### Findings addressed

1. **Bounded connection recovery (High):** `RabbitConnectionManager` now installs a shutdown listener, performs at most `maxAttempts` reconnect attempts with bounded backoff, redeclares the complete topology on every new connection, and exposes a usable `createChannel()` path on the recreated connection. `close()` sets the closing gate before cleanup, so shutdown disables recovery.
2. **Bounded retry/dead-letter topology (High):** each consumer queue routes to an explicitly named first retry key. The first retry queue expires into a second retry key, and the exhausted retry queue expires into `DEAD_LETTER_EXCHANGE` with the original routing key. All exchanges, queues, bindings, TTLs, and dead-letter arguments are explicit and durable; topology assertions verify these arguments.
3. **Close/create race (Medium):** channel creation and shutdown cleanup use the same manager monitor. A channel cannot be added after shutdown has cleared the registry.
4. **Integration coverage (Medium):** the previous placeholder is now an opt-in RabbitMQ test. With `-Drabbitmq.integration=true` and configured environment credentials it connects to RabbitMQ, redeclares topology idempotently, invokes the deterministic recovery hook, creates a channel on the recreated connection, redeclares topology, and passively verifies consumer and dead-letter queues. It skips only when the opt-in property is absent.

### Tests and exact results

- Command: `mvn -q -Dtest=RabbitTopologyTest,RabbitConnectionManagerTest test` (JDK 25, `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot`)
- Result: **BUILD SUCCESS**; focused topology/recovery tests passed.
- Command: `mvn clean test` (same JDK 25)
- Result: **BUILD SUCCESS**; 32 tests, 0 failures, 0 errors, 1 skipped (the opt-in RabbitMQ integration test because `rabbitmq.integration` was not enabled).
- The opt-in integration test was not enabled in this environment because no broker/integration credentials were supplied; its skip is intentional and controlled solely by `rabbitmq.integration`.

### Remaining concerns

- A live RabbitMQ broker is still required to execute the opt-in integration test. Configure `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_VHOST`, `RABBITMQ_USERNAME`, and `RABBITMQ_PASSWORD`, then run `mvn -Drabbitmq.integration=true test`.
- Existing producer and consumer mains remain unchanged.

## Fix report — remaining Task 3 review finding (2026-10-18)

### Finding addressed

Consumer queues now set `x-dead-letter-routing-key` to their corresponding
first-stage retry routing keys (`consumidor1.retry.1` and
`consumidor2.retry.1`). Behavioral topology assertions verify that each
consumer queue's dead-letter routing key matches the binding used by its first
retry queue, while preserving the existing bounded second-stage retry and
dead-letter chain.

### Verification

- Focused topology tests pass with JDK 25:
  `mvn -q -Dtest=RabbitTopologyTest test`
- Full Maven suite passes with JDK 25:
  `mvn clean test`
