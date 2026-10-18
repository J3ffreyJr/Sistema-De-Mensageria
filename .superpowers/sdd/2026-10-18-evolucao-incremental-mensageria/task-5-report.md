# Task 5 report — manual acknowledgements, retry and idempotency

## Scope

Implemented the manual-acknowledgement consumer boundary, atomic in-memory
idempotency, bounded retry headers, direct dead-letter routing for invalid or
exhausted messages, and migrated both consumer entry points to the shared
connection/topology lifecycle.

## Changes

- Added `MessageProcessor`, `IdempotencyStore`, and
  `InMemoryIdempotencyStore`.
- Added `IdempotentMessageProcessor`, which marks an ID complete only after
  business processing succeeds and releases failed attempts for redelivery.
- Added `RabbitConsumer` with `basicQos(1)`, `autoAck=false`, explicit UTF-8
  payload handling, recovery-aware channel installation, clean start/stop,
  retry-count headers, and dead-letter fallback.
- Replaced duplicated consumer loops with configuration-driven
  `RabbitConsumer` instances.
- Added focused idempotency tests and an opt-in integration-test entry point.

## TDD and validation

The focused test was run before implementation and could not compile because
the implementation types did not yet exist. The first attempt used the outer
directory and was corrected to the Maven project root. The final validation
was run with JDK 25:

```text
mvn -q clean test
BUILD SUCCESS
```

The broker integration suite remains opt-in through
`-Drabbitmq.integration=true`; it was not enabled because no broker was
available during this run.

## Self-review

- Successful processing is acknowledged only after the processor returns.
- Invalid payloads are published to the dead-letter exchange and acknowledged
  only after that publish succeeds; failures are requeued.
- Transient failures carry `x-retry-count` through the retry exchange and are
  dead-lettered once the configured bound is reached.
- Duplicate IDs are atomically claimed; failed claims are not recorded as
  completed, so a redelivery remains retryable.
- Consumer shutdown removes the recovery listener and closes its channel,
  leaving unacknowledged deliveries available for RabbitMQ redelivery.

## Concerns

The integration tests require a running RabbitMQ broker and are intentionally
skipped by default. Retry delay and final routing depend on the topology
declared by `RabbitTopology`.
