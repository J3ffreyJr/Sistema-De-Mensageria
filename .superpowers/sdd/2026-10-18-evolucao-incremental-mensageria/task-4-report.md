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

## Fix report — Task 4 review findings

- Migrated `Consumidor1` and `Consumidor2` to `MessageCodec.decode`, so both consumers now read
  `messageId`, `content`, and `routingKey` from the versioned message contract. Manual ack, retry,
  and idempotency remain deferred to Task 5.
- Coordinated publisher confirms and asynchronous `basic.return` callbacks with a per-publication
  pending record. A returned message is classified as `RETURNED` even when its confirm callback
  wins the race. Added a delayed-return behavioral unit test.
- Registered publishers with `RabbitConnectionManager` recovery callbacks. Recovery now supplies a
  fresh channel, reinstalls the return listener, and safely replaces the closed publishing channel.
- Added the missing single-argument `PublishException` constructor used for closed-channel failures.

Focused validation (JDK 25):

- `mvn -q "-Dtest=messaging.RabbitPublisherTest,messaging.MessageCodecTest,messaging.RabbitConnectionManagerTest" test`
- Passed: 25 tests, 0 failures, 0 errors.

Full validation (JDK 25):

- `mvn -q clean test`
- Passed: build successful; all tests passed (RabbitMQ integration tests remain opt-in/skipped
  without a broker).


## Fix report — remaining Task 4 review findings (2026-10-18)

- Removed the fixed 250 ms return-settle timeout. `RabbitPublisher` now enables confirms,
  correlates each publication with its publish sequence number, and waits for that publication's
  confirm callback. RabbitMQ's channel protocol ordering delivers `basic.return` before the
  corresponding confirm for a mandatory unroutable publication, so a returned message cannot be
  reported as `CONFIRMED`. The focused test models that ordering directly (return callback followed
  by confirm callback), rather than sleeping for an arbitrary duration.
- `RabbitConnectionManager.recover()` now creates a distinct recovered channel for every recovery
  listener. Two publishers/listeners therefore cannot share a channel; the regression test closes
  one recovered channel and verifies the other remains open.
- Persistent delivery mode, JSON content type, UTF-8 encoding, mandatory publishing, and existing
  public publisher/manager APIs are preserved. No Task 5 manual ack, retry, or idempotency work was
  added.

Focused command (JDK 25):

```text
mvn -q "-Dtest=RabbitPublisherTest,RabbitConnectionManagerTest" test

Process exited with code 0.
```

Full command (JDK 25):

```text
mvn -q clean test

Process exited with code 0.
Build successful; 38 tests run, 35 passed, 0 failures/errors, 3 skipped
(RabbitMQ integration tests are opt-in without a broker).
```

The focused and full Maven commands both passed with
`JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot`.

## Fix report — channel-scoped shutdown handling and recovery isolation (2026-10-18)

- Associated every pending publication with the exact channel used for its
  publish and made each channel's shutdown listener capture that channel.
  A delayed shutdown callback from an old recovered channel can therefore no
  longer fail a publication running on the replacement channel.
- Extended
  `recoversTwoPublishersWithIndependentConfirmedChannels()` to use both
  recovered publishers for mandatory-return assertions, close one publisher
  explicitly, verify its channel shutdown does not close the other recovered
  channel, and publish successfully through the surviving publisher.
- No fixed return-settle timeout or Task 5 behavior was added. Confirm
  sequencing, mandatory returns, persistent JSON, and UTF-8 semantics remain
  unchanged.

Focused validation (JDK 25):

```text
mvn -q "-Dtest=messaging.RabbitPublisherTest,messaging.RabbitPublisherIntegrationTest,messaging.RabbitConnectionManagerTest" test

Process exited with code 0.
```

Full validation (JDK 25):

```text
mvn -q clean test

Process exited with code 0.
```

The opt-in RabbitMQ integration tests remain skipped unless
`-Drabbitmq.integration=true` is supplied with a running broker.
