# Evolução incremental do sistema de mensageria Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transformar o protótipo Java/RabbitMQ num núcleo de mensageria confiável, configurável e testável, preparado para uma futura API web.

**Architecture:** Extrair configuração, contrato de mensagem, gestão de conexão, publicação e consumo para componentes pequenos com interfaces explícitas. O fluxo usará publisher confirms, ack manual, retry limitado, dead-letter e idempotência por `messageId`; a UI web e a persistência de conversas ficam fora desta fase.

**Tech Stack:** Java 25, Maven, RabbitMQ Java Client 5.34.0, Jackson 2.18.9, SLF4J 1.7.36, JUnit Jupiter 5.12.2, Docker Compose e RabbitMQ.

**Spec:** `docs/superpowers/specs/2026-10-18-evolucao-incremental-mensageria-design.md`

## Global Constraints

- Java 25 é o requisito oficial.
- RabbitMQ deve funcionar tanto por Docker Compose como por instalação local.
- O desenvolvimento deve usar volume persistente; os testes de integração devem poder usar um broker efémero.
- A semântica de entrega é `at-least-once`.
- Duplicados devem ser controlados por `messageId`.
- Não incluir frontend, WebSocket, autenticação, base de dados de conversas ou animações nesta fase.
- Não adicionar uma biblioteca de mensageria; usar o RabbitMQ Java Client existente.
- Não manter credenciais obrigatórias hard-coded no código.

## Review Focus

- Payload JSON inválido deve ir para dead-letter sem retry infinito; cobrir em `MessageCodecTest`.
- Falha depois da entrega e antes do processamento concluído deve provocar reentrega; cobrir no teste de integração de ack/nack.
- Publicação sem binding deve ser detectada como mensagem retornada, não registada como envio confirmado; cobrir em `RabbitPublisherIntegrationTest`.
- Reconexão durante indisponibilidade temporária do broker deve recriar o consumo; cobrir em `RabbitConnectionManagerIntegrationTest`.
- Mesmo `messageId` entregue duas vezes não deve executar o efeito duas vezes; cobrir em `IdempotentMessageProcessorTest`.

---

### Task 1: Fixar build, configuração e documentação de execução

**Files:**
- Modify: `pom.xml`
- Create: `docker-compose.yml`
- Create: `.gitignore`
- Create: `README.md`
- Create: `src/main/java/config/RabbitMQProperties.java`
- Modify: `dependency-list.txt` or remove it if it is not part of the supported workflow
- Test: `src/test/java/config/RabbitMQConfigTest.java`

**Interfaces:**
- `RabbitMQProperties#fromEnvironment(Map<String,String> environment) -> RabbitMQProperties`.
- `RabbitMQProperties#host() -> String`, `port() -> int`, `vhost() -> String`, `username() -> String`, `password() -> String`, `tlsEnabled() -> boolean`.
- Defines Java 25 and the dependency versions used by the implementation.

- [ ] **Step 1: Write failing configuration tests** covering default local values, environment/property overrides, invalid port values, missing password and TLS flag parsing, using `RabbitMQProperties.fromEnvironment(...)`.
- [ ] **Step 2: Run `mvn -q -Dtest=RabbitMQConfigTest test`** with Java 25 and verify the new override tests fail before the implementation exists.
- [ ] **Step 3: Implement external configuration** without exposing passwords in `toString()` or logs; support `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_VHOST`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` and `RABBITMQ_TLS_ENABLED`.
- [ ] **Step 4: Add Docker Compose** with RabbitMQ management image, explicit development credentials, named persistent volume and health check; document local installation as an alternative.
- [ ] **Step 5: Add `.gitignore` and README instructions** for Java 25, Maven, Docker Compose, local RabbitMQ, test execution and environment variables.
- [ ] **Step 6: Synchronize or remove `dependency-list.txt`** so it cannot advertise versions different from `pom.xml`.
- [ ] **Step 7: Run `mvn clean test`** with `JAVA_HOME` pointing to JDK 25 and verify all existing and new configuration tests pass.

### Task 2: Introduce the versioned message contract and codec

**Files:**
- Create: `src/main/java/messaging/Message.java`
- Create: `src/main/java/messaging/MessageCodec.java`
- Create: `src/main/java/messaging/MessageValidationException.java`
- Create: `src/test/java/messaging/MessageCodecTest.java`
- Modify: `pom.xml` only if a Jackson module required by the DTO is missing

**Interfaces:**
- `Message` is an immutable Java record with `int schemaVersion`, `String messageId`, `String content`, `Instant createdAt`, `String producer` and `String routingKey`.
- `MessageCodec#encode(Message message) -> byte[]`.
- `MessageCodec#decode(byte[] payload) -> Message`.
- `MessageCodec#validate(Message message) -> void`.

- [ ] **Step 1: Write tests** for valid schema version `1`, required fields, ISO-8601 timestamps, UTF-8 content including Portuguese characters and malformed JSON.
- [ ] **Step 2: Run `mvn -q -Dtest=MessageCodecTest test`** and verify the tests fail because the contract and codec do not yet exist.
- [ ] **Step 3: Implement the record and codec** with Jackson, `StandardCharsets.UTF_8`, explicit validation and no generic `Map` casts.
- [ ] **Step 4: Ensure malformed payloads raise `MessageValidationException`** and do not silently produce a partially populated message.
- [ ] **Step 5: Run the focused test and then `mvn clean test`**; verify all codec and existing tests pass.

### Task 3: Centralize topology and connection lifecycle

**Files:**
- Create: `src/main/java/messaging/RabbitTopology.java`
- Create: `src/main/java/messaging/RabbitConnectionManager.java`
- Create: `src/main/java/messaging/RabbitConnectionException.java`
- Create: `src/test/java/messaging/RabbitConnectionManagerTest.java`
- Create: `src/test/java/messaging/RabbitConnectionManagerIntegrationTest.java`

**Interfaces:**
- `RabbitTopology#declare(Channel channel) -> void`.
- `RabbitConnectionManager#open() -> void`.
- `RabbitConnectionManager#createChannel() -> Channel`.
- `RabbitConnectionManager#isConnected() -> boolean`.
- `RabbitConnectionManager#close() -> void`.

- [ ] **Step 1: Write unit tests** for durable direct exchange, durable consumer queues, bindings for routing keys `consumidor1`, `consumidor2` and `ambos`, retry queues and dead-letter bindings.
- [ ] **Step 2: Write lifecycle tests** proving shutdown closes channels/connections and configuration errors are surfaced as `RabbitConnectionException`.
- [ ] **Step 3: Run the focused tests** and verify they fail before implementation.
- [ ] **Step 4: Implement topology declaration** idempotently, centralizing all exchange, queue, binding, retry and dead-letter names.
- [ ] **Step 5: Implement connection creation** from the external properties, including optional TLS and bounded recovery/backoff configuration.
- [ ] **Step 6: Add shutdown handling** so no consumer is left active after `close()`.
- [ ] **Step 7: Run focused tests and `mvn clean test`** with Java 25.
- [ ] **Step 8: Run `RabbitConnectionManagerIntegrationTest` against a broker restart** and verify reconnection recreates the channel and topology without duplicating declarations.

### Task 4: Implement confirmed publishing

**Files:**
- Create: `src/main/java/messaging/RabbitPublisher.java`
- Create: `src/main/java/messaging/PublishResult.java`
- Create: `src/main/java/messaging/PublishException.java`
- Create: `src/test/java/messaging/RabbitPublisherTest.java`
- Create: `src/test/java/messaging/RabbitPublisherIntegrationTest.java`
- Modify: `src/main/java/producer/Produtor.java`

**Interfaces:**
- `RabbitPublisher#publish(Message message) -> PublishResult`.
- `PublishResult` distinguishes `CONFIRMED`, `RETURNED` and `FAILED`.
- `RabbitPublisher#close() -> void`.

- [ ] **Step 1: Write unit tests** asserting message properties `deliveryMode=2`, content type `application/json`, UTF-8 encoding, AMQP `messageId`, `mandatory=true` and routing key selection.
- [ ] **Step 2: Write integration tests** for a bound routing key and for a deliberately unbound routing key.
- [ ] **Step 3: Run focused unit tests** and verify they fail before implementation.
- [ ] **Step 4: Implement publisher confirms and returned-message handling**; only return `CONFIRMED` after broker confirmation and never label a returned message as successfully sent.
- [ ] **Step 5: Replace producer-side `Map` construction** with `Message` and route input `1`, `2` and `3` through the publisher.
- [ ] **Step 6: Run the integration tests against the Docker/local broker** and verify the three routing destinations.

### Task 5: Implement manual-ack consumers, retry and idempotency

**Files:**
- Create: `src/main/java/messaging/MessageProcessor.java`
- Create: `src/main/java/messaging/IdempotencyStore.java`
- Create: `src/main/java/messaging/InMemoryIdempotencyStore.java`
- Create: `src/main/java/messaging/RabbitConsumer.java`
- Create: `src/test/java/messaging/IdempotentMessageProcessorTest.java`
- Create: `src/test/java/messaging/RabbitConsumerIntegrationTest.java`
- Modify: `src/main/java/consumer/Consumidor1.java`
- Modify: `src/main/java/consumer/Consumidor2.java`

**Interfaces:**
- `MessageProcessor#process(Message message) -> void`.
- `IdempotencyStore#tryStart(String messageId) -> boolean`.
- `IdempotencyStore#markCompleted(String messageId) -> void`.
- `RabbitConsumer#start() -> void`.
- `RabbitConsumer#stop() -> void`.

- [ ] **Step 1: Write unit tests** proving a repeated `messageId` is processed once, a failed processing attempt remains retryable, and a completed ID is not executed again.
- [ ] **Step 2: Write integration tests** for `basicQos(1)`, manual ack after successful processing, requeue/retry on transient failure, and dead-letter after the configured maximum.
- [ ] **Step 3: Run focused tests** and verify they fail before implementation.
- [ ] **Step 4: Implement the idempotency store and processor boundary** without treating logging as the business effect.
- [ ] **Step 5: Implement the consumer callback** with explicit UTF-8 decoding, manual ack, nack policy, retry count headers and dead-letter routing.
- [ ] **Step 6: Replace both duplicated consumer mains** with configuration-driven instances of `RabbitConsumer`.
- [ ] **Step 7: Run integration tests** after starting the broker and verify no message is silently lost during forced consumer interruption.

### Task 6: Replace file-per-event logging and add controlled shutdown

**Files:**
- Modify: `pom.xml` if a logging backend is required
- Create: `src/main/resources/simplelogger.properties` or the selected logging configuration
- Create: `src/main/java/utility/MessageLog.java`
- Modify: `src/main/java/utility/Registo.java`
- Modify: `src/main/java/producer/Produtor.java`
- Modify: `src/main/java/consumer/Consumidor1.java`
- Modify: `src/main/java/consumer/Consumidor2.java`
- Modify: `src/test/java/utility/RegistoTest.java`

**Interfaces:**
- `MessageLog` exposes structured events for publish, confirm, receive, retry, ack, nack, dead-letter, connection and shutdown.
- Existing callers may retain a compatibility facade only while migration is completed; normal operation must not truncate logs.

- [ ] **Step 1: Extend tests** for message IDs, error levels, preserved existing log content and no full payload requirement.
- [ ] **Step 2: Run `mvn -q -Dtest=RegistoTest test`** and verify the new expectations fail.
- [ ] **Step 3: Implement structured SLF4J logging** with configurable levels and rolling file output; remove normal-use truncation from `iniciarNovoLog()`.
- [ ] **Step 4: Add JVM shutdown hooks** to producer and consumers, stopping consumption before closing channels and connections.
- [ ] **Step 5: Run focused tests and a manual start/stop check**; verify shutdown events and original failures remain visible.

### Task 7: Add end-to-end validation and operational verification

**Files:**
- Create: `src/test/java/integration/RoutingIntegrationTest.java`
- Create: `src/test/java/integration/FailureRecoveryIntegrationTest.java`
- Modify: `README.md`
- Modify: `pom.xml` to add the opt-in integration-test profile
- Modify: `docker-compose.yml` only if health/test wiring requires it

**Interfaces:**
- Uses the public publisher, connection manager, topology, consumer and processor interfaces from Tasks 1–6.

- [ ] **Step 1: Add routing tests** asserting destination `1` reaches only consumer 1, destination `2` reaches only consumer 2 and destination `3` reaches both.
- [ ] **Step 2: Add recovery tests** for broker restart, consumer interruption and duplicate delivery.
- [ ] **Step 3: Run unit tests with `mvn clean test`** and verify they do not require a broker.
- [ ] **Step 4: Start the development broker with `docker compose up -d`** and run integration tests using an isolated test topology and `mvn verify -Pintegration`.
- [ ] **Step 5: Verify the acceptance criteria** from the specification, including confirmed publishing, manual ack, dead-letter, idempotency and reconnection.
- [ ] **Step 6: Update README** with exact commands, expected outputs, cleanup commands and known distinction between local and ephemeral broker modes.

## Execution Notes

The workspace currently has no Git repository, so commit steps can only be performed after version control is initialized. Until then, each task should still end with its focused test run and a clean `mvn clean test` checkpoint where practical.

## Final Verification

Run with JDK 25 selected:

```powershell
mvn clean verify
```

Then, with the broker running:

```powershell
docker compose up -d
mvn verify -Pintegration
```

Expected result: unit and integration tests pass, routing is correct for all three destinations, publisher confirms distinguish returned messages, failed processing is re-delivered or dead-lettered according to policy, and duplicate `messageId` values do not execute effects twice.
