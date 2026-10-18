# Java Upgrade Summary: sistema-mensageria

- **Session**: 20260922095145
- **Date**: 2026-09-22
- **Result**: Completed successfully
- **Target**: Java 25 LTS

## Changes

- Updated Maven compiler source and target from Java 11 to Java 25 in `pom.xml`.
- Installed JDK 25.0.2 and Maven 3.9.16 for validation.
- Upgraded RabbitMQ Java Client from 5.16.0 to 5.34.0.
- Upgraded Jackson modules from 2.15.2 to 2.18.9 after the CVE scan.

## Verification

- Java 11 baseline compilation: passed.
- Java 11 baseline tests: passed.
- Java 25 test compilation: passed.
- Java 25 tests: passed with no failures.
- Final `mvn clean verify -Djacoco.skip=false`: passed.
- Initial dependency scan: 15 CVEs reported.
- Follow-up dependency scan: no known CVEs found.

## Risks and Limitations

- No Git repository is present in the workspace, so no branch or commit was created.
- No JaCoCo plugin is configured, so coverage metrics were not available.
- RabbitMQ integration behavior still depends on external broker availability at runtime.

## Task 4 Review Fix Report (2026-10-18)

### Fixes

- `RabbitPublisher` now installs a shutdown listener on every publisher channel, including
  channels supplied after recovery. A pending publish is failed and its confirmation latch is
  released when RabbitMQ shuts the channel down, so the publish lock cannot remain blocked.
- Shutdown failure is checked before return/confirmation classification. A confirmed publication
  remains confirmed when its acknowledgement wins the shutdown race; a returned publication is
  never converted to confirmed because return classification still precedes confirmation status.
- Publishers constructed with a shared `RabbitConnectionManager` no longer close the shared manager.
  Each publisher keeps an independent channel and closes only its own channel.
- Added a regression test that uses two real publishers sharing one manager, checks distinct
  channels before and after recovery, and confirms successful publishes through both recovered
  channels. Added a unit regression test proving shutdown-before-confirmation fails promptly.

### Exact verification evidence

- `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot mvn -q -Dtest=RabbitPublisherTest test`
  — passed, 6 focused tests, 0 failures, 0 errors.
- `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot mvn -q test`
  — passed, 43 tests, 0 failures, 0 errors; 1 RabbitMQ integration test skipped because
  `rabbitmq.integration` was not enabled (the integration class was compiled).
- `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot mvn -q clean verify`
  — passed, including clean compilation and the full Maven test phase.
- Broker-backed integration tests were not executed because no RabbitMQ integration flag/broker was
  available in this environment.
