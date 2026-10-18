package integration;

import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import messaging.InMemoryIdempotencyStore;
import messaging.Message;
import messaging.RabbitConnectionManager;
import messaging.RabbitConsumer;
import messaging.RabbitPublisher;
import messaging.RabbitTopology;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class FailureRecoveryIntegrationTest {
    @Test
    void recoversPublishingAndConsumersAfterConnectionInterruption() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        String suffix = UUID.randomUUID().toString();
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger firstCalls = new AtomicInteger();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, RabbitMQConfig.QUEUE_CONSUMIDOR1,
                     message -> { calls.incrementAndGet(); firstCalls.incrementAndGet(); })) {
            purge(properties, RabbitMQConfig.QUEUE_CONSUMIDOR1);
            consumer.start();
            try (RabbitPublisher publisher = new RabbitPublisher(manager)) {
                assertEquals(messaging.PublishResult.Status.CONFIRMED,
                        publisher.publish(message("before-recovery-" + suffix, RabbitMQConfig.ROUTING_KEY_1)).status());
            }
            assertTrue(waitForCalls(firstCalls, 1, Duration.ofSeconds(5)));

            consumer.stop();
            manager.close();
            manager.open();
            consumer.start();
            try (RabbitPublisher publisher = new RabbitPublisher(manager)) {
                assertEquals(messaging.PublishResult.Status.CONFIRMED,
                        publisher.publish(message("after-recovery-" + suffix, RabbitMQConfig.ROUTING_KEY_1)).status());
            }
            assertTrue(waitForCalls(calls, 2, Duration.ofSeconds(5)));
        }
    }

    @Test
    void duplicateMessageIdsExecuteBusinessEffectOnlyOnce() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        String suffix = UUID.randomUUID().toString();
        AtomicInteger effects = new AtomicInteger();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitPublisher publisher = new RabbitPublisher(manager);
             RabbitConsumer consumer = new RabbitConsumer(manager, RabbitMQConfig.QUEUE_CONSUMIDOR1,
                     message -> effects.incrementAndGet(), new InMemoryIdempotencyStore(), 0)) {
            purge(properties, RabbitMQConfig.QUEUE_CONSUMIDOR1);
            consumer.start();
            Message duplicate = message("duplicate-" + suffix, RabbitMQConfig.ROUTING_KEY_1);
            assertEquals(messaging.PublishResult.Status.CONFIRMED, publisher.publish(duplicate).status());
            assertEquals(messaging.PublishResult.Status.CONFIRMED, publisher.publish(duplicate).status());
            assertTrue(waitForCalls(effects, 1, Duration.ofSeconds(5)));
            Thread.sleep(250);
            assertEquals(1, effects.get());
        }
    }

    @Test
    void failedProcessingIsDeadLetteredAfterRetryPolicy() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        String suffix = UUID.randomUUID().toString();
        AtomicInteger attempts = new AtomicInteger();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitPublisher publisher = new RabbitPublisher(manager);
             RabbitConsumer consumer = new RabbitConsumer(manager, RabbitMQConfig.QUEUE_CONSUMIDOR1,
                     message -> { attempts.incrementAndGet(); throw new IllegalStateException("expected"); }, 0)) {
            purge(properties, RabbitMQConfig.QUEUE_CONSUMIDOR1);
            purge(properties, RabbitTopology.DEAD_LETTER_QUEUE_1);
            consumer.start();
            Message failed = message("dead-letter-" + suffix, RabbitMQConfig.ROUTING_KEY_1);
            assertEquals(messaging.PublishResult.Status.CONFIRMED, publisher.publish(failed).status());
            assertTrue(waitForCalls(attempts, 1, Duration.ofSeconds(5)));
            assertTrue(waitForDeadLetter(properties, failed.messageId(), Duration.ofSeconds(5)));
        }
    }

    private static boolean waitForDeadLetter(RabbitMQProperties properties, String id, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties)) {
            manager.open();
            try (Channel channel = manager.createChannel()) {
                while (System.nanoTime() < deadline) {
                    com.rabbitmq.client.GetResponse response =
                            channel.basicGet(RabbitTopology.DEAD_LETTER_QUEUE_1, true);
                    if (response != null && id.equals(response.getProps().getMessageId())) return true;
                    Thread.sleep(50);
                }
            }
        }
        return false;
    }

    private static void purge(RabbitMQProperties properties, String queue) throws Exception {
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties)) {
            manager.open();
            try (Channel channel = manager.createChannel()) {
                channel.queuePurge(queue);
            }
        }
    }

    private static boolean waitForCalls(AtomicInteger calls, int expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (calls.get() < expected && System.nanoTime() < deadline) Thread.sleep(25);
        return calls.get() >= expected;
    }

    private static Message message(String id, String routingKey) {
        return new Message(1, id, "integration", Instant.now(), "task-7", routingKey);
    }
}
