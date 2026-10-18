package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * These tests use the application's declared topology and unique message IDs.
 * They are deliberately opt-in because they require a reachable RabbitMQ broker.
 */
class RabbitConsumerIntegrationTest {
    private static final String QUEUE = RabbitMQConfig.QUEUE_CONSUMIDOR1;
    private static final String DEAD_QUEUE = RabbitTopology.DEAD_LETTER_QUEUE_1;

    @Test
    void successfulProcessingUsesQosOneAndManualAck() throws Exception {
        assumeIntegration();
        RabbitMQProperties properties = properties();
        CountDownLatch entered = new CountDownLatch(1);
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, QUEUE, message -> entered.countDown());
             Channel publisher = publishChannel(manager)) {
            purge(publisher);
            consumer.start();
            publish(publisher, new Message(1, id(), "integration", Instant.now(), "ok",
                    RabbitMQConfig.ROUTING_KEY_1));
            assertNotNull(publisher.waitForConfirms() ? entered.await(5, TimeUnit.SECONDS) : null);
            assertNull(publisher.basicGet(QUEUE, true), "successful delivery must be acknowledged");
        }
    }

    @Test
    void interruptionLeavesDeliveryForRedelivery() throws Exception {
        assumeIntegration();
        RabbitMQProperties properties = properties();
        CountDownLatch first = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, QUEUE, message -> {
                 if (attempts.incrementAndGet() == 1) first.countDown();
             });
             Channel publisher = publishChannel(manager)) {
            purge(publisher);
            consumer.start();
            publish(publisher, new Message(1, id(), "integration", Instant.now(), "redelivery",
                    RabbitMQConfig.ROUTING_KEY_1));
            assertEquals(true, first.await(5, TimeUnit.SECONDS));
            manager.recoverNowForTest();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (attempts.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(2, attempts.get(), "unacked delivery should be redelivered");
        }
    }

    @Test
    void transientFailuresStayBoundedAndExhaustedMessagesDeadLetter() throws Exception {
        assumeIntegration();
        RabbitMQProperties properties = properties();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, QUEUE, message -> {
                 throw new IllegalStateException("transient");
             }, 1);
             Channel publisher = publishChannel(manager)) {
            purge(publisher);
            consumer.start();
            publish(publisher, new Message(1, id(), "integration", Instant.now(), "retry",
                    RabbitMQConfig.ROUTING_KEY_1));
            GetResponse deadLetter = awaitGet(publisher, DEAD_QUEUE, 15_000);
            assertNotNull(deadLetter,
                    "message must reach DLX after the configured retry bound");
            assertEquals(1, deadLetter.getProps().getHeaders().get(RabbitConsumer.RETRY_COUNT_HEADER),
                    "dead-letter copy must preserve the bounded retry count");
            assertNull(publisher.basicGet(DEAD_QUEUE, true),
                    "bounded retry must produce only one dead-letter copy");
            assertNull(publisher.basicGet(QUEUE, true));
        }
    }

    @Test
    void malformedUtf8IsDeadLetteredWithoutProcessing() throws Exception {
        assumeIntegration();
        RabbitMQProperties properties = properties();
        AtomicInteger processed = new AtomicInteger();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, QUEUE,
                     message -> processed.incrementAndGet());
             Channel publisher = publishChannel(manager)) {
            purge(publisher);
            consumer.start();
            publisher.basicPublish(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_1,
                    new AMQP.BasicProperties.Builder().contentType("application/json").build(),
                    new byte[] {(byte) 0xc3, (byte) 0x28});
            publisher.waitForConfirmsOrDie();
            assertNotNull(awaitGet(publisher, DEAD_QUEUE, 5_000));
            assertEquals(0, processed.get());
        }
    }

    @Test
    void stopClosesConsumerAndLeavesNewMessagesAvailable() throws Exception {
        assumeIntegration();
        RabbitMQProperties properties = properties();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, QUEUE, message -> { });
             Channel publisher = publishChannel(manager)) {
            purge(publisher);
            consumer.start();
            consumer.stop();
            publish(publisher, new Message(1, id(), "integration", Instant.now(), "stopped",
                    RabbitMQConfig.ROUTING_KEY_1));
            assertNotNull(publisher.basicGet(QUEUE, true));
        }
    }

    private static RabbitMQProperties properties() {
        return RabbitMQProperties.fromEnvironment(System.getenv());
    }

    private static void assumeIntegration() {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"),
                "Set -Drabbitmq.integration=true to run broker integration tests");
    }

    private static Channel publishChannel(RabbitConnectionManager manager) {
        manager.open();
        Channel channel = manager.createChannel();
        try {
            channel.confirmSelect();
            return channel;
        } catch (Exception failure) {
            try { channel.close(); } catch (Exception ignored) { }
            throw new AssertionError(failure);
        }
    }

    private static void purge(Channel channel) throws Exception {
        channel.queuePurge(QUEUE);
        channel.queuePurge(RabbitTopology.RETRY_QUEUE_1);
        channel.queuePurge(DEAD_QUEUE);
    }

    private static void publish(Channel channel, Message message) throws Exception {
        channel.basicPublish(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_1,
                new AMQP.BasicProperties.Builder().contentType("application/json")
                        .contentEncoding("UTF-8").deliveryMode(2).build(),
                new MessageCodec().encode(message));
        channel.waitForConfirmsOrDie();
    }

    private static GetResponse awaitGet(Channel channel, String queue, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        GetResponse response;
        while ((response = channel.basicGet(queue, true)) == null
                && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        return response;
    }

    private static String id() {
        return "consumer-integration-" + System.nanoTime();
    }
}
