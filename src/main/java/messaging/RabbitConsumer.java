package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import config.RabbitMQConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A manually acknowledged consumer. A failed business operation is retried
 * through the topology's bounded retry queues; malformed messages go directly
 * to the dead-letter exchange.
 */
public final class RabbitConsumer implements AutoCloseable {
    public static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final RabbitConnectionManager manager;
    private final String queue;
    private final MessageProcessor processor;
    private final MessageCodec codec;
    private final int maxRetries;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Channel channel;
    private final java.util.function.Consumer<Channel> recoveredChannel = this::installChannel;

    public RabbitConsumer(RabbitConnectionManager manager, String queue, MessageProcessor processor) {
        this(manager, queue, processor, new InMemoryIdempotencyStore(), 2);
    }

    public RabbitConsumer(RabbitConnectionManager manager, String queue, MessageProcessor processor,
                          IdempotencyStore idempotencyStore, int maxRetries) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.processor = new IdempotentMessageProcessor(
                Objects.requireNonNull(idempotencyStore, "idempotencyStore"),
                Objects.requireNonNull(processor, "processor"));
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must not be negative");
        this.maxRetries = maxRetries;
        this.codec = new MessageCodec();
    }

    public RabbitConsumer(RabbitConnectionManager manager, String queue, MessageProcessor processor,
                          int maxRetries) {
        this(manager, queue, processor, new InMemoryIdempotencyStore(), maxRetries);
    }

    public synchronized void start() {
        if (running.get()) return;
        manager.open();
        running.set(true);
        manager.addRecoveryListener(recoveredChannel);
        installChannel(manager.createChannel());
    }

    public synchronized void stop() {
        if (!running.getAndSet(false)) return;
        manager.removeRecoveryListener(recoveredChannel);
        Channel active = channel;
        channel = null;
        if (active != null) {
            try { if (active.isOpen()) active.close(); } catch (Exception ignored) { }
        }
    }

    @Override
    public void close() {
        stop();
    }

    private synchronized void installChannel(Channel newChannel) {
        if (!running.get()) {
            closeQuietly(newChannel);
            return;
        }
        try {
            newChannel.basicQos(1);
            newChannel.basicConsume(queue, false, deliverCallback(), ignored -> { });
            channel = newChannel;
        } catch (IOException | RuntimeException failure) {
            closeQuietly(newChannel);
            throw new RabbitConnectionException("Unable to start consumer for " + queue, failure);
        }
    }

    private DeliverCallback deliverCallback() {
        return (ignored, delivery) -> handle(delivery);
    }

    private void handle(Delivery delivery) {
        Channel active = channel;
        if (active == null || !running.get()) return;
        long tag = delivery.getEnvelope().getDeliveryTag();
        Message message;
        try {
            // MessageCodec also validates the versioned contract.
            message = codec.decode(new String(delivery.getBody(), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException invalid) {
            deadLetter(active, delivery, tag);
            return;
        }
        try {
            processor.process(message);
            active.basicAck(tag, false);
        } catch (RuntimeException transientFailure) {
            retryOrDeadLetter(active, delivery, tag);
        } catch (IOException channelFailure) {
            // A closed channel leaves the delivery unacked and RabbitMQ redelivers it.
        }
    }

    private void retryOrDeadLetter(Channel active, Delivery delivery, long tag) {
        int retryCount = retryCount(delivery.getProperties());
        if (retryCount >= maxRetries) {
            deadLetter(active, delivery, tag);
            return;
        }
        try {
            Map<String, Object> headers = new HashMap<>();
            if (delivery.getProperties().getHeaders() != null) {
                headers.putAll(delivery.getProperties().getHeaders());
            }
            headers.put(RETRY_COUNT_HEADER, retryCount + 1);
            String retryKey = queue.equals(RabbitMQConfig.QUEUE_CONSUMIDOR2)
                    ? RabbitTopology.RETRY_ROUTING_KEY_2 : RabbitTopology.RETRY_ROUTING_KEY_1;
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType(delivery.getProperties().getContentType())
                    .contentEncoding("UTF-8").headers(headers).deliveryMode(2).build();
            active.basicPublish(RabbitTopology.RETRY_EXCHANGE, retryKey, properties, delivery.getBody());
            active.basicAck(tag, false);
        } catch (IOException | RuntimeException publishFailure) {
            try { active.basicNack(tag, false, true); } catch (IOException ignored) { }
        }
    }

    private void deadLetter(Channel active, Delivery delivery, long tag) {
        try {
            String routingKey = queue.equals(RabbitMQConfig.QUEUE_CONSUMIDOR2)
                    ? RabbitMQConfig.ROUTING_KEY_2 : RabbitMQConfig.ROUTING_KEY_1;
            active.basicPublish(RabbitTopology.DEAD_LETTER_EXCHANGE, routingKey,
                    delivery.getProperties(), delivery.getBody());
            active.basicAck(tag, false);
        } catch (IOException | RuntimeException failure) {
            try { active.basicNack(tag, false, true); } catch (IOException ignored) { }
        }
    }

    private static int retryCount(AMQP.BasicProperties properties) {
        Object value = properties == null || properties.getHeaders() == null
                ? null : properties.getHeaders().get(RETRY_COUNT_HEADER);
        if (value instanceof Number number) return Math.max(0, number.intValue());
        try { return value == null ? 0 : Math.max(0, Integer.parseInt(value.toString())); }
        catch (NumberFormatException ignored) { return 0; }
    }

    private static void closeQuietly(Channel channel) {
        try { if (channel != null && channel.isOpen()) channel.close(); } catch (Exception ignored) { }
    }
}
