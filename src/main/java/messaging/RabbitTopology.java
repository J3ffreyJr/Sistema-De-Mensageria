package messaging;

import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;

import java.io.IOException;
import java.util.Map;

public final class RabbitTopology {
    public static final String RETRY_EXCHANGE = RabbitMQConfig.EXCHANGE_NAME + ".retry";
    public static final String DEAD_LETTER_EXCHANGE = RabbitMQConfig.EXCHANGE_NAME + ".dead-letter";
    public static final String RETRY_QUEUE_1 = RabbitMQConfig.QUEUE_CONSUMIDOR1 + "_retry";
    public static final String RETRY_QUEUE_2 = RabbitMQConfig.QUEUE_CONSUMIDOR2 + "_retry";
    public static final String RETRY_QUEUE_1_EXHAUSTED = RETRY_QUEUE_1 + "_exhausted";
    public static final String RETRY_QUEUE_2_EXHAUSTED = RETRY_QUEUE_2 + "_exhausted";
    public static final String RETRY_ROUTING_KEY_1 = RabbitMQConfig.ROUTING_KEY_1 + ".retry.1";
    public static final String RETRY_ROUTING_KEY_2 = RabbitMQConfig.ROUTING_KEY_2 + ".retry.1";
    private static final String RETRY_ROUTING_KEY_1_EXHAUSTED = RabbitMQConfig.ROUTING_KEY_1 + ".retry.2";
    private static final String RETRY_ROUTING_KEY_2_EXHAUSTED = RabbitMQConfig.ROUTING_KEY_2 + ".retry.2";
    public static final String DEAD_LETTER_QUEUE_1 = RabbitMQConfig.QUEUE_CONSUMIDOR1 + "_dead_letter";
    public static final String DEAD_LETTER_QUEUE_2 = RabbitMQConfig.QUEUE_CONSUMIDOR2 + "_dead_letter";
    public static final int RETRY_TTL_MILLIS = 5_000;

    public void declare(Channel channel) {
        try {
            channel.exchangeDeclare(RabbitMQConfig.EXCHANGE_NAME, "direct", true);
            channel.exchangeDeclare(RETRY_EXCHANGE, "direct", true);
            channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "direct", true);

            declareConsumerQueue(channel, RabbitMQConfig.QUEUE_CONSUMIDOR1, RabbitMQConfig.ROUTING_KEY_1,
                    RETRY_ROUTING_KEY_1);
            declareConsumerQueue(channel, RabbitMQConfig.QUEUE_CONSUMIDOR2, RabbitMQConfig.ROUTING_KEY_2,
                    RETRY_ROUTING_KEY_2);
            declareRetryQueue(channel, RETRY_QUEUE_1, RETRY_ROUTING_KEY_1, RETRY_ROUTING_KEY_1_EXHAUSTED);
            declareRetryQueue(channel, RETRY_QUEUE_2, RETRY_ROUTING_KEY_2, RETRY_ROUTING_KEY_2_EXHAUSTED);
            declareExhaustedRetryQueue(channel, RETRY_QUEUE_1_EXHAUSTED,
                    RETRY_ROUTING_KEY_1_EXHAUSTED, RabbitMQConfig.ROUTING_KEY_1);
            declareExhaustedRetryQueue(channel, RETRY_QUEUE_2_EXHAUSTED,
                    RETRY_ROUTING_KEY_2_EXHAUSTED, RabbitMQConfig.ROUTING_KEY_2);
            declareDeadLetterQueue(channel, DEAD_LETTER_QUEUE_1, RabbitMQConfig.ROUTING_KEY_1);
            declareDeadLetterQueue(channel, DEAD_LETTER_QUEUE_2, RabbitMQConfig.ROUTING_KEY_2);
        } catch (IOException | RuntimeException exception) {
            throw new RabbitConnectionException("Unable to declare RabbitMQ topology", exception);
        }
    }

    private void declareConsumerQueue(Channel channel, String queue, String routingKey,
                                      String firstRetryRoutingKey) throws IOException {
        channel.queueDeclare(queue, true, false, false, Map.of(
                "x-dead-letter-exchange", RETRY_EXCHANGE,
                "x-dead-letter-routing-key", firstRetryRoutingKey));
        channel.queueBind(queue, RabbitMQConfig.EXCHANGE_NAME, routingKey);
        channel.queueBind(queue, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
    }

    private void declareRetryQueue(Channel channel, String queue, String routingKey, String nextRoutingKey)
            throws IOException {
        channel.queueDeclare(queue, true, false, false, Map.of(
                "x-message-ttl", RETRY_TTL_MILLIS,
                "x-dead-letter-exchange", RETRY_EXCHANGE,
                "x-dead-letter-routing-key", nextRoutingKey));
        channel.queueBind(queue, RETRY_EXCHANGE, routingKey);
    }

    private void declareExhaustedRetryQueue(Channel channel, String queue, String routingKey,
                                            String originalRoutingKey) throws IOException {
        channel.queueDeclare(queue, true, false, false, Map.of(
                "x-message-ttl", RETRY_TTL_MILLIS,
                "x-dead-letter-exchange", DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key", originalRoutingKey));
        channel.queueBind(queue, RETRY_EXCHANGE, routingKey);
    }

    private void declareDeadLetterQueue(Channel channel, String queue, String routingKey) throws IOException {
        channel.queueDeclare(queue, true, false, false, Map.of());
        channel.queueBind(queue, DEAD_LETTER_EXCHANGE, routingKey);
    }
}
