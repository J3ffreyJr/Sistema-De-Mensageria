package messaging;

import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RabbitPublisherIntegrationTest {

    @Test
    void confirmsMessageDeliveredToBoundDestination() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        try (RabbitPublisher publisher = new RabbitPublisher(properties);
             RabbitConnectionManager manager = new RabbitConnectionManager(properties)) {
            manager.open();
            try (Channel channel = manager.createChannel()) {
                channel.queuePurge(RabbitMQConfig.QUEUE_CONSUMIDOR1);
                channel.queuePurge(RabbitMQConfig.QUEUE_CONSUMIDOR2);
                String id1 = "integration-1-" + System.nanoTime();
                String id2 = "integration-2-" + System.nanoTime();
                String id3 = "integration-3-" + System.nanoTime();
                assertEquals(PublishResult.Status.CONFIRMED,
                        publisher.publish(message(id1, RabbitMQConfig.ROUTING_KEY_1)).status());
                assertEquals(PublishResult.Status.CONFIRMED,
                        publisher.publish(message(id2, RabbitMQConfig.ROUTING_KEY_2)).status());
                assertEquals(PublishResult.Status.CONFIRMED,
                        publisher.publish(message(id3, RabbitMQConfig.ROUTING_KEY_ALL)).status());
                MessageCodec codec = new MessageCodec();
                assertEquals(id1, codec.decode(channel.basicGet(RabbitMQConfig.QUEUE_CONSUMIDOR1, true)
                        .getBody()).messageId());
                assertEquals(id3, codec.decode(channel.basicGet(RabbitMQConfig.QUEUE_CONSUMIDOR1, true)
                        .getBody()).messageId());
                assertEquals(id2, codec.decode(channel.basicGet(RabbitMQConfig.QUEUE_CONSUMIDOR2, true)
                        .getBody()).messageId());
                assertEquals(id3, codec.decode(channel.basicGet(RabbitMQConfig.QUEUE_CONSUMIDOR2, true)
                        .getBody()).messageId());
            }
        }
    }

    @Test
    void reportsUnboundMandatoryMessageAsReturned() {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        try (RabbitPublisher publisher = new RabbitPublisher(properties)) {
            PublishResult result = publisher.publish(message("integration-" + System.nanoTime(),
                    "deliberately-unbound"));
            assertEquals(PublishResult.Status.RETURNED, result.status());
            assertNotEquals(PublishResult.Status.CONFIRMED, result.status());
        }
    }

    private static Message message(String id, String routingKey) {
        return new Message(1, id, "integration", Instant.now(), "integration-test", routingKey);
    }
}
