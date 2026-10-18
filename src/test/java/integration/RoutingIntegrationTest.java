package integration;

import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import messaging.Message;
import messaging.MessageCodec;
import messaging.PublishResult;
import messaging.RabbitConnectionManager;
import messaging.RabbitPublisher;
import messaging.RabbitTopology;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RoutingIntegrationTest {
    @Test
    void routesEachDestinationToTheExpectedConsumers() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        String suffix = UUID.randomUUID().toString();
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitPublisher publisher = new RabbitPublisher(manager)) {
            manager.open();
            try (Channel channel = manager.createChannel()) {
                String queue1 = channel.queueDeclare("it-" + suffix + "-1", false, true, true, null).getQueue();
                String queue2 = channel.queueDeclare("it-" + suffix + "-2", false, true, true, null).getQueue();
                channel.queueBind(queue1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_1);
                channel.queueBind(queue1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
                channel.queueBind(queue2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_2);
                channel.queueBind(queue2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);

                Message one = message("destination-1-" + suffix, RabbitMQConfig.ROUTING_KEY_1);
                Message two = message("destination-2-" + suffix, RabbitMQConfig.ROUTING_KEY_2);
                Message both = message("destination-3-" + suffix, RabbitMQConfig.ROUTING_KEY_ALL);
                assertEquals(PublishResult.Status.CONFIRMED, publisher.publish(one).status());
                assertEquals(PublishResult.Status.CONFIRMED, publisher.publish(two).status());
                assertEquals(PublishResult.Status.CONFIRMED, publisher.publish(both).status());

                assertEquals(List.of(one.messageId(), both.messageId()), readIds(channel, queue1));
                assertEquals(List.of(two.messageId(), both.messageId()), readIds(channel, queue2));
                assertEquals(PublishResult.Status.RETURNED,
                        publisher.publish(message("unbound-" + suffix, "unbound-" + suffix)).status());
            }
        }
    }

    private static List<String> readIds(Channel channel, String queue) throws Exception {
        MessageCodec codec = new MessageCodec();
        List<String> ids = new ArrayList<>();
        com.rabbitmq.client.GetResponse response;
        while ((response = channel.basicGet(queue, true)) != null) {
            ids.add(codec.decode(response.getBody()).messageId());
        }
        assertTrue(ids.size() >= 2);
        return ids;
    }

    private static Message message(String id, String routingKey) {
        return new Message(1, id, "integration", Instant.now(), "task-7", routingKey);
    }
}
