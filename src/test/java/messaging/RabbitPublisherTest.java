package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RabbitPublisherTest {

    @Test
    void publishesPersistentJsonUtf8MessageWithMandatoryRoutingAndMessageId() throws Exception {
        AtomicReference<AMQP.BasicProperties> properties = new AtomicReference<>();
        AtomicReference<String> routingKey = new AtomicReference<>();
        AtomicReference<byte[]> body = new AtomicReference<>();
        Channel channel = channelFor((exchange, key, mandatory, basicProperties, payload) -> {
            routingKey.set(key);
            properties.set(basicProperties);
            body.set(payload);
        });
        RabbitPublisher publisher = new RabbitPublisher(channel, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec());

        Message message = new Message(1, "id-1", "olá", Instant.parse("2026-01-01T00:00:00Z"),
                "test", RabbitMQConfig.ROUTING_KEY_2);
        PublishResult result = publisher.publish(message);

        assertEquals(PublishResult.Status.CONFIRMED, result.status());
        assertEquals(RabbitMQConfig.ROUTING_KEY_2, routingKey.get());
        assertEquals(2, properties.get().getDeliveryMode());
        assertEquals("application/json", properties.get().getContentType());
        assertEquals("UTF-8", properties.get().getContentEncoding());
        assertEquals("id-1", properties.get().getMessageId());
        assertEquals("{\"schemaVersion\":1,\"messageId\":\"id-1\",\"content\":\"olá\","
                + "\"createdAt\":\"2026-01-01T00:00:00Z\",\"producer\":\"test\","
                + "\"routingKey\":\"consumidor2\"}", new String(body.get(), StandardCharsets.UTF_8));
        assertTrue(channel.isOpen());
    }

    @Test
    void returnsReturnedWhenMandatoryPublishIsReturnedEvenIfConfirmed() throws Exception {
        AtomicReference<com.rabbitmq.client.ReturnListener> listener = new AtomicReference<>();
        AtomicReference<com.rabbitmq.client.ConfirmListener> confirms = new AtomicReference<>();
        Channel channel = (Channel) java.lang.reflect.Proxy.newProxyInstance(
                Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (proxy, method, args) -> {
                    if (method.getName().equals("addReturnListener")) {
                        listener.set((com.rabbitmq.client.ReturnListener) args[0]);
                        return null;
                    }
                    if (method.getName().equals("confirmSelect")) return null;
                    if (method.getName().equals("getNextPublishSeqNo")) return 1L;
                    if (method.getName().equals("addConfirmListener")) {
                        confirms.set((com.rabbitmq.client.ConfirmListener) args[0]);
                        return null;
                    }
                    if (method.getName().equals("removeConfirmListener")) return null;
                    if (method.getName().equals("basicPublish")) {
                        listener.get().handleReturn(312, "NO_ROUTE", (String) args[0], (String) args[1],
                                (AMQP.BasicProperties) args[3], (byte[]) args[4]);
                        confirms.get().handleAck(1L, false);
                        return null;
                    }
                    if (method.getName().equals("isOpen")) return true;
                    return method.getReturnType().isPrimitive() ? false : null;
                });
        RabbitPublisher publisher = new RabbitPublisher(channel, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec());

        PublishResult result = publisher.publish(message(RabbitMQConfig.ROUTING_KEY_1));

        assertEquals(PublishResult.Status.RETURNED, result.status());
        assertEquals("NO_ROUTE", result.reason());
    }

    @Test
    void classifiesReturnedPublishUsingRabbitProtocolOrdering() throws Exception {
        AtomicReference<com.rabbitmq.client.ReturnListener> listener = new AtomicReference<>();
        AtomicReference<com.rabbitmq.client.ConfirmListener> confirms = new AtomicReference<>();
        Channel channel = (Channel) java.lang.reflect.Proxy.newProxyInstance(
                Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (proxy, method, args) -> {
                    if (method.getName().equals("addReturnListener")) {
                        listener.set((com.rabbitmq.client.ReturnListener) args[0]);
                        return null;
                    }
                    if (method.getName().equals("confirmSelect")) return null;
                    if (method.getName().equals("getNextPublishSeqNo")) return 1L;
                    if (method.getName().equals("addConfirmListener")) {
                        confirms.set((com.rabbitmq.client.ConfirmListener) args[0]);
                        return null;
                    }
                    if (method.getName().equals("removeConfirmListener")) return null;
                    if (method.getName().equals("basicPublish")) {
                        // RabbitMQ's protocol ordering contract is return before
                        // confirm for a mandatory unroutable publication.
                        listener.get().handleReturn(312, "NO_ROUTE", (String) args[0],
                                (String) args[1], (AMQP.BasicProperties) args[3], (byte[]) args[4]);
                        confirms.get().handleAck(1L, false);
                        return null;
                    }
                    if (method.getName().equals("isOpen")) return true;
                    return method.getReturnType().isPrimitive() ? false : null;
                });
        RabbitPublisher publisher = new RabbitPublisher(channel, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec());

        PublishResult result = publisher.publish(message(RabbitMQConfig.ROUTING_KEY_1));

        assertEquals(PublishResult.Status.RETURNED, result.status());
        assertEquals("NO_ROUTE", result.reason());
    }

    @Test
    void wrapsPublishFailures() {
        Channel channel = channelFor((exchange, key, mandatory, properties, payload) -> {
            throw new java.io.IOException("broker unavailable");
        });
        RabbitPublisher publisher = new RabbitPublisher(channel, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec());

        assertThrows(PublishException.class, () -> publisher.publish(message(RabbitMQConfig.ROUTING_KEY_1)));
    }

    private static Message message(String routingKey) {
        return new Message(1, "id-1", "hello", Instant.parse("2026-01-01T00:00:00Z"), "test", routingKey);
    }

    private static Channel channelFor(PublishCall call) {
        AtomicReference<com.rabbitmq.client.ConfirmListener> confirms = new AtomicReference<>();
        return (Channel) java.lang.reflect.Proxy.newProxyInstance(
                Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (proxy, method, args) -> {
                    if (method.getName().equals("basicPublish")) {
                        call.publish((String) args[0], (String) args[1], (Boolean) args[2],
                                (AMQP.BasicProperties) args[3], (byte[]) args[4]);
                        confirms.get().handleAck(1L, false);
                        return null;
                    }
                    if (method.getName().equals("confirmSelect")) return null;
                    if (method.getName().equals("getNextPublishSeqNo")) return 1L;
                    if (method.getName().equals("addConfirmListener")) {
                        confirms.set((com.rabbitmq.client.ConfirmListener) args[0]);
                        return null;
                    }
                    if (method.getName().equals("removeConfirmListener")) return null;
                    if (method.getName().equals("isOpen")) return true;
                    if (method.getName().equals("close")) return null;
                    if (method.getName().equals("addReturnListener")) {
                        return null;
                    }
                    return method.getReturnType().isPrimitive() ? false : null;
                });
    }

    @FunctionalInterface
    private interface PublishCall {
        void publish(String exchange, String key, boolean mandatory, AMQP.BasicProperties properties, byte[] body)
                throws Exception;
    }
}
