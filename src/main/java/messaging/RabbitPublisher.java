package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;

import java.util.concurrent.atomic.AtomicReference;

public final class RabbitPublisher implements AutoCloseable {
    private final RabbitConnectionManager connectionManager;
    private final Channel channel;
    private final String exchange;
    private final MessageCodec codec;
    private final AtomicReference<String> returnedReason = new AtomicReference<>();
    private final boolean ownsManager;

    public RabbitPublisher(RabbitMQProperties properties) {
        this(new RabbitConnectionManager(properties), RabbitMQConfig.EXCHANGE_NAME, new MessageCodec(), true);
    }

    public RabbitPublisher(RabbitConnectionManager connectionManager) {
        this(connectionManager, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec(), true);
    }

    RabbitPublisher(Channel channel, String exchange, MessageCodec codec) {
        this.connectionManager = null;
        this.channel = channel;
        this.exchange = exchange;
        this.codec = codec;
        this.ownsManager = false;
        installReturnListener();
    }

    private RabbitPublisher(RabbitConnectionManager manager, String exchange, MessageCodec codec,
                            boolean ownsManager) {
        manager.open();
        this.connectionManager = manager;
        this.channel = manager.createChannel();
        this.exchange = exchange;
        this.codec = codec;
        this.ownsManager = ownsManager;
        installReturnListener();
    }

    public PublishResult publish(Message message) {
        codec.validate(message);
        returnedReason.set(null);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .contentType("application/json")
                .contentEncoding("UTF-8")
                .messageId(message.messageId())
                .build();
        try {
            channel.basicPublish(exchange, message.routingKey(), true, properties, codec.encode(message));
            boolean confirmed = channel.waitForConfirms();
            String returned = returnedReason.get();
            if (returned != null) {
                return PublishResult.returned(returned);
            }
            return confirmed ? PublishResult.confirmed() : PublishResult.failed("Broker did not confirm publish");
        } catch (Exception exception) {
            throw new PublishException("Unable to publish message " + message.messageId(), exception);
        }
    }

    private void installReturnListener() {
        try {
            channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) ->
                    returnedReason.compareAndSet(null, replyText));
        } catch (RuntimeException exception) {
            throw new PublishException("Unable to install returned-message listener", exception);
        }
    }

    @Override
    public void close() {
        try {
            if (connectionManager != null && ownsManager) {
                connectionManager.close();
            } else if (channel.isOpen()) {
                channel.close();
            }
        } catch (Exception exception) {
            throw new PublishException("Unable to close publisher", exception);
        }
    }
}
