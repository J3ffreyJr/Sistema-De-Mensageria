package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class RabbitPublisher implements AutoCloseable {
    private final RabbitConnectionManager connectionManager;
    private volatile Channel channel;
    private final String exchange;
    private final MessageCodec codec;
    private volatile PendingPublish pendingPublish;
    private final boolean ownsManager;
    private final Object publishLock = new Object();
    private final java.util.function.Consumer<Channel> recoveryListener;
    private static final long RETURN_SETTLE_MILLIS = 250;

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
        this.recoveryListener = null;
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
        this.recoveryListener = recovered -> {
            synchronized (publishLock) {
                Channel previous = this.channel;
                this.channel = recovered;
                installReturnListener();
                if (previous != null && previous.isOpen()) {
                    try {
                        previous.close();
                    } catch (Exception ignored) {
                        // The manager already closed the old channel during recovery.
                    }
                }
            }
        };
        installReturnListener();
        manager.addRecoveryListener(recoveryListener);
    }

    public PublishResult publish(Message message) {
        codec.validate(message);
        synchronized (publishLock) {
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2)
                    .contentType("application/json")
                    .contentEncoding("UTF-8")
                    .messageId(message.messageId())
                    .build();
            PendingPublish pending = new PendingPublish(message.messageId());
            pendingPublish = pending;
            try {
                Channel activeChannel = channel;
                if (activeChannel == null || !activeChannel.isOpen()) {
                    throw new PublishException("Publishing channel is not open");
                }
                activeChannel.basicPublish(exchange, message.routingKey(), true, properties, codec.encode(message));
                boolean confirmed = activeChannel.waitForConfirms();
                // A return is delivered on the connection callback executor and
                // can race with waitForConfirms. Keep the pending publication
                // alive until that callback has had a bounded chance to arrive.
                pending.returnArrived.await(RETURN_SETTLE_MILLIS, TimeUnit.MILLISECONDS);
                String returned = pending.returnReason.get();
                if (returned != null) {
                    return PublishResult.returned(returned);
                }
                return confirmed ? PublishResult.confirmed() : PublishResult.failed("Broker did not confirm publish");
            } catch (PublishException exception) {
                throw exception;
            } catch (Exception exception) {
                throw new PublishException("Unable to publish message " + message.messageId(), exception);
            } finally {
                pendingPublish = null;
            }
        }
    }

    private void installReturnListener() {
        try {
            channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
                PendingPublish pending = pendingPublish;
                if (pending != null && properties != null
                        && pending.messageId.equals(properties.getMessageId())) {
                    pending.returnReason.compareAndSet(null, replyText);
                    pending.returnArrived.countDown();
                }
            });
        } catch (RuntimeException exception) {
            throw new PublishException("Unable to install returned-message listener", exception);
        }
    }

    @Override
    public void close() {
        try {
            if (connectionManager != null && recoveryListener != null) {
                connectionManager.removeRecoveryListener(recoveryListener);
            }
            if (connectionManager != null && ownsManager) {
                connectionManager.close();
            } else if (channel.isOpen()) {
                channel.close();
            }
        } catch (Exception exception) {
            throw new PublishException("Unable to close publisher", exception);
        }
    }

    private static final class PendingPublish {
        private final String messageId;
        private final AtomicReference<String> returnReason = new AtomicReference<>();
        private final CountDownLatch returnArrived = new CountDownLatch(1);

        private PendingPublish(String messageId) {
            this.messageId = messageId;
        }
    }
}
