package messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import utility.MessageLog;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;

public final class RabbitPublisher implements AutoCloseable {
    private final RabbitConnectionManager connectionManager;
    private volatile Channel channel;
    private final String exchange;
    private final MessageCodec codec;
    private volatile PendingPublish pendingPublish;
    private final boolean ownsManager;
    private final Object publishLock = new Object();
    private final java.util.function.Consumer<Channel> recoveryListener;

    public RabbitPublisher(RabbitMQProperties properties) {
        this(new RabbitConnectionManager(properties), RabbitMQConfig.EXCHANGE_NAME, new MessageCodec(), true);
    }

    public RabbitPublisher(RabbitConnectionManager connectionManager) {
        this(connectionManager, RabbitMQConfig.EXCHANGE_NAME, new MessageCodec(), false);
    }

    RabbitPublisher(Channel channel, String exchange, MessageCodec codec) {
        this.connectionManager = null;
        this.channel = channel;
        this.exchange = exchange;
        this.codec = codec;
        this.ownsManager = false;
        this.recoveryListener = null;
        confirmChannel();
        installReturnListener();
        installShutdownListener(channel);
    }

    private RabbitPublisher(RabbitConnectionManager manager, String exchange, MessageCodec codec,
                            boolean ownsManager) {
        manager.open();
        this.connectionManager = manager;
        this.channel = manager.createChannel();
        confirmChannel();
        this.exchange = exchange;
        this.codec = codec;
        this.ownsManager = ownsManager;
        this.recoveryListener = recovered -> {
            synchronized (publishLock) {
                Channel previous = this.channel;
                this.channel = recovered;
                confirmChannel();
                installReturnListener();
                installShutdownListener(recovered);
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
        installShutdownListener(channel);
        manager.addRecoveryListener(recoveryListener);
    }

    public PublishResult publish(Message message) {
        codec.validate(message);
        MessageLog.publish(message.messageId(), null, message.routingKey());
        synchronized (publishLock) {
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2)
                    .contentType("application/json")
                    .contentEncoding("UTF-8")
                    .messageId(message.messageId())
                    .build();
            PendingPublish pending = new PendingPublish(message.messageId());
            pendingPublish = pending;
            com.rabbitmq.client.ConfirmListener confirmListener = new com.rabbitmq.client.ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    if (pending.deliveryTag == deliveryTag || (multiple && pending.deliveryTag <= deliveryTag)) {
                        pending.confirmed.set(true);
                        pending.completed.set(true);
                        pending.confirmationArrived.countDown();
                    }
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    if (pending.deliveryTag == deliveryTag || (multiple && pending.deliveryTag <= deliveryTag)) {
                        pending.completed.set(true);
                        pending.confirmationArrived.countDown();
                    }
                }
            };
            try {
                Channel activeChannel = channel;
                if (activeChannel == null || !activeChannel.isOpen()) {
                    throw new PublishException("Publishing channel is not open");
                }
                pending.channel = activeChannel;
                pending.deliveryTag = activeChannel.getNextPublishSeqNo();
                activeChannel.addConfirmListener(confirmListener);
                activeChannel.basicPublish(exchange, message.routingKey(), true, properties, codec.encode(message));
                pending.confirmationArrived.await();
                PublishException failure = pending.failure.get();
                if (failure != null) {
                    throw failure;
                }
                String returned = pending.returnReason.get();
                if (returned != null) {
                    return PublishResult.returned(returned);
                }
                PublishResult result = pending.confirmed.get()
                        ? PublishResult.confirmed()
                        : PublishResult.failed("Broker did not confirm publish");
                if (result.status() == PublishResult.Status.CONFIRMED) {
                    MessageLog.publishConfirmed(message.messageId(), null);
                } else {
                    MessageLog.publishError(message.messageId(), "publish", result.reason());
                }
                return result;
            } catch (PublishException exception) {
                throw exception;
            } catch (Exception exception) {
                throw new PublishException("Unable to publish message " + message.messageId(), exception);
            } finally {
                try {
                    Channel activeChannel = channel;
                    if (activeChannel != null) {
                        activeChannel.removeConfirmListener(confirmListener);
                    }
                } catch (RuntimeException ignored) {
                    // The channel may have been closed while publishing.
                }
                pendingPublish = null;
            }
        }
    }

    private void confirmChannel() {
        try {
            channel.confirmSelect();
        } catch (Exception exception) {
            throw new PublishException("Unable to enable publisher confirms", exception);
        }
    }

    private void installReturnListener() {
        try {
            channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
                PendingPublish pending = pendingPublish;
                if (pending != null && properties != null
                        && pending.messageId.equals(properties.getMessageId())) {
                    pending.returnReason.compareAndSet(null, replyText);
                }
            });
        } catch (RuntimeException exception) {
            throw new PublishException("Unable to install returned-message listener", exception);
        }
    }

    private void installShutdownListener(Channel publisherChannel) {
        try {
            publisherChannel.addShutdownListener(
                    cause -> failPendingPublish(publisherChannel, cause));
        } catch (RuntimeException exception) {
            throw new PublishException("Unable to install publisher shutdown listener", exception);
        }
    }

    private void failPendingPublish(Channel failingChannel, ShutdownSignalException cause) {
        PendingPublish pending = pendingPublish;
        if (pending != null && pending.channel == failingChannel
                && pending.completed.compareAndSet(false, true)) {
            pending.failure.compareAndSet(null,
                    new PublishException("Publishing channel shut down before confirmation", cause));
            pending.confirmationArrived.countDown();
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
        private final AtomicReference<Boolean> confirmed = new AtomicReference<>(false);
        private final AtomicReference<PublishException> failure = new AtomicReference<>();
        private final java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean();
        private final CountDownLatch confirmationArrived = new CountDownLatch(1);
        private Channel channel;
        private long deliveryTag;

        private PendingPublish(String messageId) {
            this.messageId = messageId;
        }
    }
}
