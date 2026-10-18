package messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import config.RabbitMQConfig;
import config.RabbitMQProperties;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RabbitConnectionManager implements AutoCloseable {
    private final RabbitMQProperties properties;
    private final ConnectionFactory factory;
    private final RabbitTopology topology;
    private final int maxAttempts;
    private final Duration backoff;
    private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean recoveryRunning = new AtomicBoolean();
    private volatile Connection connection;
    private volatile boolean closing;

    public RabbitConnectionManager(RabbitMQProperties properties) {
        this(properties, RabbitMQConfig.getConnectionFactory(properties), new RabbitTopology(), 3, Duration.ofMillis(250));
    }

    public RabbitConnectionManager(RabbitMQProperties properties, ConnectionFactory factory,
                                   RabbitTopology topology, int maxAttempts, Duration backoff) {
        if (maxAttempts < 1 || backoff.isNegative()) {
            throw new IllegalArgumentException("Recovery attempts and backoff must be bounded and non-negative");
        }
        this.properties = properties;
        this.factory = factory;
        this.topology = topology;
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
    }

    public synchronized void open() {
        if (isConnected()) return;
        closing = false;
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                installConnection(factory.newConnection());
                return;
            } catch (Exception exception) {
                last = exception;
                closeQuietly();
                if (attempt < maxAttempts) sleepBackoff();
            }
        }
        throw new RabbitConnectionException("Unable to connect to RabbitMQ at "
                + properties.host() + ":" + properties.port(), last);
    }

    public synchronized Channel createChannel() {
        Connection active = connection;
        if (closing || active == null || !active.isOpen()) {
            throw new RabbitConnectionException("RabbitMQ connection is not open");
        }
        try {
            Channel channel = active.createChannel();
            channels.add(channel);
            return channel;
        } catch (IOException | RuntimeException exception) {
            throw new RabbitConnectionException("Unable to create RabbitMQ channel", exception);
        }
    }

    public boolean isConnected() {
        Connection active = connection;
        return !closing && active != null && active.isOpen();
    }

    private synchronized void installConnection(Connection newConnection) throws IOException {
        newConnection.addShutdownListener(cause -> {
            if (!closing && recoveryRunning.compareAndSet(false, true)) {
                Thread recovery = new Thread(() -> {
                    try {
                        recover();
                    } finally {
                        recoveryRunning.set(false);
                    }
                }, "rabbitmq-recovery");
                recovery.setDaemon(true);
                recovery.start();
            }
        });
        connection = newConnection;
        topology.declare(createChannel());
    }

    private synchronized void recover() {
        if (closing || isConnected()) {
            return;
        }
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts && !closing; attempt++) {
            try {
                closeQuietly();
                installConnection(factory.newConnection());
                return;
            } catch (Exception exception) {
                last = exception;
                closeQuietly();
                if (attempt < maxAttempts) {
                    sleepBackoff();
                }
            }
        }
        if (!closing) {
            throw new RabbitConnectionException("Unable to recover RabbitMQ connection", last);
        }
    }

    /** Deterministic hook used by the opt-in integration test to exercise recovery. */
    void recoverNowForTest() {
        if (!recoveryRunning.compareAndSet(false, true)) {
            throw new RabbitConnectionException("RabbitMQ recovery is already in progress");
        }
        try {
            synchronized (this) {
                Connection active = connection;
                if (active != null) {
                    try {
                        active.close();
                    } catch (Exception ignored) {
                        // Recovery below still verifies that a new connection can be established.
                    }
                    connection = null;
                }
            }
            recover();
        } finally {
            recoveryRunning.set(false);
        }
    }

    @Override
    public synchronized void close() {
        closing = true;
        closeQuietly();
    }

    private void closeQuietly() {
        channels.forEach(channel -> {
            try { if (channel.isOpen()) channel.close(); } catch (Exception ignored) { }
        });
        channels.clear();
        Connection active = connection;
        connection = null;
        if (active != null) {
            try { if (active.isOpen()) active.close(); } catch (Exception ignored) { }
        }
    }

    private void sleepBackoff() {
        try {
            Thread.sleep(backoff.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RabbitConnectionException("Interrupted while recovering RabbitMQ connection", interrupted);
        }
    }
}
