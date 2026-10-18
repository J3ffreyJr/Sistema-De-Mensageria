package messaging;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import config.RabbitMQProperties;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RabbitConnectionManagerTest {

    @Test
    void surfacesConnectionConfigurationFailuresAsRabbitConnectionException() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("invalid host");
        RabbitConnectionManager manager = new RabbitConnectionManager(
                new RabbitMQProperties("invalid host", 5672, "/", "user", "password", false),
                factory, new RabbitTopology(), 1, Duration.ZERO);

        assertThrows(RabbitConnectionException.class, manager::open);
    }

    @Test
    void closeClosesAllChannelsAndConnection() throws Exception {
        AtomicInteger channelCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        ConnectionFactory factory = factoryReturning(channelCloses, connectionCloses);
        RabbitConnectionManager manager = new RabbitConnectionManager(
                new RabbitMQProperties("localhost", 5672, "/", "user", "password", false),
                factory, new RabbitTopology(), 1, Duration.ZERO);

        manager.open();
        manager.createChannel();
        manager.close();

        assertEquals(2, channelCloses.get());
        assertEquals(1, connectionCloses.get());
        assertFalse(manager.isConnected());
    }

    @Test
    void boundedRecoveryRecreatesAUsableChannelPath() {
        RabbitConnectionManager manager = new RabbitConnectionManager(
                new RabbitMQProperties("localhost", 5672, "/", "user", "password", false),
                factoryReturning(new AtomicInteger(), new AtomicInteger()),
                new RabbitTopology(), 2, Duration.ZERO);

        manager.open();
        manager.recoverNowForTest();

        assertTrue(manager.isConnected());
        assertDoesNotThrow(() -> manager.createChannel().close());
        manager.close();
    }

    @Test
    void notifiesPublishersWithAChannelCreatedAfterRecovery() {
        RabbitConnectionManager manager = new RabbitConnectionManager(
                new RabbitMQProperties("localhost", 5672, "/", "user", "password", false),
                factoryReturning(new AtomicInteger(), new AtomicInteger()),
                new RabbitTopology(), 2, Duration.ZERO);
        AtomicReference<Channel> recovered = new AtomicReference<>();

        manager.open();
        manager.addRecoveryListener(recovered::set);
        manager.recoverNowForTest();

        assertNotNull(recovered.get());
        assertDoesNotThrow(() -> recovered.get().close());
        manager.close();
    }

    @Test
    void givesEachRecoveryListenerAnIndependentChannel() throws Exception {
        RabbitConnectionManager manager = new RabbitConnectionManager(
                new RabbitMQProperties("localhost", 5672, "/", "user", "password", false),
                factoryReturning(new AtomicInteger(), new AtomicInteger()),
                new RabbitTopology(), 2, Duration.ZERO);
        AtomicReference<Channel> first = new AtomicReference<>();
        AtomicReference<Channel> second = new AtomicReference<>();

        manager.open();
        manager.addRecoveryListener(first::set);
        manager.addRecoveryListener(second::set);
        manager.recoverNowForTest();

        assertNotNull(first.get());
        assertNotNull(second.get());
        assertNotSame(first.get(), second.get());
        first.get().close();
        assertFalse(first.get().isOpen());
        assertTrue(second.get().isOpen());
        second.get().close();
        manager.close();
    }

    private static ConnectionFactory factoryReturning(AtomicInteger channelCloses, AtomicInteger connectionCloses) {
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public com.rabbitmq.client.Connection newConnection() {
                return (com.rabbitmq.client.Connection) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[]{com.rabbitmq.client.Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("hashCode")) return System.identityHashCode(proxy);
                            if (method.getName().equals("equals")) return proxy == args[0];
                            if (method.getName().equals("toString")) return "connection";
                            if (method.getName().equals("createChannel")) {
                                return recordingChannel(channelCloses);
                            }
                            if (method.getName().equals("isOpen")) return true;
                            if (method.getName().equals("close")) {
                                connectionCloses.incrementAndGet();
                            }
                            return method.getReturnType().isPrimitive() ? false : null;
                        });
            }
        };
        return factory;
    }

    private static Channel recordingChannel(AtomicInteger closes) {
        AtomicBoolean open = new AtomicBoolean(true);
        return (Channel) Proxy.newProxyInstance(
                Channel.class.getClassLoader(),
                new Class<?>[]{Channel.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("hashCode")) return System.identityHashCode(proxy);
                    if (method.getName().equals("equals")) return proxy == args[0];
                    if (method.getName().equals("toString")) return "channel";
                    if (method.getName().equals("close")) {
                        if (open.compareAndSet(true, false)) closes.incrementAndGet();
                        return null;
                    }
                    if (method.getName().equals("isOpen")) return open.get();
                    return method.getReturnType().isPrimitive() ? false : null;
                });
    }
}
