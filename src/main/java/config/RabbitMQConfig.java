package config;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQConfig {

    public static final String EXCHANGE_NAME = "exchange_direto";
    public static final String QUEUE_CONSUMIDOR1 = "fila_consumidor1";
    public static final String QUEUE_CONSUMIDOR2 = "fila_consumidor2";
    public static final String ROUTING_KEY_1 = "consumidor1";
    public static final String ROUTING_KEY_2 = "consumidor2";
    public static final String ROUTING_KEY_ALL = "ambos";
    
    public static ConnectionFactory getConnectionFactory() {
        return getConnectionFactory(RabbitMQProperties.fromEnvironment(System.getenv()));
    }

    public static ConnectionFactory getConnectionFactory(RabbitMQProperties properties) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(properties.host());
        factory.setPort(properties.port());
        factory.setVirtualHost(properties.vhost());
        factory.setUsername(properties.username());
        factory.setPassword(properties.password());
        if (properties.tlsEnabled()) {
            try {
                factory.useSslProtocol();
            } catch (Exception exception) {
                throw new IllegalStateException("Unable to enable RabbitMQ TLS", exception);
            }
        }
        return factory;
    }
}