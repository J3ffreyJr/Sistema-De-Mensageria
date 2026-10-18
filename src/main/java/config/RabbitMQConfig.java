package config;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQConfig {
    
    private static final String HOST = "localhost";
    private static final int PORT = 5672;
    private static final String USERNAME = "guest";
    private static final String PASSWORD = "guest";
    
    public static final String EXCHANGE_NAME = "exchange_direto";
    public static final String QUEUE_CONSUMIDOR1 = "fila_consumidor1";
    public static final String QUEUE_CONSUMIDOR2 = "fila_consumidor2";
    public static final String ROUTING_KEY_1 = "consumidor1";
    public static final String ROUTING_KEY_2 = "consumidor2";
    public static final String ROUTING_KEY_ALL = "ambos";
    
    public static ConnectionFactory getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        return factory;
    }
}