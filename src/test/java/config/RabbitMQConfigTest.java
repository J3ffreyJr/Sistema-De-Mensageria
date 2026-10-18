package config;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RabbitMQConfigTest {

    @Test
    void createsFactoryWithConfiguredConnectionSettings() {
        ConnectionFactory factory = RabbitMQConfig.getConnectionFactory();

        assertEquals("localhost", factory.getHost());
        assertEquals(5672, factory.getPort());
        assertEquals("guest", factory.getUsername());
        assertEquals("guest", factory.getPassword());
    }

    @Test
    void exposesExpectedRoutingConfiguration() {
        assertEquals("exchange_direto", RabbitMQConfig.EXCHANGE_NAME);
        assertEquals("fila_consumidor1", RabbitMQConfig.QUEUE_CONSUMIDOR1);
        assertEquals("fila_consumidor2", RabbitMQConfig.QUEUE_CONSUMIDOR2);
        assertEquals("consumidor1", RabbitMQConfig.ROUTING_KEY_1);
        assertEquals("consumidor2", RabbitMQConfig.ROUTING_KEY_2);
        assertEquals("ambos", RabbitMQConfig.ROUTING_KEY_ALL);
    }
}