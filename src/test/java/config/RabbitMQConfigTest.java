package config;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void usesLocalDefaultsWhenNoEnvironmentValuesAreProvided() {
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(Map.of());

        assertEquals("localhost", properties.host());
        assertEquals(5672, properties.port());
        assertEquals("/", properties.vhost());
        assertEquals("guest", properties.username());
        assertEquals("guest", properties.password());
        assertFalse(properties.tlsEnabled());
    }

    @Test
    void appliesEnvironmentOverrides() {
        Map<String, String> environment = Map.of(
                "RABBITMQ_HOST", "broker.internal",
                "RABBITMQ_PORT", "5673",
                "RABBITMQ_VHOST", "/production",
                "RABBITMQ_USERNAME", "app",
                "RABBITMQ_PASSWORD", "secret",
                "RABBITMQ_TLS_ENABLED", "true");

        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(environment);

        assertEquals("broker.internal", properties.host());
        assertEquals(5673, properties.port());
        assertEquals("/production", properties.vhost());
        assertEquals("app", properties.username());
        assertEquals("secret", properties.password());
        assertTrue(properties.tlsEnabled());
    }

    @Test
    void rejectsInvalidPortValues() {
        Map<String, String> environment = new HashMap<>();
        environment.put("RABBITMQ_PORT", "not-a-port");

        assertThrows(IllegalArgumentException.class,
                () -> RabbitMQProperties.fromEnvironment(environment));
    }

    @Test
    void rejectsMissingPassword() {
        Map<String, String> environment = new HashMap<>();
        environment.put("RABBITMQ_PASSWORD", " ");

        assertThrows(IllegalArgumentException.class,
                () -> RabbitMQProperties.fromEnvironment(environment));
    }

    @Test
    void parsesFalseTlsFlag() {
        Map<String, String> environment = Map.of("RABBITMQ_TLS_ENABLED", "false");

        assertFalse(RabbitMQProperties.fromEnvironment(environment).tlsEnabled());
    }

    @Test
    void doesNotExposePasswordInToString() {
        Map<String, String> environment = Map.of("RABBITMQ_PASSWORD", "super-secret");

        assertFalse(RabbitMQProperties.fromEnvironment(environment).toString().contains("super-secret"));
    }
}