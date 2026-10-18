package config;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMQConfigTest {

    private static final String[] PROPERTY_NAMES = {
            "rabbitmq.host", "rabbitmq.port", "rabbitmq.vhost",
            "rabbitmq.username", "rabbitmq.password", "rabbitmq.tlsEnabled"
    };
    private Map<String, String> originalProperties;

    @BeforeEach
    void captureSystemProperties() {
        originalProperties = new HashMap<>();
        for (String name : PROPERTY_NAMES) {
            originalProperties.put(name, System.getProperty(name));
            System.clearProperty(name);
        }
    }

    @AfterEach
    void restoreSystemProperties() {
        for (String name : PROPERTY_NAMES) {
            String original = originalProperties.get(name);
            if (original == null) {
                System.clearProperty(name);
            } else {
                System.setProperty(name, original);
            }
        }
    }

    @Test
    void createsFactoryWithConfiguredConnectionSettings() {
        RabbitMQProperties properties = new RabbitMQProperties(
                "localhost", 5672, "/", "development-user", "development-password", false);
        ConnectionFactory factory = RabbitMQConfig.getConnectionFactory(properties);

        assertEquals("localhost", factory.getHost());
        assertEquals(5672, factory.getPort());
        assertEquals("development-user", factory.getUsername());
        assertEquals("development-password", factory.getPassword());
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
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(Map.of(
                "RABBITMQ_USERNAME", "development-user",
                "RABBITMQ_PASSWORD", "development-password"));

        assertEquals("localhost", properties.host());
        assertEquals(5672, properties.port());
        assertEquals("/", properties.vhost());
        assertEquals("development-user", properties.username());
        assertEquals("development-password", properties.password());
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
    void appliesJavaPropertyOverrides() {
        System.setProperty("rabbitmq.host", "property-broker");
        System.setProperty("rabbitmq.port", "5674");
        System.setProperty("rabbitmq.vhost", "/property");
        System.setProperty("rabbitmq.username", "property-user");
        System.setProperty("rabbitmq.password", "property-password");
        System.setProperty("rabbitmq.tlsEnabled", "true");

        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(Map.of());

        assertEquals("property-broker", properties.host());
        assertEquals(5674, properties.port());
        assertEquals("/property", properties.vhost());
        assertEquals("property-user", properties.username());
        assertEquals("property-password", properties.password());
        assertTrue(properties.tlsEnabled());
    }

    @Test
    void environmentOverridesJavaProperties() {
        System.setProperty("rabbitmq.host", "property-broker");
        System.setProperty("rabbitmq.username", "property-user");
        System.setProperty("rabbitmq.password", "property-password");

        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(Map.of(
                "RABBITMQ_HOST", "environment-broker",
                "RABBITMQ_USERNAME", "environment-user",
                "RABBITMQ_PASSWORD", "environment-password"));

        assertEquals("environment-broker", properties.host());
        assertEquals("environment-user", properties.username());
        assertEquals("environment-password", properties.password());
    }

    @Test
    void rejectsMissingCredentials() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RabbitMQProperties.fromEnvironment(Map.of()));

        assertTrue(exception.getMessage().contains("RABBITMQ_USERNAME"));
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
        environment.put("RABBITMQ_USERNAME", "development-user");
        environment.put("RABBITMQ_PASSWORD", " ");

        assertThrows(IllegalArgumentException.class,
                () -> RabbitMQProperties.fromEnvironment(environment));
    }

    @Test
    void parsesFalseTlsFlag() {
        Map<String, String> environment = Map.of(
                "RABBITMQ_USERNAME", "development-user",
                "RABBITMQ_PASSWORD", "development-password",
                "RABBITMQ_TLS_ENABLED", "false");

        assertFalse(RabbitMQProperties.fromEnvironment(environment).tlsEnabled());
    }

    @Test
    void doesNotExposePasswordInToString() {
        Map<String, String> environment = Map.of(
                "RABBITMQ_USERNAME", "development-user",
                "RABBITMQ_PASSWORD", "super-secret");

        assertFalse(RabbitMQProperties.fromEnvironment(environment).toString().contains("super-secret"));
    }
}