package config;

import java.util.Map;

public record RabbitMQProperties(
        String host,
        int port,
        String vhost,
        String username,
        String password,
        boolean tlsEnabled) {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_VHOST = "/";
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";

    public RabbitMQProperties {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("RabbitMQ host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("RabbitMQ port must be between 1 and 65535");
        }
        if (vhost == null || vhost.isBlank()) {
            throw new IllegalArgumentException("RabbitMQ virtual host must not be blank");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("RabbitMQ username must not be blank");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("RabbitMQ password must not be blank");
        }
    }

    public static RabbitMQProperties fromEnvironment(Map<String, String> environment) {
        Map<String, String> values = environment == null ? Map.of() : environment;
        return new RabbitMQProperties(
                value(values, "RABBITMQ_HOST", "rabbitmq.host", DEFAULT_HOST),
                parsePort(value(values, "RABBITMQ_PORT", "rabbitmq.port", String.valueOf(DEFAULT_PORT))),
                value(values, "RABBITMQ_VHOST", "rabbitmq.vhost", DEFAULT_VHOST),
                value(values, "RABBITMQ_USERNAME", "rabbitmq.username", DEFAULT_USERNAME),
                value(values, "RABBITMQ_PASSWORD", "rabbitmq.password", DEFAULT_PASSWORD),
                parseTls(value(values, "RABBITMQ_TLS_ENABLED", "rabbitmq.tlsEnabled", "false")));
    }

    private static String value(
            Map<String, String> environment,
            String environmentName,
            String propertyName,
            String defaultValue) {
        String value = environment.get(environmentName);
        if (value == null) {
            value = System.getProperty(propertyName);
        }
        return value == null ? defaultValue : value.trim();
    }

    private static int parsePort(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("RABBITMQ_PORT must be a valid integer", exception);
        }
    }

    private static boolean parseTls(String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("RABBITMQ_TLS_ENABLED must be true or false");
    }

    @Override
    public String toString() {
        return "RabbitMQProperties[host=%s, port=%d, vhost=%s, username=%s, password=<redacted>, tlsEnabled=%s]"
                .formatted(host, port, vhost, username, tlsEnabled);
    }
}
