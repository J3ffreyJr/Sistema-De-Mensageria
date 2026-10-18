package utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Structured, payload-safe application events. Message content is deliberately
 * never accepted by this API.
 */
public final class MessageLog {
    private static final Logger LOGGER = LoggerFactory.getLogger("mensageria.events");

    private MessageLog() { }

    public static void publish(String messageId, String correlationId, String routingKey) {
        event("publish", messageId, correlationId, "routingKey", routingKey);
    }

    public static void publishConfirmed(String messageId, String correlationId) {
        event("publish_confirm", messageId, correlationId, null, null);
    }

    public static void publishError(String messageId, String operation, String error) {
        LOGGER.error("level=ERROR " + format("publish_error", messageId, null, "operation", operation, "error", error));
    }

    public static void receive(String messageId, String correlationId, String consumer) {
        event("receive", messageId, correlationId, "consumer", consumer);
    }

    public static void retry(String messageId, String correlationId, int attempt, String reason) {
        event("retry", messageId, correlationId, "attempt", Integer.toString(attempt), "reason", reason);
    }

    public static void ack(String messageId, String correlationId) {
        event("ack", messageId, correlationId, null, null);
    }

    public static void nack(String messageId, String correlationId, boolean requeue) {
        event("nack", messageId, correlationId, "requeue", Boolean.toString(requeue));
    }

    public static void deadLetter(String messageId, String correlationId, String reason) {
        event("dead_letter", messageId, correlationId, "reason", reason);
    }

    public static void connection(String state, String details) {
        LOGGER.info(format("connection", null, null, "state", state, "details", details));
    }

    public static void shutdown(String component) {
        LOGGER.info(format("shutdown", null, null, "component", component));
    }

    public static void info(String message) {
        LOGGER.info(format("info", null, null, "message", message));
    }

    private static void event(String name, String messageId, String correlationId, String... attributes) {
        LOGGER.info(format(name, messageId, correlationId, attributes));
    }

    private static String format(String event, String messageId, String correlationId,
                                 String... attributes) {
        StringBuilder result = new StringBuilder("event=").append(safe(event));
        if (messageId != null) result.append(" messageId=").append(safe(messageId));
        if (correlationId != null) result.append(" correlationId=").append(safe(correlationId));
        for (int i = 0; i + 1 < attributes.length; i += 2) {
            if (attributes[i] != null) {
                result.append(' ').append(safe(attributes[i])).append('=').append(safe(attributes[i + 1]));
            }
        }
        return result.toString();
    }

    private static String safe(String value) {
        return value == null ? "null" : value.replace('\n', ' ').replace('\r', ' ');
    }
}
