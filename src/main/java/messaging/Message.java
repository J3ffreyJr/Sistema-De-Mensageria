package messaging;

import java.time.Instant;

public record Message(
        int schemaVersion,
        String messageId,
        String content,
        Instant createdAt,
        String producer,
        String routingKey) {
}
