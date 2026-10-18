package messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class MessageCodec {

    private static final int SUPPORTED_SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public MessageCodec() {
        objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .build();
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public byte[] encode(Message message) {
        validate(message);
        try {
            return objectMapper.writeValueAsString(message).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException exception) {
            throw new MessageValidationException("Unable to encode message", exception);
        }
    }

    public Message decode(byte[] payload) {
        if (payload == null) {
            throw new MessageValidationException("Message payload must not be null");
        }

        try {
            Message message = objectMapper.readValue(payload, Message.class);
            validate(message);
            return message;
        } catch (MessageValidationException exception) {
            throw exception;
        } catch (IOException | RuntimeException exception) {
            throw new MessageValidationException("Invalid message payload", exception);
        }
    }

    public void validate(Message message) {
        if (message == null) {
            throw new MessageValidationException("Message must not be null");
        }
        if (message.schemaVersion() != SUPPORTED_SCHEMA_VERSION) {
            throw new MessageValidationException(
                    "Unsupported schemaVersion: " + message.schemaVersion());
        }
        requireNonBlank(message.messageId(), "messageId");
        requireNonBlank(message.content(), "content");
        if (message.createdAt() == null) {
            throw new MessageValidationException("createdAt must not be null");
        }
        requireNonBlank(message.producer(), "producer");
        requireNonBlank(message.routingKey(), "routingKey");
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new MessageValidationException(fieldName + " must not be blank");
        }
    }
}
