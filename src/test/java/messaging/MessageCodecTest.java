package messaging;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageCodecTest {

    private final MessageCodec codec = new MessageCodec();

    @Test
    void encodesAndDecodesAValidVersionOneMessage() {
        Message original = new Message(
                1, "msg-1", "Olá, mundo!", Instant.parse("2026-10-18T15:20:00Z"),
                "producer-cli", "consumidor1");

        Message decoded = codec.decode(codec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void encodesJsonAsUtf8AndPreservesPortugueseContent() {
        Message message = new Message(
                1, "msg-utf8", "ação — informação", Instant.parse("2026-10-18T15:20:00Z"),
                "produtor", "consumidor1");

        byte[] payload = codec.encode(message);
        String json = new String(payload, StandardCharsets.UTF_8);

        assertEquals(message, codec.decode(payload));
        assertTrue(json.contains("ação"));
    }

    @Test
    void rejectsUnsupportedSchemaVersion() {
        Message message = new Message(
                2, "msg-2", "content", Instant.now(), "producer", "routing");

        MessageValidationException exception = assertThrows(
                MessageValidationException.class, () -> codec.validate(message));

        assertEquals("Unsupported schemaVersion: 2", exception.getMessage());
    }

    @Test
    void rejectsMissingRequiredFields() {
        Message message = new Message(1, " ", null, null, "producer", "routing");

        MessageValidationException exception = assertThrows(
                MessageValidationException.class, () -> codec.validate(message));

        assertEquals("messageId must not be blank", exception.getMessage());
    }

    @Test
    void rejectsMissingContent() {
        Message message = new Message(1, "id", null, Instant.now(), "producer", "routing");

        MessageValidationException exception = assertThrows(
                MessageValidationException.class, () -> codec.validate(message));

        assertEquals("content must not be blank", exception.getMessage());
    }

    @Test
    void rejectsMissingCreatedAt() {
        Message message = new Message(1, "id", "content", null, "producer", "routing");

        MessageValidationException exception = assertThrows(
                MessageValidationException.class, () -> codec.validate(message));

        assertEquals("createdAt must not be null", exception.getMessage());
    }

    @Test
    void rejectsMalformedJsonWithCause() {
        MessageValidationException exception = assertThrows(
                MessageValidationException.class,
                () -> codec.decode("{\"schemaVersion\":1,".getBytes(StandardCharsets.UTF_8)));

        assertEquals("Invalid message payload", exception.getMessage());
        assertInstanceOf(Exception.class, exception.getCause());
    }

    @Test
    void rejectsJsonWithMissingRequiredValues() {
        byte[] payload = "{\"schemaVersion\":1,\"messageId\":\"id\"}"
                .getBytes(StandardCharsets.UTF_8);

        assertThrows(MessageValidationException.class, () -> codec.decode(payload));
    }

    @Test
    void serializesInstantAsIso8601() {
        Message message = new Message(
                1, "msg-time", "content", Instant.parse("2026-10-18T15:20:00Z"),
                "producer", "routing");

        String json = new String(codec.encode(message), StandardCharsets.UTF_8);

        assertTrue(json.contains("\"createdAt\":\"2026-10-18T15:20:00Z\""));
    }
}
