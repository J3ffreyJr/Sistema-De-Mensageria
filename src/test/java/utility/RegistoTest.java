package utility;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistoTest {
    private static final Path LOG_FILE = Path.of("logs", "mensageria.log");

    @Test
    void structuredEventsPreserveIdsAndDoNotWritePayloads() throws Exception {
        MessageLog.publish("msg-1", "corr-1", "messages.1");
        MessageLog.receive("msg-1", "corr-1", "consumer-1");
        MessageLog.publishError("msg-1", "publish", "connection failed");

        String content = Files.readString(LOG_FILE);
        assertTrue(content.contains("event=publish"));
        assertTrue(content.contains("messageId=msg-1"));
        assertTrue(content.contains("correlationId=corr-1"));
        assertTrue(content.contains("level=ERROR"));
    }

    @Test
    void compatibilityFacadeDoesNotTruncateExistingLog() throws Exception {
        Files.createDirectories(LOG_FILE.getParent());
        Files.writeString(LOG_FILE, "existing-entry\n");

        Registo.iniciarNovoLog();

        String content = Files.readString(LOG_FILE);
        assertTrue(content.startsWith("existing-entry\n"));
        assertTrue(content.contains("event=connection"));
    }

    @Test
    void compatibilityReceiveLogsStatusInsteadOfConsumer() throws Exception {
        Registo.logRecebimento("status-msg-1", "sensitive payload", "SUCESSO");

        String content = Files.readString(LOG_FILE);
        assertTrue(content.contains("event=receive messageId=status-msg-1 status=SUCESSO"));
        assertFalse(content.contains("messageId=status-msg-1 consumer=SUCESSO"));
        assertFalse(content.contains("sensitive payload"));
    }
}
