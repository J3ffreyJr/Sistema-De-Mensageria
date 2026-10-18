package messaging;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class IdempotentMessageProcessorTest {
    private final Message message = new Message(1, "same-id", "payload", Instant.now(), "test", "route");

    @Test
    void processesCompletedMessageOnlyOnce() {
        AtomicInteger calls = new AtomicInteger();
        MessageProcessor processor = new IdempotentMessageProcessor(new InMemoryIdempotencyStore(),
                ignored -> calls.incrementAndGet());

        processor.process(message);
        processor.process(message);

        assertEquals(1, calls.get());
    }

    @Test
    void failedAttemptRemainsRetryable() {
        AtomicInteger calls = new AtomicInteger();
        MessageProcessor processor = new IdempotentMessageProcessor(new InMemoryIdempotencyStore(),
                ignored -> { if (calls.incrementAndGet() == 1) throw new IllegalStateException("temporary"); });

        assertThrows(IllegalStateException.class, () -> processor.process(message));
        processor.process(message);

        assertEquals(2, calls.get());
    }
}
