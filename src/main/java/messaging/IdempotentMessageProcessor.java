package messaging;

import java.util.Objects;

/** Decorates business processing with an atomic, completion-only idempotency boundary. */
public final class IdempotentMessageProcessor implements MessageProcessor {
    private final IdempotencyStore store;
    private final MessageProcessor delegate;

    public IdempotentMessageProcessor(IdempotencyStore store, MessageProcessor delegate) {
        this.store = Objects.requireNonNull(store, "store");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void process(Message message) {
        Objects.requireNonNull(message, "message");
        if (!store.tryStart(message.messageId())) return;
        try {
            delegate.process(message);
            store.markCompleted(message.messageId());
        } catch (RuntimeException | Error failure) {
            store.markFailed(message.messageId());
            throw failure;
        }
    }
}
