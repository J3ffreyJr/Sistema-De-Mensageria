package messaging;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryIdempotencyStore implements IdempotencyStore {
    private enum State { STARTED, COMPLETED }
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();

    @Override
    public boolean tryStart(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        return states.putIfAbsent(messageId, State.STARTED) == null;
    }

    @Override
    public void markCompleted(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        states.computeIfPresent(messageId, (ignored, state) -> State.COMPLETED);
    }

    @Override
    public void markFailed(String messageId) {
        if (messageId != null) {
            states.remove(messageId, State.STARTED);
        }
    }

    public boolean isCompleted(String messageId) {
        return states.get(messageId) == State.COMPLETED;
    }
}
