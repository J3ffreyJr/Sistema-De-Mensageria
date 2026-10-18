package messaging;

public interface IdempotencyStore {
    boolean tryStart(String messageId);
    void markCompleted(String messageId);

    default void markFailed(String messageId) {
        // Stores that track in-flight work may release it so delivery can retry.
    }
}
