package messaging;

public record PublishResult(Status status, String reason) {
    public enum Status {
        CONFIRMED, RETURNED, FAILED
    }

    public static PublishResult confirmed() {
        return new PublishResult(Status.CONFIRMED, null);
    }

    public static PublishResult returned(String reason) {
        return new PublishResult(Status.RETURNED, reason);
    }

    public static PublishResult failed(String reason) {
        return new PublishResult(Status.FAILED, reason);
    }
}
