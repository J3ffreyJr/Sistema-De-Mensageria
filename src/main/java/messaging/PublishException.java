package messaging;

public final class PublishException extends RuntimeException {
    public PublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
