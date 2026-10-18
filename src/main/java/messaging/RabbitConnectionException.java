package messaging;

public class RabbitConnectionException extends RuntimeException {
    public RabbitConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RabbitConnectionException(String message) {
        super(message);
    }
}
