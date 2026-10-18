package messaging;

@FunctionalInterface
public interface MessageProcessor {
    void process(Message message);
}
