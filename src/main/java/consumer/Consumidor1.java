package consumer;

import config.RabbitMQConfig;
import config.RabbitMQProperties;
import messaging.*;
import utility.MessageLog;

public final class Consumidor1 {
    public static void main(String[] args) {
        run("Consumidor1", RabbitMQConfig.QUEUE_CONSUMIDOR1);
    }

    static void run(String name, String queue) {
        MessageLog.info(name + "_started");
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        RabbitConnectionManager manager = new RabbitConnectionManager(properties);
        RabbitConsumer consumer = new RabbitConsumer(manager, queue, message -> process(name, message));
        Thread shutdownHook = new Thread(() -> {
            MessageLog.shutdown(name);
            consumer.stop();
            manager.close();
        }, name.toLowerCase() + "-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            consumer.start();
            MessageLog.connection("connected", name + " queue=" + queue);
            System.out.println("\n🟢 " + name.toUpperCase() + " ATIVO - Pressione Ctrl+C para parar\n");
            while (!Thread.currentThread().isInterrupted()) Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception failure) {
            MessageLog.publishError("N/A", "connection-" + name, failure.getMessage());
        } finally {
            consumer.stop();
            manager.close();
        }
    }

    private static void process(String name, Message message) {
        try {
            Thread.sleep(1000);
            MessageLog.receive(message.messageId(), null, name);
            MessageLog.ack(message.messageId(), null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Processamento interrompido", interrupted);
        }
    }
}
