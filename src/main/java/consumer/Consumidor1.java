package consumer;

import config.RabbitMQConfig;
import config.RabbitMQProperties;
import messaging.*;
import utility.Registo;

public final class Consumidor1 {
    public static void main(String[] args) {
        run("Consumidor1", RabbitMQConfig.QUEUE_CONSUMIDOR1);
    }

    static void run(String name, String queue) {
        Registo.logInfo("=== SISTEMA " + name + " INICIADO ===");
        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties);
             RabbitConsumer consumer = new RabbitConsumer(manager, queue, message -> process(name, message))) {
            consumer.start();
            Registo.logInfo(name + " conectado ao RabbitMQ; aguardando mensagens em " + queue);
            System.out.println("\n🟢 " + name.toUpperCase() + " ATIVO - Pressione Ctrl+C para parar\n");
            while (!Thread.currentThread().isInterrupted()) Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception failure) {
            Registo.logErro("N/A", "Conexão " + name, failure.getMessage());
        }
    }

    private static void process(String name, Message message) {
        try {
            Thread.sleep(1000);
            Registo.logRecebimento(message.messageId(),
                    "Conteúdo: \"" + message.content() + "\" | RoutingKey: "
                            + message.routingKey() + " | Por: " + name, "SUCESSO");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Processamento interrompido", interrupted);
        }
    }
}
