package consumer;

import config.RabbitMQConfig;

public final class Consumidor2 {
    public static void main(String[] args) {
        Consumidor1.run("Consumidor2", RabbitMQConfig.QUEUE_CONSUMIDOR2);
    }
}
