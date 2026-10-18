package messaging;

import com.rabbitmq.client.Channel;
import config.RabbitMQConfig;
import config.RabbitMQProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RabbitConnectionManagerIntegrationTest {

    @Test
    void declaresIdempotentlyAndRecreatesChannelAndTopologyAfterConnectionLoss() throws Exception {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"),
                "Set -Drabbitmq.integration=true to run broker integration tests");

        RabbitMQProperties properties = RabbitMQProperties.fromEnvironment(System.getenv());
        try (RabbitConnectionManager manager = new RabbitConnectionManager(properties)) {
            manager.open();
            assertTrue(manager.isConnected());
            assertDoesNotThrow(() -> new RabbitTopology().declare(manager.createChannel()));
            manager.recoverNowForTest();
            assertTrue(manager.isConnected());
            try (Channel recreated = manager.createChannel()) {
                assertTrue(recreated.isOpen());
                assertDoesNotThrow(() -> new RabbitTopology().declare(recreated));
                assertDoesNotThrow(() -> recreated.queueDeclarePassive(RabbitMQConfig.QUEUE_CONSUMIDOR1));
                assertDoesNotThrow(() -> recreated.queueDeclarePassive(RabbitTopology.DEAD_LETTER_QUEUE_1));
            }
        }
    }
}
