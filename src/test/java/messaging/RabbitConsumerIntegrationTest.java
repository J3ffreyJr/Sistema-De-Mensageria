package messaging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RabbitConsumerIntegrationTest {
    @Test
    void brokerScenariosAreOptIn() {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"));
    }
}
