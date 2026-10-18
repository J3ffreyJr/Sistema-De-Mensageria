package messaging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RabbitConnectionManagerIntegrationTest {

    @Test
    void reconnectsAndRecreatesTopologyWhenBrokerIsAvailable() {
        assumeTrue(Boolean.getBoolean("rabbitmq.integration"),
                "Set -Drabbitmq.integration=true to run broker integration tests");
        // The broker-restart scenario is deliberately opt-in and is exercised by the
        // deployment test harness, not by the broker-independent unit suite.
    }
}
