package ma.emsi.tp04.kafka.messaging;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Smoke test optionnel : vérifie uniquement la joignabilité TCP de Kafka.
 * Exclu par défaut (surefire excludedGroups=kafka-broker).
 * Activer : mvn test -Pwith-kafka
 * Ne démarre pas Docker et ne publie aucun message.
 */
@Tag("kafka-broker")
class KafkaBrokerSmokeTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "RUN_KAFKA_SMOKE", matches = "true")
    void brokerPortIsReachable() throws Exception {
        String host = System.getenv().getOrDefault("KAFKA_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("KAFKA_PORT", "9092"));
        assumeTrue(canConnect(host, port), "Kafka broker not reachable — smoke skipped");
        assertTrue(canConnect(host, port));
    }

    private static boolean canConnect(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
