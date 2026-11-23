package ma.emsi.tp04.kafka.messaging;

import ma.emsi.tp04.kafka.domain.VisitEvent;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VisitPublisherTest {

    @Test
    void delegatesToStreamBridge() {
        StreamBridge bridge = mock(StreamBridge.class);
        VisitPublisher publisher = new VisitPublisher(bridge);
        VisitEvent event = VisitEvent.of("home", "u1", 10);
        when(bridge.send("visit-ingress", event)).thenReturn(true);

        assertTrue(publisher.publish("visit-ingress", event));
        verify(bridge).send("visit-ingress", event);
    }

    @Test
    void rejectsBlankTopic() {
        VisitPublisher publisher = new VisitPublisher(mock(StreamBridge.class));
        assertThrows(IllegalArgumentException.class,
                () -> publisher.publish(" ", VisitEvent.of("home", "u1", 1)));
    }
}
