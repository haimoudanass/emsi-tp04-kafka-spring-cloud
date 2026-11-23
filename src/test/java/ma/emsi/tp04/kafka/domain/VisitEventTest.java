package ma.emsi.tp04.kafka.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VisitEventTest {

    @Test
    void createsValidEvent() {
        VisitEvent event = VisitEvent.of("home", "u1", 120);
        assertEquals("home", event.pageId());
        assertEquals("u1", event.visitorId());
        assertEquals(120, event.dwellMs());
        assertTrue(event.observedAt().isBefore(Instant.now().plusSeconds(1)));
    }

    @Test
    void rejectsBlankPage() {
        assertThrows(IllegalArgumentException.class, () -> VisitEvent.of(" ", "u1", 10));
    }

    @Test
    void rejectsNegativeDwell() {
        assertThrows(IllegalArgumentException.class,
                () -> new VisitEvent("home", "u1", Instant.now(), -1));
    }
}
