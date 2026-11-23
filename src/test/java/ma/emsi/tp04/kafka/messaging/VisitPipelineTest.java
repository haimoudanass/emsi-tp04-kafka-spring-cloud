package ma.emsi.tp04.kafka.messaging;

import ma.emsi.tp04.kafka.domain.VisitEvent;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VisitPipelineTest {

    private final VisitPipeline pipeline = new VisitPipeline();

    @Test
    void supplierProducesValidEvent() {
        Supplier<VisitEvent> supplier = pipeline.visitPulse();
        VisitEvent event = supplier.get();
        assertNotNull(event.pageId());
        assertNotNull(event.visitorId());
        assertTrue(event.dwellMs() >= 50);
    }

    @Test
    void consumerAcceptsEvent() {
        Consumer<VisitEvent> consumer = pipeline.visitLogger();
        assertDoesNotThrow(() -> consumer.accept(VisitEvent.of("home", "visitor-a", 100)));
    }

    @Test
    void windowFunctionBeanIsCreated() {
        assertNotNull(pipeline.visitWindowCount());
    }
}
