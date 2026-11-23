package ma.emsi.tp04.kafka.api;

import jakarta.validation.Valid;
import ma.emsi.tp04.kafka.domain.PublishRequest;
import ma.emsi.tp04.kafka.domain.VisitEvent;
import ma.emsi.tp04.kafka.messaging.VisitPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/api/visits")
public class VisitPublishController {

    private final VisitPublisher visitPublisher;

    public VisitPublishController(VisitPublisher visitPublisher) {
        this.visitPublisher = visitPublisher;
    }

    @PostMapping
    public ResponseEntity<VisitEvent> publish(@Valid @RequestBody PublishRequest request) {
        String visitor = (request.visitorId() == null || request.visitorId().isBlank())
                ? "anonymous-" + ThreadLocalRandom.current().nextInt(1000, 9999)
                : request.visitorId();
        long dwell = request.dwellMs() == null ? 100L : request.dwellMs();

        VisitEvent event = VisitEvent.of(request.pageId(), visitor, dwell);
        boolean accepted = visitPublisher.publish(request.topic(), event);
        if (!accepted) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
        }
        return ResponseEntity.accepted().body(event);
    }

    @PostMapping("/demo")
    public ResponseEntity<Map<String, Object>> publishDemo() {
        VisitEvent event = VisitEvent.of("home", "demo-user", 250L);
        boolean accepted = visitPublisher.publish("visit-ingress", event);
        return ResponseEntity.accepted().body(Map.of(
                "accepted", accepted,
                "topic", "visit-ingress",
                "event", event
        ));
    }
}
