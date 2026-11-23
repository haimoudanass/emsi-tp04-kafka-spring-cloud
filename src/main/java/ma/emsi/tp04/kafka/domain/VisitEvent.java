package ma.emsi.tp04.kafka.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Événement de visite émis vers Kafka.
 * Modèle métier propre au TP04 EMSI (réimplémentation indépendante).
 */
public record VisitEvent(
        String pageId,
        String visitorId,
        Instant observedAt,
        long dwellMs
) {
    public VisitEvent {
        Objects.requireNonNull(pageId, "pageId");
        Objects.requireNonNull(visitorId, "visitorId");
        Objects.requireNonNull(observedAt, "observedAt");
        if (pageId.isBlank()) {
            throw new IllegalArgumentException("pageId must not be blank");
        }
        if (visitorId.isBlank()) {
            throw new IllegalArgumentException("visitorId must not be blank");
        }
        if (dwellMs < 0) {
            throw new IllegalArgumentException("dwellMs must be >= 0");
        }
    }

    public static VisitEvent of(String pageId, String visitorId, long dwellMs) {
        return new VisitEvent(pageId, visitorId, Instant.now(), dwellMs);
    }
}
