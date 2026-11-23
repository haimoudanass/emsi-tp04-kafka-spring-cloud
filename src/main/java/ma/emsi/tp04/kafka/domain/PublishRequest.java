package ma.emsi.tp04.kafka.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

public record PublishRequest(
        @NotBlank String pageId,
        String visitorId,
        @PositiveOrZero Long dwellMs,
        @NotBlank String topic
) {
}
