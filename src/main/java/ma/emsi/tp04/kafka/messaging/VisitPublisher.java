package ma.emsi.tp04.kafka.messaging;

import ma.emsi.tp04.kafka.domain.VisitEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
public class VisitPublisher {

    private final StreamBridge streamBridge;

    public VisitPublisher(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    public boolean publish(String topic, VisitEvent event) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        return streamBridge.send(topic, event);
    }
}
