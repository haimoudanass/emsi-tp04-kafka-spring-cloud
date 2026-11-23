package ma.emsi.tp04.kafka.api;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final InteractiveQueryService interactiveQueryService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-sse");
        t.setDaemon(true);
        return t;
    });

    public MetricsController(InteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping(path = "/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter liveWindowCounts() {
        SseEmitter emitter = new SseEmitter(0L);
        AtomicBoolean open = new AtomicBoolean(true);
        emitter.onCompletion(() -> open.set(false));
        emitter.onTimeout(() -> open.set(false));
        emitter.onError(ex -> open.set(false));

        var future = scheduler.scheduleAtFixedRate(() -> {
            if (!open.get()) {
                return;
            }
            try {
                emitter.send(SseEmitter.event().name("metrics").data(snapshot()));
            } catch (IOException ex) {
                open.set(false);
                emitter.completeWithError(ex);
            }
        }, 0, 1, TimeUnit.SECONDS);

        emitter.onCompletion(() -> future.cancel(true));
        emitter.onTimeout(() -> future.cancel(true));
        return emitter;
    }

    @GetMapping("/snapshot")
    public Map<String, Long> snapshot() {
        Map<String, Long> counts = new LinkedHashMap<>();
        try {
            ReadOnlyWindowStore<String, Long> store = interactiveQueryService.getQueryableStore(
                    "visit-count-store",
                    QueryableStoreTypes.windowStore()
            );
            Instant now = Instant.now();
            Instant from = now.minusMillis(5_000);
            try (KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(from, now)) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<String>, Long> next = iterator.next();
                    counts.merge(next.key.key(), next.value, Long::sum);
                }
            }
        } catch (Exception ex) {
            counts.put("_storeUnavailable", 0L);
        }
        return counts;
    }
}
