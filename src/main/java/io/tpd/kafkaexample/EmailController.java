package io.tpd.kafkaexample;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@RestController
public class EmailController {

    private static final int AMOUNT_TO_SUBTRACT = 30;
    private static final String WINDOWED_RESULT_TEXT = "start [%s] | end [%s] | key [%s] | value [%s]";

    @Value("${tpd.email-store}")
    private String emailStore;

    @Value("${tpd.windowed-email-store}")
    private String windowedEmailStore;

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/emails")
    public Result getOverallUniqueEmailCount() {
        final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(emailStore, QueryableStoreTypes.keyValueStore()));
        final Map<String, Long> domainCounter = new HashMap<>();
        counts.all().forEachRemaining(keyValue -> domainCounter.put(keyValue.key, keyValue.value));
        return new Result(domainCounter, domainCounter.size());
    }

    @GetMapping("/emails/windowed")
    public List<String> getWindowedUniqueEmailCount() {
        final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        final ReadOnlyWindowStore<String, Long> windowStore = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(windowedEmailStore,
                        QueryableStoreTypes.windowStore()));

        final Instant endTime = Instant.now();
        final Instant initialTime = endTime.minus(AMOUNT_TO_SUBTRACT, ChronoUnit.SECONDS);
        final KeyValueIterator<Windowed<String>, Long> windowedLongKeyValueIterator =
                windowStore.fetchAll(initialTime, endTime);
        return Lists.newArrayList(windowedLongKeyValueIterator)
                .stream()
                .map(this::mapKeyValueToResult)
                .collect(Collectors.toList());
    }

    private String mapKeyValueToResult(final KeyValue<Windowed<String>, Long> keyValue) {
        final Window window = keyValue.key.window();
        return String.format(WINDOWED_RESULT_TEXT,
                window.startTime().atZone(ZoneOffset.UTC),
                window.endTime().atZone(ZoneOffset.UTC),
                keyValue.key.key(),
                keyValue.value);
    }
}