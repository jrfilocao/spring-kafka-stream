package io.tpd.kafkaexample;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
@RestController
public class DomainController {

    private final StreamsBuilderFactoryBean factoryBean;

    @Value("${tpd.domain-store}")
    private String domainStore;

    @GetMapping("/domains")
    public Result getDomainCount() {
        final KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(domainStore, QueryableStoreTypes.keyValueStore()));
        final Map<String, Long> domainCounter = new HashMap<>();
        counts.all().forEachRemaining(keyValue -> domainCounter.put(keyValue.key, keyValue.value));
        return new Result(domainCounter, domainCounter.size());
    }
}
