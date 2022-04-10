package io.tpd.kafkaexample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;

@Component
public class UniqueCountProcessor {

    private static final int DURATION = 30;
    private static final String AT = "@";

    @Value("${tpd.input-topic}")
    private String inputTopic;

    @Value("${tpd.domain-store}")
    private String domainStore;

    @Value("${tpd.email-store}")
    private String emailStore;

    @Value("${tpd.windowed-email-store}")
    private String windowedEmailStore;

    @Autowired
    void buildPipeline(final StreamsBuilder streamsBuilder) {
        final JsonSerializer<EmailEvent> serializer = new JsonSerializer<>();
        final JsonDeserializer<EmailEvent> deserializer = new JsonDeserializer<>(EmailEvent.class);
        final KStream<String, EmailEvent> messageStream =
                streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(),
                                                                Serdes.serdeFrom(serializer,
                                                                                 deserializer)));

        final KGroupedStream<String, String> groupByEmail = messageStream.mapValues(EmailEvent::getEmail)
                                                                         .mapValues(value -> value.toLowerCase(Locale.getDefault()))
                                                                         .groupBy((key, email) -> email);
        groupByEmail.count(Materialized.as(Stores.inMemoryKeyValueStore(emailStore)));

        groupByEmail.windowedBy(TimeWindows.of(Duration.ofSeconds(DURATION)))
                    .count(Materialized.as(windowedEmailStore));

        messageStream.mapValues(EmailEvent::getEmail)
                     .mapValues(email -> email.toLowerCase(Locale.getDefault()))
                     .mapValues(email -> email.substring(email.indexOf(AT) + 1))
                     .groupBy((key, domain) -> domain)
                     .count(Materialized.as(Stores.inMemoryKeyValueStore(domainStore)));
    }
}


//        messageStream.mapValues(value -> value.toLowerCase(Locale.getDefault()))
//                     .groupBy((key, email) -> email)
//                     .reduce((aggValue, newValue) -> DUMMY_VALUE)
//                     .toStream()
//                     .map((key, value) -> KeyValue.pair(EMAIL_KEY, value))
//                     .groupByKey()
//                     .count(Materialized.as("total-emails"));

//        messageStream.mapValues(email -> email.toLowerCase(Locale.getDefault()))
//                     .mapValues(email -> email.substring(email.indexOf("@") + 1))
//                     .groupBy((key, domain) -> domain)
//                     .reduce((aggValue, newValue) -> DUMMY_VALUE)
//                     .toStream()
//                     .map((key, value) -> KeyValue.pair(DOMAIN_KEY, value))
//                     .groupByKey()
//                     .count(Materialized.as("total-domains"));

//    private static final String EMAIL_KEY = "emails";
//    private static final String DOMAIN_KEY = "domains";
//    private static final String DUMMY_VALUE = "dummyValue";

// https://stackoverflow.com/questions/46607435/how-to-count-unique-users-in-a-fixed-time-window-in-a-kafka-stream-app
// https://stackoverflow.com/questions/51048125/apache-kafka-grouping-twice
// https://stackoverflow.com/questions/51063041/kafka-streams-exception-could-not-find-a-public-no-argument-constructor-for-org