package io.tpd.kafkaexample;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

    private static final String APP_NAME = "streams-app";
    private static final String STATE_DIRECTORY = "/tmp";
    private static final int PARTITION_AMOUNT = 1;
    private static final short REPLICATION_FACTOR = (short) 1;

    @Value("${tpd.input-topic}")
    private String inputTopic;

    @Autowired
    private KafkaProperties kafkaProperties;


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        final Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
        props.put(APPLICATION_ID_CONFIG, APP_NAME);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRECTORY);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KafkaTemplate<String, EmailEvent> kafkaTemplate() {
        final Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        final DefaultKafkaProducerFactory<String, EmailEvent> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties);
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic inputTopic() {
        return new NewTopic(inputTopic, PARTITION_AMOUNT, REPLICATION_FACTOR);
    }
}
