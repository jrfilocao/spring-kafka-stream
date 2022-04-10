package io.tpd.kafkaexample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class ConsumerController {

    private static final String KEY_PAYLOAD = "Key {}: | Payload: {}";

    @KafkaListener(topics = "input-topic")
    public void consume(final ConsumerRecord<String, EmailEvent> record) {
        log.info(KEY_PAYLOAD, record.key(), record.value());
    }
}