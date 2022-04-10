package io.tpd.kafkaexample;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static net.andreinc.mockneat.unit.user.Emails.emails;
import static net.andreinc.mockneat.unit.user.Users.users;

@Slf4j
@RequiredArgsConstructor
@RestController
public class ProducerController {

    private static final int SLEEP_TIME = 2000;
    private static final String KEY_VALUE_CONTENT = "Key %s: | Payload: %s";

    private final KafkaTemplate<String, EmailEvent> template;

    @Value("${tpd.input-topic}")
    private String inputTopic;

    @Value("${tpd.messages-per-request}")
    private int messagesPerRequest;

    @PostMapping("/events")
    public List<String> createEvents() throws Exception {
        final List<String> sentMessages = new ArrayList<>();

        for (int rounds = 0; rounds < 1; rounds++) {
            final String email = emails().get();
            final String user = users().get();
            IntStream.range(0, messagesPerRequest/2)
                     .forEach(index -> sendToKafka(index, user, email, sentMessages));
            IntStream.range(messagesPerRequest/2, messagesPerRequest)
                    .forEach(index -> sendToKafka(index, users().get(), emails().get(), sentMessages));
        }
        return sentMessages;
    }

    private void sendToKafka(final int id, final String user, final String email, final List<String> sendMessages) {
        final EmailEvent emailEvent = new EmailEvent(user, email);
        this.template.send(inputTopic, String.valueOf(id), emailEvent);
        sendMessages.add(String.format(KEY_VALUE_CONTENT, id, emailEvent));
    }
}