package io.tpd.kafkaexample;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final Gson gson = new GsonBuilder().create();

    // default constructor needed by Kafka
    public JsonSerializer() {
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null)
            return null;

        try {
            return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}