package io.tpd.kafkaexample;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new GsonBuilder().create();

    private Class<T> destinationClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(final Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public JsonDeserializer(final Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // nothing to do
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            final Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
            return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), type);
        } catch (final Exception e) {
            throw new SerializationException("Error deserializing message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}