package com.example.risk.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public final class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper mapper;
    private final Class<T> clazz;

    public JsonSerde(Class<T> clazz) {
        this.clazz = clazz;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override public Serializer<T> serializer() {
        return new Serializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) { }
            @Override public byte[] serialize(String topic, T data) {
                if (data == null) return null;
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("JSON serialize failed for " + clazz.getName(), e);
                }
            }
            @Override public void close() { }
        };
    }

    @Override public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) { }
            @Override public T deserialize(String topic, byte[] data) {
                if (data == null || data.length == 0) return null;
                try {
                    return mapper.readValue(data, clazz);
                } catch (Exception e) {
                    throw new SerializationException("JSON deserialize failed for " + clazz.getName(), e);
                }
            }
            @Override public void close() { }
        };
    }
}