package com.example.risk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class DltDeserializationExceptionHandlerTest {

    private MockProducer<byte[], byte[]> mockProducer;
    private DltDeserializationExceptionHandler handler;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        handler = new DltDeserializationExceptionHandler((Producer<byte[], byte[]>) mockProducer);
    }

    @Test
    void returnsContinueSoStreamDoesNotHalt() {
        var response = handler.handle(context("trades", 0, 0),
                record("trades", 0, 0, "bad-json"), new RuntimeException("parse error"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
    }

    @Test
    void routesToCorrectDltTopic() {
        handler.handle(context("trades", 0, 5),
                record("trades", 0, 5, "bad-json"), new RuntimeException("parse error"));

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo("trades-dlt");
    }

    @Test
    void dltTopicDerivedFromSourceTopic() {
        handler.handle(context("limits", 0, 1),
                record("limits", 0, 1, "bad-json"), new RuntimeException("parse error"));

        assertThat(mockProducer.history().get(0).topic()).isEqualTo("limits-dlt");
    }

    @Test
    void dltRecordPreservesOriginalKeyAndValue() {
        byte[] key   = "ACC-1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "not-json".getBytes(StandardCharsets.UTF_8);

        handler.handle(context("trades", 2, 99),
                new ConsumerRecord<>("trades", 2, 99, key, value),
                new RuntimeException("parse error"));

        ProducerRecord<byte[], byte[]> sent = mockProducer.history().get(0);
        assertThat(sent.key()).isEqualTo(key);
        assertThat(sent.value()).isEqualTo(value);
    }

    @Test
    void dltHeadersContainOriginalTopicPartitionOffset() {
        handler.handle(context("trades", 3, 42),
                record("trades", 3, 42, "bad"), new RuntimeException("oops"));

        Headers headers = mockProducer.history().get(0).headers();
        assertThat(utf8(headers, "dlt-original-topic")).isEqualTo("trades");
        assertThat(intVal(headers, "dlt-original-partition")).isEqualTo(3);
        assertThat(longVal(headers, "dlt-original-offset")).isEqualTo(42L);
    }

    @Test
    void dltHeadersContainExceptionDetails() {
        var ex = new IllegalArgumentException("cannot parse field X");

        handler.handle(context("trades", 0, 0), record("trades", 0, 0, "bad"), ex);

        Headers headers = mockProducer.history().get(0).headers();
        assertThat(utf8(headers, "dlt-exception-class")).isEqualTo(IllegalArgumentException.class.getName());
        assertThat(utf8(headers, "dlt-exception-message")).isEqualTo("cannot parse field X");
    }

    @Test
    void returnsContinueEvenWhenProducerThrows() {
        mockProducer.errorNext(new RuntimeException("broker unavailable"));

        var response = handler.handle(context("trades", 0, 0),
                record("trades", 0, 0, "bad"), new RuntimeException("parse error"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
    }

    // --- helpers ---

    private static ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset, String value) {
        return new ConsumerRecord<>(topic, partition, offset,
                new byte[0], value.getBytes(StandardCharsets.UTF_8));
    }

    private static ErrorHandlerContext context(String topic, int partition, long offset) {
        return new ErrorHandlerContext() {
            @Override public String topic()            { return topic; }
            @Override public int partition()           { return partition; }
            @Override public long offset()             { return offset; }
            @Override public Headers headers()         { return new RecordHeaders(); }
            @Override public String processorNodeId()  { return "test-node"; }
            @Override public TaskId taskId()           { return new TaskId(0, 0); }
            @Override public long timestamp()          { return 0L; }
            @Override public byte[] sourceRawKey()     { return new byte[0]; }
            @Override public byte[] sourceRawValue()   { return new byte[0]; }
        };
    }

    private static String utf8(Headers headers, String key) {
        var header = headers.lastHeader(key);
        assertThat(header).as("header '%s' missing", key).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int intVal(Headers headers, String key) {
        var header = headers.lastHeader(key);
        assertThat(header).as("header '%s' missing", key).isNotNull();
        return java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    private static long longVal(Headers headers, String key) {
        var header = headers.lastHeader(key);
        assertThat(header).as("header '%s' missing", key).isNotNull();
        return java.nio.ByteBuffer.wrap(header.value()).getLong();
    }
}
