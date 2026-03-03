package com.example.risk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class DltDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DltDeserializationExceptionHandler.class);

    private Producer<byte[], byte[]> producer;

    public DltDeserializationExceptionHandler() {}

    // package-private for testing
    DltDeserializationExceptionHandler(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
            ConsumerRecord<byte[], byte[]> record, Exception exception) {

        String dltTopic = record.topic() + "-dlt";
        log.warn("Deserialization error on topic={} partition={} offset={}, routing to {}",
                record.topic(), record.partition(), record.offset(), dltTopic, exception);

        try {
            var headers = new RecordHeaders()
                    .add("dlt-original-topic",     utf8(record.topic()))
                    .add("dlt-original-partition",  intBytes(record.partition()))
                    .add("dlt-original-offset",     longBytes(record.offset()))
                    .add("dlt-exception-class",     utf8(exception.getClass().getName()))
                    .add("dlt-exception-message",   utf8(exception.getMessage()));

            producer.send(new ProducerRecord<>(dltTopic, null, record.key(), record.value(), headers));
        } catch (Exception e) {
            log.error("Failed to forward record to DLT topic {}", dltTopic, e);
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    private static byte[] utf8(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }
}
