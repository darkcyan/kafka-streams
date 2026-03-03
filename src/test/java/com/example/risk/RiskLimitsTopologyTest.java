package com.example.risk;

import com.example.risk.config.StreamsTopologyConfig;
import com.example.risk.model.*;
import com.example.risk.serde.JsonSerde;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class RiskLimitsTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Trade> tradesTopic;
    private TestInputTopic<String, Limit> limitsTopic;
    private TestOutputTopic<String, Decision> approvedTopic;
    private TestOutputTopic<String, Decision> rejectedTopic;

    @BeforeEach
    void setUp() throws Exception {
        var registry = new SimpleMeterRegistry();
        var builder = new StreamsBuilder();
        new StreamsTopologyConfig().riskLimitsTopology(builder, registry);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-risk");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstreams-test-").toString());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        driver = new TopologyTestDriver(builder.build(), props);

        tradesTopic = driver.createInputTopic("trades",
                Serdes.String().serializer(), new JsonSerde<>(Trade.class).serializer());
        limitsTopic = driver.createInputTopic("limits",
                Serdes.String().serializer(), new JsonSerde<>(Limit.class).serializer());
        approvedTopic = driver.createOutputTopic("approved-trades",
                Serdes.String().deserializer(), new JsonSerde<>(Decision.class).deserializer());
        rejectedTopic = driver.createOutputTopic("rejected-trades",
                Serdes.String().deserializer(), new JsonSerde<>(Decision.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private Trade trade(String tradeId, String accountId, double notional, Side side) {
        return new Trade(tradeId, accountId, notional, side, System.currentTimeMillis());
    }

    @Test
    void buyTradeApprovedWhenWithinLimit() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("any-key", trade("T1", "ACC-1", 50_000, Side.BUY));

        var record = approvedTopic.readRecord();
        assertThat(record.key()).isEqualTo("ACC-1");
        Decision d = record.value();
        assertThat(d.approved()).isTrue();
        assertThat(d.previousExposure()).isEqualTo(0.0);
        assertThat(d.newExposure()).isEqualTo(50_000.0);
        assertThat(rejectedTopic.isEmpty()).isTrue();
    }

    @Test
    void buyTradeRejectedWhenBreachingLimit() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("any-key", trade("T1", "ACC-1", 150_000, Side.BUY));

        var record = rejectedTopic.readRecord();
        assertThat(record.key()).isEqualTo("ACC-1");
        Decision d = record.value();
        assertThat(d.approved()).isFalse();
        assertThat(d.reason()).isEqualTo("LIMIT_BREACH");
        assertThat(d.newExposure()).isEqualTo(0.0); // prev=0, store not mutated
        assertThat(approvedTopic.isEmpty()).isTrue();
    }

    @Test
    void tradeRejectedWhenNoLimit() {
        tradesTopic.pipeInput("any-key", trade("T1", "ACC-1", 50_000, Side.BUY));

        var record = rejectedTopic.readRecord();
        Decision d = record.value();
        assertThat(d.approved()).isFalse();
        assertThat(d.reason()).isEqualTo("NO_LIMIT");
        assertThat(approvedTopic.isEmpty()).isTrue();
    }

    @Test
    void exposureAccumulatesAcrossApprovedTrades() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("k1", trade("T1", "ACC-1", 40_000, Side.BUY));
        tradesTopic.pipeInput("k2", trade("T2", "ACC-1", 30_000, Side.BUY));

        Decision r1 = approvedTopic.readRecord().value();
        assertThat(r1.previousExposure()).isEqualTo(0.0);
        assertThat(r1.newExposure()).isEqualTo(40_000.0);

        Decision r2 = approvedTopic.readRecord().value();
        assertThat(r2.previousExposure()).isEqualTo(40_000.0);
        assertThat(r2.newExposure()).isEqualTo(70_000.0);

        assertThat(rejectedTopic.isEmpty()).isTrue();
    }

    @Test
    void storeNotUpdatedOnRejection() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("k1", trade("T1", "ACC-1", 80_000, Side.BUY));  // ok → store=80k
        tradesTopic.pipeInput("k2", trade("T2", "ACC-1", 30_000, Side.BUY));  // breach → store unchanged
        tradesTopic.pipeInput("k3", trade("T3", "ACC-1", 10_000, Side.BUY));  // ok → prev should be 80k

        Decision t1 = approvedTopic.readRecord().value();
        assertThat(t1.newExposure()).isEqualTo(80_000.0);

        Decision t2 = rejectedTopic.readRecord().value();
        assertThat(t2.reason()).isEqualTo("LIMIT_BREACH");

        Decision t3 = approvedTopic.readRecord().value();
        assertThat(t3.previousExposure()).isEqualTo(80_000.0); // T2 didn't mutate store
        assertThat(t3.newExposure()).isEqualTo(90_000.0);
    }

    @Test
    void sellTradeReducesExposure() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("k1", trade("T1", "ACC-1", 60_000, Side.BUY));
        tradesTopic.pipeInput("k2", trade("T2", "ACC-1", 20_000, Side.SELL));

        Decision buy = approvedTopic.readRecord().value();
        assertThat(buy.newExposure()).isEqualTo(60_000.0);

        Decision sell = approvedTopic.readRecord().value();
        assertThat(sell.newExposure()).isEqualTo(40_000.0);

        assertThat(rejectedTopic.isEmpty()).isTrue();
    }

    @Test
    void largeSellRejectedByAbsoluteLimit() {
        limitsTopic.pipeInput("ACC-1", new Limit(100_000));
        tradesTopic.pipeInput("k1", trade("T1", "ACC-1", 150_000, Side.SELL)); // |0-150k|=150k > 100k

        Decision d = rejectedTopic.readRecord().value();
        assertThat(d.approved()).isFalse();
        assertThat(d.reason()).isEqualTo("LIMIT_BREACH");
        assertThat(approvedTopic.isEmpty()).isTrue();
    }

    @Test
    void accountsAreIsolated() {
        limitsTopic.pipeInput("ACC-A", new Limit(100_000));
        limitsTopic.pipeInput("ACC-B", new Limit(50_000));
        tradesTopic.pipeInput("k1", trade("T1", "ACC-A", 80_000, Side.BUY));
        tradesTopic.pipeInput("k2", trade("T2", "ACC-B", 40_000, Side.BUY));

        assertThat(approvedTopic.getQueueSize()).isEqualTo(2);
        assertThat(rejectedTopic.isEmpty()).isTrue();

        var decisions = approvedTopic.readValuesToList();
        assertThat(decisions).allMatch(d -> d.previousExposure() == 0.0);
        assertThat(decisions).anyMatch(d -> d.newExposure() == 80_000.0);
        assertThat(decisions).anyMatch(d -> d.newExposure() == 40_000.0);
    }
}
