package com.example.risk.config;

import com.example.risk.ExposureLimitSupplier;
import com.example.risk.RiskStreamsUncaughtExceptionHandler;
import com.example.risk.model.*;
import com.example.risk.serde.JsonSerde;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.time.ZoneId;

@Configuration
@EnableKafkaStreams
@Slf4j
public class StreamsTopologyConfig {

    public static final String STORE_EXPOSURE = "exposure-store";

    @Bean
    public KStream<String, Decision> riskLimitsTopology(StreamsBuilder builder, MeterRegistry registry) {

        var tradeSerde = new JsonSerde<>(Trade.class);
        var limitSerde = new JsonSerde<>(Limit.class);
        var decisionSerde = new JsonSerde<>(Decision.class);

        KStream<String, Trade> tradesByAccountId =
                builder.stream("trades", Consumed.with(Serdes.String(), tradeSerde))
                        .selectKey((k,t) -> t.accountId());

        KTable<String, Limit> limitsByAccount =
                builder.table("limits", Consumed.with(Serdes.String(), limitSerde));

        KStream<String, EnrichedTrade> enriched =
                tradesByAccountId.leftJoin(limitsByAccount, EnrichedTrade::new);

        enriched.peek((key,value) -> System.out.println(key + "->" + value));

        KStream<String, Decision> decisions =
                enriched.processValues(processorSupplier(registry));

        decisions.split()
                .branch((k, d) -> "PROCESSOR_ERROR".equals(d.reason()), Branched.withConsumer(ks ->
                        ks.to("trades-processor-dlt", Produced.with(Serdes.String(), decisionSerde))))
                .branch((k, d) -> d.approved(), Branched.withConsumer(ks ->
                        ks.to("approved-trades", Produced.with(Serdes.String(), decisionSerde))))
                .defaultBranch(Branched.withConsumer(ks ->
                        ks.to("rejected-trades", Produced.with(Serdes.String(), decisionSerde))));

        return decisions;
    }

    protected FixedKeyProcessorSupplier<String, EnrichedTrade, Decision> processorSupplier(MeterRegistry registry) {
        return new ExposureLimitSupplier(ZoneId.of("Europe/London"), STORE_EXPOSURE, registry);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer uncaughtExceptionHandlerConfigurer() {
        return factoryBean -> factoryBean.setKafkaStreamsCustomizer(
                streams -> streams.setUncaughtExceptionHandler(new RiskStreamsUncaughtExceptionHandler()));
    }

}
