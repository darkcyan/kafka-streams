package com.example.risk.config;

import com.example.risk.ExposureLimitSupplier;
import com.example.risk.model.*;
import com.example.risk.serde.JsonSerde;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.ZoneId;

@Configuration
@EnableKafkaStreams
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

        KStream<String, Decision> decisions =
                enriched.processValues(new ExposureLimitSupplier(ZoneId.of("Europe/London"), STORE_EXPOSURE, registry));

        decisions.split()
                .branch((k, d) -> d.approved(), Branched.withConsumer(ks ->
                        ks.to("approved-trades", Produced.with(Serdes.String(), decisionSerde))))
                .defaultBranch(Branched.withConsumer(ks ->
                        ks.to("rejected-trades", Produced.with(Serdes.String(), decisionSerde))));

        return decisions;
    }


}
