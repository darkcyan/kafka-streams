package com.example.risk;

import com.example.risk.model.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;

public final class ExposureLimitProcessor implements FixedKeyProcessor<String, EnrichedTrade, Decision> {

    private static final Logger log = LoggerFactory.getLogger(ExposureLimitProcessor.class);

    private final ZoneId zone;
    private final String storeName;

    private FixedKeyProcessorContext<String, Decision> context;
    private KeyValueStore<String, Exposure> exposureStore;

    private final Counter recordsProcessed;

    public ExposureLimitProcessor(ZoneId zone, String storeName, MeterRegistry registry) {
        this.zone = zone;
        this.storeName = storeName;
        this.recordsProcessed = Counter.builder("risk.records.processed")
                .description("Number of records processed by risk topology")
                .register(registry);
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Decision> context) {
        this.context = context;
        this.exposureStore = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedTrade> record) {
        recordsProcessed.increment();
        final String accountId = record.key();
        final EnrichedTrade enriched = record.value();

        if (enriched == null || enriched.trade() == null) {
            log.warn("Null trade received for key={}, routing to processor DLT", accountId);
            context.forward(record.withValue(new Decision(null, 0, 0, 0, false, "PROCESSOR_ERROR")));
            return;
        }

        final Trade trade = enriched.trade();
        final Limit limitObj = enriched.limit();

        try {
            final double prev = getOrZero(accountId);
            final double delta = signedNotional(trade);
            final double next = prev + delta;

            // Safety policy: if we don't know the maxNotional, reject (change if you want default-approve)
            if (limitObj == null) {
                Decision d = new Decision(trade, prev, prev, 0.0, false, "NO_LIMIT");
                context.forward(record.withValue(d));
                return;
            }

            final double maxNotional = limitObj.maxNotional();
            final boolean ok = Math.abs(next) <= maxNotional;

            if (ok) {
                LocalDate today = LocalDate.now(zone);
                exposureStore.put(accountId, new Exposure(today, next));
                Decision d = new Decision(trade, prev, next, maxNotional, true, "OK");
                context.forward(record.withValue(d));
            } else {
                // don't mutate store on reject
                Decision d = new Decision(trade, prev, prev, maxNotional, false, "LIMIT_BREACH");
                context.forward(record.withValue(d));
            }
        } catch (Exception e) {
            log.error("Processor error for account={}, routing to DLT", accountId, e);
            context.forward(record.withValue(new Decision(trade, 0, 0, 0, false, "PROCESSOR_ERROR")));
        }
    }

    @Override
    public void close() {
        // nothing special required; Streams will handle store lifecycle
        // but it's fine to keep this for clarity
    }

    private double getOrZero(String accountId) {
        Exposure e = exposureStore.get(accountId);
        if (e == null) {
            return 0.0;
        }
        LocalDate today = LocalDate.now(zone);
        return e.date().equals(today) ? e.value() : 0.0;
    }

    private static double signedNotional(Trade t) {
        return t.side() == Side.BUY ? t.notional() : -t.notional();
    }
}