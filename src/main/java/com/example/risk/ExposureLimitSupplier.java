package com.example.risk;

import com.example.risk.model.Decision;
import com.example.risk.model.EnrichedTrade;
import com.example.risk.model.Exposure;
import com.example.risk.serde.JsonSerde;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.ZoneId;
import java.util.Set;

public final class ExposureLimitSupplier
        implements FixedKeyProcessorSupplier<String, EnrichedTrade, Decision> {

    private final ZoneId zone;
    private final String storeName;

    private final MeterRegistry registry;

    public ExposureLimitSupplier(ZoneId zone, String storeName, MeterRegistry registry) {
        this.zone = zone;
        this.storeName = storeName;
        this.registry = registry;
    }

    @Override
    public FixedKeyProcessor<String, EnrichedTrade, Decision> get() {
        return new ExposureLimitProcessor(zone, storeName, registry);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<String, Exposure>> exposureStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        new JsonSerde<>(Exposure.class)
                );

        return Set.of(exposureStore);
    }
}