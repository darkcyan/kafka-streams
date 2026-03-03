package com.example.risk.config;


import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Configuration
public class MicrometerConfig {

    @Bean
    public BeanPostProcessor kafkaStreamsMetricsBinder(MeterRegistry registry) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {

                if (bean instanceof StreamsBuilderFactoryBean factoryBean) {
                    factoryBean.addListener(new MetricsListener(registry));
                }
                return bean;
            }
        };
    }

    static class MetricsListener implements StreamsBuilderFactoryBean.Listener {

        private final MeterRegistry registry;

        MetricsListener(MeterRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void streamsAdded(String id, KafkaStreams streams) {
            new KafkaStreamsMetrics(streams).bindTo(registry);
        }

        @Override
        public void streamsRemoved(String id, KafkaStreams streams) {}
    }


}
