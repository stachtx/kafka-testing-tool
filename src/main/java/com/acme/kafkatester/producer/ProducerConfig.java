package com.acme.kafkatester.producer;

import java.util.EnumMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

/**
 * Producer wiring. One {@link KafkaTemplate} per {@link MessageFormat} (they differ only in the
 * value serializer class), held in {@link ProducerTemplates} so the service selects by enum with
 * no format branching. All templates share the security config and the Gateway safe defaults
 * (auto.register.schemas=false, use.latest.version=true, TopicNameStrategy), so nothing is ever
 * auto-registered.
 */
@Configuration(proxyBeanMethods = false)
public class ProducerConfig {

    /** Format-keyed lookup of templates. Closes every producer factory on shutdown (W4). */
    public static class ProducerTemplates implements DisposableBean {

        private static final Logger log = LoggerFactory.getLogger(ProducerTemplates.class);

        private final Map<MessageFormat, KafkaTemplate<String, Object>> templates;

        public ProducerTemplates(Map<MessageFormat, KafkaTemplate<String, Object>> templates) {
            this.templates = new EnumMap<>(templates);
        }

        @Override
        public void destroy() {
            for (KafkaTemplate<String, Object> t : templates.values()) {
                try {
                    if (t.getProducerFactory() instanceof DisposableBean factory) {
                        factory.destroy();
                    }
                } catch (Exception e) { // keep closing the remaining factories
                    log.warn("Failed to close producer factory", e);
                }
            }
        }

        public KafkaTemplate<String, Object> forFormat(MessageFormat format) {
            KafkaTemplate<String, Object> t = templates.get(format);
            if (t == null) {
                throw new IllegalArgumentException("No producer configured for format " + format);
            }
            return t;
        }
    }

    @Bean
    public ProducerTemplates producerTemplates(KafkaSecurityConfig security, SchemaRegistryGateway gateway) {
        Map<MessageFormat, Class<?>> valueSerializers = Map.of(
                MessageFormat.AVRO, KafkaAvroSerializer.class,
                MessageFormat.PROTOBUF, KafkaProtobufSerializer.class);
        Map<MessageFormat, KafkaTemplate<String, Object>> templates = new EnumMap<>(MessageFormat.class);
        valueSerializers.forEach((format, serializer) -> {
            Map<String, Object> config = gateway.serializerConfig(security.producerProperties());
            config.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            config.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
            templates.put(format, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config)));
        });
        return new ProducerTemplates(templates);
    }
}
