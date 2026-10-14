package com.acme.kafkatester.registry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.acme.kafkatester.security.KafkaSecurityConfig;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

/**
 * Wiring for the registry package. The {@link SchemaRegistryClient} is built from the same
 * environment-supplied {@code spring.kafka.properties.schema.registry.*} / {@code basic.auth.*}
 * settings the serializers use. Creating the client performs no network call.
 */
@Configuration(proxyBeanMethods = false)
class SchemaRegistryConfig {

    static final String URL_PROPERTY = "schema.registry.url";
    static final int CACHE_CAPACITY = 100;

    @Bean
    SchemaRegistryClient schemaRegistryClient(KafkaSecurityConfig security) {
        Map<String, Object> props = new java.util.HashMap<>(security.producerProperties());
        props.put(io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL,
                SchemaRegistryGateway.LATEST_CACHE_TTL_SEC); // guard reads latest through this client; keep it fresh
        Object url = props.get(URL_PROPERTY);
        if (url == null || url.toString().isBlank()) {
            throw new IllegalStateException(
                    "Missing " + URL_PROPERTY + " (set SCHEMA_REGISTRY_URL / spring.kafka.properties." + URL_PROPERTY + ")");
        }
        List<String> urls = Arrays.stream(url.toString().split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        List<SchemaProvider> providers = List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider());
        return new CachedSchemaRegistryClient(urls, CACHE_CAPACITY, providers, props);
    }

    @Bean
    SubjectNamingStrategy subjectNamingStrategy() {
        return SubjectNamingStrategy.topicValue();
    }
}
