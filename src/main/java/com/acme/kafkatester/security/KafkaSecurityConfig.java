package com.acme.kafkatester.security;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Single place where environment-supplied Kafka/Schema Registry connection and security settings
 * ({@code spring.kafka.*}) are turned into client property maps. Shared by the producer and consumer
 * factories so both use identical security configuration. Secrets are never logged: use
 * {@link #redacted(Map)} for any diagnostic output.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaSecurityConfig {

    static final String REDACTED = "******";
    private static final Set<String> SECRET_MARKERS =
            Set.of("password", "secret", "jaas", "user.info", "token", "credential", "key");

    private final KafkaProperties kafkaProperties;

    public KafkaSecurityConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /** Client properties (bootstrap, security.protocol, SASL/SSL, registry settings) for producers. */
    public Map<String, Object> producerProperties() {
        return new LinkedHashMap<>(kafkaProperties.buildProducerProperties());
    }

    /** Client properties (bootstrap, security.protocol, SASL/SSL, registry settings) for consumers. */
    public Map<String, Object> consumerProperties() {
        return new LinkedHashMap<>(kafkaProperties.buildConsumerProperties());
    }

    /** Copy of the given properties with every secret-looking value masked; safe to log. */
    public Map<String, Object> redacted(Map<String, ?> properties) {
        Map<String, Object> copy = new LinkedHashMap<>();
        properties.forEach((key, value) -> copy.put(key, isSecret(key) ? REDACTED : value));
        return copy;
    }

    private static boolean isSecret(String key) {
        String lower = key.toLowerCase(Locale.ROOT);
        return SECRET_MARKERS.stream().anyMatch(lower::contains);
    }
}
