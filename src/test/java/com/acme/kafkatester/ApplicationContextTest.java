package com.acme.kafkatester;

import static org.assertj.core.api.Assertions.assertThat;

import com.acme.kafkatester.security.KafkaSecurityConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Scaffold tests: context loads without any Kafka env, defaults bind, security config maps without leaking secrets. */
class ApplicationContextTest {

    @SpringBootTest
    static class DefaultsTest {
        @Autowired ApplicationContext context;
        @Autowired KafkaTesterProperties properties;
        @Autowired org.springframework.core.env.Environment env;

        @Test
        void bindsLoopbackOnlyByDefaultAndSpringdocEnabled() {
            assertThat(env.getProperty("server.address")).isEqualTo("127.0.0.1");
            assertThat(env.getProperty("springdoc.swagger-ui.enabled")).isEqualTo("true");
            assertThat(properties.getConsumer().getMaxSessions()).isEqualTo(10);
        }

        @Test
        void contextLoadsWithoutEnvAndNoListenerContainersAutoStart() {
            assertThat(context.getBean(KafkaSecurityConfig.class)).isNotNull();
            KafkaListenerEndpointRegistry registry = context.getBeanProvider(KafkaListenerEndpointRegistry.class).getIfAvailable();
            if (registry != null) {
                assertThat(registry.getListenerContainers()).isEmpty();
            }
        }

        @Test
        void earliestCapDefaultsTo1000() {
            assertThat(properties.getConsumer().getEarliestCap()).isEqualTo(1000);
        }
    }

    @SpringBootTest(properties = "kafkatester.consumer.earliest-cap=250")
    static class OverrideTest {
        @Autowired KafkaTesterProperties properties;

        @Test
        void earliestCapOverridesFromConfiguration() {
            assertThat(properties.getConsumer().getEarliestCap()).isEqualTo(250);
        }
    }

    @SpringBootTest(properties = {
            "spring.kafka.bootstrap-servers=broker.example.invalid:9093",
            "spring.kafka.security.protocol=SASL_SSL",
            "spring.kafka.properties.sasl.mechanism=PLAIN",
            "spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"s3cr3t-value\";",
            "spring.kafka.properties.schema.registry.url=https://registry.example.invalid",
            "spring.kafka.properties.basic.auth.user.info=u:s3cr3t-value"
    })
    static class SecurityMappingTest {
        @Autowired KafkaSecurityConfig securityConfig;

        @Test
        void mapsSecurityPropertiesForProducerAndConsumer() {
            Map<String, Object> producer = securityConfig.producerProperties();
            Map<String, Object> consumer = securityConfig.consumerProperties();
            assertThat(producer).containsEntry("security.protocol", "SASL_SSL").containsEntry("sasl.mechanism", "PLAIN");
            assertThat(consumer).containsEntry("security.protocol", "SASL_SSL").containsEntry("sasl.mechanism", "PLAIN");
            assertThat(producer).containsKey("schema.registry.url");
        }

        @Test
        void redactedViewNeverContainsSecrets() {
            String rendered = securityConfig.redacted(securityConfig.producerProperties()).toString();
            assertThat(rendered).doesNotContain("s3cr3t-value");
            assertThat(rendered).contains("security.protocol");
        }
    }
}
