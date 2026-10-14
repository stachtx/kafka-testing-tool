package com.acme.kafkatester.architecture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.consumer.ConsumerSessionManager;
import com.acme.kafkatester.producer.ProducerConfig.ProducerTemplates;
import com.acme.kafkatester.registry.SchemaRegistryGateway;

/**
 * ADR-007: every serializer/deserializer configuration used outside the registration path has
 * {@code auto.register.schemas=false}, TopicNameStrategy pinned, and {@code use.latest.version=true} for\n * serializers / {@code false} for deserializers (decode by writer schema id). The context is started with
 * hostile operator overrides ({@code auto.register.schemas=true}, {@code use.latest.version=false}
 * supplied through {@code spring.kafka.properties.*}) to prove the safe defaults win.
 */
@SpringBootTest(properties = {
        "spring.kafka.properties.auto.register.schemas=true",
        "spring.kafka.properties.use.latest.version=false",
        "spring.kafka.properties.latest.cache.ttl.sec=-1",
        "spring.kafka.properties.value.subject.name.strategy=io.confluent.kafka.serializers.subject.RecordNameStrategy",
        "spring.kafka.bootstrap-servers=localhost:1" // nothing listens here; no automated test touches a real cluster
})
class SafeSerdeDefaultsTest {

    private static final String TOPIC_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.TopicNameStrategy";

    @Autowired ProducerTemplates templates;
    @Autowired ConsumerSessionManager consumers;
    @MockitoSpyBean SchemaRegistryGateway gateway;

    @Test
    void everyProducerTemplateSerializerConfigIsSafeEvenWhenOperatorSuppliesUnsafeValues() {
        for (MessageFormat format : MessageFormat.values()) {
            Map<String, Object> config = templates.forFormat(format).getProducerFactory().getConfigurationProperties();

            assertThat(config).as("producer config for %s", format)
                    .containsEntry("auto.register.schemas", false)
                    .containsEntry("use.latest.version", true)
                    .containsEntry("latest.compatibility.strict", true)
                    .containsEntry("latest.cache.ttl.sec", SchemaRegistryGateway.LATEST_CACHE_TTL_SEC)
                    .containsEntry("value.subject.name.strategy", TOPIC_NAME_STRATEGY);
            assertThat(config.get("value.serializer").toString()).contains("io.confluent.kafka.serializers");
        }
    }

    @Test
    void consumerDeserializerConfigIsSafeEvenWhenOperatorSuppliesUnsafeValues() {
        AtomicReference<Map<String, Object>> captured = new AtomicReference<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = (Map<String, Object>) invocation.callRealMethod();
            captured.set(result);
            return result;
        }).when(gateway).deserializerConfig(anyMap());

        String sessionId = null;
        try {
            for (MessageFormat format : MessageFormat.values()) {
                sessionId = consumers.startSession("safe-defaults-topic", format, OffsetMode.LATEST).id();
                assertThat(captured.get()).as("deserializer config for %s", format)
                        .containsEntry("auto.register.schemas", false)
                        .containsEntry("use.latest.version", false)
                        .containsEntry("value.subject.name.strategy", TOPIC_NAME_STRATEGY);
                captured.set(null);
                consumers.stopSession(sessionId);
                sessionId = null;
            }
        } finally {
            if (sessionId != null) {
                consumers.stopSession(sessionId);
            }
        }
    }
}
