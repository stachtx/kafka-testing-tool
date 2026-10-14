package com.acme.kafkatester.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.codec.CodecRegistry;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerRequest;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerResult;
import com.acme.kafkatester.registry.SchemaRegistryException;
import com.google.protobuf.Message;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

/**
 * Producer send round trip against a real broker and registry (Testcontainers). Because the producer
 * runs with {@code auto.register.schemas=false} (ADR-007), the path is: send fails while the subject is
 * unknown, the explicit registration endpoint registers the schema, then the send succeeds and the
 * record decodes back to the payload that was sent.
 */
@EnabledIfDockerAvailable
class ProducerRoundTripIntegrationTest extends AbstractBrokerIntegrationTest {

    @Autowired SchemaCatalog catalog;
    @Autowired CodecRegistry codecs;

    @Test
    void avroPingEventRoundTripRequiresExplicitRegistrationFirst() throws Exception {
        roundTrip(MessageFormat.AVRO);
    }

    @Test
    void protobufPingEventRoundTripRequiresExplicitRegistrationFirst() throws Exception {
        roundTrip(MessageFormat.PROTOBUF);
    }

    private void roundTrip(MessageFormat format) throws Exception {
        String topic = uniqueTopic("roundtrip-" + format.name().toLowerCase());
        String subject = topic + "-value";
        String payload = catalog.generateExample("PingEvent", format.name());
        createTopic(topic);

        // 1. auto-register is off: the pre-send guard rejects the unknown subject and nothing is registered implicitly
        assertThatThrownBy(() -> send(topic, format.name(), "k1", payload))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageContaining("register it first");
        assertThat(registryVersionCount(subject)).as("no implicit registration").isZero();

        // 2. explicit registration through the dedicated endpoint
        registerViaApi(format.name(), topic);
        assertThat(registryVersionCount(subject)).isEqualTo(1);

        // 3. send now succeeds and the record is readable with the registry-resolved schema
        ProducerResult result = send(topic, format.name(), "k1", payload);
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.offset()).isZero();
        assertThat(registryVersionCount(subject)).as("sending never registers").isEqualTo(1);

        ConsumerRecord<String, Object> record = readSingleRecord(topic, format);
        assertThat(record.key()).isEqualTo("k1");
        assertThat(new String(record.headers().lastHeader("source").value(), StandardCharsets.UTF_8)).isEqualTo("it");

        String expected = producer.validate(
                new ProducerRequest(topic, "PingEvent", format.name(), null, null, null, payload)).normalizedPayload();
        Object schema = format == MessageFormat.AVRO
                ? ((GenericRecord) record.value()).getSchema()
                : ((Message) record.value()).getDescriptorForType();
        assertThat(codecs.decode(format, schema, record.value())).isEqualTo(expected);
    }

    private ConsumerRecord<String, Object> readSingleRecord(String topic, MessageFormat format) {
        Map<String, Object> config = Map.of(
                "bootstrap.servers", KafkaRegistryInfra.bootstrapServers(),
                "group.id", "it-reader-" + UUID.randomUUID(),
                "auto.offset.reset", "earliest",
                "schema.registry.url", KafkaRegistryInfra.registryUrl());
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Deserializer<Object> valueDeserializer = format == MessageFormat.AVRO
                ? (Deserializer) new KafkaAvroDeserializer()
                : (Deserializer) new KafkaProtobufDeserializer<>();
        valueDeserializer.configure(config, false);
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
                valueDeserializer)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, Object> r : consumer.poll(Duration.ofMillis(500))) {
                    return r;
                }
            }
        }
        throw new AssertionError("No record read from " + topic);
    }
}
