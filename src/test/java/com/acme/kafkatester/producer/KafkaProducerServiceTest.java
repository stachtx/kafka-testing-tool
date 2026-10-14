package com.acme.kafkatester.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;
import com.acme.kafkatester.codec.AvroSchemaCodec;
import com.acme.kafkatester.codec.CodecRegistry;
import com.acme.kafkatester.codec.InvalidPayloadException;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.codec.ProtobufSchemaCodec;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerRequest;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerResult;
import com.acme.kafkatester.producer.KafkaProducerService.ValidationResult;
import com.acme.kafkatester.producer.ProducerConfig.ProducerTemplates;
import com.acme.kafkatester.registry.SchemaRegistryException;
import com.acme.kafkatester.registry.SchemaRegistryGateway;

class KafkaProducerServiceTest {

    private final SchemaCatalog catalog =
            new SchemaCatalog(List.of(new AvroCatalogProvider(), new ProtobufCatalogProvider()));
    private final CodecRegistry codecs = new CodecRegistry(List.of(new AvroSchemaCodec(), new ProtobufSchemaCodec()));

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> avroTemplate = mock(KafkaTemplate.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> protoTemplate = mock(KafkaTemplate.class);

    private final SchemaRegistryGateway gateway = mock(SchemaRegistryGateway.class);

    private KafkaProducerService service;
    private String avroPayload;

    @BeforeEach
    void setUp() {
        service = new KafkaProducerService(catalog, codecs, new ProducerTemplates(
                Map.of(MessageFormat.AVRO, avroTemplate, MessageFormat.PROTOBUF, protoTemplate)), gateway);
        avroPayload = catalog.generateExample("PingEvent", "AVRO");
    }

    @Test
    void validateIsRegistryFree() {
        service.validate(request("AVRO", avroPayload));

        verifyNoInteractions(gateway);
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendRefusesWhenLocalSchemaDiffersFromRegistryLatest() {
        doThrow(new SchemaRegistryException("Local schema for PingEvent differs from registry latest (v2); "
                + "register the new version via the Event Catalog first", null, 409))
                .when(gateway).assertLocalMatchesRegistryLatest("PingEvent", "AVRO", "sandbox.ping");

        assertThatThrownBy(() -> service.send(request("AVRO", avroPayload)))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageContaining("differs from registry latest")
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(409);
        verifyNoInteractions(avroTemplate, protoTemplate);
    }

    @Test
    void sendRefusesWhenSubjectIsNotRegistered() {
        doThrow(new SchemaRegistryException("no schema registered for subject sandbox.ping-value; register it first",
                null, 409)).when(gateway).assertLocalMatchesRegistryLatest("PingEvent", "PROTOBUF", "sandbox.ping");

        assertThatThrownBy(() -> service.send(request("PROTOBUF", catalog.generateExample("PingEvent", "PROTOBUF"))))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageContaining("no schema registered for subject");
        verifyNoInteractions(avroTemplate, protoTemplate);
    }

    @Test
    void sendSurfacesRegistryUnreachableInsteadOfSending() {
        doThrow(new SchemaRegistryException("Schema Registry latest version lookup failed: connection refused"))
                .when(gateway).assertLocalMatchesRegistryLatest("PingEvent", "AVRO", "sandbox.ping");

        assertThatThrownBy(() -> service.send(request("AVRO", avroPayload)))
                .isInstanceOf(SchemaRegistryException.class)
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(-1);
        verifyNoInteractions(avroTemplate, protoTemplate);
    }

    @Test
    void invalidPayloadFailsBeforeAnyRegistryCall() {
        assertThatThrownBy(() -> service.send(request("AVRO", "{\"bogus\":1}")))
                .isInstanceOf(InvalidPayloadException.class);
        verifyNoInteractions(gateway);
    }

    private ProducerRequest request(String format, String payload) {
        return new ProducerRequest("sandbox.ping", "PingEvent", format, "k1",
                Map.of("trace", "abc"), 2, payload);
    }

    @Test
    void validateReturnsEncodedResultWithoutTouchingKafka() {
        ValidationResult r = service.validate(request("AVRO", avroPayload));

        assertThat(r.valid()).isTrue();
        assertThat(r.format()).isEqualTo("AVRO");
        assertThat(r.eventType()).isEqualTo("PingEvent");
        assertThat(r.normalizedPayload()).isNotBlank();
        verifyNoInteractions(avroTemplate, protoTemplate);
    }

    @Test
    void validateRejectsPayloadThatDoesNotMatchSchema() {
        assertThatThrownBy(() -> service.validate(request("AVRO", "{\"bogus\":1}")))
                .isInstanceOf(InvalidPayloadException.class);
        verifyNoInteractions(avroTemplate, protoTemplate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendBuildsRecordWithTopicKeyHeadersPartitionAndMapsMetadata() {
        RecordMetadata md = new RecordMetadata(new TopicPartition("sandbox.ping", 2), 5L, 0, 1700000000000L, 0, 0);
        when(avroTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, md)));

        ProducerResult result = service.send(request("AVRO", avroPayload));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(avroTemplate).send(captor.capture());
        ProducerRecord<String, Object> sent = captor.getValue();
        assertThat(sent.topic()).isEqualTo("sandbox.ping");
        assertThat(sent.key()).isEqualTo("k1");
        assertThat(sent.partition()).isEqualTo(2);
        assertThat(sent.value()).isInstanceOf(org.apache.avro.generic.GenericRecord.class);
        assertThat(new String(sent.headers().lastHeader("trace").value(), StandardCharsets.UTF_8)).isEqualTo("abc");
        assertThat(result).isEqualTo(new ProducerResult("sandbox.ping", 2, 5L, 1700000000000L));
        verifyNoInteractions(protoTemplate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendRoutesProtobufToProtobufTemplate() {
        RecordMetadata md = new RecordMetadata(new TopicPartition("sandbox.ping", 0), 1L, 0, 1L, 0, 0);
        when(protoTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, md)));

        service.send(request("PROTOBUF", catalog.generateExample("PingEvent", "PROTOBUF")));

        verify(protoTemplate).send(any(ProducerRecord.class));
        verifyNoInteractions(avroTemplate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendFailureSurfacesKafkaSendFailed() {
        when(avroTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> service.send(request("AVRO", avroPayload)))
                .isInstanceOf(KafkaSendFailedException.class)
                .hasMessageContaining("broker down")
                .extracting(e -> ((KafkaSendFailedException) e).errorCode()).isEqualTo("KAFKA_SEND_FAILED");
    }

    @Test
    @SuppressWarnings("unchecked")
    void blankKeyNoHeadersAndNoPartitionYieldNullKeyEmptyHeadersAndNullPartition() {
        RecordMetadata md = new RecordMetadata(new TopicPartition("sandbox.ping", 0), 1L, 0, 1L, 0, 0);
        when(avroTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, md)));

        service.send(new ProducerRequest("sandbox.ping", "PingEvent", "AVRO", "  ", null, null, avroPayload));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(avroTemplate).send(captor.capture());
        assertThat(captor.getValue().key()).isNull();
        assertThat(captor.getValue().partition()).isNull();
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousSerializerFailureAlsoSurfacesAsSendFailed() {
        when(avroTemplate.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("schema not found"));

        assertThatThrownBy(() -> service.send(request("AVRO", avroPayload)))
                .isInstanceOf(KafkaSendFailedException.class);
    }
}
