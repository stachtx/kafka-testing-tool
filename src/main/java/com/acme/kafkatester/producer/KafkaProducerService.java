package com.acme.kafkatester.producer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.codec.CodecRegistry;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.producer.ProducerConfig.ProducerTemplates;
import com.acme.kafkatester.registry.SchemaRegistryGateway;

/**
 * Validates and sends user-authored JSON events. Encoding errors surface as
 * {@link com.acme.kafkatester.codec.InvalidPayloadException}; send errors as {@link KafkaSendFailedException}.
 */
@Service
public class KafkaProducerService {

    static final long SEND_TIMEOUT_SECONDS = 30;

    /**
     * @param format   optional ("AVRO"/"PROTOBUF"); when null the catalog's first match for eventType is used
     * @param key      optional; blank means null key
     * @param headers  optional; values are sent as UTF-8
     * @param partition optional explicit partition
     */
    public record ProducerRequest(String topic, String eventType, String format, String key,
            Map<String, String> headers, Integer partition, String payload) {
    }

    /** Result of a dry run: the payload encoded fine; {@code normalizedPayload} is its decoded JSON round-trip. */
    public record ValidationResult(boolean valid, String eventType, String format, String topic,
            String normalizedPayload) {
    }

    /** Broker acknowledgement mapped from {@link RecordMetadata}. */
    public record ProducerResult(String topic, int partition, long offset, long timestamp) {
    }

    private record Encoded(EventTypeDescriptor descriptor, MessageFormat format, Object schema, Object value) {
    }

    private final SchemaCatalog catalog;
    private final CodecRegistry codecs;
    private final ProducerTemplates templates;
    private final SchemaRegistryGateway gateway;

    public KafkaProducerService(SchemaCatalog catalog, CodecRegistry codecs, ProducerTemplates templates,
            SchemaRegistryGateway gateway) {
        this.catalog = catalog;
        this.codecs = codecs;
        this.templates = templates;
        this.gateway = gateway;
    }

    /** Same encode path as {@link #send}, but nothing is sent. */
    public ValidationResult validate(ProducerRequest request) {
        Encoded e = encode(request);
        return new ValidationResult(true, e.descriptor().name(), e.format().name(), request.topic(),
                codecs.decode(e.format(), e.schema(), e.value()));
    }

    /**
     * Encodes against the local catalog schema, then (W1 guard) requires that schema to equal the registry's
     * latest for the topic's value subject, because the serializer writes with the registry's latest.
     *
     * @throws com.acme.kafkatester.registry.SchemaRegistryException subject missing / schema differs (409),
     *         or registry unreachable (502); nothing is sent in either case
     */
    public ProducerResult send(ProducerRequest request) {
        Encoded e = encode(request);
        gateway.assertLocalMatchesRegistryLatest(e.descriptor().name(), e.descriptor().format(), request.topic());
        ProducerRecord<String, Object> record = new ProducerRecord<>(request.topic(), request.partition(),
                StringUtils.hasText(request.key()) ? request.key() : null, e.value(), headers(request.headers()));
        try {
            SendResult<String, Object> result = templates.forFormat(e.format()).send(record)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RecordMetadata md = result.getRecordMetadata();
            return new ProducerResult(md.topic(), md.partition(), md.offset(), md.timestamp());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaSendFailedException("Interrupted while sending to topic " + request.topic(), ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            throw new KafkaSendFailedException(
                    "Send to topic " + request.topic() + " failed: " + cause.getMessage(), cause);
        } catch (TimeoutException ex) {
            throw new KafkaSendFailedException("Send to topic " + request.topic() + " timed out", ex);
        } catch (RuntimeException ex) { // synchronous serializer / registry / config failures
            throw new KafkaSendFailedException(
                    "Send to topic " + request.topic() + " failed: " + ex.getMessage(), ex);
        }
    }

    private Encoded encode(ProducerRequest request) {
        if (!StringUtils.hasText(request.topic())) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        EventTypeDescriptor descriptor = StringUtils.hasText(request.format())
                ? catalog.resolve(request.eventType(), request.format())
                : catalog.resolve(request.eventType());
        MessageFormat format = MessageFormat.valueOf(descriptor.format());
        Object schema = catalog.schema(descriptor.name(), descriptor.format());
        Object value = codecs.encode(format, schema, request.payload());
        return new Encoded(descriptor, format, schema, value);
    }

    private static List<Header> headers(Map<String, String> headers) {
        List<Header> out = new ArrayList<>();
        if (headers != null) {
            headers.forEach((k, v) -> out.add(
                    new RecordHeader(k, v == null ? null : v.getBytes(StandardCharsets.UTF_8))));
        }
        return out;
    }
}
