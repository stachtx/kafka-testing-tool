package com.acme.kafkatester.consumer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;

import com.acme.kafkatester.consumer.ConsumerSession.Event;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.registry.SchemaRegistryException;

/**
 * Record listener of one session. Values arrive as raw bytes and are deserialized here (Confluent
 * deserializer, schema resolved from the registry by id) and rendered through {@link ValueDecoder}, so
 * a bad record becomes a raw-with-error entry instead of killing the container. As a
 * {@link ConsumerSeekAware} it seeks LATEST sessions to the end on partition assignment and only then
 * flips the session to LISTENING. Offsets are never committed (manual ack mode, never acknowledged).
 */
final class SessionRecordListener implements AcknowledgingMessageListener<String, byte[]>, ConsumerSeekAware {

    static final int MAX_RAW_BYTES = 4096;
    static final int MAX_ERROR_CHARS = 300;
    static final String TRUNCATION_MARKER = "... [truncated]";

    private final ConsumerSession session;
    private final Deserializer<Object> deserializer;
    private final ValueDecoder decoder;
    private final Runnable onCapReached;
    private final int maxPayloadBytes;
    /** Partitions that were already assigned once; LATEST seeks to the end only on first sight (I3). */
    private final Set<TopicPartition> seenPartitions = ConcurrentHashMap.newKeySet();

    SessionRecordListener(ConsumerSession session, Deserializer<Object> deserializer, ValueDecoder decoder,
            Runnable onCapReached, int maxPayloadBytes) {
        this.session = session;
        this.deserializer = deserializer;
        this.decoder = decoder;
        this.onCapReached = onCapReached;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    @Override
    public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        ConsumerSession.Status status = session.status();
        if (status == ConsumerSession.Status.CAP_REACHED || status == ConsumerSession.Status.STOPPED) {
            return;
        }
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            headers.put(h.key(), h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8));
        }
        String payload = null;
        String error = null;
        String raw = null;
        boolean truncated = false;
        byte[] value = record.value();
        if (value != null) {
            try {
                Object deserialized = deserializer.deserialize(record.topic(), record.headers(), value);
                payload = decoder.toJson(session.format(), deserialized);
                if (payload != null && payload.length() * 3L > maxPayloadBytes
                        && payload.getBytes(StandardCharsets.UTF_8).length > maxPayloadBytes) {
                    payload = truncate(payload, maxPayloadBytes);
                    truncated = true;
                }
            } catch (RuntimeException e) {
                String message = SchemaRegistryException.sanitize(describe(e));
                if (message.length() > MAX_ERROR_CHARS) {
                    message = message.substring(0, MAX_ERROR_CHARS) + TRUNCATION_MARKER;
                }
                error = "Could not decode " + session.format() + " record: " + message;
                payload = null;
                byte[] head = value.length > MAX_RAW_BYTES ? Arrays.copyOf(value, MAX_RAW_BYTES) : value;
                raw = Base64.getEncoder().encodeToString(head);
            }
        }
        long count = session.append(new Event(0, record.topic(), record.partition(), record.offset(),
                record.timestamp(), record.key(), headers, payload, error, raw, truncated));
        Integer cap = session.effectiveCap();
        if (cap != null && count >= cap) {
            session.markCapReached();
            onCapReached.run();
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (session.offsetMode() == OffsetMode.LATEST) {
            // only partitions seen for the first time: a rebalance must not skip records of already-owned ones
            Set<TopicPartition> fresh = assignments.keySet().stream().filter(seenPartitions::add)
                    .collect(Collectors.toSet());
            if (!fresh.isEmpty()) {
                callback.seekToEnd(fresh);
            }
        }
        session.markListening();
    }

    /** "ClassName: message", or just the class name when the exception has no message. */
    static String describe(RuntimeException e) {
        String name = e.getClass().getSimpleName();
        return e.getMessage() == null ? name : name + ": " + e.getMessage();
    }

    /** Cuts {@code text} to at most {@code maxBytes} UTF-8 bytes (on a character boundary) plus a marker. */
    static String truncate(String text, int maxBytes) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return text;
        }
        int end = maxBytes;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) { // do not split a multi-byte character
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8) + TRUNCATION_MARKER;
    }

    /** Release deserializer resources (registry client). */
    void close() {
        try {
            deserializer.close();
        } catch (RuntimeException ignored) {
            // best effort
        }
    }
}
