package com.acme.kafkatester.web.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.acme.kafkatester.consumer.ConsumerSession;

import jakarta.validation.constraints.NotBlank;

/** Request/response DTOs of {@code /api/consumers}. */
public final class ConsumerDtos {

    private ConsumerDtos() {
    }

    /** {@code format}: AVRO|PROTOBUF. {@code offsetMode}: LATEST|EARLIEST. */
    public record StartConsumerRequest(@NotBlank String topic, @NotBlank String format, @NotBlank String offsetMode) {
    }

    /** {@code effectiveCap} is null for LATEST sessions. {@code status}: STARTING|LISTENING|CAP_REACHED|STOPPED. */
    public record ConsumerSessionDto(String id, String groupId, String topic, String format, String offsetMode,
            Instant startedAt, long eventCount, String status, Integer effectiveCap) {

        public static ConsumerSessionDto from(ConsumerSession.Info i) {
            return new ConsumerSessionDto(i.id(), i.groupId(), i.topic(), i.format().name(), i.offsetMode().name(),
                    i.startedAt(), i.eventCount(), i.status().name(), i.effectiveCap());
        }
    }

    /**
     * {@code payload} is JSON text, or null when decoding failed ({@code error} + {@code rawBase64} set).
     * {@code payloadTruncated} is true when the decoded payload exceeded the configured byte cap and was cut
     * (the text then ends with a marker and is no longer valid JSON).
     */
    public record ConsumedEventDto(long sequence, String topic, int partition, long offset, long timestamp,
            String key, Map<String, String> headers, String payload, String error, String rawBase64,
            boolean payloadTruncated) {

        public static ConsumedEventDto from(ConsumerSession.Event e) {
            return new ConsumedEventDto(e.sequence(), e.topic(), e.partition(), e.offset(), e.timestamp(), e.key(),
                    e.headers(), e.payload(), e.error(), e.rawBase64(), e.payloadTruncated());
        }
    }

    /** Send {@code nextOffset} back as {@code sinceOffset} on the next poll. */
    public record PollResponse(String sessionId, List<ConsumedEventDto> events, long nextOffset, long missed,
            String status, long eventCount) {

        public static PollResponse from(ConsumerSession.PollResult r) {
            return new PollResponse(r.sessionId(), r.events().stream().map(ConsumedEventDto::from).toList(),
                    r.nextOffset(), r.missed(), r.status().name(), r.eventCount());
        }
    }
}
