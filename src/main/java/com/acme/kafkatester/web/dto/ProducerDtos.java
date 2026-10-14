package com.acme.kafkatester.web.dto;

import java.util.Map;

import com.acme.kafkatester.producer.KafkaProducerService;

import jakarta.validation.constraints.NotBlank;
import tools.jackson.databind.JsonNode;

/** Request/response DTOs of {@code /api/producer}. */
public final class ProducerDtos {

    private ProducerDtos() {
    }

    /**
     * @param topic     required
     * @param eventType required, catalog name
     * @param format    optional {@code AVRO}/{@code PROTOBUF} (needed when the event type exists in both)
     * @param key       optional record key
     * @param headers   optional string headers
     * @param partition optional explicit partition
     * @param payload   the event as a JSON object (a JSON string holding JSON text is also accepted)
     */
    public record ProducerRequestDto(@NotBlank String topic, @NotBlank String eventType, String format, String key,
            Map<String, String> headers, Integer partition, JsonNode payload) {

        public KafkaProducerService.ProducerRequest toDomain() {
            String text = payload == null || payload.isNull() ? "" : payload.isString() ? payload.asString() : payload.toString();
            return new KafkaProducerService.ProducerRequest(topic.trim(), eventType.trim(),
                    format == null || format.isBlank() ? null : format.trim(), key, headers, partition, text);
        }
    }

    public record ValidationResponse(boolean valid, String eventType, String format, String topic,
            String normalizedPayload) {

        public static ValidationResponse from(KafkaProducerService.ValidationResult r) {
            return new ValidationResponse(r.valid(), r.eventType(), r.format(), r.topic(), r.normalizedPayload());
        }
    }

    /** Broker acknowledgement. {@code timestamp} is epoch millis. */
    public record SendResponse(String topic, int partition, long offset, long timestamp) {

        public static SendResponse from(KafkaProducerService.ProducerResult r) {
            return new SendResponse(r.topic(), r.partition(), r.offset(), r.timestamp());
        }
    }
}
