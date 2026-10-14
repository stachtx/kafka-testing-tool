package com.acme.kafkatester.web.dto;

import java.util.List;

import com.acme.kafkatester.registry.RegistrationPreview;
import com.acme.kafkatester.registry.SchemaRegistrationService.RegistrationResult;

import jakarta.validation.constraints.NotBlank;

/** DTOs of {@code /api/schemas}. */
public final class RegistryDtos {

    private RegistryDtos() {
    }

    /** {@code format} and {@code topic} optional (defaults: first matching format, event type default topic). */
    public record RegisterRequest(@NotBlank String eventType, String format, String topic) {
    }

    public record RegistrationPreviewDto(String eventType, String format, String topic, String subject,
            boolean firstRegistration, Integer registryVersion, Integer registrySchemaId, String registrySchema,
            String localSchema, String compatibilityMode, boolean compatible, List<String> compatibilityMessages,
            boolean identical) {

        public static RegistrationPreviewDto from(RegistrationPreview p) {
            return new RegistrationPreviewDto(p.eventType(), p.format(), p.topic(), p.subject(),
                    p.firstRegistration(), p.registryVersion(), p.registrySchemaId(), p.registrySchema(),
                    p.localSchema(), p.compatibilityMode(), p.compatible(), p.compatibilityMessages(), p.identical());
        }
    }

    public record RegistrationResultDto(String subject, int schemaId, int version, String eventType, String format) {

        public static RegistrationResultDto from(RegistrationResult r) {
            return new RegistrationResultDto(r.subject(), r.schemaId(), r.version(), r.eventType(), r.format());
        }
    }
}
