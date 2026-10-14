package com.acme.kafkatester.registry;

import java.util.List;

/**
 * Read-only result of {@link SchemaRegistryGateway#preview}. Carries everything the registration
 * modal needs to render a side-by-side diff; no diff is computed server-side.
 *
 * @param eventType             catalog event type name
 * @param format                {@code AVRO} or {@code PROTOBUF}
 * @param topic                 topic the subject was derived from
 * @param subject               registry subject (e.g. {@code sandbox.ping-value})
 * @param firstRegistration     true when the subject does not exist yet in the registry
 * @param registryVersion       latest registry version, or {@code null} for a first registration
 * @param registrySchemaId      latest registry schema id, or {@code null} for a first registration
 * @param registrySchema        latest registry schema text (normalized for display), or {@code null}
 * @param localSchema           local schema that would be registered (normalized for display)
 * @param compatibilityMode     effective compatibility mode (subject level, else global)
 * @param compatible            registry compatibility verdict for the local schema (true for a first registration)
 * @param compatibilityMessages reasons reported by the registry when incompatible
 * @param identical             local schema equals the registry latest (registering would be a no-op)
 */
public record RegistrationPreview(
        String eventType,
        String format,
        String topic,
        String subject,
        boolean firstRegistration,
        Integer registryVersion,
        Integer registrySchemaId,
        String registrySchema,
        String localSchema,
        String compatibilityMode,
        boolean compatible,
        List<String> compatibilityMessages,
        boolean identical) {

    public RegistrationPreview {
        compatibilityMessages = compatibilityMessages == null ? List.of() : List.copyOf(compatibilityMessages);
    }
}
