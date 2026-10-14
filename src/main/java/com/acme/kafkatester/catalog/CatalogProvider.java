package com.acme.kafkatester.catalog;

import java.util.List;
import java.util.Optional;

/**
 * Strategy: one implementation per schema format. Every {@code CatalogProvider} bean is
 * picked up automatically by {@link SchemaCatalog}; a new format needs only a new bean.
 */
public interface CatalogProvider {

    /** Format id served by this provider (same value as {@link EventTypeDescriptor#format()}). */
    String format();

    /** All event types this provider discovered, in stable (name) order. */
    List<EventTypeDescriptor> listEventTypes();

    /** Resolve an event type by exact name within this provider's format. */
    Optional<EventTypeDescriptor> resolve(String name);

    /**
     * Generate an example JSON payload for the event type.
     *
     * @throws UnknownEventTypeException if this provider does not know {@code name}
     */
    String generateExample(String name);

    /** Format-native schema for codec use (Avro Schema / Protobuf Descriptor); empty if unknown. */
    Optional<Object> nativeSchema(String name);
}
