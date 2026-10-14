package com.acme.kafkatester.catalog;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

/**
 * Facade over all {@link CatalogProvider} beans. Contains no format-specific logic.
 *
 * <p>The same event name may exist in several formats (the samples ship {@code PingEvent} as
 * Avro and Protobuf). {@link #resolve(String)} returns the first match in provider order
 * (Avro providers are ordered first); {@link #resolve(String, String)} selects a format.
 */
@Service
public class SchemaCatalog {

    public static final int MAX_LIMIT = 500;

    /** One page of results. */
    public record Page<T>(List<T> items, int offset, int limit, int total) {
    }

    private final List<CatalogProvider> providers;

    public SchemaCatalog(List<CatalogProvider> providers) {
        this.providers = List.copyOf(providers);
    }

    /** All event types across formats, ordered by name then format. */
    public List<EventTypeDescriptor> listAll() {
        return providers.stream()
                .flatMap(p -> p.listEventTypes().stream())
                .sorted(Comparator.comparing(EventTypeDescriptor::name)
                        .thenComparing(EventTypeDescriptor::format))
                .toList();
    }

    /** Paginated listing; negative offset is treated as 0, limit is clamped to 1..{@value #MAX_LIMIT}. */
    public Page<EventTypeDescriptor> list(int offset, int limit) {
        List<EventTypeDescriptor> all = listAll();
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.min(MAX_LIMIT, Math.max(1, limit));
        int from = Math.min(safeOffset, all.size());
        int to = Math.min(all.size(), from + safeLimit);
        return new Page<>(all.subList(from, to), safeOffset, safeLimit, all.size());
    }

    /** @throws UnknownEventTypeException if no provider knows {@code name} */
    public EventTypeDescriptor resolve(String name) {
        return providers.stream()
                .map(p -> p.resolve(name))
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow(() -> new UnknownEventTypeException(name));
    }

    /** @throws UnknownEventTypeException if no provider of {@code format} knows {@code name} */
    public EventTypeDescriptor resolve(String name, String format) {
        return providerFor(format)
                .flatMap(p -> p.resolve(name))
                .orElseThrow(() -> new UnknownEventTypeException(name, format));
    }

    /** Example JSON payload for the first matching event type. */
    public String generateExample(String name) {
        EventTypeDescriptor d = resolve(name);
        return generateExample(d.name(), d.format());
    }

    /** Example JSON payload for the event type in a specific format. */
    public String generateExample(String name, String format) {
        EventTypeDescriptor d = resolve(name, format);
        return providerFor(format)
                .orElseThrow(() -> new UnknownEventTypeException(name, format))
                .generateExample(d.name());
    }

    /**
     * Format-native schema (Avro Schema / Protobuf Descriptor) to hand to the codec.
     *
     * @throws UnknownEventTypeException if no provider of {@code format} knows {@code name}
     */
    public Object schema(String name, String format) {
        return providerFor(format)
                .flatMap(p -> p.nativeSchema(name))
                .orElseThrow(() -> new UnknownEventTypeException(name, format));
    }

    private Optional<CatalogProvider> providerFor(String format) {
        return providers.stream()
                .filter(p -> p.format().equalsIgnoreCase(format))
                .findFirst();
    }
}
